// The client of a sharded table: routes rows by key and merges listing pages from every shard.
import { sharded } from "../sharded/client.js";
import { comparePositions, decodeCursor, encodeCursor, pageRequest, queryFingerprint, } from "./schema.js";
export class TableClient {
    name;
    client;
    primaryKey;
    constructor(namespace, name, schema) {
        this.name = name;
        this.primaryKey = schema.primaryKey;
        this.client = sharded(namespace, name);
    }
    async get(key) {
        return (await this.getMany([key]))[0];
    }
    /** Rows of these keys, in order (null: absent). */
    async getMany(keys) {
        return this.byPosition(keys, "tableGet", null);
    }
    /** Writes a whole row; `ifVersion: 0` only if absent, `ifVersion: n` only if at version n. */
    async put(row, options = {}) {
        return (await this.putMany([{ row, ...options }]))[0];
    }
    async putMany(writes) {
        return this.byPosition(writes.map((w) => this.keyOf(w.row)), "tablePut", writes);
    }
    /** Changes some columns of an existing row (`ok: false` if absent or at another version). */
    async patch(key, changes, options = {}) {
        return (await this.byPosition([key], "tablePatch", [{ changes, ...options }]))[0];
    }
    async delete(key, options = {}) {
        return (await this.byPosition([key], "tableDelete", [options]))[0];
    }
    /** Rows in the whole table: exact unless a split commits during the count. */
    async count() {
        return (await this.client.scan("tableCount")).reduce((a, b) => a + b, 0);
    }
    /**
     * One page of a listing across every shard. Pass the page's cursor back for the next one;
     * a null cursor means the listing is done. A page may hold fewer than `limit` rows (a sparse
     * filter ran out of budget) and still have a cursor.
     */
    async list(query = {}) {
        const fingerprint = queryFingerprint(pageRequest(query, null));
        const after = query.cursor ? decodeCursor(query.cursor, fingerprint) : null;
        const request = pageRequest(query, after);
        const pages = (await this.client.scan("tablePage", request));
        const merged = mergePages(pages, request);
        return {
            rows: merged.rows.map(({ row, version }) => ({ row: row, version })),
            cursor: merged.next === null ? null : encodeCursor(fingerprint, merged.next),
        };
    }
    /** Every row of a query, page after page. */
    async *all(query = {}) {
        let cursor = null;
        do {
            const page = await this.list({ ...query, cursor });
            yield* page.rows;
            cursor = page.cursor;
        } while (cursor !== null);
    }
    keyOf(row) {
        const key = row[this.primaryKey];
        if (typeof key !== "string")
            throw new Error(`the row has no ${this.primaryKey}`);
        return key;
    }
    async byPosition(keys, method, payloads) {
        const out = new Array(keys.length);
        for (const group of await this.client.call(keys, method, payloads))
            group.positions.forEach((p, i) => (out[p] = group.result[i]));
        return out;
    }
}
/**
 * Merges one page from every shard: every shard reported every match up to the smallest frontier,
 * so rows up to it are complete. Returns them in order, up to `limit`, and where the next page
 * starts (null: done).
 */
export function mergePages(pages, request) {
    const cmp = (a, b) => comparePositions(a, b, request.direction);
    let frontier = null;
    for (const page of pages)
        if (page.frontier && (!frontier || cmp(page.frontier, frontier) < 0))
            frontier = page.frontier;
    const seen = new Set();
    const complete = pages
        .flatMap((p) => p.rows)
        .filter((r) => frontier === null || cmp(r.position, frontier) <= 0)
        .filter((r) => {
        // A key reported by two shards in one page (one before a split committed, one after).
        if (seen.has(r.key))
            return false;
        seen.add(r.key);
        return true;
    })
        .sort((a, b) => cmp(a.position, b.position));
    const rows = complete.slice(0, request.limit);
    if (rows.length === request.limit && (complete.length > request.limit || frontier !== null))
        return { rows, next: rows.at(-1).position };
    return { rows, next: frontier };
}
/**
 * The client of the sharded table `name`: `namespace` is the binding of a `ShardedTable` class,
 * `schema` the table it declares (the client routes rows by its primary key).
 */
export function table(namespace, name, schema) {
    return new TableClient(namespace, name, schema);
}
//# sourceMappingURL=client.js.map