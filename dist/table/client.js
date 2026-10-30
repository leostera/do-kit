// The client of a sharded table: routes rows by key and merges listing pages from every shard.
import { sharded } from "../sharded/client.js";
import { comparePositions, decodeCursor, encodeCursor, pageRequest, queryFingerprint, } from "./schema.js";
/** Rounds of shard calls one page may take to fill up, when shards returned less than needed. */
const MAX_ROUNDS = 3;
/**
 * Rows to ask each of `shards` shards for, to fill `wanted`: keys spread over shards by hash, so
 * each holds about wanted/shards of a page; twice that, plus two, rarely comes up short.
 */
export function perShardLimit(wanted, shards) {
    return Math.min(wanted, Math.ceil(wanted / shards) * 2 + 2);
}
export class TableClient {
    name;
    client;
    primaryKey;
    indexes;
    constructor(namespace, name, schema, options = {}) {
        this.name = name;
        this.primaryKey = schema.primaryKey;
        this.indexes = schema.indexes ?? {};
        this.client = sharded(namespace, name, options);
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
     * Rows of the whole table per value of a count group's column (values with no rows left out),
     * kept with every write: as exact as `count()`.
     */
    async countBy(group) {
        return sumGroups((await this.client.scan("tableCountBy", group)));
    }
    /**
     * One page of a listing across every shard. Pass the page's cursor back for the next one;
     * a null cursor means the listing is done. A page may hold fewer than `limit` rows (a sparse
     * filter ran out of budget) and still have a cursor.
     */
    async list(query = {}) {
        const request0 = pageRequest(query, null);
        const fingerprint = queryFingerprint(request0, request0.index === null ? [] : (this.indexes[request0.index] ?? []));
        const after = query.cursor ? decodeCursor(query.cursor, fingerprint) : null;
        const request = pageRequest(query, after);
        const rows = [];
        let next = after;
        let counted = {};
        for (let round = 0; round < MAX_ROUNDS; round++) {
            const wanted = request.limit - rows.length;
            const limit = perShardLimit(wanted, await this.client.knownShardCount());
            // Counts come with the first round's pages: the same calls to the same shards.
            const withCount = round === 0 && query.withCount ? { withCount: query.withCount } : {};
            const pages = (await this.client.scan("tablePage", { ...request, limit, after: next, ...withCount }));
            if (round === 0 && query.withCount) {
                counted = { total: pages.reduce((n, p) => n + (p.total ?? 0), 0) };
                if (typeof query.withCount === "string")
                    counted.counts = sumGroups(pages.map((p) => p.counts ?? []));
            }
            const merged = mergePages(pages, { limit: wanted, direction: request.direction });
            rows.push(...merged.rows);
            next = merged.next;
            if (next === null || rows.length === request.limit)
                break;
        }
        return {
            rows: rows.map(({ row, version }) => ({ row: row, version })),
            cursor: next === null ? null : encodeCursor(fingerprint, next),
            ...counted,
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
/** Sums counts per value over shards, leaving out values with none. */
function sumGroups(perShard) {
    const sums = new Map();
    for (const counts of perShard) {
        for (const { value, count } of counts) {
            const key = JSON.stringify(value);
            sums.set(key, { value, count: (sums.get(key)?.count ?? 0) + count });
        }
    }
    return [...sums.values()].filter((c) => c.count !== 0);
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
export function table(namespace, name, schema, options = {}) {
    return new TableClient(namespace, name, schema, options);
}
//# sourceMappingURL=client.js.map