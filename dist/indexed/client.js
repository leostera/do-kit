// The client of an indexed table: a table client that reserves unique values before it writes,
// and looks up and lists through global indexes (RFD0003).
import { sharded } from "../sharded/client.js";
import { TableClient } from "../table/client.js";
import { decodeCursor, encodeCursor, encodeValue, pageRequest, queryFingerprint } from "../table/schema.js";
import { entryOf, globalIndexes, routeOf } from "./entries.js";
/** Times a write is tried again when a reservation was given up before the row was written. */
const WRITE_ATTEMPTS = 3;
/** Times a reservation is asked for while another write holds the value. */
const RESERVE_ATTEMPTS = 6;
/** Rounds of index shard calls one page may take, when a sparse filter ran out of budget. */
const MAX_ROUNDS = 3;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/**
 * The row is written, but not all of its index entries were within the row shard's wait: lookups
 * and listings may not show it yet. The row shard goes on delivering them. Treat the write as done,
 * or retry it.
 */
export class IndexPending extends Error {
    key;
    version;
    constructor(key, version) {
        super(`row ${key} is written at version ${version}, its index entries are still being delivered`);
        this.key = key;
        this.version = version;
        this.name = "IndexPending";
    }
}
/** Another write kept holding a unique value: try again later. Nothing was written. */
export class IndexContended extends Error {
    key;
    index;
    constructor(key, index) {
        super(`another write holds the ${index} value row ${key} wants`);
        this.key = key;
        this.index = index;
        this.name = "IndexContended";
    }
}
export class IndexedTableClient extends TableClient {
    index;
    schema;
    globals;
    constructor(namespace, indexNamespace, name, schema, options = {}) {
        super(namespace, name, schema, options);
        this.schema = schema;
        this.globals = globalIndexes(schema);
        this.index = sharded(indexNamespace, name, options);
    }
    async put(row, options = {}) {
        return (await this.putMany([{ row, ...options }]))[0];
    }
    /**
     * Writes whole rows. Reserves each row's unique values first: a value another row has fails that
     * write with `conflict`. Returns once every index has the rows, or throws IndexPending.
     */
    async putMany(writes) {
        const out = new Array(writes.length);
        let todo = writes.map((_, i) => i);
        for (let attempt = 1; todo.length > 0; attempt++) {
            const reserved = await this.reserveAll(todo.map((i) => ({ key: this.keyOf(writes[i].row), row: writes[i].row })));
            const go = [];
            todo.forEach((i, j) => {
                const r = reserved[j];
                if ("conflict" in r)
                    out[i] = { ok: false, version: 0, conflict: r.conflict };
                else
                    go.push({ i, tokens: r.tokens });
            });
            if (go.length === 0)
                break;
            const payloads = go.map(({ i, tokens }) => ({ row: writes[i].row, ...(writes[i].ifVersion === undefined ? {} : { ifVersion: writes[i].ifVersion }), tokens }));
            const results = await this.byPosition(go.map(({ i }) => this.keyOf(writes[i].row)), "tablePut", payloads);
            todo = this.settle(go.map(({ i }) => i), results, out, attempt, (i) => this.keyOf(writes[i].row));
        }
        return out;
    }
    /** Changes some columns of an existing row; reserves the unique values it changes first. */
    async patch(key, changes, options = {}) {
        const out = new Array(1);
        for (let attempt = 1;; attempt++) {
            const touched = this.globals.filter((ix) => ix.unique && ix.columns.some((c) => c in changes));
            let tokens = {};
            if (touched.length) {
                // A unique index whose columns the patch sets only some of: its other values are the row's.
                const partial = touched.some((ix) => ix.columns.some((c) => !(c in changes)));
                const current = partial ? await this.get(key) : null;
                if (partial && !current)
                    return { ok: false, version: 0 };
                const row = { ...(current?.row ?? {}), ...changes, [this.primaryKey]: key };
                const [t] = await this.reserveAll([{ key, row }], touched);
                if ("conflict" in t)
                    return { ok: false, version: 0, conflict: t.conflict };
                tokens = t.tokens;
            }
            const results = await this.byPosition([key], "tablePatch", [{ changes, ...options, tokens }]);
            if (this.settle([0], results, out, attempt, () => key).length === 0)
                return out[0];
        }
    }
    /** Deletes a row; returns once its index entries are removed, or throws IndexPending. */
    async delete(key, options = {}) {
        const out = new Array(1);
        const results = await this.byPosition([key], "tableDelete", [options]);
        this.settle([0], results, out, WRITE_ATTEMPTS, () => key);
        return out[0];
    }
    /** The row whose values of a unique global index are `values`, in one call to one index shard. */
    async lookup(index, values) {
        const ix = this.globalIndex(index);
        if (!ix.unique)
            throw new Error(`global index ${index} is not unique: list it with a partition`);
        if (values.length !== ix.columns.length)
            throw new Error(`global index ${index} has ${ix.columns.length} column(s)`);
        const row = Object.fromEntries(ix.columns.map((c, i) => [c, values[i]]));
        const e = entryOf(this.schema, ix, { ...row, [this.primaryKey]: "" });
        if (!e)
            return null;
        const [group] = await this.index.call([e.route], "indexLookup", [{ entry: e.entry }]);
        return group.result[0] ?? null;
    }
    /**
     * One page of a listing. Through a global index, of one partition (`partition`: the values of its
     * leading columns), from one index shard; otherwise as a sharded table lists.
     */
    async list(query = {}) {
        const ix = query.index === undefined ? undefined : this.globals.find((g) => g.name === query.index);
        if (!ix) {
            if (query.partition !== undefined)
                throw new Error("partition is for global indexes");
            return super.list(query);
        }
        if (query.withCount)
            throw new Error("withCount is not supported through a global index");
        const partition = query.partition ?? [];
        if (partition.length !== ix.partition)
            throw new Error(`global index ${ix.name} is partitioned by ${ix.partition} column(s): pass partition with their values`);
        const values = ix.columns.slice(0, ix.partition).map((c, i) => encodeValue(this.schema, c, partition[i]));
        const route = routeOf(ix.name, values);
        const where = (query.where ?? []).map((c) => this.encodeCondition(c));
        const request0 = pageRequest({ ...query, where }, null);
        const fingerprint = queryFingerprint(request0, [...ix.columns, JSON.stringify(values)]);
        let after = query.cursor ? decodeCursor(query.cursor, fingerprint) : null;
        const rows = [];
        for (let round = 0; round < MAX_ROUNDS; round++) {
            const [group] = await this.index.call([route], "indexPage", [{ direction: request0.direction, where, limit: request0.limit - rows.length, budget: request0.budget, after }]);
            const page = group.result[0];
            rows.push(...page.rows.map((r) => ({ row: r.row, version: r.version })));
            after = page.frontier;
            if (after === null || rows.length === request0.limit)
                break;
        }
        return { rows, cursor: after === null ? null : encodeCursor(fingerprint, after) };
    }
    /** Results back in `out` at `positions`; returns the positions to write again (their reservation was given up). */
    settle(positions, results, out, attempt, keyAt) {
        const again = [];
        positions.forEach((i, j) => {
            const r = results[j];
            if (r.refused !== undefined && attempt < WRITE_ATTEMPTS)
                return void again.push(i);
            if (r.refused !== undefined)
                throw new IndexContended(keyAt(i), r.refused);
            if (r.indexed === false)
                throw new IndexPending(keyAt(i), r.version);
            out[i] = { ok: r.ok, version: r.version };
        });
        return again;
    }
    /** Reserves the unique values of these rows, each in one call per index shard. */
    async reserveAll(rows, indexes = this.globals.filter((ix) => ix.unique)) {
        const wanted = [];
        rows.forEach(({ row }, i) => {
            for (const ix of indexes) {
                const e = entryOf(this.schema, ix, row);
                if (e)
                    wanted.push({ row: i, e });
            }
        });
        const out = rows.map(() => ({ tokens: {} }));
        let todo = wanted;
        for (let attempt = 1; todo.length > 0; attempt++) {
            const answers = new Array(todo.length);
            for (const group of await this.index.call(todo.map((w) => w.e.route), "indexReserve", todo.map((w) => ({ entry: w.e.entry, owner: rows[w.row].key }))))
                group.positions.forEach((p, k) => (answers[p] = group.result[k]));
            const again = [];
            todo.forEach((w, k) => {
                const a = answers[k];
                const slot = out[w.row];
                if ("conflict" in slot)
                    return;
                if ("reserved" in a)
                    slot.tokens[w.e.index] = a.reserved;
                else if ("taken" in a)
                    out[w.row] = { conflict: w.e.index };
                else if (attempt < RESERVE_ATTEMPTS)
                    again.push(w);
                else
                    throw new IndexContended(rows[w.row].key, w.e.index);
            });
            todo = again;
            if (todo.length)
                await sleep(20 * 2 ** attempt * (0.5 + Math.random()));
        }
        return out;
    }
    globalIndex(name) {
        const ix = this.globals.find((g) => g.name === name);
        if (!ix)
            throw new Error(`no global index ${name}`);
        return ix;
    }
    /** A condition with its value encoded as rows store it, checked against the column's type. */
    encodeCondition(c) {
        const type = this.schema.columns[c.column];
        if (!type)
            throw new Error(`no column ${c.column}`);
        if (c.op === "isNull" || c.op === "notNull" || c.op === "prefix" || c.op === "contains")
            return c;
        if (type === "json")
            throw new Error(`json column ${c.column} cannot be compared`);
        return { column: c.column, op: c.op, value: encodeValue(this.schema, c.column, c.value) };
    }
}
/**
 * The client of the indexed table `name`: `namespace` is the binding of an IndexedTable class,
 * `indexNamespace` the binding of its GlobalIndex class, `schema` the table both declare.
 */
export function indexedTable(namespace, indexNamespace, name, schema, options = {}) {
    return new IndexedTableClient(namespace, indexNamespace, name, schema, options);
}
//# sourceMappingURL=client.js.map