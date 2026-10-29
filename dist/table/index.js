// ShardedTable: typed rows on a sharded object, with indexes kept by each shard for its own rows,
// conditional writes, listings merged across shards, and counts. See docs/design/sharded-table.md.
import { keyHash, ShardedObject } from "../sharded/index.js";
import { pageRequest } from "./schema.js";
export * from "./schema.js";
export { table, TableClient } from "./client.js";
const TABLE = "sharded_table";
const COUNTS = "sharded_table_counts";
const INDEX_PREFIX = "sharded_table_ix_";
/** Rows per hash prefix are counted with this many hex digits (16 bits: exact to depth 16). */
const BUCKET_DIGITS = 4;
const CLEANUP_BATCH = 250;
const AFFINITY = { text: "TEXT", integer: "INTEGER", real: "REAL", boolean: "INTEGER", json: "TEXT" };
/**
 * Extend with `protected table = defineTable({...})` and `nodes()`. The client (`table()`) calls
 * the `table*` methods. Custom transactional methods read and write rows with `getRow`, `putRow`,
 * `patchRow` and `deleteRow` inside `transact`.
 *
 * Subclasses that override `cleanupForeign` must call `super.cleanupForeign`.
 */
export class ShardedTable extends ShardedObject {
    migrated = false;
    // ---- Called by the client ----
    async tableGet(keys, _payloads) {
        this.migrate();
        this.assertOwns(keys);
        return keys.map((key) => this.getRow(key));
    }
    async tablePut(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => payloads.map(({ row, ifVersion }, i) => this.putRow(this.checkKey(keys[i], row), { ifVersion })));
    }
    async tablePatch(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => payloads.map(({ changes, ifVersion }, i) => this.patchRow(keys[i], changes, { ifVersion })));
    }
    async tableDelete(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => keys.map((key, i) => this.deleteRow(key, { ifVersion: payloads?.[i]?.ifVersion })));
    }
    /** One shard's part of a listing page. */
    async tablePage(request) {
        this.migrate();
        this.assertOwns([]);
        const checked = pageRequest({ index: request.index ?? undefined, direction: request.direction, where: request.where, limit: request.limit, budget: request.budget }, request.after);
        return this.page(checked);
    }
    /** Rows this shard owns. */
    async tableCount() {
        this.migrate();
        this.assertOwns([]);
        const [lo, hi] = this.ownedHashRange();
        const row = this.db.exec(`SELECT SUM(n) AS n FROM ${COUNTS} WHERE bucket >= ? AND bucket <= ?`, lo.slice(0, BUCKET_DIGITS), hi.slice(0, BUCKET_DIGITS)).one();
        return row.n ?? 0;
    }
    // ---- Row helpers for subclasses (writes inside `transact`) ----
    /** The row of `key`, or null. */
    getRow(key) {
        this.migrate();
        const found = this.db.exec(`SELECT * FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).toArray()[0];
        return found ? { row: this.decode(found), version: found._version } : null;
    }
    /** Writes a whole row if its version is `ifVersion` (0: absent). Call inside `transact`. */
    putRow(row, options = {}) {
        this.migrate();
        const key = row[this.table.primaryKey];
        const current = this.version(key);
        if (options.ifVersion !== undefined && options.ifVersion !== current)
            return { ok: false, version: current };
        const names = Object.keys(this.table.columns);
        const values = names.map((name) => this.encode(name, row[name]));
        const version = current + 1;
        this.db.exec(`INSERT INTO ${TABLE} (${names.map((n) => this.q(n)).join(", ")}, _hash, _version) VALUES (${names.map(() => "?").join(", ")}, ?, ?) ` +
            `ON CONFLICT (${this.q(this.table.primaryKey)}) DO UPDATE SET ${names.map((n) => `${this.q(n)} = excluded.${this.q(n)}`).join(", ")}, _version = excluded._version`, ...values, keyHash(key), version);
        if (current === 0)
            this.countRow(key, 1);
        return { ok: true, version };
    }
    /** Changes some columns of an existing row if its version is `ifVersion`. Call inside `transact`. */
    patchRow(key, changes, options = {}) {
        this.migrate();
        const current = this.version(key);
        if (current === 0 || (options.ifVersion !== undefined && options.ifVersion !== current))
            return { ok: false, version: current };
        const names = Object.keys(changes).filter((name) => name !== this.table.primaryKey);
        for (const name of names)
            if (!(name in this.table.columns))
                throw new Error(`no column ${name}`);
        const version = current + 1;
        this.db.exec(`UPDATE ${TABLE} SET ${[...names.map((n) => `${this.q(n)} = ?`), "_version = ?"].join(", ")} WHERE ${this.q(this.table.primaryKey)} = ?`, ...names.map((name) => this.encode(name, changes[name])), version, key);
        return { ok: true, version };
    }
    /** Deletes a row if its version is `ifVersion`. Call inside `transact`. */
    deleteRow(key, options = {}) {
        this.migrate();
        const current = this.version(key);
        if (options.ifVersion !== undefined && options.ifVersion !== current)
            return { ok: false, version: current };
        if (current === 0)
            return { ok: true, version: 0 };
        this.db.exec(`DELETE FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key);
        this.countRow(key, -1);
        return { ok: true, version: 0 };
    }
    // ---- Splits ----
    /** Deletes rows (and counts) of hashes this shard gave away, a batch at a time. */
    cleanupForeign(owns, limit) {
        this.migrate();
        const [lo, hi] = this.ownedHashRange();
        const pk = this.q(this.table.primaryKey);
        let deleted = this.db.exec(`DELETE FROM ${TABLE} WHERE ${pk} IN (SELECT ${pk} FROM ${TABLE} WHERE _hash < ? OR _hash > ? LIMIT ?)`, lo, hi, Math.min(limit, CLEANUP_BATCH)).rowsWritten;
        if (deleted === 0)
            deleted = this.db.exec(`DELETE FROM ${COUNTS} WHERE bucket < ? OR bucket > ?`, lo.slice(0, BUCKET_DIGITS), hi.slice(0, BUCKET_DIGITS)).rowsWritten;
        return deleted + super.cleanupForeign(owns, limit);
    }
    // ---- Listing ----
    page(request) {
        const schema = this.table;
        const columns = request.index === null ? [] : schema.indexes?.[request.index];
        if (!columns)
            throw new Error(`no index ${request.index}`);
        const order = [...columns, schema.primaryKey];
        const dir = request.direction === "asc" ? "ASC" : "DESC";
        const [lo, hi] = this.ownedHashRange();
        const params = [];
        const matches = request.where.length ? request.where.map((c) => this.condition(c, params)).join(" AND ") : "1";
        const whereParams = params.splice(0);
        const after = request.after ? this.after(order, request.after, request.direction, params) : "1";
        const sql = `SELECT *, (${matches}) AS _m FROM ${TABLE} WHERE _hash >= ? AND _hash <= ? AND ${after} ` +
            `ORDER BY ${order.map((c) => `${this.q(c)} ${dir}`).join(", ")} LIMIT ?`;
        const rows = [];
        let last = null;
        let scanned = 0;
        for (const found of this.db.exec(sql, ...whereParams, lo, hi, ...params, request.budget)) {
            scanned++;
            last = order.map((c) => found[c] ?? null);
            if (found._m) {
                rows.push({ row: this.decode(found), version: found._version, position: last });
                if (rows.length === request.limit)
                    return { rows, frontier: last };
            }
        }
        // Out of budget: reported everything up to the last row scanned. Out of rows: the end.
        return { rows, frontier: scanned === request.budget ? last : null };
    }
    /** Rows strictly after `position` in listing order, as SQLite orders (nulls first ascending). */
    after(order, position, direction, params) {
        if (position.length !== order.length)
            throw new Error("the cursor does not fit this index");
        const term = (i) => {
            const col = this.q(order[i]);
            const v = position[i];
            const isKey = i === order.length - 1;
            if (isKey) {
                params.push(v);
                return `${col} ${direction === "asc" ? ">" : "<"} ?`;
            }
            if (direction === "asc") {
                if (v === null)
                    return `(${col} IS NOT NULL OR (${col} IS NULL AND ${term(i + 1)}))`;
                params.push(v, v);
                return `(${col} > ? OR (${col} = ? AND ${term(i + 1)}))`;
            }
            if (v === null)
                return `(${col} IS NULL AND ${term(i + 1)})`;
            params.push(v, v);
            return `(${col} < ? OR ${col} IS NULL OR (${col} = ? AND ${term(i + 1)}))`;
        };
        // A plain bound on the first column lets SQLite seek in the index before the exact test.
        const first = this.q(order[0]);
        const v = position[0];
        let seek = "1";
        if (order.length > 1) {
            if (direction === "asc" && v !== null) {
                params.push(v);
                seek = `${first} >= ?`;
            }
            else if (direction === "desc") {
                if (v === null)
                    seek = `${first} IS NULL`;
                else {
                    params.push(v);
                    seek = `(${first} <= ? OR ${first} IS NULL)`;
                }
            }
        }
        return `(${seek} AND ${term(0)})`;
    }
    condition(c, params) {
        const type = this.table.columns[c.column];
        if (!type)
            throw new Error(`no column ${c.column}`);
        const col = this.q(c.column);
        switch (c.op) {
            case "isNull":
                return `${col} IS NULL`;
            case "notNull":
                return `${col} IS NOT NULL`;
            case "prefix":
                params.push(String(c.value));
                return `instr(${col}, ?) = 1`;
            case "contains":
                params.push(String(c.value));
                return `instr(lower(${col}), lower(?)) > 0`;
            case "=":
            case "!=":
            case "<":
            case "<=":
            case ">":
            case ">=":
                if (type === "json")
                    throw new Error(`json column ${c.column} cannot be compared`);
                params.push(this.encode(c.column, c.value));
                return `${col} ${c.op} ?`;
            default:
                throw new Error(`unknown op ${String(c.op)}`);
        }
    }
    // ---- Storage ----
    /** Brings this shard's storage up to the schema: once per instance, on its first use. */
    migrate() {
        if (this.migrated)
            return;
        const schema = this.table;
        const pk = this.q(schema.primaryKey);
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${TABLE} (${pk} TEXT PRIMARY KEY NOT NULL, _hash TEXT NOT NULL, _version INTEGER NOT NULL)`);
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNTS} (bucket TEXT PRIMARY KEY NOT NULL, n INTEGER NOT NULL) WITHOUT ROWID`);
        this.db.exec(`CREATE INDEX IF NOT EXISTS ${TABLE}_hash ON ${TABLE} (_hash)`);
        const existing = this.db.exec(`PRAGMA table_info(${TABLE})`).toArray();
        const key = existing.find((c) => c.pk === 1)?.name;
        if (key !== schema.primaryKey)
            throw new Error(`the table's primary key is ${key}, not ${schema.primaryKey}: it cannot change`);
        const have = new Set(existing.map((c) => c.name));
        for (const [name, type] of Object.entries(schema.columns)) {
            if (!have.has(name))
                this.db.exec(`ALTER TABLE ${TABLE} ADD COLUMN ${this.q(name)} ${AFFINITY[type]}`);
        }
        const wanted = new Map();
        for (const [name, cols] of Object.entries(schema.indexes ?? {}))
            wanted.set(`${INDEX_PREFIX}${name}_${shortHash(cols.join(","))}`, cols);
        for (const { name } of this.db.exec(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE ?`, TABLE, `${INDEX_PREFIX}%`).toArray()) {
            if (!wanted.has(name))
                this.db.exec(`DROP INDEX IF EXISTS "${name}"`);
        }
        for (const [name, cols] of wanted)
            this.db.exec(`CREATE INDEX IF NOT EXISTS "${name}" ON ${TABLE} (${[...cols, schema.primaryKey].map((c) => this.q(c)).join(", ")})`);
        this.migrated = true;
    }
    version(key) {
        return this.db.exec(`SELECT _version AS v FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).toArray()[0]?.v ?? 0;
    }
    countRow(key, delta) {
        this.db.exec(`INSERT INTO ${COUNTS} (bucket, n) VALUES (?, ?) ON CONFLICT (bucket) DO UPDATE SET n = n + excluded.n`, keyHash(key).slice(0, BUCKET_DIGITS), delta);
    }
    checkKey(key, row) {
        if (row[this.table.primaryKey] !== key)
            throw new Error(`row key ${String(row[this.table.primaryKey])} is not ${key}`);
        return row;
    }
    encode(column, value) {
        const type = this.table.columns[column];
        if (value === null || value === undefined) {
            if (column === this.table.primaryKey)
                throw new Error("the primary key cannot be null");
            return null;
        }
        switch (type) {
            case "text":
                if (typeof value !== "string")
                    throw new Error(`${column}: expected text`);
                return value;
            case "integer":
                if (typeof value !== "number" || !Number.isInteger(value))
                    throw new Error(`${column}: expected an integer`);
                return value;
            case "real":
                if (typeof value !== "number")
                    throw new Error(`${column}: expected a number`);
                return value;
            case "boolean":
                if (typeof value !== "boolean")
                    throw new Error(`${column}: expected a boolean`);
                return value ? 1 : 0;
            case "json":
                return JSON.stringify(value);
            default:
                throw new Error(`no column ${column}`);
        }
    }
    decode(stored) {
        const row = {};
        for (const [name, type] of Object.entries(this.table.columns)) {
            const v = stored[name] ?? null;
            row[name] = v === null ? null : type === "boolean" ? v === 1 : type === "json" ? JSON.parse(v) : v;
        }
        return row;
    }
    q(identifier) {
        return `"${identifier}"`;
    }
    get db() {
        return this.ctx.storage.sql;
    }
}
function shortHash(text) {
    let h = 0x811c9dc5;
    for (let i = 0; i < text.length; i++)
        h = Math.imul(h ^ text.charCodeAt(i), 0x01000193);
    return (h >>> 0).toString(36);
}
//# sourceMappingURL=index.js.map