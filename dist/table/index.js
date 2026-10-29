// ShardedTable: typed rows on a sharded object, with indexes kept by each shard for its own rows,
// conditional writes, listings merged across shards, and counts. See docs/design/sharded-table.md.
import { keyHash, ShardedObject } from "../sharded/index.js";
import { encodeIndexKey, encodeValue, pageRequest } from "./schema.js";
export * from "./schema.js";
export { table, TableClient } from "./client.js";
const TABLE = "sharded_table";
const COUNTS = "sharded_table_counts";
/**
 * One row, "deleted": the highest version a deleted row of this shard had. A row written where
 * none is starts above it, so a version is never reused for a key, and a write conditional on a
 * version read before a delete cannot land on the row written since. Copied with every split.
 */
const VERSIONS = "sharded_table_versions";
const INDEX_PREFIX = "sharded_table_ix_";
/** Each index keeps one hidden column of index keys (see encodeIndexKey), indexed alone. */
const KEY_COLUMN_PREFIX = "_ix_";
/** Rows per hash prefix are counted with this many hex digits (16 bits: exact to depth 16). */
const BUCKET_DIGITS = 4;
const MAX_TABLE_DEPTH = BUCKET_DIGITS * 4;
const CLEANUP_BATCH = 250;
/** Rows read at a time to fill in the keys of a new index. */
const BACKFILL_PAGE = 500;
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
    /** SQLite's index on the primary key, for listings in key order. */
    keyIndex = "";
    // ---- Called by the client ----
    async tableGet(keys, _payloads) {
        this.migrate();
        this.assertOwns(keys);
        return keys.map((key) => this.getRow(key));
    }
    async tablePut(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => payloads.map(({ row, ifVersion }, i) => this.putRow(this.checkKey(keys[i], row), ifVersion === undefined ? {} : { ifVersion })));
    }
    async tablePatch(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => payloads.map(({ changes, ifVersion }, i) => this.patchRow(keys[i], changes, ifVersion === undefined ? {} : { ifVersion })));
    }
    async tableDelete(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => keys.map((key, i) => {
            const ifVersion = payloads?.[i]?.ifVersion;
            return this.deleteRow(key, ifVersion === undefined ? {} : { ifVersion });
        }));
    }
    /** One shard's part of a listing page. */
    async tablePage(request) {
        this.migrate();
        this.assertOwns([]);
        const checked = pageRequest({ ...(request.index == null ? {} : { index: request.index }), direction: request.direction, where: request.where, limit: request.limit, budget: request.budget }, request.after);
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
        const stored = {};
        for (const name of Object.keys(this.table.columns))
            stored[name] = this.encode(name, row[name]);
        for (const ix of this.indexes())
            stored[ix.keyColumn] = this.indexKey(ix, stored);
        const names = Object.keys(stored);
        const version = (current === 0 ? this.deletedVersion() : current) + 1;
        this.db.exec(`INSERT INTO ${TABLE} (${names.map((n) => this.q(n)).join(", ")}, _hash, _version) VALUES (${names.map(() => "?").join(", ")}, ?, ?) ` +
            `ON CONFLICT (${this.q(this.table.primaryKey)}) DO UPDATE SET ${names.map((n) => `${this.q(n)} = excluded.${this.q(n)}`).join(", ")}, _version = excluded._version`, ...names.map((n) => stored[n]), keyHash(key), version);
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
        const changed = Object.keys(changes).filter((name) => name !== this.table.primaryKey);
        for (const name of changed)
            if (!(name in this.table.columns))
                throw new Error(`no column ${name}`);
        const stored = this.db.exec(`SELECT * FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).one();
        const updates = {};
        for (const name of changed)
            updates[name] = stored[name] = this.encode(name, changes[name]);
        for (const ix of this.indexes())
            if (ix.columns.some((c) => changed.includes(c)))
                updates[ix.keyColumn] = this.indexKey(ix, stored);
        const names = Object.keys(updates);
        const version = current + 1;
        this.db.exec(`UPDATE ${TABLE} SET ${[...names.map((n) => `${this.q(n)} = ?`), "_version = ?"].join(", ")} WHERE ${this.q(this.table.primaryKey)} = ?`, ...names.map((n) => updates[n]), version, key);
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
        this.db.exec(`INSERT INTO ${VERSIONS} (name, version) VALUES ('deleted', ?) ON CONFLICT (name) DO UPDATE SET version = max(version, excluded.version)`, current);
        return { ok: true, version: 0 };
    }
    /** The highest version a deleted row of this shard had (0: none). */
    deletedVersion() {
        return this.db.exec(`SELECT version FROM ${VERSIONS} WHERE name = 'deleted'`).toArray()[0]?.version ?? 0;
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
        const pk = this.table.primaryKey;
        const ix = request.index === null ? null : this.indexes().find((i) => i.name === request.index);
        if (request.index !== null && !ix)
            throw new Error(`no index ${request.index}`);
        // One column holds each row's position: its index key, or its primary key. SQLite seeks to the
        // cursor in that column's index and reads on in order.
        const column = this.q(ix ? ix.keyColumn : pk);
        const sqlIndex = ix ? ix.sqlIndex : this.keyIndex;
        const [lo, hi] = this.ownedHashRange();
        const params = [];
        const matches = request.where.length ? request.where.map((c) => this.condition(c, params)).join(" AND ") : "1";
        const after = request.after === null ? "" : `${column} ${request.direction === "asc" ? ">" : "<"} ? AND `;
        const sql = `SELECT *, (${matches}) AS _m FROM ${TABLE} INDEXED BY "${sqlIndex}" WHERE ${after}_hash >= ? AND _hash <= ? ` +
            `ORDER BY ${column} ${request.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`;
        const rows = [];
        let last = null;
        let scanned = 0;
        for (const found of this.db.exec(sql, ...params, ...(request.after === null ? [] : [request.after]), lo, hi, request.budget)) {
            scanned++;
            last = found[ix ? ix.keyColumn : pk];
            if (found._m) {
                rows.push({ key: found[pk], row: this.decode(found), version: found._version, position: last });
                if (rows.length === request.limit)
                    return { rows, frontier: last };
            }
        }
        // Out of budget: reported everything up to the last row scanned. Out of rows: the end.
        return { rows, frontier: scanned === request.budget ? last : null };
    }
    indexes() {
        return Object.entries(this.table.indexes ?? {}).map(([name, columns]) => {
            const id = `${name}_${shortHash(columns.join(","))}`;
            return { name, columns, keyColumn: `${KEY_COLUMN_PREFIX}${id}`, sqlIndex: `${INDEX_PREFIX}${id}` };
        });
    }
    /** A row's key in an index, from its stored values. */
    indexKey(ix, stored) {
        return encodeIndexKey([...ix.columns.map((c) => stored[c] ?? null), stored[this.table.primaryKey]]);
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
        // Rows are counted per 16-bit hash prefix: a deeper shard would count its sibling's rows too.
        if (this.splitPolicy.maxDepth > MAX_TABLE_DEPTH)
            throw new Error(`a sharded table splits at most ${MAX_TABLE_DEPTH} deep: lower splitPolicy.maxDepth`);
        const schema = this.table;
        const pk = this.q(schema.primaryKey);
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${TABLE} (${pk} TEXT PRIMARY KEY NOT NULL, _hash TEXT NOT NULL, _version INTEGER NOT NULL)`);
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNTS} (bucket TEXT PRIMARY KEY NOT NULL, n INTEGER NOT NULL) WITHOUT ROWID`);
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${VERSIONS} (name TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID`);
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
        const wanted = this.indexes();
        // Ours by exact prefix (LIKE would match any character for "_", and any case).
        const sqlIndexes = this.db.exec(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND substr(name, 1, ?) = ?`, TABLE, INDEX_PREFIX.length, INDEX_PREFIX).toArray();
        for (const { name } of sqlIndexes)
            if (!wanted.some((ix) => ix.sqlIndex === name))
                this.db.exec(`DROP INDEX IF EXISTS "${name}"`);
        for (const { name } of existing)
            if (name.startsWith(KEY_COLUMN_PREFIX) && !wanted.some((ix) => ix.keyColumn === name))
                this.db.exec(`ALTER TABLE ${TABLE} DROP COLUMN "${name}"`);
        for (const ix of wanted) {
            if (!have.has(ix.keyColumn)) {
                this.db.exec(`ALTER TABLE ${TABLE} ADD COLUMN "${ix.keyColumn}" TEXT`);
                // A new index on existing rows: fill in their keys, a page at a time (never every row in
                // memory). It all commits together: the writes of one synchronous call are atomic.
                for (let after = null;;) {
                    const page = this.db.exec(`SELECT * FROM ${TABLE} WHERE (? IS NULL OR ${pk} > ?) ORDER BY ${pk} LIMIT ?`, after, after, BACKFILL_PAGE).toArray();
                    for (const stored of page)
                        this.db.exec(`UPDATE ${TABLE} SET "${ix.keyColumn}" = ? WHERE ${pk} = ?`, this.indexKey(ix, stored), stored[schema.primaryKey]);
                    if (page.length < BACKFILL_PAGE)
                        break;
                    after = page.at(-1)[schema.primaryKey];
                }
            }
            this.db.exec(`CREATE INDEX IF NOT EXISTS "${ix.sqlIndex}" ON ${TABLE} ("${ix.keyColumn}")`);
        }
        this.keyIndex = this.db.exec(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE 'sqlite_autoindex_%'`, TABLE).one().name;
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
        return encodeValue(this.table, column, value);
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