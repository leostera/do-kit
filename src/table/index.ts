// ShardedTable: typed rows on a sharded object, with indexes kept by each shard for its own rows,
// conditional writes, listings merged across shards, and counts. See docs/design/sharded-table.md.
import { keyHash, ShardedObject } from "../sharded/index.js";
import { encodeIndexKey, encodeValue, pageRequest, type ColumnType, type Condition, type GroupCount, type RowShape, type ShardPage, type ShardPageRequest, type SqlValue, type TableSchema, type Versioned, type WriteResult } from "./schema.js";

export * from "./schema.js";
export { table, TableClient } from "./client.js";

const TABLE = "sharded_table";
const COUNTS = "sharded_table_counts";
/** Rows per value of a count group's column, per hash prefix like COUNTS; and which groups are kept. */
const GROUP_COUNTS = "sharded_table_group_counts";
const COUNT_GROUPS = "sharded_table_count_groups";
/**
 * One row, "deleted": the highest version a deleted row of this shard had. A row written where
 * none is starts above it, so a version is never reused for a key, and a write conditional on a
 * version read before a delete cannot land on the row written since. Copied with every split.
 */
const VERSIONS = "sharded_table_versions";
const INDEX_PREFIX = "sharded_table_ix_";
/** Each index keeps one hidden column of index keys (see encodeIndexKey), indexed alone. */
const KEY_COLUMN_PREFIX = "_ix_";

/** A declared index: its columns, its hidden key column and the SQL index on it. */
interface IndexDef {
  readonly name: string;
  readonly columns: readonly string[];
  readonly keyColumn: string;
  readonly sqlIndex: string;
}
/** Rows per hash prefix are counted with this many hex digits (16 bits: exact to depth 16). */
const BUCKET_DIGITS = 4;
const MAX_TABLE_DEPTH = BUCKET_DIGITS * 4;
const CLEANUP_BATCH = 250;
/** Rows read at a time to fill in the keys of a new index. */
const BACKFILL_PAGE = 500;

const AFFINITY: Record<ColumnType, string> = { text: "TEXT", integer: "INTEGER", real: "REAL", boolean: "INTEGER", json: "TEXT" };

/** A put: the whole row, written if the row's version is `ifVersion` (0: absent), or always. */
export interface PutOptions {
  readonly ifVersion?: number;
}

/**
 * Extend with `protected table = defineTable({...})` and `nodes()`. The client (`table()`) calls
 * the `table*` methods. Custom transactional methods read and write rows with `getRow`, `putRow`,
 * `patchRow` and `deleteRow` inside `transact`.
 *
 * Subclasses that override `cleanupForeign` must call `super.cleanupForeign`.
 */
export abstract class ShardedTable<Env, Row extends RowShape> extends ShardedObject<Env, never> {
  protected abstract readonly table: TableSchema<Row>;
  private migrated = false;
  /** SQLite's index on the primary key, for listings in key order. */
  private keyIndex = "";

  // ---- Called by the client ----

  async tableGet(keys: readonly string[], _payloads: null): Promise<(Versioned<Row> | null)[]> {
    this.migrate();
    this.assertOwns(keys);
    return keys.map((key) => this.getRow(key));
  }

  async tablePut(keys: readonly string[], payloads: readonly { readonly row: Row; readonly ifVersion?: number }[]): Promise<WriteResult[]> {
    this.migrate();
    return this.transact(keys, () => payloads.map(({ row, ifVersion }, i) => this.putRow(this.checkKey(keys[i]!, row), ifVersion === undefined ? {} : { ifVersion })));
  }

  async tablePatch(keys: readonly string[], payloads: readonly { readonly changes: Partial<Row>; readonly ifVersion?: number }[]): Promise<WriteResult[]> {
    this.migrate();
    return this.transact(keys, () => payloads.map(({ changes, ifVersion }, i) => this.patchRow(keys[i]!, changes, ifVersion === undefined ? {} : { ifVersion })));
  }

  async tableDelete(keys: readonly string[], payloads: readonly { readonly ifVersion?: number }[] | null): Promise<WriteResult[]> {
    this.migrate();
    return this.transact(keys, () => keys.map((key, i) => {
      const ifVersion = payloads?.[i]?.ifVersion;
      return this.deleteRow(key, ifVersion === undefined ? {} : { ifVersion });
    }));
  }

  /** One shard's part of a listing page. */
  async tablePage(request: ShardPageRequest): Promise<ShardPage> {
    this.migrate();
    this.assertOwns([]);
    const checked = pageRequest({ ...(request.index == null ? {} : { index: request.index }), direction: request.direction, where: request.where, limit: request.limit, budget: request.budget }, request.after);
    const counts = typeof request.withCount === "string" ? this.countBy(request.withCount) : undefined;
    const page = this.page(checked);
    if (!request.withCount) return page;
    return { ...page, total: this.count(), ...(counts ? { counts } : {}) };
  }

  /** Rows this shard owns. */
  async tableCount(): Promise<number> {
    this.migrate();
    this.assertOwns([]);
    return this.count();
  }

  /** Rows this shard owns, per value of a count group's column (values with no rows left out). */
  async tableCountBy(group: string): Promise<GroupCount[]> {
    this.migrate();
    this.assertOwns([]);
    return this.countBy(group);
  }

  // ---- Row helpers for subclasses (writes inside `transact`) ----

  /** The row of `key`, or null. */
  protected getRow(key: string): Versioned<Row> | null {
    this.migrate();
    const found = this.db.exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).toArray()[0];
    return found ? { row: this.decode(found), version: found._version as number } : null;
  }

  /** Writes a whole row if its version is `ifVersion` (0: absent). Call inside `transact`. */
  protected putRow(row: Row, options: PutOptions = {}): WriteResult {
    this.migrate();
    const key = row[this.table.primaryKey] as string;
    const current = this.version(key);
    if (options.ifVersion !== undefined && options.ifVersion !== current) return { ok: false, version: current };
    const before = current === 0 ? null : this.storedGroupValues(key);
    const stored: Record<string, SqlValue> = {};
    for (const name of Object.keys(this.table.columns)) stored[name] = this.encode(name, row[name]);
    for (const ix of this.indexes()) stored[ix.keyColumn] = this.indexKey(ix, stored);
    const names = Object.keys(stored);
    const version = (current === 0 ? this.deletedVersion() : current) + 1;
    this.db.exec(
      `INSERT INTO ${TABLE} (${names.map((n) => this.q(n)).join(", ")}, _hash, _version) VALUES (${names.map(() => "?").join(", ")}, ?, ?) ` +
        `ON CONFLICT (${this.q(this.table.primaryKey)}) DO UPDATE SET ${names.map((n) => `${this.q(n)} = excluded.${this.q(n)}`).join(", ")}, _version = excluded._version`,
      ...names.map((n) => stored[n]!),
      keyHash(key),
      version,
    );
    if (current === 0) this.countRow(key, 1);
    this.countGroups(key, before, stored);
    return { ok: true, version };
  }

  /** Changes some columns of an existing row if its version is `ifVersion`. Call inside `transact`. */
  protected patchRow(key: string, changes: Partial<Row>, options: PutOptions = {}): WriteResult {
    this.migrate();
    const current = this.version(key);
    if (current === 0 || (options.ifVersion !== undefined && options.ifVersion !== current)) return { ok: false, version: current };
    const changed = Object.keys(changes).filter((name) => name !== this.table.primaryKey);
    for (const name of changed) if (!(name in this.table.columns)) throw new Error(`no column ${name}`);
    const stored = this.db.exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).one();
    const before = { ...stored };
    const updates: Record<string, SqlValue> = {};
    for (const name of changed) updates[name] = stored[name] = this.encode(name, (changes as RowShape)[name]);
    for (const ix of this.indexes()) if (ix.columns.some((c) => changed.includes(c))) updates[ix.keyColumn] = this.indexKey(ix, stored);
    const names = Object.keys(updates);
    const version = current + 1;
    this.db.exec(
      `UPDATE ${TABLE} SET ${[...names.map((n) => `${this.q(n)} = ?`), "_version = ?"].join(", ")} WHERE ${this.q(this.table.primaryKey)} = ?`,
      ...names.map((n) => updates[n]!),
      version,
      key,
    );
    this.countGroups(key, before, stored);
    return { ok: true, version };
  }

  /** Deletes a row if its version is `ifVersion`. Call inside `transact`. */
  protected deleteRow(key: string, options: PutOptions = {}): WriteResult {
    this.migrate();
    const current = this.version(key);
    if (options.ifVersion !== undefined && options.ifVersion !== current) return { ok: false, version: current };
    if (current === 0) return { ok: true, version: 0 };
    const before = this.storedGroupValues(key);
    this.db.exec(`DELETE FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key);
    this.countRow(key, -1);
    this.countGroups(key, before, null);
    this.db.exec(`INSERT INTO ${VERSIONS} (name, version) VALUES ('deleted', ?) ON CONFLICT (name) DO UPDATE SET version = max(version, excluded.version)`, current);
    return { ok: true, version: 0 };
  }

  /** The highest version a deleted row of this shard had (0: none). */
  private deletedVersion(): number {
    return this.db.exec<{ version: number }>(`SELECT version FROM ${VERSIONS} WHERE name = 'deleted'`).toArray()[0]?.version ?? 0;
  }

  // ---- Splits ----

  /** Deletes rows (and counts) of hashes this shard gave away, a batch at a time. */
  protected override cleanupForeign(owns: (key: string) => boolean, limit: number): number {
    this.migrate();
    const [lo, hi] = this.ownedHashRange();
    const pk = this.q(this.table.primaryKey);
    let deleted = this.db.exec(`DELETE FROM ${TABLE} WHERE ${pk} IN (SELECT ${pk} FROM ${TABLE} WHERE _hash < ? OR _hash > ? LIMIT ?)`, lo, hi, Math.min(limit, CLEANUP_BATCH)).rowsWritten;
    if (deleted === 0) {
      const [blo, bhi] = [lo.slice(0, BUCKET_DIGITS), hi.slice(0, BUCKET_DIGITS)];
      deleted = this.db.exec(`DELETE FROM ${COUNTS} WHERE bucket < ? OR bucket > ?`, blo, bhi).rowsWritten;
      deleted += this.db.exec(`DELETE FROM ${GROUP_COUNTS} WHERE bucket < ? OR bucket > ?`, blo, bhi).rowsWritten;
    }
    return deleted + super.cleanupForeign(owns, limit);
  }

  // ---- Listing ----

  private page(request: ShardPageRequest): ShardPage {
    const pk = this.table.primaryKey;
    const ix = request.index === null ? null : this.indexes().find((i) => i.name === request.index);
    if (request.index !== null && !ix) throw new Error(`no index ${request.index}`);
    // One column holds each row's position: its index key, or its primary key. SQLite seeks to the
    // cursor in that column's index and reads on in order.
    const column = this.q(ix ? ix.keyColumn : pk);
    const sqlIndex = ix ? ix.sqlIndex : this.keyIndex;
    const [lo, hi] = this.ownedHashRange();
    const params: SqlValue[] = [];
    const matches = request.where.length ? request.where.map((c) => this.condition(c, params)).join(" AND ") : "1";
    const after = request.after === null ? "" : `${column} ${request.direction === "asc" ? ">" : "<"} ? AND `;
    const sql =
      `SELECT *, (${matches}) AS _m FROM ${TABLE} INDEXED BY "${sqlIndex}" WHERE ${after}_hash >= ? AND _hash <= ? ` +
      `ORDER BY ${column} ${request.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`;
    const rows: { key: string; row: RowShape; version: number; position: string }[] = [];
    let last: string | null = null;
    let scanned = 0;
    for (const found of this.db.exec<Record<string, SqlValue>>(sql, ...params, ...(request.after === null ? [] : [request.after]), lo, hi, request.budget)) {
      scanned++;
      last = found[ix ? ix.keyColumn : pk] as string;
      if (found._m) {
        rows.push({ key: found[pk] as string, row: this.decode(found), version: found._version as number, position: last });
        if (rows.length === request.limit) return { rows, frontier: last };
      }
    }
    // Out of budget: reported everything up to the last row scanned. Out of rows: the end.
    return { rows, frontier: scanned === request.budget ? last : null };
  }

  private indexes(): IndexDef[] {
    return Object.entries(this.table.indexes ?? {}).map(([name, columns]) => {
      const id = `${name}_${shortHash(columns.join(","))}`;
      return { name, columns, keyColumn: `${KEY_COLUMN_PREFIX}${id}`, sqlIndex: `${INDEX_PREFIX}${id}` };
    });
  }

  /** A row's key in an index, from its stored values. */
  private indexKey(ix: IndexDef, stored: Record<string, SqlValue>): string {
    return encodeIndexKey([...ix.columns.map((c) => stored[c] ?? null), stored[this.table.primaryKey]!]);
  }

  private condition(c: Condition, params: SqlValue[]): string {
    const type = this.table.columns[c.column as keyof Row & string];
    if (!type) throw new Error(`no column ${c.column}`);
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
        if (type === "json") throw new Error(`json column ${c.column} cannot be compared`);
        params.push(this.encode(c.column, c.value));
        return `${col} ${c.op} ?`;
      default:
        throw new Error(`unknown op ${String(c.op)}`);
    }
  }

  // ---- Storage ----

  /** Brings this shard's storage up to the schema: once per instance, on its first use. */
  private migrate(): void {
    if (this.migrated) return;
    // Rows are counted per 16-bit hash prefix: a deeper shard would count its sibling's rows too.
    if (this.splitPolicy.maxDepth > MAX_TABLE_DEPTH) throw new Error(`a sharded table splits at most ${MAX_TABLE_DEPTH} deep: lower splitPolicy.maxDepth`);
    const schema = this.table;
    const pk = this.q(schema.primaryKey);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${TABLE} (${pk} TEXT PRIMARY KEY NOT NULL, _hash TEXT NOT NULL, _version INTEGER NOT NULL)`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNTS} (bucket TEXT PRIMARY KEY NOT NULL, n INTEGER NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${VERSIONS} (name TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${GROUP_COUNTS} (grp TEXT NOT NULL, value TEXT NOT NULL, bucket TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (grp, value, bucket)) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNT_GROUPS} (grp TEXT PRIMARY KEY NOT NULL, col TEXT NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS ${TABLE}_hash ON ${TABLE} (_hash)`);
    const existing = this.db.exec<{ name: string; pk: number }>(`PRAGMA table_info(${TABLE})`).toArray();
    const key = existing.find((c) => c.pk === 1)?.name;
    if (key !== schema.primaryKey) throw new Error(`the table's primary key is ${key}, not ${schema.primaryKey}: it cannot change`);
    const have = new Set(existing.map((c) => c.name));
    for (const [name, type] of Object.entries(schema.columns) as [string, ColumnType][]) {
      if (!have.has(name)) this.db.exec(`ALTER TABLE ${TABLE} ADD COLUMN ${this.q(name)} ${AFFINITY[type]}`);
    }
    const wanted = this.indexes();
    // Ours by exact prefix (LIKE would match any character for "_", and any case).
    const sqlIndexes = this.db.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND substr(name, 1, ?) = ?`, TABLE, INDEX_PREFIX.length, INDEX_PREFIX).toArray();
    for (const { name } of sqlIndexes) if (!wanted.some((ix) => ix.sqlIndex === name)) this.db.exec(`DROP INDEX IF EXISTS "${name}"`);
    for (const { name } of existing) if (name.startsWith(KEY_COLUMN_PREFIX) && !wanted.some((ix) => ix.keyColumn === name)) this.db.exec(`ALTER TABLE ${TABLE} DROP COLUMN "${name}"`);
    for (const ix of wanted) {
      if (!have.has(ix.keyColumn)) {
        this.db.exec(`ALTER TABLE ${TABLE} ADD COLUMN "${ix.keyColumn}" TEXT`);
        // A new index on existing rows: fill in their keys, a page at a time (never every row in
        // memory). It all commits together: the writes of one synchronous call are atomic.
        for (let after: SqlValue = null; ; ) {
          const page = this.db.exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} WHERE (? IS NULL OR ${pk} > ?) ORDER BY ${pk} LIMIT ?`, after, after, BACKFILL_PAGE).toArray();
          for (const stored of page) this.db.exec(`UPDATE ${TABLE} SET "${ix.keyColumn}" = ? WHERE ${pk} = ?`, this.indexKey(ix, stored), stored[schema.primaryKey]!);
          if (page.length < BACKFILL_PAGE) break;
          after = page.at(-1)![schema.primaryKey]!;
        }
      }
      this.db.exec(`CREATE INDEX IF NOT EXISTS "${ix.sqlIndex}" ON ${TABLE} ("${ix.keyColumn}")`);
    }
    this.migrateCountGroups();
    this.keyIndex = this.db.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE 'sqlite_autoindex_%'`, TABLE).one().name;
    this.migrated = true;
  }

  /**
   * Keeps the count groups of the schema: drops the counts of groups removed or moved to another
   * column, and counts the rows stored (owned or not: counts are per hash prefix, like the rows)
   * for groups that are new, a page at a time.
   */
  private migrateCountGroups(): void {
    const wanted = this.table.counts ?? {};
    const kept = this.db.exec<{ grp: string; col: string }>(`SELECT grp, col FROM ${COUNT_GROUPS}`).toArray();
    for (const { grp, col } of kept) {
      if (wanted[grp] === col) continue;
      this.db.exec(`DELETE FROM ${GROUP_COUNTS} WHERE grp = ?`, grp);
      this.db.exec(`DELETE FROM ${COUNT_GROUPS} WHERE grp = ?`, grp);
    }
    const pk = this.q(this.table.primaryKey);
    for (const [grp, col] of Object.entries(wanted) as [string, string][]) {
      if (kept.some((k) => k.grp === grp && k.col === col)) continue;
      for (let after: SqlValue = null; ; ) {
        const page = this.db.exec<{ key: string; hash: string; value: SqlValue }>(`SELECT ${pk} AS key, _hash AS hash, ${this.q(col)} AS value FROM ${TABLE} WHERE (? IS NULL OR ${pk} > ?) ORDER BY ${pk} LIMIT ?`, after, after, BACKFILL_PAGE).toArray();
        for (const r of page) this.countGroup(grp, r.value, r.hash, 1);
        if (page.length < BACKFILL_PAGE) break;
        after = page.at(-1)!.key;
      }
      this.db.exec(`INSERT INTO ${COUNT_GROUPS} (grp, col) VALUES (?, ?)`, grp, col);
    }
  }

  /** Rows this shard owns (its migrated storage). */
  private count(): number {
    const [lo, hi] = this.ownedBuckets();
    return this.db.exec<{ n: number | null }>(`SELECT SUM(n) AS n FROM ${COUNTS} WHERE bucket >= ? AND bucket <= ?`, lo, hi).one().n ?? 0;
  }

  private countBy(group: string): GroupCount[] {
    const col = this.table.counts?.[group];
    if (col === undefined) throw new Error(`no count group ${group}`);
    const [lo, hi] = this.ownedBuckets();
    const rows = this.db.exec<{ value: string; n: number }>(`SELECT value, SUM(n) AS n FROM ${GROUP_COUNTS} WHERE grp = ? AND bucket >= ? AND bucket <= ? GROUP BY value HAVING SUM(n) <> 0`, group, lo, hi).toArray();
    const type = this.table.columns[col as keyof Row & string];
    return rows.map(({ value, n }) => {
      const v = JSON.parse(value) as SqlValue;
      return { value: v === null ? null : type === "boolean" ? v === 1 : v, count: n };
    });
  }

  private ownedBuckets(): [string, string] {
    const [lo, hi] = this.ownedHashRange();
    return [lo.slice(0, BUCKET_DIGITS), hi.slice(0, BUCKET_DIGITS)];
  }

  /** The stored values of the count groups' columns of a row that exists. */
  private storedGroupValues(key: string): Record<string, SqlValue> | null {
    if (!this.table.counts || Object.keys(this.table.counts).length === 0) return null;
    return this.db.exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).one();
  }

  /** Moves a row between the values of each count group: from `before` (null: new) to `after` (null: deleted). */
  private countGroups(key: string, before: Record<string, SqlValue> | null, after: Record<string, SqlValue> | null): void {
    const groups = Object.entries(this.table.counts ?? {}) as [string, string][];
    if (groups.length === 0) return;
    const hash = keyHash(key);
    for (const [grp, col] of groups) {
      const from = before ? (before[col] ?? null) : undefined;
      const to = after ? (after[col] ?? null) : undefined;
      if (from === to) continue;
      if (from !== undefined) this.countGroup(grp, from, hash, -1);
      if (to !== undefined) this.countGroup(grp, to, hash, 1);
    }
  }

  private countGroup(grp: string, value: SqlValue, hash: string, delta: number): void {
    this.db.exec(`INSERT INTO ${GROUP_COUNTS} (grp, value, bucket, n) VALUES (?, ?, ?, ?) ON CONFLICT (grp, value, bucket) DO UPDATE SET n = n + excluded.n`, grp, JSON.stringify(value), hash.slice(0, BUCKET_DIGITS), delta);
  }

  private version(key: string): number {
    return this.db.exec<{ v: number }>(`SELECT _version AS v FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).toArray()[0]?.v ?? 0;
  }

  private countRow(key: string, delta: number): void {
    this.db.exec(`INSERT INTO ${COUNTS} (bucket, n) VALUES (?, ?) ON CONFLICT (bucket) DO UPDATE SET n = n + excluded.n`, keyHash(key).slice(0, BUCKET_DIGITS), delta);
  }

  private checkKey(key: string, row: Row): Row {
    if (row[this.table.primaryKey] !== key) throw new Error(`row key ${String(row[this.table.primaryKey])} is not ${key}`);
    return row;
  }

  private encode(column: string, value: unknown): SqlValue {
    return encodeValue(this.table, column, value);
  }

  private decode(stored: Record<string, SqlValue>): Row {
    const row: RowShape = {};
    for (const [name, type] of Object.entries(this.table.columns) as [string, ColumnType][]) {
      const v = stored[name] ?? null;
      row[name] = v === null ? null : type === "boolean" ? v === 1 : type === "json" ? JSON.parse(v as string) : v;
    }
    return row as Row;
  }

  private q(identifier: string): string {
    return `"${identifier}"`;
  }

  private get db(): SqlStorage {
    return this.ctx.storage.sql;
  }
}

function shortHash(text: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 0x01000193);
  return (h >>> 0).toString(36);
}
