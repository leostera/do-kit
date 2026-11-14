// ShardedTable: typed rows on a sharded object, with indexes kept by each shard for its own rows,
// conditional writes, listings merged across shards, and counts. See docs/rfds/RFD0002-sharded-table.md.
import { ShardedObject } from "../sharded/index.js";
import { DEFAULT_MAX_COUNT_VALUES, DEFAULT_MAX_ROW_BYTES, encodeIndexKey, encodeValue, RowTooLarge, pageRequest, shortDigest, type ColumnType, type Condition, type GroupCount, type RowShape, type ShardPage, type ShardPageRequest, type SqlValue, type TableSchema, type Versioned, type WriteResult, type ApplyOutcome, type ApplyRequest, type Commands, type Decision } from "./schema.js";

export * from "./schema.js";
export { table, TableClient, type TableClientOptions } from "./client.js";

const UTF8 = new TextEncoder();
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
/** Indexes being built over rows that were there before them: how far each has got. */
const BUILDS = "sharded_table_builds";
/** Count groups being counted over the rows stored, and how far (the last key counted). */
const COUNT_BUILDS = "sharded_table_count_builds";
/** Count groups dropped, whose counts are deleted a batch at a time. */
const COUNT_DROPS = "sharded_table_count_drops";
/** Rows written by code older than the schema the shard recorded: their index keys may be stale. */
const DIRTY = "sharded_table_dirty";
/**
 * Key columns of the earlier layout (an index was a hidden column of the table), cleared to null a
 * page at a time to free their space: how far each got, and whether it is done.
 */
const KEY_CLEARS = "sharded_table_legacy_clears";
/** Key tables of dropped indexes, renamed out of the way, emptied a batch at a time, then dropped. */
const KEY_DROPS = "sharded_table_key_drops";
/**
 * One row once a migration has nothing left to build: the schema it applied, and what the
 * migration found. A shard that wakes with the same schema reads it, and nothing else.
 */
const READY = "sharded_table_ready";
/** Raised when what migrate() does changes, so shards that recorded an older one migrate again. */
const MIGRATION_LAYOUT = 2;

/** What a migration found, kept with the ready record. */
interface ReadyState {
  readonly schema: string;
  readonly keyIndex: string;
  readonly rollback: boolean;
  readonly unknownLocal: boolean;
}
/** Writes sent with an opId: the request and what it answered, so a retry answers the same. */
const OPS = "sharded_table_ops";
/** How long a shard keeps a write's opId: a retry later than this is a new write. */
export const OP_ID_TTL_MS = 24 * 60 * 60 * 1000;
/** Expired opIds deleted by a write that records one: the table stays near its TTL's worth. */
const OP_PURGE_BATCH = 16;

/** An opId sent again with another request than the write first recorded under it. */
export class OperationMismatch extends Error {
  constructor(readonly opId: string) {
    super(`opId ${JSON.stringify(opId)} was recorded for another write: an opId names one write, sent again only as itself`);
    this.name = "OperationMismatch";
  }
}
/**
 * Each index keeps a table of its keys (see encodeIndexKey): `_key` the key, its primary key, with
 * the row's primary key and hash. A listing reads it in order and looks up each row. A new index
 * is an empty table filled a page at a time, so no statement of a build reads every row.
 */
const KEYS_PREFIX = "sharded_table_keys_";
/** A dropped index's key table is renamed to this prefix while it is emptied. */
const DROPPED_PREFIX = "sharded_table_dropped_";
/** The earlier layout: an SQL index on a hidden key column of the table. */
const LEGACY_INDEX_PREFIX = "sharded_table_ix_";
const LEGACY_COLUMN_PREFIX = "_ix_";

/** A declared index: its columns and its key table. */
interface IndexDef {
  readonly name: string;
  readonly columns: readonly string[];
  readonly keys: string;
}
/** Rows per hash prefix are counted with this many hex digits (16 bits: exact to depth 16). */
const BUCKET_DIGITS = 4;
const MAX_TABLE_DEPTH = BUCKET_DIGITS * 4;
const CLEANUP_BATCH = 250;
/** Counts of a dropped group deleted a step: each reads about four rows (the table and its key). */
const COUNT_DROP_BATCH = 120;
/** How long one alarm takes build steps for, at most, before it asks for the next. */
const ALARM_BUILD_MS = 200;

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
  /**
   * The commands `apply` may name (`defineCommands`): conditional updates that run here, in the
   * row's transaction, instead of a read and a conditional put from the client.
   */
  protected commands: Commands<Row> = {};
  /**
   * Rows whose key a new index is given per call, while it is built over rows that were there
   * before it. A shard with more answers "not ready" and goes on with the next call, so building
   * an index over millions of rows never needs more CPU than one call has.
   */
  protected indexBuildRows = 10_000;
  private migrated = false;
  /** SQLite's index on the primary key, for listings in key order. */
  private keyIndex = "";

  // ---- Called by the client ----

  async tableGet(keys: readonly string[], _payloads: null): Promise<(Versioned<Row> | null)[]> {
    this.ready();
    this.assertOwns(keys);
    return keys.map((key) => this.getRow(key));
  }

  async tablePut(keys: readonly string[], payloads: readonly { readonly row: Row; readonly ifVersion?: number; readonly opId?: string }[]): Promise<WriteResult[]> {
    this.ready();
    return this.writeBatch(keys, payloads, () => payloads.map(({ row, ifVersion, opId }, i) => this.once(opId, "put", keys[i]!, { row, ifVersion }, () => this.putRow(this.checkKey(keys[i]!, row), ifVersion === undefined ? {} : { ifVersion }))));
  }

  async tablePatch(keys: readonly string[], payloads: readonly { readonly changes: Partial<Row>; readonly ifVersion?: number; readonly opId?: string }[]): Promise<WriteResult[]> {
    this.ready();
    return this.writeBatch(keys, payloads, () => payloads.map(({ changes, ifVersion, opId }, i) => this.once(opId, "patch", keys[i]!, { changes, ifVersion }, () => this.patchRow(keys[i]!, changes, ifVersion === undefined ? {} : { ifVersion }))));
  }

  async tableDelete(keys: readonly string[], payloads: readonly { readonly ifVersion?: number; readonly opId?: string }[] | null): Promise<WriteResult[]> {
    this.ready();
    return this.writeBatch(keys, payloads, () => keys.map((key, i) => {
      const ifVersion = payloads?.[i]?.ifVersion;
      return this.once(payloads?.[i]?.opId, "delete", key, { ifVersion }, () => this.deleteRow(key, ifVersion === undefined ? {} : { ifVersion }));
    }));
  }

  /**
   * `write`, once per opId: in the write's transaction, its result is kept under the opId, and
   * the same request sent again (its answer was lost) answers that result without writing. The
   * opId sent with another request throws OperationMismatch. Kept with the row's hash, so a split
   * hands it to the shard that owns the key, and for OP_ID_TTL_MS.
   */
  private once(opId: string | undefined, kind: string, key: string, request: object, write: () => WriteResult): WriteResult {
    if (opId === undefined) return write();
    if (typeof opId !== "string" || opId.length === 0 || opId.length > 200) throw new Error("opId must be a string of 1 to 200 characters");
    const asked = JSON.stringify([kind, key, request]);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${OPS} (op_id TEXT PRIMARY KEY NOT NULL, _hash TEXT NOT NULL, request TEXT NOT NULL, result TEXT NOT NULL, at INTEGER NOT NULL)`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS ${OPS}_at ON ${OPS} (at)`);
    const now = Date.now();
    const kept = this.db.exec<{ request: string; result: string }>(`SELECT request, result FROM ${OPS} WHERE op_id = ? AND at > ?`, opId, now - OP_ID_TTL_MS).toArray()[0];
    if (kept) {
      if (kept.request !== asked) throw new OperationMismatch(opId);
      return JSON.parse(kept.result) as WriteResult;
    }
    const result = write();
    // Refused before writing (an index entry not reserved): the caller reserves and sends it again.
    if (result.refused !== undefined) return result;
    this.db.exec(`DELETE FROM ${OPS} WHERE rowid IN (SELECT rowid FROM ${OPS} WHERE at <= ? ORDER BY at LIMIT ?)`, now - OP_ID_TTL_MS, OP_PURGE_BATCH);
    this.db.exec(`INSERT INTO ${OPS} (op_id, _hash, request, result, at) VALUES (?, ?, ?, ?, ?) ON CONFLICT (op_id) DO UPDATE SET _hash = excluded._hash, request = excluded.request, result = excluded.result, at = excluded.at`, opId, this.routeHash(key), asked, JSON.stringify(result), now);
    return result;
  }

  /**
   * Runs a command on each key's row, all in one transaction: read the row, decide, write. One
   * call, no version retries. "moved" and "not ready" are answered before the transaction, so a
   * call the client routes again ran nowhere.
   */
  async tableApply(keys: readonly string[], payloads: readonly ApplyRequest[]): Promise<ApplyOutcome<unknown>[]> {
    this.ready();
    // An IndexedTable reserves a write's index entries before the write: a command can't.
    if (this.keepsGlobalIndexes()) throw new Error("apply is not supported on an IndexedTable: its writes reserve their index entries first; use put");
    const commands = payloads.map(({ command }) => {
      const fn = Object.hasOwn(this.commands, command) ? this.commands[command] : undefined;
      if (typeof fn !== "function") throw new Error(`no command ${JSON.stringify(command)}`);
      return fn;
    });
    return this.transact(keys, () => keys.map((key, i) => this.applyOne(key, payloads[i]!, commands[i]!)));
  }

  private applyOne(key: string, request: ApplyRequest, command: Commands<Row>[string]): ApplyOutcome<unknown> {
    const current = this.getRow(key);
    const decision = command(current, request.args) as Decision<Row, unknown> | PromiseLike<unknown>;
    if (decision === null || typeof decision !== "object" || "then" in decision) throw new Error(`command ${request.command} must return { result, write? } synchronously`);
    const write = decision.write ?? null;
    if (write === null) return { ok: true, result: decision.result, version: current?.version ?? 0 };
    const ifVersion = current?.version ?? 0;
    const written = write === "delete" ? this.deleteRow(key, { ifVersion }) : this.putRow(this.checkKey(key, write), { ifVersion });
    if (written.refused !== undefined) return { ok: false, conflict: "unique", constraint: written.refused, version: written.version };
    // The row was read in this transaction: its version can't have moved.
    if (!written.ok) throw new Error(`apply ${request.command} on ${key}: the row changed inside its own transaction`);
    return { ok: true, result: decision.result, version: written.version };
  }

  // ---- Hooks for layers above (IndexedTable) ----

  /**
   * Runs one client write call (`tablePut`, `tablePatch`, `tableDelete`): `write` in one
   * transaction over `keys`. A layer above wraps it (to read what the payloads carry, and to act
   * after the commit). The default is `transact(keys, write)`.
   */
  protected writeBatch(keys: readonly string[], _payloads: readonly unknown[] | null, write: () => WriteResult[]): Promise<WriteResult[]> {
    return this.transact(keys, write);
  }

  /** Whether `putRow`, `patchRow` and `deleteRow` call `refuseRowWrite` and `rowWritten`. */
  protected watchesRows(): boolean {
    return false;
  }

  /**
   * Before a write that passed its version check: a reason to refuse it (nothing is written, and the
   * result is `{ ok: false, version, refused }`), or null. `before` null: the row is new; `after`
   * null: it is deleted. Inside the write's transaction. Only if `watchesRows()`.
   */
  protected refuseRowWrite(_key: string, _before: Row | null, _after: Row | null): string | null {
    return null;
  }

  /** After a write, in its transaction: the row was `before` and is now `after` at `version`. Only if `watchesRows()`. */
  protected rowWritten(_key: string, _before: Row | null, _after: Row | null, _version: number): void {}

  /** Whether this class keeps the schema's `globalIndexes` (IndexedTable does). */
  protected keepsGlobalIndexes(): boolean {
    return false;
  }

  /** One shard's part of a listing page. */
  async tablePage(request: ShardPageRequest): Promise<ShardPage> {
    this.ready();
    this.assertOwns([]);
    const checked = pageRequest({ ...(request.index == null ? {} : { index: request.index }), direction: request.direction, where: request.where, limit: request.limit, budget: request.budget }, request.after);
    const counts = typeof request.withCount === "string" ? this.countBy(request.withCount) : undefined;
    const page = this.page(checked);
    if (!request.withCount) return page;
    return { ...page, total: this.count(), ...(counts ? { counts } : {}) };
  }

  /** Rows this shard owns. */
  async tableCount(): Promise<number> {
    this.ready();
    this.assertOwns([]);
    return this.count();
  }

  /** Rows this shard owns, per value of a count group's column (values with no rows left out). */
  async tableCountBy(group: string): Promise<GroupCount[]> {
    this.ready();
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
    const stored: Record<string, SqlValue> = {};
    for (const name of Object.keys(this.table.columns)) stored[name] = this.encode(name, row[name]);
    this.assertFits(key, stored);
    const watched = this.watchesRows() ? { before: current === 0 ? null : this.getRow(key)!.row, after: this.decode(stored) } : null;
    if (watched) {
      const refused = this.refuseRowWrite(key, watched.before, watched.after);
      if (refused !== null) return { ok: false, version: current, refused };
    }
    const before = current === 0 ? null : this.storedRow(key);
    const names = Object.keys(stored);
    const version = (current === 0 ? this.deletedVersion() : current) + 1;
    this.db.exec(
      `INSERT INTO ${TABLE} (${names.map((n) => this.q(n)).join(", ")}, _hash, _version) VALUES (${names.map(() => "?").join(", ")}, ?, ?) ` +
        `ON CONFLICT (${this.q(this.table.primaryKey)}) DO UPDATE SET ${names.map((n) => `${this.q(n)} = excluded.${this.q(n)}`).join(", ")}, _version = excluded._version`,
      ...names.map((n) => stored[n]!),
      this.routeHash(key),
      version,
    );
    if (current === 0) this.countRow(key, 1);
    this.countGroups(key, before, stored);
    this.updateKeys(key, before, stored);
    this.markDirty(key);
    if (watched) this.rowWritten(key, watched.before, watched.after, version);
    return { ok: true, version };
  }

  /** Changes some columns of an existing row if its version is `ifVersion`. Call inside `transact`. */
  /** Throws RowTooLarge unless a row's stored values fit the table's maxRowBytes. */
  private assertFits(key: string, stored: Readonly<Record<string, SqlValue>>): void {
    let bytes = 0;
    for (const name of Object.keys(this.table.columns)) {
      const v = stored[name];
      bytes += typeof v === "string" ? UTF8.encode(v).length : v === null || v === undefined ? 0 : 8;
    }
    const max = this.table.maxRowBytes ?? DEFAULT_MAX_ROW_BYTES;
    if (bytes > max) throw new RowTooLarge(key, bytes, max);
  }

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
    this.assertFits(key, stored);
    const watched = this.watchesRows() ? { before: this.decode(before), after: this.decode(stored) } : null;
    if (watched) {
      const refused = this.refuseRowWrite(key, watched.before, watched.after);
      if (refused !== null) return { ok: false, version: current, refused };
    }
    const names = Object.keys(updates);
    const version = current + 1;
    this.db.exec(
      `UPDATE ${TABLE} SET ${[...names.map((n) => `${this.q(n)} = ?`), "_version = ?"].join(", ")} WHERE ${this.q(this.table.primaryKey)} = ?`,
      ...names.map((n) => updates[n]!),
      version,
      key,
    );
    this.countGroups(key, before, stored);
    this.updateKeys(key, before, stored);
    this.markDirty(key);
    if (watched) this.rowWritten(key, watched.before, watched.after, version);
    return { ok: true, version };
  }

  /** Deletes a row if its version is `ifVersion`. Call inside `transact`. */
  protected deleteRow(key: string, options: PutOptions = {}): WriteResult {
    this.migrate();
    const current = this.version(key);
    if (options.ifVersion !== undefined && options.ifVersion !== current) return { ok: false, version: current };
    if (current === 0) return { ok: true, version: 0 };
    const watched = this.watchesRows() ? { before: this.getRow(key)!.row } : null;
    if (watched) {
      const refused = this.refuseRowWrite(key, watched.before, null);
      if (refused !== null) return { ok: false, version: current, refused };
    }
    const before = this.storedRow(key);
    this.db.exec(`DELETE FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key);
    this.countRow(key, -1);
    this.countGroups(key, before, null);
    this.updateKeys(key, before, null);
    this.markDirty(key);
    // Watched, the delete is a write of its own, at one past the row's last version: a row written
    // after it starts above that, so the two never share a version.
    const deletedAt = watched ? current + 1 : current;
    this.db.exec(`INSERT INTO ${VERSIONS} (name, version) VALUES ('deleted', ?) ON CONFLICT (name) DO UPDATE SET version = max(version, excluded.version)`, deletedAt);
    this.deletedAt = Math.max(this.deletedVersion(), deletedAt);
    if (watched) this.rowWritten(key, watched.before, null, deletedAt);
    return { ok: true, version: 0 };
  }

  /** Up to `limit` of the rows this shard owns, in key order, after `afterKey` (null: from the first). */
  protected ownedRowsAfter(afterKey: string | null, limit: number): Versioned<Row>[] {
    this.migrate();
    const [lo, hi] = this.ownedHashRange();
    const pk = this.q(this.table.primaryKey);
    // In the key's index, seeking to `afterKey`: by the hash's, SQLite would sort the whole range.
    const from = afterKey === null ? "" : `${pk} > ? AND `;
    return this.db
      .exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} INDEXED BY "${this.keyIndex}" WHERE ${from}_hash >= ? AND _hash <= ? ORDER BY ${pk} LIMIT ?`, ...(afterKey === null ? [] : [afterKey]), lo, hi, limit)
      .toArray()
      .map((found) => ({ row: this.decode(found), version: found._version as number }));
  }

  /** The highest version a deleted row of this shard had (0: none). */
  private deletedVersion(): number {
    // Read once an instance: only deleteRow raises it, and keeps this in step.
    this.deletedAt ??= this.db.exec<{ version: number }>(`SELECT version FROM ${VERSIONS} WHERE name = 'deleted'`).toArray()[0]?.version ?? 0;
    return this.deletedAt;
  }
  private deletedAt: number | null = null;

  // ---- Splits ----

  protected override forgetStorage(): void {
    this.migrated = false;
    this.keyIndex = "";
    this.buildAlarm = "none";
    this.deletedAt = null;
    this.indexDefs = null;
    super.forgetStorage();
  }

  /** Deletes rows (and counts) of hashes this shard gave away, a batch at a time. */
  protected override cleanupForeign(owns: (key: string) => boolean, limit: number): number {
    this.migrate();
    const [lo, hi] = this.ownedHashRange();
    const pk = this.q(this.table.primaryKey);
    // A batch of rows given away, with their keys in every key table (by the table's index on _pk).
    const gone = this.db.exec<{ k: string }>(`SELECT ${pk} AS k FROM ${TABLE} WHERE _hash < ? OR _hash > ? LIMIT ?`, lo, hi, Math.min(limit, CLEANUP_BATCH)).toArray().map((r) => r.k);
    let deleted = 0;
    const keyTables = gone.length > 0 ? this.keyTables() : [];
    // A statement binds at most 100 values.
    for (let i = 0; i < gone.length; i += 100) {
      const part = gone.slice(i, i + 100);
      const marks = part.map(() => "?").join(", ");
      for (const keys of keyTables) this.db.exec(`DELETE FROM "${keys}" WHERE _pk IN (${marks})`, ...part);
      deleted += this.db.exec(`DELETE FROM ${TABLE} WHERE ${pk} IN (${marks})`, ...part).rowsWritten;
    }
    if (deleted === 0) {
      const [blo, bhi] = [lo.slice(0, BUCKET_DIGITS), hi.slice(0, BUCKET_DIGITS)];
      const batch = Math.min(limit, CLEANUP_BATCH);
      deleted = this.db.exec(`DELETE FROM ${COUNTS} WHERE bucket IN (SELECT bucket FROM ${COUNTS} WHERE bucket < ? OR bucket > ? LIMIT ?)`, blo, bhi, batch).rowsWritten;
      if (this.db.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE name = ?", OPS).one().n > 0) deleted += this.db.exec(`DELETE FROM ${OPS} WHERE _hash < ? OR _hash > ?`, lo, hi).rowsWritten;
      // A batch at a time: a group of many values has a row per value and bucket.
      if (deleted === 0) deleted = this.db.exec(`DELETE FROM ${GROUP_COUNTS} WHERE (grp, value, bucket) IN (SELECT grp, value, bucket FROM ${GROUP_COUNTS} WHERE bucket < ? OR bucket > ? LIMIT ?)`, blo, bhi, batch).rowsWritten;
      if (deleted === 0 && this.db.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE name = ?", DIRTY).one().n > 0) deleted = this.db.exec(`DELETE FROM ${DIRTY} WHERE key IN (SELECT key FROM ${DIRTY} WHERE hash < ? OR hash > ? LIMIT ?)`, lo, hi, batch).rowsWritten;
    }
    return deleted + super.cleanupForeign(owns, limit);
  }

  // ---- Listing ----

  private page(request: ShardPageRequest): ShardPage {
    const pk = this.table.primaryKey;
    const ix = request.index === null ? null : this.indexes().find((i) => i.name === request.index);
    if (request.index !== null && !ix) throw new Error(`no index ${request.index}`);
    // An index being built, or with rows whose keys may be stale, lists nothing yet: the alarm
    // and the calls meanwhile build it, a step each.
    if (ix && (this.building.has(ix.keys) || this.dirtyRows)) this.notReady();
    const [lo, hi] = this.ownedHashRange();
    const params: SqlValue[] = [];
    const matches = request.where.length ? request.where.map((c) => this.condition(c, params)).join(" AND ") : "1";
    const order = request.direction === "asc" ? "ASC" : "DESC";
    // A row's position: its index key, or its primary key. SQLite seeks to the cursor in that
    // key's order and reads on: by an index, in its key table, looking up each row by its key.
    const sql = ix
      ? `SELECT t.*, (${matches}) AS _m, k._key AS _pos FROM "${ix.keys}" AS k CROSS JOIN ${TABLE} AS t ON t.${this.q(pk)} = k._pk ` +
        `WHERE k._key ${request.after === null ? "> ''" : request.direction === "asc" ? "> ?" : "< ?"} AND k._hash >= ? AND k._hash <= ? ORDER BY k._key ${order} LIMIT ?`
      : `SELECT *, (${matches}) AS _m, ${this.q(pk)} AS _pos FROM ${TABLE} INDEXED BY "${this.keyIndex}" ` +
        `WHERE ${request.after === null ? "" : `${this.q(pk)} ${request.direction === "asc" ? ">" : "<"} ? AND `}_hash >= ? AND _hash <= ? ORDER BY ${this.q(pk)} ${order} LIMIT ?`;
    const rows: { key: string; row: RowShape; version: number; position: string }[] = [];
    let last: string | null = null;
    let scanned = 0;
    for (const found of this.db.exec<Record<string, SqlValue>>(sql, ...params, ...(request.after === null ? [] : [request.after]), lo, hi, request.budget)) {
      scanned++;
      last = found._pos as string;
      if (found._m) {
        rows.push({ key: found[pk] as string, row: this.decode(found), version: found._version as number, position: last });
        if (rows.length === request.limit) return { rows, frontier: last };
      }
    }
    // Out of budget: reported everything up to the last row scanned. Out of rows: the end.
    return { rows, frontier: scanned === request.budget ? last : null };
  }

  private indexes(): IndexDef[] {
    // Per schema object (a subclass may swap its schema): every write and page asks.
    const schema = this.table;
    if (this.indexDefs?.schema !== schema) {
      const defs = Object.entries(schema.indexes ?? {}).map(([name, columns]) => {
        const id = `${name}_${shortDigest(columns.join(","))}`;
        return { name, columns, keys: `${KEYS_PREFIX}${id}` };
      });
      this.indexDefs = { schema, defs };
    }
    return this.indexDefs.defs;
  }
  private indexDefs: { schema: unknown; defs: IndexDef[] } | null = null;

  /**
   * An index's key table, empty, with its index on the row's key (for refreshes and cleanup). The
   * index's name is unique to this table, so a dropped table renamed away keeps its own.
   */
  private createKeyTable(ix: IndexDef): void {
    this.db.exec(`CREATE TABLE "${ix.keys}" (_key TEXT PRIMARY KEY NOT NULL, _pk TEXT NOT NULL, _hash TEXT NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE INDEX "${ix.keys}_pk_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}" ON "${ix.keys}" (_pk)`);
  }

  /** The key tables in storage: the schema's, and those a newer schema kept (a rollback's). */
  private keyTables(): string[] {
    return this.db.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'table' AND substr(name, 1, ?) = ?`, KEYS_PREFIX.length, KEYS_PREFIX).toArray().map((r) => r.name);
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

  /**
   * Brings this shard's storage up to the schema, once per instance, on its first use: cheap
   * statements only (tables, columns, and what is left to build). What is proportional to the
   * rows (filling a new index's keys, counting a new group, refreshing rows a rollback wrote) is
   * done by `buildStep`, a bounded step at a time, by calls and by the alarm.
   */
  protected migrate(): void {
    if (this.migrated) return;
    // Rows are counted per 16-bit hash prefix: a deeper shard would count its sibling's rows too.
    if (this.splitPolicy.maxDepth > MAX_TABLE_DEPTH) throw new Error(`a sharded table splits at most ${MAX_TABLE_DEPTH} deep: lower splitPolicy.maxDepth`);
    const schema = this.table;
    if (schema.globalIndexes && Object.keys(schema.globalIndexes).length > 0 && !this.keepsGlobalIndexes()) throw new Error("the table declares globalIndexes: extend IndexedTable, not ShardedTable");
    // Woken with the schema it migrated to, and nothing left to build: one statement.
    const ready = this.readyState();
    if (ready && ready.schema === this.schemaFingerprint()) {
      this.keyIndex = ready.keyIndex;
      this.rollback = ready.rollback;
      this.unknownLocal = ready.unknownLocal;
      this.building = new Set();
      this.counting = new Map();
      this.countDrops = this.keyClears = this.keyDrops = this.dirtyRows = false;
      this.migrated = true;
      return;
    }
    this.migrateAll();
    this.saveReady();
  }

  /** The schema as migrate() applies it: any change to it, or to what migrate() does, differs. */
  private schemaFingerprint(): string {
    return JSON.stringify([MIGRATION_LAYOUT, this.table]);
  }

  private readyState(): ReadyState | null {
    try {
      const row = this.db.exec<{ state: string }>(`SELECT state FROM ${READY}`).toArray()[0];
      return row ? (JSON.parse(row.state) as ReadyState) : null;
    } catch {
      return null; // no such table: a shard of an earlier release, or new
    }
  }

  /** Records that the migration is done, once nothing is left to build (or that it is not). */
  private saveReady(): void {
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${READY} (state TEXT NOT NULL)`);
    this.db.exec(`DELETE FROM ${READY}`);
    if (this.buildWorkLeft()) return;
    const state: ReadyState = { schema: this.schemaFingerprint(), keyIndex: this.keyIndex, rollback: this.rollback, unknownLocal: this.unknownLocal };
    this.db.exec(`INSERT INTO ${READY} (state) VALUES (?)`, JSON.stringify(state));
  }

  private migrateAll(): void {
    const schema = this.table;
    const pk = this.q(schema.primaryKey);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${TABLE} (${pk} TEXT PRIMARY KEY NOT NULL, _hash TEXT NOT NULL, _version INTEGER NOT NULL)`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNTS} (bucket TEXT PRIMARY KEY NOT NULL, n INTEGER NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${VERSIONS} (name TEXT PRIMARY KEY NOT NULL, version INTEGER NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${GROUP_COUNTS} (grp TEXT NOT NULL, value TEXT NOT NULL, bucket TEXT NOT NULL, n INTEGER NOT NULL, PRIMARY KEY (grp, value, bucket)) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNT_GROUPS} (grp TEXT PRIMARY KEY NOT NULL, col TEXT NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNT_BUILDS} (grp TEXT PRIMARY KEY NOT NULL, col TEXT NOT NULL, after TEXT) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${COUNT_DROPS} (grp TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${BUILDS} (ix TEXT PRIMARY KEY NOT NULL, after TEXT) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${DIRTY} (key TEXT PRIMARY KEY NOT NULL, hash TEXT NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${KEY_CLEARS} (col TEXT PRIMARY KEY NOT NULL, after TEXT, done INTEGER NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE TABLE IF NOT EXISTS ${KEY_DROPS} (name TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID`);
    this.db.exec(`CREATE INDEX IF NOT EXISTS ${TABLE}_hash ON ${TABLE} (_hash)`);
    const existing = this.db.exec<{ name: string; pk: number }>(`PRAGMA table_info(${TABLE})`).toArray();
    const key = existing.find((c) => c.pk === 1)?.name;
    if (key !== schema.primaryKey) throw new Error(`the table's primary key is ${key}, not ${schema.primaryKey}: it cannot change`);
    const have = new Set(existing.map((c) => c.name));
    for (const [name, type] of Object.entries(schema.columns) as [string, ColumnType][]) {
      if (!have.has(name)) this.db.exec(`ALTER TABLE ${TABLE} ADD COLUMN ${this.q(name)} ${AFFINITY[type]}`);
    }
    // Older code than the newest schema this shard ran (a rollback, or a deploy halfway) keeps
    // what it doesn't know, and marks the rows it writes for the newer code to refresh.
    const mine = schema.version ?? 0;
    const recorded = this.db.exec<{ version: number }>(`SELECT version FROM ${VERSIONS} WHERE name = 'schema'`).toArray()[0]?.version ?? 0;
    this.rollback = mine < recorded;
    if (mine > recorded) this.db.exec(`INSERT INTO ${VERSIONS} (name, version) VALUES ('schema', ?) ON CONFLICT (name) DO UPDATE SET version = excluded.version`, mine);
    const wanted = this.indexes();
    // The earlier layout's SQL indexes go (they are not kept from here on), and their columns are
    // cleared a page at a time. Ours by exact prefix (LIKE would match any character for "_").
    const legacy = this.db.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND substr(name, 1, ?) = ?`, TABLE, LEGACY_INDEX_PREFIX.length, LEGACY_INDEX_PREFIX).toArray().map((r) => r.name);
    for (const name of legacy) this.db.exec(`DROP INDEX IF EXISTS "${name}"`);
    for (const column of have) if (column.startsWith(LEGACY_COLUMN_PREFIX)) this.db.exec(`INSERT OR IGNORE INTO ${KEY_CLEARS} (col, after, done) VALUES (?, NULL, 0)`, column);
    const tables = this.keyTables();
    let unknown = false;
    for (const name of tables) {
      if (wanted.some((ix) => ix.keys === name)) continue;
      // A rollback's code keeps what it doesn't know, for the newer code.
      if (this.rollback) unknown = true;
      // Dropped: renamed out of the way (a rename is cheap), and emptied a batch at a time.
      else {
        const dropped = `${DROPPED_PREFIX}${name.slice(KEYS_PREFIX.length)}_${Date.now().toString(36)}`;
        this.ctx.storage.transactionSync(() => {
          this.db.exec(`ALTER TABLE "${name}" RENAME TO "${dropped}"`);
          this.db.exec(`INSERT INTO ${KEY_DROPS} (name) VALUES (?)`, dropped);
        });
      }
    }
    const builds = new Map(this.db.exec<{ ix: string; after: string | null }>(`SELECT ix, after FROM ${BUILDS}`).toArray().map((r) => [r.ix, r.after]));
    for (const [ix] of builds) {
      if (wanted.some((w) => w.keys === ix)) continue;
      if (this.rollback && ix.startsWith(KEYS_PREFIX)) unknown = true;
      else {
        this.db.exec(`DELETE FROM ${BUILDS} WHERE ix = ?`, ix);
        builds.delete(ix);
      }
    }
    // No rows yet: a new index is built in the statement that makes it.
    const empty = this.db.exec(`SELECT 1 FROM ${TABLE} LIMIT 1`).toArray().length === 0;
    for (const ix of wanted) {
      if (tables.includes(ix.keys)) continue;
      // New, or declared again: an empty key table, and on a table with rows the build's record,
      // in one transaction, so a failure between them leaves neither.
      this.ctx.storage.transactionSync(() => {
        this.createKeyTable(ix);
        if (!empty) this.db.exec(`INSERT OR REPLACE INTO ${BUILDS} (ix, after) VALUES (?, NULL)`, ix.keys);
      });
      if (!empty) builds.set(ix.keys, null);
    }
    this.building = new Set([...builds.keys()].filter((ix) => wanted.some((w) => w.keys === ix)));
    this.keyClears = this.db.exec(`SELECT 1 FROM ${KEY_CLEARS} WHERE done = 0 LIMIT 1`).toArray().length > 0;
    this.keyDrops = this.db.exec(`SELECT 1 FROM ${KEY_DROPS} LIMIT 1`).toArray().length > 0;
    this.migrateCountGroups(empty);
    this.unknownLocal = unknown;
    this.dirtyRows = !this.rollback && this.db.exec(`SELECT 1 FROM ${DIRTY} LIMIT 1`).toArray().length > 0;
    this.keyIndex = this.db.exec<{ name: string }>(`SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ? AND name LIKE 'sqlite_autoindex_%'`, TABLE).one().name;
    this.migrated = true;
    if (this.buildWorkLeft()) this.askBuildAlarm();
  }

  /**
   * Asks for the alarm to take build steps. If asking fails (a storage error), the next call asks
   * again: without the alarm, a build goes on only a step a call, as slow as the calls are.
   */
  private askBuildAlarm(): void {
    if (this.buildAlarm !== "none") return;
    this.buildAlarm = "asking";
    this.scheduleAlarm(Date.now()).then(
      () => void (this.buildAlarm = "asked"),
      () => void (this.buildAlarm = "none"),
    );
  }
  private buildAlarm: "none" | "asking" | "asked" = "none";
  /** Local indexes (by key column) whose keys are being filled in. */
  private building = new Set<string>();
  /** Count groups being counted, and the last key counted (null: none yet). */
  private counting = new Map<string, string | null>();
  /** Count groups whose counts are being deleted. */
  private countDrops = false;
  /** Key columns of the earlier layout are being cleared. */
  private keyClears = false;
  /** Key tables of dropped indexes are being emptied. */
  private keyDrops = false;
  /** Rows marked by older code are left to refresh. */
  private dirtyRows = false;
  /** This code is older than the newest schema the shard ran. */
  private rollback = false;
  /** ...and that schema has local indexes this code doesn't know. */
  private unknownLocal = false;
  /** Rows this code writes are marked: it is older than the schema, which has indexes it doesn't know. */
  private get keepUnknown(): boolean {
    return this.rollback && (this.unknownLocal || this.keepsUnknownSchema());
  }

  /**
   * Whether this code runs older than the newest schema its shard ran (see TableSchema.version).
   * Valid once migrated.
   */
  protected schemaRollback(): boolean {
    return this.rollback;
  }

  /** A layer above: whether it keeps schema this code doesn't know (IndexedTable: global indexes). Called by migrate. */
  protected keepsUnknownSchema(): boolean {
    return false;
  }

  /** A write's row, marked for newer code to refresh, while this code keeps what it doesn't know. */
  private markDirty(key: string): void {
    if (this.keepUnknown) this.db.exec(`INSERT OR IGNORE INTO ${DIRTY} (key, hash) VALUES (?, ?)`, key, this.routeHash(key));
  }

  /** Whether rows older code wrote are left to refresh (their index entries may be stale). */
  protected rowsToRefresh(): boolean {
    return this.dirtyRows;
  }

  /** Whether any build step is left. */
  private buildWorkLeft(): boolean {
    return this.building.size > 0 || this.counting.size > 0 || this.countDrops || this.keyClears || this.keyDrops || this.dirtyRows;
  }

  /** Build steps, from the alarm: a few, then the alarm again if more is left. */
  protected override async layerAlarm(): Promise<void> {
    await super.layerAlarm();
    this.migrate();
    const until = Date.now() + ALARM_BUILD_MS;
    this.buildAlarm = "none";
    let more = this.buildWorkLeft();
    while (more && Date.now() < until) more = this.buildStep();
    if (more) {
      await this.scheduleAlarm(Date.now());
      this.buildAlarm = "asked";
    }
  }

  /** The migration, and a build step if one is left: what every call does first. */
  protected ready(): void {
    // The call that migrates does no step too: either is about as much as a call should do.
    if (!this.migrated) return this.migrate();
    if (!this.buildWorkLeft()) return;
    this.askBuildAlarm();
    // The call doesn't need the step (what it needs answers not ready until built): a step that
    // fails is rolled back, and taken again by the next call or the alarm.
    try {
      this.buildStep();
    } catch (error) {
      console.error(JSON.stringify({ event: "table-build-step-failed", node: this.nodeLabel(), error: String(error).slice(0, 200) }));
      this.buildAlarm = "none";
      void this.scheduleAlarm(Date.now() + 1_000).catch(() => {});
    }
  }

  /**
   * One step of what the migration left to do, in one transaction, at most `indexBuildRows` rows:
   * deleting the counts of a dropped group, refreshing rows older code wrote, filling in a new
   * index's keys, or counting a new group. True if more is left. Calls and the alarm take them.
   */
  protected buildStep(): boolean {
    this.migrate();
    if (!this.buildWorkLeft()) return false;
    const more = this.oneStep();
    // The last step: a shard that wakes from now on reads the ready record, and nothing else.
    if (!more) this.saveReady();
    return more;
  }

  private oneStep(): boolean {
    const budget = this.indexBuildRows;
    const pk = this.q(this.table.primaryKey);
    // The step's rows, in key order after `after`: two statements, so SQLite seeks in the key's index.
    const pageAfter = (columns: string, after: string | null, limit: number) =>
      after === null
        ? this.db.exec<Record<string, SqlValue>>(`SELECT ${columns} FROM ${TABLE} ORDER BY ${pk} LIMIT ?`, limit).toArray()
        : this.db.exec<Record<string, SqlValue>>(`SELECT ${columns} FROM ${TABLE} WHERE ${pk} > ? ORDER BY ${pk} LIMIT ?`, after, limit).toArray();
    if (this.countDrops) {
      this.ctx.storage.transactionSync(() => {
        const grp = this.db.exec<{ grp: string }>(`SELECT grp FROM ${COUNT_DROPS} LIMIT 1`).toArray()[0]?.grp;
        if (grp === undefined) return void (this.countDrops = false);
        const deleted = this.db.exec(`DELETE FROM ${GROUP_COUNTS} WHERE (grp, value, bucket) IN (SELECT grp, value, bucket FROM ${GROUP_COUNTS} WHERE grp = ? LIMIT ?)`, grp, COUNT_DROP_BATCH).rowsWritten;
        if (deleted === 0) this.db.exec(`DELETE FROM ${COUNT_DROPS} WHERE grp = ?`, grp);
      });
      return this.buildWorkLeft();
    }
    if (this.keyClears) {
      this.ctx.storage.transactionSync(() => {
        const clear = this.db.exec<{ col: string; after: string | null }>(`SELECT col, after FROM ${KEY_CLEARS} WHERE done = 0 LIMIT 1`).toArray()[0];
        if (!clear) return void (this.keyClears = false);
        const page = pageAfter(pk, clear.after, budget).map((r) => r[this.table.primaryKey] as string);
        if (page.length > 0) this.db.exec(`UPDATE ${TABLE} SET "${clear.col}" = NULL WHERE ${pk} >= ? AND ${pk} <= ? AND "${clear.col}" IS NOT NULL`, page[0]!, page.at(-1)!);
        // Kept, done: the column stays (dropping it rewrites every row), and is not cleared again.
        this.db.exec(`UPDATE ${KEY_CLEARS} SET after = ?, done = ? WHERE col = ?`, page.at(-1) ?? clear.after, page.length === budget ? 0 : 1, clear.col);
      });
      return this.buildWorkLeft();
    }
    if (this.keyDrops) {
      this.ctx.storage.transactionSync(() => {
        const name = this.db.exec<{ name: string }>(`SELECT name FROM ${KEY_DROPS} LIMIT 1`).toArray()[0]?.name;
        if (name === undefined) return void (this.keyDrops = false);
        // Each key deleted reads about four rows (the key, its _pk index entry, and the subquery's).
        const deleted = this.db.exec(`DELETE FROM "${name}" WHERE _key IN (SELECT _key FROM "${name}" LIMIT ?)`, Math.max(1, Math.floor(budget / 4))).rowsWritten;
        if (deleted > 0) return;
        // Empty: dropping it costs nothing proportional to what it held.
        this.db.exec(`DROP TABLE IF EXISTS "${name}"`);
        this.db.exec(`DELETE FROM ${KEY_DROPS} WHERE name = ?`, name);
      });
      return this.buildWorkLeft();
    }
    if (this.dirtyRows) {
      this.ctx.storage.transactionSync(() => {
        const keys = this.db.exec<{ key: string }>(`SELECT key FROM ${DIRTY} LIMIT ?`, budget).toArray().map((r) => r.key);
        for (const key of keys) {
          const stored = this.db.exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} WHERE ${pk} = ?`, key).toArray()[0];
          // Its keys, as they are now: whatever older code left under the row goes first.
          for (const ix of this.indexes()) {
            if (this.building.has(ix.keys)) continue;
            this.db.exec(`DELETE FROM "${ix.keys}" WHERE _pk = ?`, key);
            if (stored) this.db.exec(`INSERT OR REPLACE INTO "${ix.keys}" (_key, _pk, _hash) VALUES (?, ?, ?)`, this.indexKey(ix, stored), key, stored._hash!);
          }
          this.refreshRow(key, stored ? { row: this.decode(stored), version: stored._version as number } : null);
          this.db.exec(`DELETE FROM ${DIRTY} WHERE key = ?`, key);
        }
        if (keys.length < budget) this.dirtyRows = false;
      });
      this.afterBuildStep();
      return this.buildWorkLeft();
    }
    const ix = this.indexes().find((i) => this.building.has(i.keys));
    if (ix) {
      this.ctx.storage.transactionSync(() => {
        const after = this.db.exec<{ after: string | null }>(`SELECT after FROM ${BUILDS} WHERE ix = ?`, ix.keys).toArray()[0]?.after ?? null;
        // Each key is two rows written (the key and its _pk index entry): half a step of rows.
        const page = pageAfter("*", after, Math.max(1, Math.floor(budget / 2)));
        for (const stored of page) this.db.exec(`INSERT OR REPLACE INTO "${ix.keys}" (_key, _pk, _hash) VALUES (?, ?, ?)`, this.indexKey(ix, stored), stored[this.table.primaryKey]!, stored._hash!);
        if (page.length === Math.max(1, Math.floor(budget / 2))) return void this.db.exec(`UPDATE ${BUILDS} SET after = ? WHERE ix = ?`, page.at(-1)![this.table.primaryKey]!, ix.keys);
        // Every row has its key: the index serves.
        this.db.exec(`DELETE FROM ${BUILDS} WHERE ix = ?`, ix.keys);
        this.building.delete(ix.keys);
      });
      return this.buildWorkLeft();
    }
    const [grp, after] = [...this.counting][0] ?? [];
    if (grp !== undefined) {
      const col = this.table.counts![grp]!;
      this.ctx.storage.transactionSync(() => {
        const page = pageAfter(`${pk} AS _key, _hash, ${this.q(col)} AS _value`, after ?? null, budget);
        for (const r of page) this.countGroup(grp, r._value ?? null, r._hash as string, 1);
        if (page.length === budget) {
          const last = page.at(-1)!._key as string;
          this.db.exec(`UPDATE ${COUNT_BUILDS} SET after = ? WHERE grp = ?`, last, grp);
          this.counting.set(grp, last);
          return;
        }
        this.db.exec(`INSERT OR REPLACE INTO ${COUNT_GROUPS} (grp, col) VALUES (?, ?)`, grp, col);
        this.db.exec(`DELETE FROM ${COUNT_BUILDS} WHERE grp = ?`, grp);
        this.counting.delete(grp);
      });
      return this.buildWorkLeft();
    }
    return false;
  }

  /**
   * A layer above: a row older code wrote (null: it deleted it), refreshed in a build step's
   * transaction (IndexedTable puts its global entries in its outbox). Synchronous.
   */
  protected refreshRow(_key: string, _row: Versioned<Row> | null): void {}

  /** A layer above: after a step that refreshed rows, outside its transaction (IndexedTable delivers). */
  protected afterBuildStep(): void {}

  /**
   * Keeps the count groups of the schema. A group removed or moved to another column has its
   * counts deleted, a batch a step (a rollback's code drops those it doesn't know too: its writes
   * would not keep them). A new one is counted over the rows stored, a page a step, while writes
   * count their change only for rows the count has passed.
   */
  private migrateCountGroups(empty: boolean): void {
    const wanted = this.table.counts ?? {};
    const kept = this.db.exec<{ grp: string; col: string }>(`SELECT grp, col FROM ${COUNT_GROUPS}`).toArray();
    const counting = this.db.exec<{ grp: string; col: string; after: string | null }>(`SELECT grp, col, after FROM ${COUNT_BUILDS}`).toArray();
    for (const { grp, col } of [...kept, ...counting]) {
      if (wanted[grp] === col) continue;
      this.ctx.storage.transactionSync(() => {
        this.db.exec(`DELETE FROM ${COUNT_GROUPS} WHERE grp = ?`, grp);
        this.db.exec(`DELETE FROM ${COUNT_BUILDS} WHERE grp = ?`, grp);
        this.db.exec(`INSERT OR IGNORE INTO ${COUNT_DROPS} (grp) VALUES (?)`, grp);
      });
    }
    this.counting = new Map();
    this.countDrops = this.db.exec(`SELECT 1 FROM ${COUNT_DROPS} LIMIT 1`).toArray().length > 0;
    for (const [grp, col] of Object.entries(wanted) as [string, string][]) {
      if (kept.some((k) => k.grp === grp && k.col === col)) continue;
      const going = counting.find((c) => c.grp === grp && c.col === col);
      if (!going && empty && !this.countDrops) {
        this.db.exec(`INSERT OR REPLACE INTO ${COUNT_GROUPS} (grp, col) VALUES (?, ?)`, grp, col);
        continue;
      }
      if (!going) this.db.exec(`INSERT OR REPLACE INTO ${COUNT_BUILDS} (grp, col, after) VALUES (?, ?, NULL)`, grp, col);
      this.counting.set(grp, going?.after ?? null);
    }
    this.countDrops = this.db.exec(`SELECT 1 FROM ${COUNT_DROPS} LIMIT 1`).toArray().length > 0;
  }

  /** Rows this shard owns (its migrated storage). */
  private count(): number {
    const [lo, hi] = this.ownedBuckets();
    return this.db.exec<{ n: number | null }>(`SELECT SUM(n) AS n FROM ${COUNTS} WHERE bucket >= ? AND bucket <= ?`, lo, hi).one().n ?? 0;
  }

  private countBy(group: string): GroupCount[] {
    const col = this.table.counts?.[group];
    if (col === undefined) throw new Error(`no count group ${group}`);
    if (this.counting.has(group) || this.countDrops) this.notReady();
    const [lo, hi] = this.ownedBuckets();
    // The first values, one more than reported, so the client can tell it is cut.
    const max = (this.table.maxCountValues ?? DEFAULT_MAX_COUNT_VALUES) + 1;
    // In the key's order (grp, value, bucket): grouped as read, nothing sorted, and it stops after
    // the first values. `+bucket` keeps SQLite on the key (a WITHOUT ROWID table's own order).
    const rows = this.db.exec<{ value: string; n: number }>(`SELECT value, SUM(n) AS n FROM ${GROUP_COUNTS} WHERE grp = ? AND +bucket >= ? AND +bucket <= ? GROUP BY value HAVING SUM(n) <> 0 ORDER BY value LIMIT ?`, group, lo, hi, max).toArray();
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

  /** The stored values of a row that exists, if a count group or an index needs them (else null). */
  private storedRow(key: string): Record<string, SqlValue> | null {
    if (Object.keys(this.table.counts ?? {}).length === 0 && this.indexes().length === 0) return null;
    return this.db.exec<Record<string, SqlValue>>(`SELECT * FROM ${TABLE} WHERE ${this.q(this.table.primaryKey)} = ?`, key).one();
  }

  /**
   * Moves a row's keys in each index whose key changed: from `before` (null: new) to `after`
   * (null: deleted). An index being built is kept too: its build writes the same keys again.
   */
  private updateKeys(key: string, before: Record<string, SqlValue> | null, after: Record<string, SqlValue> | null): void {
    const indexes = this.indexes();
    if (indexes.length === 0) return;
    const hash = this.routeHash(key);
    for (const ix of indexes) {
      const from = before ? this.indexKey(ix, before) : null;
      const to = after ? this.indexKey(ix, after) : null;
      if (from === to) continue;
      if (from !== null) this.db.exec(`DELETE FROM "${ix.keys}" WHERE _key = ?`, from);
      if (to !== null) this.db.exec(`INSERT OR REPLACE INTO "${ix.keys}" (_key, _pk, _hash) VALUES (?, ?, ?)`, to, key, hash);
    }
  }

  /** Moves a row between the values of each count group: from `before` (null: new) to `after` (null: deleted). */
  private countGroups(key: string, before: Record<string, SqlValue> | null, after: Record<string, SqlValue> | null): void {
    const groups = Object.entries(this.table.counts ?? {}) as [string, string][];
    if (groups.length === 0) return;
    const hash = this.routeHash(key);
    for (const [grp, col] of groups) {
      // Being counted: a row past the build's cursor is counted when the build gets there.
      if (this.counting.has(grp)) {
        const after = this.counting.get(grp)!;
        if (after === null || key > after) continue;
      }
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
    this.db.exec(`INSERT INTO ${COUNTS} (bucket, n) VALUES (?, ?) ON CONFLICT (bucket) DO UPDATE SET n = n + excluded.n`, this.routeHash(key).slice(0, BUCKET_DIGITS), delta);
  }

  private checkKey(key: string, row: Row): Row {
    if (row[this.table.primaryKey] !== key) throw new Error(`row key ${String(row[this.table.primaryKey])} is not ${key}`);
    return row;
  }

  private encode(column: string, value: unknown): SqlValue {
    return encodeValue(this.table, column, value);
  }

  protected decode(stored: Record<string, SqlValue>): Row {
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

