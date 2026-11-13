// The schema of a sharded table, the queries a listing takes, and the sort order shared by the
// shards (SQLite) and the client that merges their pages (here): they must agree exactly.
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";

/** How a column is stored: `boolean` as 0/1, `json` as text (not indexable). */
export type ColumnType = "text" | "integer" | "real" | "boolean" | "json";

/** A row: column names to values. */
export type RowShape = Record<string, unknown>;

export interface TableSchema<Row extends RowShape> {
  /** A `text` column: the row's key, which routes it to its shard. */
  readonly primaryKey: keyof Row & string;
  readonly columns: { readonly [K in keyof Row & string]: ColumnType };
  /** Index name to the columns it sorts by. The primary key is appended to every index. */
  readonly indexes?: Readonly<Record<string, readonly (keyof Row & string)[]>>;
  /**
   * Count group name to a column: rows are counted per value of that column, kept with every
   * write, so `countBy` (and a listing's `withCount`) is exact without reading rows.
   */
  readonly counts?: Readonly<Record<string, keyof Row & string>>;
  /**
   * IndexedTable only: index name to a global index, kept apart from the rows on its own shards
   * (see RFD0003). A write returns once its entries are written.
   */
  readonly globalIndexes?: Readonly<Record<string, GlobalIndexDef<Row>>>;
}

/** A global index of an IndexedTable. */
export interface GlobalIndexDef<Row extends RowShape> {
  /** The columns it sorts by. The primary key is appended unless the index is unique. */
  readonly columns: readonly (keyof Row & string)[];
  /** At most one row per value of `columns`. Rows with a null in them are not indexed. */
  readonly unique?: boolean;
  /**
   * How many leading columns pick the index shard: entries with the same values of those live on
   * one shard, sorted by the rest. A unique index is partitioned by all its columns (the default).
   */
  readonly partition?: number;
}

/** Rows with one value of a count group's column. */
export interface GroupCount {
  readonly value: unknown;
  readonly count: number;
}

const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

/** Checks a schema and returns it: names are plain identifiers, the key is text, indexes name columns. */
export function defineTable<Row extends RowShape>(schema: TableSchema<Row>): TableSchema<Row> {
  const columns = Object.entries(schema.columns) as [string, ColumnType][];
  for (const [name, type] of columns) {
    if (!IDENTIFIER.test(name) || name.startsWith("_")) throw new Error(`column ${JSON.stringify(name)}: a plain identifier not starting with _`);
    if (!["text", "integer", "real", "boolean", "json"].includes(type)) throw new Error(`column ${name}: unknown type ${type}`);
  }
  if (schema.columns[schema.primaryKey] !== "text") throw new Error(`primary key ${schema.primaryKey} must be a text column`);
  for (const [name, cols] of Object.entries(schema.indexes ?? {})) {
    if (!IDENTIFIER.test(name)) throw new Error(`index ${JSON.stringify(name)}: a plain identifier`);
    if (cols.length === 0) throw new Error(`index ${name}: no columns`);
    for (const col of cols) {
      const type = schema.columns[col];
      if (!type) throw new Error(`index ${name}: no column ${col}`);
      if (type === "json") throw new Error(`index ${name}: json column ${col} cannot be indexed`);
    }
  }
  for (const [name, def] of Object.entries(schema.globalIndexes ?? {})) {
    if (!IDENTIFIER.test(name)) throw new Error(`global index ${JSON.stringify(name)}: a plain identifier`);
    if (schema.indexes?.[name]) throw new Error(`global index ${name}: an index has that name`);
    if (def.columns.length === 0) throw new Error(`global index ${name}: no columns`);
    for (const col of def.columns) {
      const type = schema.columns[col];
      if (!type) throw new Error(`global index ${name}: no column ${col}`);
      if (type === "json") throw new Error(`global index ${name}: json column ${col} cannot be indexed`);
    }
    const partition = def.partition ?? def.columns.length;
    if (partition === 0) throw new Error(`global index ${name}: partition 0 (by range) is not built yet`);
    if (!Number.isInteger(partition) || partition < 1 || partition > def.columns.length) throw new Error(`global index ${name}: partition must be from 1 to ${def.columns.length}`);
    if (def.unique && partition !== def.columns.length) throw new Error(`global index ${name}: a unique index is partitioned by all its columns`);
  }
  for (const [name, col] of Object.entries(schema.counts ?? {})) {
    if (!IDENTIFIER.test(name)) throw new Error(`count group ${JSON.stringify(name)}: a plain identifier`);
    const type = schema.columns[col];
    if (!type) throw new Error(`count group ${name}: no column ${col}`);
    if (type === "json") throw new Error(`count group ${name}: json column ${col} cannot be counted by value`);
  }
  return schema;
}

/**
 * A column's value as SQLite stores it, checked against the column's type: `boolean` as 0/1,
 * `json` as text. NaN is refused: SQLite would store it as null.
 */
export function encodeValue<Row extends RowShape>(schema: TableSchema<Row>, column: string, value: unknown): SqlValue {
  const type = schema.columns[column as keyof Row & string];
  if (!type) throw new Error(`no column ${column}`);
  if (value === null || value === undefined) {
    if (column === schema.primaryKey) throw new Error("the primary key cannot be null");
    return null;
  }
  switch (type) {
    case "text":
      if (typeof value !== "string") throw new Error(`${column}: expected text`);
      return value;
    case "integer":
      if (typeof value !== "number" || !Number.isInteger(value)) throw new Error(`${column}: expected an integer`);
      return value;
    case "real":
      if (typeof value !== "number" || Number.isNaN(value)) throw new Error(`${column}: expected a number (not NaN)`);
      return value;
    case "boolean":
      if (typeof value !== "boolean") throw new Error(`${column}: expected a boolean`);
      return value ? 1 : 0;
    case "json": {
      const text = JSON.stringify(value);
      if (text === undefined) throw new Error(`${column}: not representable as JSON`);
      return text;
    }
  }
}

/**
 * A row and its version: one more on every write. A row written where none is starts above every
 * version a deleted row of its shard had (1 if none was deleted), so a key never has a version
 * twice: a write conditional on a version read before a delete fails on the row written since.
 */
export interface Versioned<Row> {
  readonly row: Row;
  readonly version: number;
}

/** What a conditional write did: `ok`, and the row's version now (0: absent). */
export interface WriteResult {
  readonly ok: boolean;
  readonly version: number;
  /** Refused by a layer above (IndexedTable: a unique index's name), and nothing was written. */
  readonly refused?: string;
}

/**
 * What a command decided for one row: its `result` for the caller, and what to write. No `write`
 * (or null) writes nothing; `"delete"` deletes the row; a row replaces it whole.
 */
export interface Decision<Row extends RowShape, R> {
  readonly result: R;
  readonly write?: Row | "delete" | null;
}

/**
 * A conditional update that runs inside the row's shard, in one transaction: given the row as it
 * is (null: absent) and the caller's `args`, it decides what to write and what to answer.
 *
 * A command is pure and synchronous. It reads nothing but the row and `args`: no clock (pass the
 * time in `args`), no storage, no network, no random values. It may run again for the same call
 * only if the call is made again; a throw writes nothing and fails the call.
 */
export type Command<Row extends RowShape, A, R> = (current: Versioned<Row> | null, args: A) => Decision<Row, R>;

/** A table's commands, by name: functions don't cross an RPC, so a call names one. */
export type Commands<Row extends RowShape> = Readonly<Record<string, Command<Row, any, any>>>;

/**
 * Types a table's commands by its row, keeping each command's own argument and result types:
 * `defineCommands<Doc>()({ rename: (current, args: { name: string }) => ... })`. The shard class
 * registers the same object as `commands`, and the client takes it as its `commands` option.
 */
export function defineCommands<Row extends RowShape>(): <C extends Commands<Row>>(commands: C) => C {
  return (commands) => commands;
}

export type CommandArgs<C> = C extends Command<any, infer A, any> ? A : never;
export type CommandResult<C> = C extends Command<any, any, infer R> ? R : never;

/**
 * What `apply` answers. `ok`: the command ran, its write (if any) committed, and the row is now at
 * `version` (0: absent). Not `ok`: the write would break a unique constraint, and nothing was
 * written; the command's result is dropped.
 */
export type ApplyOutcome<R> = { readonly ok: true; readonly result: R; readonly version: number } | { readonly ok: false; readonly conflict: "unique"; readonly constraint: string; readonly version: number };

/** What the client sends a shard for one `apply`. */
export interface ApplyRequest {
  readonly command: string;
  readonly args: unknown;
}

export type Op = "=" | "!=" | "<" | "<=" | ">" | ">=" | "isNull" | "notNull" | "prefix" | "contains";

/** A condition on one column; conditions are ANDed. `contains` ignores case. */
export interface Condition {
  readonly column: string;
  readonly op: Op;
  readonly value?: unknown;
}

export interface ListQuery {
  /** A declared index, or none for primary-key order. */
  readonly index?: string;
  readonly direction?: "asc" | "desc";
  readonly where?: readonly Condition[];
  /** Rows per page: 50 by default, at most 1,000. */
  readonly limit?: number;
  /** The cursor of the previous page. */
  readonly cursor?: string | null;
  /** Rows each shard may scan for one page: 1,000 or 10 × limit by default, at most 10,000. */
  readonly budget?: number;
  /**
   * Also count, in the same calls to the shards: `true` for every row (`total`), or a count
   * group's name for its counts per value too (`counts`). Counts ignore `where`.
   */
  readonly withCount?: boolean | string;
  /**
   * `"query"` (the default): rows in the query's order across the whole table; each page asks
   * every shard. `"shard"`: rows in the query's order within each shard, shard after shard; each
   * page asks one shard (up to three, if one runs out partway), so walking a table of N shards
   * costs about 1/N of the calls. For walks that need every row and not their order: exports,
   * backfills, scans. Every row that existed when the walk began is listed once, through splits;
   * `withCount` is not supported.
   */
  readonly order?: "query" | "shard";
}

export interface ListPage<Row> {
  readonly rows: readonly Versioned<Row>[];
  /** For the next page; null when the listing is done. */
  readonly cursor: string | null;
  /** With `withCount`: rows in the table, as `count()` says. */
  readonly total?: number;
  /** With `withCount` naming a count group: its counts, as `countBy` says. */
  readonly counts?: readonly GroupCount[];
}

/**
 * A row's place in an index: its index key (the index's values and the row's key, in an
 * order-preserving encoding, as hex), or the row's key for primary-key order. Positions of one
 * listing compare as plain strings, by code point.
 */
export type Position = string;
export type SqlValue = string | number | null;

/** What a shard answers for one page. `frontier`: up to where it reported every match; null: the end. */
export interface ShardPage {
  readonly rows: readonly { readonly key: string; readonly row: RowShape; readonly version: number; readonly position: Position }[];
  readonly frontier: Position | null;
  /** Asked for: this shard's rows, and its counts of a group. */
  readonly total?: number;
  readonly counts?: readonly GroupCount[];
}

/** What the client asks each shard for one page. */
export interface ShardPageRequest {
  readonly index: string | null;
  readonly direction: "asc" | "desc";
  readonly where: readonly Condition[];
  readonly limit: number;
  readonly budget: number;
  readonly after: Position | null;
  /** Count this shard's rows too, and (a group's name) its counts per value. */
  readonly withCount?: boolean | string;
}

export const MAX_LIMIT = 1_000;
export const MAX_BUDGET = 10_000;

/** The request for a query, with defaults and bounds applied. */
export function pageRequest(query: ListQuery, after: Position | null): ShardPageRequest {
  const limit = query.limit ?? 50;
  if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT) throw new Error(`limit must be an integer from 1 to ${MAX_LIMIT}`);
  const budget = query.budget ?? Math.max(1_000, limit * 10);
  if (!Number.isInteger(budget) || budget < limit || budget > MAX_BUDGET) throw new Error(`budget must be an integer from limit to ${MAX_BUDGET}`);
  const direction = query.direction ?? "asc";
  if (direction !== "asc" && direction !== "desc") throw new Error("direction must be asc or desc");
  return { index: query.index ?? null, direction, where: query.where ?? [], limit, budget, after };
}

/** Compares two positions in listing order: code point order, reversed for `desc`. */
export function comparePositions(a: Position, b: Position, direction: "asc" | "desc"): number {
  const d = compareText(a, b);
  return direction === "asc" ? d : -d;
}

/** Code point order: the order of UTF-8 bytes, which is how SQLite compares text. */
export function compareText(a: string, b: string): number {
  if (a === b) return 0;
  const ia = a[Symbol.iterator]();
  const ib = b[Symbol.iterator]();
  for (;;) {
    const ca = ia.next();
    const cb = ib.next();
    if (ca.done || cb.done) return ca.done ? (cb.done ? 0 : -1) : 1;
    const d = ca.value.codePointAt(0)! - cb.value.codePointAt(0)!;
    if (d !== 0) return d;
  }
}

/**
 * The index key of these values, as hex: byte order is the values' order. Null (0x00) sorts
 * before numbers (0x01, then an order-preserving float64) and text (0x02, then its UTF-8 with
 * 0x00 escaped as 0x00 0xFF, then 0x00): nulls first ascending, last descending.
 */
export function encodeIndexKey(values: readonly SqlValue[]): string {
  let hex = "";
  for (const v of values) {
    if (v === null) hex += "00";
    else if (typeof v === "number") {
      if (Number.isNaN(v)) throw new Error("NaN cannot be indexed");
      FLOAT.setFloat64(0, v === 0 ? 0 : v);
      const negative = (FLOAT.getUint8(0) & 0x80) !== 0;
      hex += "01";
      for (let i = 0; i < 8; i++) {
        const b = FLOAT.getUint8(i);
        hex += HEX[negative ? ~b & 0xff : i === 0 ? b | 0x80 : b]!;
      }
    } else {
      hex += "02";
      const bytes = ENCODER.encode(v);
      for (let i = 0; i < bytes.length; i++) {
        const b = bytes[i]!;
        hex += HEX[b]!;
        if (b === 0x00) hex += "ff";
      }
      hex += "00";
    }
  }
  return hex;
}
/** Each byte as two hex digits: an index key is built for every row written, per index. */
const HEX = Array.from({ length: 256 }, (_, b) => b.toString(16).padStart(2, "0"));
const ENCODER = new TextEncoder();
const FLOAT = new DataView(new ArrayBuffer(8));

/**
 * A short fingerprint of what a cursor belongs to: the index (its name and columns, so a cursor
 * from before an index was redefined is refused, not resumed at a meaningless position), the
 * direction and the conditions.
 */
export function queryFingerprint(request: Pick<ShardPageRequest, "index" | "direction" | "where">, columns: readonly string[]): string {
  return shortDigest(JSON.stringify([request.index, columns, request.direction, request.where]));
}

/** The first 8 hex digits of `text`'s SHA-256: a name or fingerprint, not a secret. */
export function shortDigest(text: string): string {
  return createHash("sha256").update(text).digest("hex").slice(0, 8);
}

export function encodeCursor(fingerprint: string, position: Position): string {
  return Buffer.from(JSON.stringify({ f: fingerprint, p: position })).toString("base64url");
}

/**
 * Where a listing in shard order is: in shard `leaf`, after `after`; every shard under `floor.bits`
 * from after `floor.after`. A walk starts at the node `start`, goes to the last shard, wraps around
 * (`wrapped`) to the first, and ends when it is back under `start`: walks that start together
 * spread over the shards instead of all asking the first one.
 */
export interface ShardCursor {
  readonly leaf: string;
  readonly after: Position | null;
  /** Set once a shard split partway through it: its shards go on from where it was. */
  readonly floor: { readonly bits: string; readonly after: Position | null } | null;
  readonly start: string;
  readonly wrapped: boolean;
}

export function encodeShardCursor(fingerprint: string, at: ShardCursor): string {
  return Buffer.from(JSON.stringify({ f: fingerprint, s: at })).toString("base64url");
}

export function decodeShardCursor(cursor: string, fingerprint: string): ShardCursor {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString());
  } catch {
    throw new Error("not a cursor of this table");
  }
  const { f, s } = (parsed ?? {}) as { f?: unknown; s?: Partial<ShardCursor> };
  if (f !== fingerprint) throw new Error("the cursor belongs to another query (index, direction, conditions or order)");
  const bits = (b: unknown) => typeof b === "string" && /^[01]*$/.test(b);
  const position = (p: unknown) => p === null || typeof p === "string";
  if (!s || !bits(s.leaf) || !position(s.after) || !bits(s.start) || typeof s.wrapped !== "boolean" || !(s.floor === null || (s.floor !== undefined && bits(s.floor.bits) && position(s.floor.after)))) throw new Error("not a cursor of this table");
  return s as ShardCursor;
}

/**
 * Where a walk in shard order goes once shard `at.leaf` is done: the node right after its subtree
 * (the sibling of its nearest ancestor-or-self that is a left half), which exists since splits make
 * both halves; after the last shard, the root (its first shard, once asked), unless the walk
 * already wrapped. From that node's start, or from the floor's position if it is under the shard
 * that split. Null once the walk is back under its start: done. Found from the bits alone, not the
 * client's map of splits, which may know less than the cursor does.
 */
export function nextShard(at: ShardCursor): ShardCursor | null {
  const left = at.leaf.lastIndexOf("0");
  if (left < 0 && at.wrapped) return null;
  const next = left < 0 ? "" : `${at.leaf.slice(0, left)}1`;
  const wrapped = at.wrapped || left < 0;
  if (wrapped && next.startsWith(at.start)) return null;
  const floor = at.floor !== null && next.startsWith(at.floor.bits) ? at.floor : null;
  return { ...at, leaf: next, after: floor?.after ?? null, floor, wrapped };
}

export function decodeCursor(cursor: string, fingerprint: string): Position {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, "base64url").toString());
  } catch {
    throw new Error("not a cursor of this table");
  }
  const { f, p } = (parsed ?? {}) as { f?: unknown; p?: unknown };
  if (f !== fingerprint) throw new Error("the cursor belongs to another query (index, direction or conditions)");
  if (typeof p !== "string") throw new Error("not a cursor of this table");
  return p;
}

/**
 * Merges one page from every shard: every shard reported every match up to the smallest frontier,
 * so rows up to it are complete. Returns them in order, up to `limit`, and where the next page
 * starts (null: done).
 */
export function mergePages(pages: readonly ShardPage[], request: Pick<ShardPageRequest, "limit" | "direction">): { rows: ShardPage["rows"][number][]; next: Position | null } {
  const cmp = (a: Position, b: Position) => comparePositions(a, b, request.direction);
  let frontier: Position | null = null;
  // (null, not falsy: the empty string is a position, that of a row whose key is "".)
  for (const page of pages) if (page.frontier !== null && (frontier === null || cmp(page.frontier, frontier) < 0)) frontier = page.frontier;
  const seen = new Set<string>();
  const complete = pages
    .flatMap((p) => p.rows)
    .filter((r) => frontier === null || cmp(r.position, frontier) <= 0)
    .filter((r) => {
      // A key reported by two shards in one page (one before a split committed, one after).
      if (seen.has(r.key)) return false;
      seen.add(r.key);
      return true;
    })
    .sort((a, b) => cmp(a.position, b.position));
  const rows = complete.slice(0, request.limit);
  if (rows.length === request.limit && (complete.length > request.limit || frontier !== null)) return { rows, next: rows.at(-1)!.position };
  return { rows, next: frontier };
}
