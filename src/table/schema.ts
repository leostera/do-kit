// The schema of a sharded table, the queries a listing takes, and the sort order shared by the
// shards (SQLite) and the client that merges their pages (here): they must agree exactly.

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
}

export interface ListPage<Row> {
  readonly rows: readonly Versioned<Row>[];
  /** For the next page; null when the listing is done. */
  readonly cursor: string | null;
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
}

/** What the client asks each shard for one page. */
export interface ShardPageRequest {
  readonly index: string | null;
  readonly direction: "asc" | "desc";
  readonly where: readonly Condition[];
  readonly limit: number;
  readonly budget: number;
  readonly after: Position | null;
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
  const bytes: number[] = [];
  for (const v of values) {
    if (v === null) bytes.push(0x00);
    else if (typeof v === "number") {
      if (Number.isNaN(v)) throw new Error("NaN cannot be indexed");
      const view = new DataView(new ArrayBuffer(8));
      view.setFloat64(0, v === 0 ? 0 : v);
      const b = new Uint8Array(view.buffer);
      if (b[0]! & 0x80) for (let i = 0; i < 8; i++) b[i] = ~b[i]! & 0xff;
      else b[0] = b[0]! | 0x80;
      bytes.push(0x01, ...b);
    } else {
      bytes.push(0x02);
      for (const byte of new TextEncoder().encode(v)) {
        bytes.push(byte);
        if (byte === 0x00) bytes.push(0xff);
      }
      bytes.push(0x00);
    }
  }
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return hex;
}

/**
 * A short fingerprint of what a cursor belongs to: the index (its name and columns, so a cursor
 * from before an index was redefined is refused, not resumed at a meaningless position), the
 * direction and the conditions.
 */
export function queryFingerprint(request: Pick<ShardPageRequest, "index" | "direction" | "where">, columns: readonly string[]): string {
  const text = JSON.stringify([request.index, columns, request.direction, request.where]);
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 0x01000193);
  return (h >>> 0).toString(36);
}

export function encodeCursor(fingerprint: string, position: Position): string {
  return btoa(unescape(encodeURIComponent(JSON.stringify({ f: fingerprint, p: position })))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function decodeCursor(cursor: string, fingerprint: string): Position {
  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeURIComponent(escape(atob(cursor.replace(/-/g, "+").replace(/_/g, "/")))));
  } catch {
    throw new Error("not a cursor of this table");
  }
  const { f, p } = (parsed ?? {}) as { f?: unknown; p?: unknown };
  if (f !== fingerprint) throw new Error("the cursor belongs to another query (index, direction or conditions)");
  if (typeof p !== "string") throw new Error("not a cursor of this table");
  return p;
}
