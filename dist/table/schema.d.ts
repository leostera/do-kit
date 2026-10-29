/** How a column is stored: `boolean` as 0/1, `json` as text (not indexable). */
export type ColumnType = "text" | "integer" | "real" | "boolean" | "json";
/** A row: column names to values. */
export type RowShape = Record<string, unknown>;
export interface TableSchema<Row extends RowShape> {
    /** A `text` column: the row's key, which routes it to its shard. */
    readonly primaryKey: keyof Row & string;
    readonly columns: {
        readonly [K in keyof Row & string]: ColumnType;
    };
    /** Index name to the columns it sorts by. The primary key is appended to every index. */
    readonly indexes?: Readonly<Record<string, readonly (keyof Row & string)[]>>;
}
/** Checks a schema and returns it: names are plain identifiers, the key is text, indexes name columns. */
export declare function defineTable<Row extends RowShape>(schema: TableSchema<Row>): TableSchema<Row>;
/** A row and its version: 1 when written first, one more on every write. */
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
    readonly rows: readonly {
        readonly key: string;
        readonly row: RowShape;
        readonly version: number;
        readonly position: Position;
    }[];
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
export declare const MAX_LIMIT = 1000;
export declare const MAX_BUDGET = 10000;
/** The request for a query, with defaults and bounds applied. */
export declare function pageRequest(query: ListQuery, after: Position | null): ShardPageRequest;
/** Compares two positions in listing order: code point order, reversed for `desc`. */
export declare function comparePositions(a: Position, b: Position, direction: "asc" | "desc"): number;
/** Code point order: the order of UTF-8 bytes, which is how SQLite compares text. */
export declare function compareText(a: string, b: string): number;
/**
 * The index key of these values, as hex: byte order is the values' order. Null (0x00) sorts
 * before numbers (0x01, then an order-preserving float64) and text (0x02, then its UTF-8 with
 * 0x00 escaped as 0x00 0xFF, then 0x00): nulls first ascending, last descending.
 */
export declare function encodeIndexKey(values: readonly SqlValue[]): string;
/** A short fingerprint of what a cursor belongs to: the index, direction and conditions. */
export declare function queryFingerprint(request: Pick<ShardPageRequest, "index" | "direction" | "where">): string;
export declare function encodeCursor(fingerprint: string, position: Position): string;
export declare function decodeCursor(cursor: string, fingerprint: string): Position;
//# sourceMappingURL=schema.d.ts.map