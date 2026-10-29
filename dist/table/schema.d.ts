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
/** A row's place in an index: the stored values of its columns, then its key. */
export type Position = readonly SqlValue[];
export type SqlValue = string | number | null;
/** What a shard answers for one page. `frontier`: up to where it reported every match; null: the end. */
export interface ShardPage {
    readonly rows: readonly {
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
/** SQLite's order: null, then numbers, then text by code point (the order of its UTF-8 bytes). */
export declare function compareValues(a: SqlValue, b: SqlValue): number;
/** Compares two positions in listing order: `desc` reverses every column, the key included. */
export declare function comparePositions(a: Position, b: Position, direction: "asc" | "desc"): number;
/** A short fingerprint of what a cursor belongs to: the index, direction and conditions. */
export declare function queryFingerprint(request: Pick<ShardPageRequest, "index" | "direction" | "where">): string;
export declare function encodeCursor(fingerprint: string, position: Position): string;
export declare function decodeCursor(cursor: string, fingerprint: string): Position;
//# sourceMappingURL=schema.d.ts.map