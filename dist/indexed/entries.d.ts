import { type Condition, type RowShape, type SqlValue, type TableSchema } from "../table/schema.js";
/** A global index, resolved from the schema. */
export interface GlobalIndexInfo {
    readonly name: string;
    readonly columns: readonly string[];
    readonly unique: boolean;
    readonly partition: number;
}
/**
 * Where an entry lives: `route` is its index shard's routing key (the index's name and the
 * partition values), `entry` its key there (the values, and the row's key unless unique), both
 * in the order-preserving encoding of RFD0002.
 */
export interface EntryAddress {
    readonly index: string;
    readonly route: string;
    readonly entry: string;
}
export declare function globalIndexes<Row extends RowShape>(schema: TableSchema<Row>): GlobalIndexInfo[];
/** The routing key of an index's partition. */
export declare function routeOf(index: string, partitionValues: readonly SqlValue[]): string;
/** The entry a row has in an index, or null (a unique index skips rows with a null in its columns). */
export declare function entryOf<Row extends RowShape>(schema: TableSchema<Row>, ix: GlobalIndexInfo, row: RowShape): EntryAddress | null;
/** Every entry a row has, keyed by `route entry`. */
export declare function entriesOf<Row extends RowShape>(schema: TableSchema<Row>, row: RowShape | null): Map<string, EntryAddress>;
export declare function addressKey(e: Pick<EntryAddress, "route" | "entry">): string;
/**
 * A change a row shard delivers to an index shard: a put (the row's entry at `version`, with a
 * copy of the row; with the reservation token it was written with, for a unique index) or a
 * remove (the row no longer has the entry since `version`).
 */
export type IndexOp = {
    readonly kind: "put";
    readonly route: string;
    readonly entry: string;
    readonly owner: string;
    readonly version: number;
    readonly row: RowShape;
    readonly token: number | null;
} | {
    readonly kind: "remove";
    readonly route: string;
    readonly entry: string;
    readonly owner: string;
    readonly version: number;
};
/** What reserving a unique value answered. */
export type ReserveAnswer = {
    readonly reserved: number;
} | {
    readonly taken: string;
} | {
    readonly contended: true;
};
/** What a row shard answers about a reservation: the row has the entry, or not (and its token is fenced). */
export type ResolveAnswer = {
    readonly committed: true;
    readonly version: number;
    readonly row: RowShape;
} | {
    readonly committed: false;
    readonly version: number;
};
/** One index shard's part of a listing within a partition. */
export interface IndexPageRequest {
    readonly direction: "asc" | "desc";
    /** Conditions on the row copies, their values encoded as the rows store them. */
    readonly where: readonly Condition[];
    readonly limit: number;
    readonly budget: number;
    /** The entry key to start after. */
    readonly after: string | null;
}
export interface IndexPage {
    readonly rows: readonly {
        readonly row: RowShape;
        readonly version: number;
        readonly entry: string;
    }[];
    /** Up to where every match was reported; null: the end. */
    readonly frontier: string | null;
}
//# sourceMappingURL=entries.d.ts.map