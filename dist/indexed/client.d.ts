import { type ShardedClientOptions } from "../sharded/client.js";
import { TableClient } from "../table/client.js";
import { type ListPage, type ListQuery, type RowShape, type TableSchema, type Versioned, type WriteResult } from "../table/schema.js";
/**
 * The row is written, but not all of its index entries were within the row shard's wait: lookups
 * and listings may not show it yet. The row shard goes on delivering them. Treat the write as done,
 * or retry it.
 */
export declare class IndexPending extends Error {
    readonly key: string;
    readonly version: number;
    constructor(key: string, version: number);
}
/** Another write kept holding a unique value: try again later. Nothing was written. */
export declare class IndexContended extends Error {
    readonly key: string;
    readonly index: string;
    constructor(key: string, index: string);
}
/** A write's result: `conflict` names the unique index another row has the value of (nothing was written; `version` is 0). */
export type IndexedWriteResult = WriteResult & {
    readonly conflict?: string;
};
/** A listing: through a global index with `partition`, its values for the index's leading columns. */
export type IndexedListQuery = ListQuery & {
    readonly partition?: readonly unknown[];
};
type Write<Row> = {
    readonly row: Row;
    readonly ifVersion?: number;
};
export declare class IndexedTableClient<Row extends RowShape> extends TableClient<Row> {
    private readonly index;
    private readonly schema;
    private readonly globals;
    constructor(namespace: DurableObjectNamespace<Rpc.DurableObjectBranded>, indexNamespace: DurableObjectNamespace<Rpc.DurableObjectBranded>, name: string, schema: TableSchema<Row>, options?: ShardedClientOptions);
    put(row: Row, options?: {
        ifVersion?: number;
    }): Promise<IndexedWriteResult>;
    /**
     * Writes whole rows. Reserves each row's unique values first: a value another row has fails that
     * write with `conflict`. Returns once every index has the rows, or throws IndexPending.
     */
    putMany(writes: readonly Write<Row>[]): Promise<IndexedWriteResult[]>;
    /** Changes some columns of an existing row; reserves the unique values it changes first. */
    patch(key: string, changes: Partial<Row>, options?: {
        ifVersion?: number;
    }): Promise<IndexedWriteResult>;
    /** Deletes a row; returns once its index entries are removed, or throws IndexPending. */
    delete(key: string, options?: {
        ifVersion?: number;
    }): Promise<IndexedWriteResult>;
    /** The row whose values of a unique global index are `values`, in one call to one index shard. */
    lookup(index: string, values: readonly unknown[]): Promise<Versioned<Row> | null>;
    /**
     * One page of a listing. Through a global index, of one partition (`partition`: the values of its
     * leading columns), from one index shard; otherwise as a sharded table lists.
     */
    list(query?: IndexedListQuery): Promise<ListPage<Row>>;
    /** Results back in `out` at `positions`; returns the positions to write again (their reservation was given up). */
    private settle;
    /** Reserves the unique values of these rows, each in one call per index shard. */
    private reserveAll;
    private globalIndex;
    /** A condition with its value encoded as rows store it, checked against the column's type. */
    private encodeCondition;
}
/**
 * The client of the indexed table `name`: `namespace` is the binding of an IndexedTable class,
 * `indexNamespace` the binding of its GlobalIndex class, `schema` the table both declare.
 */
export declare function indexedTable<T extends Rpc.DurableObjectBranded, I extends Rpc.DurableObjectBranded, Row extends RowShape>(namespace: DurableObjectNamespace<T>, indexNamespace: DurableObjectNamespace<I>, name: string, schema: TableSchema<Row>, options?: ShardedClientOptions): IndexedTableClient<Row>;
export {};
//# sourceMappingURL=client.d.ts.map