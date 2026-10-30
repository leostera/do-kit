import { type ShardedClientOptions } from "../sharded/client.js";
import { type GroupCount, type ListPage, type ListQuery, type Position, type RowShape, type ShardPage, type TableSchema, type ShardPageRequest, type Versioned, type WriteResult } from "./schema.js";
/** The `table*` methods, as the client calls them through the sharded client. */
interface TableMethods {
    tableGet(keys: readonly string[], payloads: null): Promise<(Versioned<RowShape> | null)[]>;
    tablePut(keys: readonly string[], payloads: readonly {
        row: RowShape;
        ifVersion?: number;
    }[]): Promise<WriteResult[]>;
    tablePatch(keys: readonly string[], payloads: readonly {
        changes: RowShape;
        ifVersion?: number;
    }[]): Promise<WriteResult[]>;
    tableDelete(keys: readonly string[], payloads: readonly {
        ifVersion?: number;
    }[] | null): Promise<WriteResult[]>;
    tablePage(request: ShardPageRequest): Promise<ShardPage>;
    tableCount(): Promise<number>;
    tableCountBy(group: string): Promise<GroupCount[]>;
}
export type Untyped = {
    call(keys: readonly string[], method: string, payloads: readonly unknown[] | null): Promise<{
        positions: readonly number[];
        result: readonly unknown[];
    }[]>;
    scan(method: string, ...args: unknown[]): Promise<unknown[]>;
    knownShardCount(): Promise<number>;
};
/**
 * Rows to ask each of `shards` shards for, to fill `wanted`: keys spread over shards by hash, so
 * each holds about wanted/shards of a page; twice that, plus two, rarely comes up short.
 */
export declare function perShardLimit(wanted: number, shards: number): number;
export declare class TableClient<Row extends RowShape> {
    readonly name: string;
    protected readonly client: Untyped;
    protected readonly primaryKey: keyof Row & string;
    private readonly indexes;
    constructor(namespace: DurableObjectNamespace<Rpc.DurableObjectBranded>, name: string, schema: TableSchema<Row>, options?: ShardedClientOptions);
    get(key: string): Promise<Versioned<Row> | null>;
    /** Rows of these keys, in order (null: absent). */
    getMany(keys: readonly string[]): Promise<(Versioned<Row> | null)[]>;
    /** Writes a whole row; `ifVersion: 0` only if absent, `ifVersion: n` only if at version n. */
    put(row: Row, options?: {
        ifVersion?: number;
    }): Promise<WriteResult>;
    putMany(writes: readonly {
        row: Row;
        ifVersion?: number;
    }[]): Promise<WriteResult[]>;
    /** Changes some columns of an existing row (`ok: false` if absent or at another version). */
    patch(key: string, changes: Partial<Row>, options?: {
        ifVersion?: number;
    }): Promise<WriteResult>;
    delete(key: string, options?: {
        ifVersion?: number;
    }): Promise<WriteResult>;
    /** Rows in the whole table: exact unless a split commits during the count. */
    count(): Promise<number>;
    /**
     * Rows of the whole table per value of a count group's column (values with no rows left out),
     * kept with every write: as exact as `count()`.
     */
    countBy(group: string): Promise<GroupCount[]>;
    /**
     * One page of a listing across every shard. Pass the page's cursor back for the next one;
     * a null cursor means the listing is done. A page may hold fewer than `limit` rows (a sparse
     * filter ran out of budget) and still have a cursor.
     */
    list(query?: ListQuery): Promise<ListPage<Row>>;
    /** Every row of a query, page after page. */
    all(query?: Omit<ListQuery, "cursor">): AsyncGenerator<Versioned<Row>>;
    protected keyOf(row: Row): string;
    protected byPosition<R>(keys: readonly string[], method: keyof TableMethods, payloads: readonly unknown[] | null): Promise<R[]>;
}
/**
 * Merges one page from every shard: every shard reported every match up to the smallest frontier,
 * so rows up to it are complete. Returns them in order, up to `limit`, and where the next page
 * starts (null: done).
 */
export declare function mergePages(pages: readonly ShardPage[], request: Pick<ShardPageRequest, "limit" | "direction">): {
    rows: ShardPage["rows"][number][];
    next: Position | null;
};
/**
 * The client of the sharded table `name`: `namespace` is the binding of a `ShardedTable` class,
 * `schema` the table it declares (the client routes rows by its primary key).
 */
export declare function table<T extends Rpc.DurableObjectBranded, Row extends RowShape>(namespace: DurableObjectNamespace<T>, name: string, schema: TableSchema<Row>, options?: ShardedClientOptions): TableClient<Row>;
export {};
//# sourceMappingURL=client.d.ts.map