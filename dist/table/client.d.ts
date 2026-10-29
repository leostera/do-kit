import { type ListPage, type ListQuery, type Position, type RowShape, type ShardPage, type TableSchema, type ShardPageRequest, type Versioned, type WriteResult } from "./schema.js";
export declare class TableClient<Row extends RowShape> {
    readonly name: string;
    private readonly client;
    private readonly primaryKey;
    constructor(namespace: DurableObjectNamespace<Rpc.DurableObjectBranded>, name: string, schema: TableSchema<Row>);
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
     * One page of a listing across every shard. Pass the page's cursor back for the next one;
     * a null cursor means the listing is done. A page may hold fewer than `limit` rows (a sparse
     * filter ran out of budget) and still have a cursor.
     */
    list(query?: ListQuery): Promise<ListPage<Row>>;
    /** Every row of a query, page after page. */
    all(query?: Omit<ListQuery, "cursor">): AsyncGenerator<Versioned<Row>>;
    private keyOf;
    private byPosition;
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
export declare function table<T extends Rpc.DurableObjectBranded, Row extends RowShape>(namespace: DurableObjectNamespace<T>, name: string, schema: TableSchema<Row>): TableClient<Row>;
//# sourceMappingURL=client.d.ts.map