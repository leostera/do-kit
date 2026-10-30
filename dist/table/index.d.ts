import { ShardedObject } from "../sharded/index.js";
import { type GroupCount, type RowShape, type ShardPage, type ShardPageRequest, type SqlValue, type TableSchema, type Versioned, type WriteResult } from "./schema.js";
export * from "./schema.js";
export { table, TableClient } from "./client.js";
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
export declare abstract class ShardedTable<Env, Row extends RowShape> extends ShardedObject<Env, never> {
    protected abstract readonly table: TableSchema<Row>;
    private migrated;
    /** SQLite's index on the primary key, for listings in key order. */
    private keyIndex;
    tableGet(keys: readonly string[], _payloads: null): Promise<(Versioned<Row> | null)[]>;
    tablePut(keys: readonly string[], payloads: readonly {
        readonly row: Row;
        readonly ifVersion?: number;
    }[]): Promise<WriteResult[]>;
    tablePatch(keys: readonly string[], payloads: readonly {
        readonly changes: Partial<Row>;
        readonly ifVersion?: number;
    }[]): Promise<WriteResult[]>;
    tableDelete(keys: readonly string[], payloads: readonly {
        readonly ifVersion?: number;
    }[] | null): Promise<WriteResult[]>;
    /**
     * Runs one client write call (`tablePut`, `tablePatch`, `tableDelete`): `write` in one
     * transaction over `keys`. A layer above wraps it (to read what the payloads carry, and to act
     * after the commit). The default is `transact(keys, write)`.
     */
    protected writeBatch(keys: readonly string[], _payloads: readonly unknown[] | null, write: () => WriteResult[]): Promise<WriteResult[]>;
    /** Whether `putRow`, `patchRow` and `deleteRow` call `refuseRowWrite` and `rowWritten`. */
    protected watchesRows(): boolean;
    /**
     * Before a write that passed its version check: a reason to refuse it (nothing is written, and the
     * result is `{ ok: false, version, refused }`), or null. `before` null: the row is new; `after`
     * null: it is deleted. Inside the write's transaction. Only if `watchesRows()`.
     */
    protected refuseRowWrite(_key: string, _before: Row | null, _after: Row | null): string | null;
    /** After a write, in its transaction: the row was `before` and is now `after` at `version`. Only if `watchesRows()`. */
    protected rowWritten(_key: string, _before: Row | null, _after: Row | null, _version: number): void;
    /** Whether this class keeps the schema's `globalIndexes` (IndexedTable does). */
    protected keepsGlobalIndexes(): boolean;
    /** One shard's part of a listing page. */
    tablePage(request: ShardPageRequest): Promise<ShardPage>;
    /** Rows this shard owns. */
    tableCount(): Promise<number>;
    /** Rows this shard owns, per value of a count group's column (values with no rows left out). */
    tableCountBy(group: string): Promise<GroupCount[]>;
    /** The row of `key`, or null. */
    protected getRow(key: string): Versioned<Row> | null;
    /** Writes a whole row if its version is `ifVersion` (0: absent). Call inside `transact`. */
    protected putRow(row: Row, options?: PutOptions): WriteResult;
    /** Changes some columns of an existing row if its version is `ifVersion`. Call inside `transact`. */
    protected patchRow(key: string, changes: Partial<Row>, options?: PutOptions): WriteResult;
    /** Deletes a row if its version is `ifVersion`. Call inside `transact`. */
    protected deleteRow(key: string, options?: PutOptions): WriteResult;
    /** The highest version a deleted row of this shard had (0: none). */
    private deletedVersion;
    /** Deletes rows (and counts) of hashes this shard gave away, a batch at a time. */
    protected cleanupForeign(owns: (key: string) => boolean, limit: number): number;
    private page;
    private indexes;
    /** A row's key in an index, from its stored values. */
    private indexKey;
    private condition;
    /** Brings this shard's storage up to the schema: once per instance, on its first use. */
    protected migrate(): void;
    /**
     * Keeps the count groups of the schema: drops the counts of groups removed or moved to another
     * column, and counts the rows stored (owned or not: counts are per hash prefix, like the rows)
     * for groups that are new, a page at a time.
     */
    private migrateCountGroups;
    /** Rows this shard owns (its migrated storage). */
    private count;
    private countBy;
    private ownedBuckets;
    /** The stored values of the count groups' columns of a row that exists. */
    private storedGroupValues;
    /** Moves a row between the values of each count group: from `before` (null: new) to `after` (null: deleted). */
    private countGroups;
    private countGroup;
    private version;
    private countRow;
    private checkKey;
    private encode;
    protected decode(stored: Record<string, SqlValue>): Row;
    private q;
    private get db();
}
//# sourceMappingURL=index.d.ts.map