import { ShardedTable } from "../table/index.js";
import type { RowShape, WriteResult } from "../table/schema.js";
import { type ResolveAnswer } from "./entries.js";
/** What the client sends with a write: a reservation token per unique index it reserved. */
export interface WriteTokens {
    readonly tokens?: Readonly<Record<string, number>>;
}
/** A write's result on the row shard: `indexed` false when its entries were not all delivered in time. */
export type IndexedShardResult = WriteResult & {
    readonly indexed?: boolean;
};
/**
 * Extend with `protected table = defineTable({ ..., globalIndexes })`, `nodes()` (this class's
 * binding) and `indexNodes()` (the binding of its GlobalIndex class). The client
 * (`indexedTable()`) reserves unique values, writes, looks up and lists.
 *
 * Custom transactional methods that write rows use `indexedTransact`, not `transact`: it delivers
 * the writes' index changes before returning. A custom write that changes a unique column's value
 * is refused (`refused: index`): those go through the client, which reserves first.
 *
 * Subclasses that override `onAlarm`, `afterClone` or `cleanupForeign` must call `super`.
 */
export declare abstract class IndexedTable<Env, Row extends RowShape> extends ShardedTable<Env, Row> {
    /** The binding of the GlobalIndex class holding this table's global indexes. */
    protected abstract indexNodes(): DurableObjectNamespace<any>;
    /** How long a write waits for its index entries before it returns `IndexPending`. */
    protected indexWaitMs: number;
    private indexMigrated;
    private batch;
    private indexClient;
    /** Outbox entries being delivered now. */
    private readonly delivering;
    private wakeAt;
    /**
     * Whether row `key` has an entry a reservation is pending for. If not, fences `token`, so no
     * write carrying it (or an older one) lands afterwards.
     */
    indexResolve(keys: readonly string[], payloads: readonly {
        readonly route: string;
        readonly entry: string;
        readonly token: number;
    }[]): Promise<ResolveAnswer[]>;
    /**
     * Like `transact`, for custom methods that write rows: delivers their index changes before it
     * returns, and throws `IndexPending` if that takes longer than `indexWaitMs` (the rows are
     * written; the alarm goes on delivering).
     */
    protected indexedTransact<R>(keys: readonly string[], fn: () => R): Promise<R>;
    protected writeBatch(keys: readonly string[], payloads: readonly unknown[] | null, write: () => WriteResult[]): Promise<IndexedShardResult[]>;
    private inBatch;
    protected watchesRows(): boolean;
    protected keepsGlobalIndexes(): boolean;
    /** A unique value the row takes needs a reservation token above the fence. */
    protected refuseRowWrite(key: string, before: Row | null, after: Row | null): string | null;
    /** Appends the row's index changes to the outbox: a put for each entry it has, a remove for each it lost. */
    protected rowWritten(key: string, before: Row | null, after: Row | null, version: number): void;
    /** Delivers these outbox entries; true if they all were within `ms` (they go on being delivered either way). */
    private deliverWithin;
    /**
     * Sends outbox entries to their index shards, and deletes them once applied. Entries being sent
     * by another delivery are waited for, not sent twice.
     */
    private deliver;
    /** Waits while another delivery sends some of these entries; throws if they are still in the outbox after. */
    private waitDelivered;
    /** Delivers what is left in the outbox. Call `super.onAlarm()`. */
    protected onAlarm(): Promise<void>;
    /** The copy drops the outbox: the original goes on delivering all of it. Call `super.afterClone`. */
    protected afterClone(owns: (key: string) => boolean): void;
    /** Drops the fences of keys given away (never the outbox: see afterClone). Call `super.cleanupForeign`. */
    protected cleanupForeign(owns: (key: string) => boolean, limit: number): number;
    protected migrate(): void;
    private fence;
    private raiseFence;
    private uniqueIndexes;
    private wake;
    private indexShards;
    private get store();
}
//# sourceMappingURL=table.d.ts.map