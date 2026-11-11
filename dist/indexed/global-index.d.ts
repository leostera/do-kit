import { ShardedObject } from "../sharded/index.js";
import type { RowShape, Versioned } from "../table/schema.js";
import { type ApplyAnswer, type IndexOp, type IndexPage, type IndexPageRequest, type ReserveAnswer } from "./entries.js";
/** What `indexStats` reports, for tests and operators. */
export interface GlobalIndexStats {
    readonly entries: number;
    readonly pending: number;
    readonly removed: number;
    readonly resolved: {
        readonly committed: number;
        readonly aborted: number;
    };
    readonly applied: number;
    readonly ignored: number;
}
/**
 * Extend with `nodes()` (this class's binding) and `rowNodes()` (the IndexedTable's binding):
 *
 *     export class UserIndexes extends GlobalIndex<Env> {
 *       protected nodes() { return this.env.USER_INDEXES; }
 *       protected rowNodes() { return this.env.USERS; }
 *     }
 *
 * Subclasses that override `onAlarm` or `cleanupForeign` must call `super`.
 */
export declare abstract class GlobalIndex<Env> extends ShardedObject<Env, never> {
    constructor(ctx: DurableObjectState, env: Env);
    /** The binding of the IndexedTable whose rows this indexes. */
    protected abstract rowNodes(): DurableObjectNamespace<any>;
    /** How long a reservation may stay pending before the alarm asks its row shard. */
    protected reservationTimeoutMs: number;
    /** How long an entry a row let go of is kept, so a late older change cannot bring it back. */
    protected tombstoneMs: number;
    /** Entries of dropped indexes one alarm deletes, at most; the next alarm goes on. */
    protected dropBatch: number;
    private migrated;
    private rowClient;
    /** Reservations being resolved: one question per entry at a time. */
    private readonly resolving;
    private wakeAt;
    private stats;
    /** Reserves unique values for their rows: a token, or the row that has the value, or "contended". */
    indexReserve(routes: readonly string[], payloads: readonly {
        readonly entry: string;
        readonly owner: string;
    }[]): Promise<ReserveAnswer[]>;
    /** Applies changes delivered by row shards, idempotently and by version. */
    indexApply(routes: readonly string[], ops: readonly IndexOp[]): Promise<ApplyAnswer[]>;
    /**
     * Drops every entry of the index with this id, which its table no longer has. Sent by the row
     * shards (each that saw it go) to every index shard; the alarm deletes the entries.
     */
    indexDrop(id: string): Promise<void>;
    /** The rows of unique entries (null: none, or only reserved). */
    indexLookup(routes: readonly string[], payloads: readonly {
        readonly entry: string;
    }[]): Promise<(Versioned<RowShape> | null)[]>;
    /** One page of the entries of one partition, in entry order. */
    indexPage(routes: readonly string[], payloads: readonly IndexPageRequest[]): Promise<IndexPage[]>;
    /** Counts, for tests and operators. */
    indexStats(): Promise<GlobalIndexStats>;
    private reserve;
    /** Asks the row shard of `owner` whether its row has the entry, and applies the answer. */
    private resolve;
    private settle;
    private apply;
    private applied;
    private page;
    /** Resolves reservations left pending, and drops tombstones past their time. Call `super.onAlarm()`. */
    protected onAlarm(): Promise<void>;
    /** Deletes up to `dropBatch` entries of dropped indexes. True if there may be more. */
    private deleteDroppedEntries;
    private hasDroppedEntries;
    private droppedIds;
    private dropped;
    /** Deletes entries of routes this shard gave away. Call `super.cleanupForeign`. */
    protected cleanupForeign(owns: (key: string) => boolean, limit: number): number;
    private migrate;
    private get;
    /** Tokens only grow, on this shard and on any split from it (the counter is copied). */
    private nextToken;
    /** Asks for the alarm at `time`, unless it is asked for earlier already. */
    private wake;
    private rows;
    private get db();
}
//# sourceMappingURL=global-index.d.ts.map