import { DurableObject } from "cloudflare:workers";
export { SpansShards, ShardedClient, sharded, type ShardedClientOptions, type ShardResult } from "./client.js";
declare global {
    /** An opaque handle on a point-in-time copy of an object's storage; it can only be sent over RPC. */
    interface DurableObjectSnapshot {
    }
    interface DurableObjectStorage {
        snapshot?(options?: {
            bookmark?: string;
        }): Promise<DurableObjectSnapshot>;
        onNextSessionRestore?(target: DurableObjectSnapshot | string): Promise<string>;
    }
}
/** When a shard splits, and how. */
export interface SplitPolicy {
    /** Load, in calls/s, above which a shard splits (a call weighs 1 + keys / keysPerCall). */
    readonly callsPerSecond: number;
    readonly windowSeconds: number;
    /** No shard deeper than this (2^maxDepth shards at most). */
    readonly maxDepth: number;
    /**
     * Keys that cost as much as one call: a batch of many keys is load as well. Workers cannot time
     * synchronous work (clocks move only on I/O), so keys stand in for it. Default 25.
     */
    readonly keysPerCall?: number;
    /**
     * Depth a new object splits to before its first call, while empty: for uniform keys (hashes), so
     * a fresh object starts with 2^initialDepth shards and nothing to copy.
     */
    readonly initialDepth?: number;
    /**
     * How the right half is copied: "native" uses storage snapshots (compatibility flag
     * `durable_object_snapshots`); "logical" copies the rows over RPC; "auto" (the default) uses
     * snapshots where the runtime has them and rows elsewhere (local development).
     */
    readonly copy?: "auto" | "native" | "logical";
    /** A split that has not finished copying after this long is abandoned. Default 60 s. */
    readonly splitTimeoutMs?: number;
    /**
     * How long a write to the half being copied waits inside the shard before it answers "not ready"
     * and the client backs off and retries. Bounds the requests a slow copy queues in the object
     * (which the runtime sheds as overloaded). Default 2 s.
     */
    readonly holdWaitMs?: number;
}
/**
 * Split at half of what one object takes: about 800 calls/s on the edge, so 400, sustained for
 * 5 s.
 */
export declare const DEFAULT_SPLIT_POLICY: SplitPolicy;
/** The deepest a shard can be: ownership is decided on 64 bits of the key's hash. */
export declare const MAX_SPLIT_DEPTH = 64;
/** Checks a split policy, so a bad one fails loudly on the first call instead of quietly never splitting. */
export declare function checkSplitPolicy(policy: SplitPolicy): SplitPolicy;
/** What a node answers a routed call: `moved` names a split shard (its ancestors split too). */
/**
 * What a node answers a routed call: `moved` names a split shard (its ancestors split too);
 * `stale` says the caller's map of splits is deeper than this object, so it outlived the storage
 * it was learned from (deleted, then the name used again): the caller forgets it.
 */
export type CallAnswer<R> = {
    readonly ok: R;
} | {
    readonly moved: string;
} | {
    readonly notReady: string;
    readonly until?: number;
} | {
    readonly stale: string;
};
/** An object's state, for tests and operators. */
export interface NodeDescription {
    /** The bits of the shard this object hosts now. */
    readonly bits: string;
    /** "copy": a split's copy that does not serve yet; "new": never used. */
    readonly state: "shard" | "splitting" | "copy" | "new";
    /** Rows of keys this object owns. */
    readonly rows: number;
    /** Rows stored, including ones a split gave away and not deleted yet. */
    readonly storedRows: number;
    readonly callsPerSecond: number;
    /** How this object's last split copied its right half, and how long until the copy was claimed. */
    readonly lastSplit: {
        readonly copy: "native" | "logical";
        readonly ms: number;
    } | null;
    /** Why the last abandoned split attempt was abandoned. */
    readonly lastSplitError: string | null;
}
/** The writes of an update (null deletes) and what the method returns. */
export interface UpdateResult<V, R> {
    readonly writes: ReadonlyMap<string, V | null>;
    readonly result: R;
}
/** The first 64 bits of the key's SHA-256, as 16 hex digits: what ownership is decided on. */
export declare function keyHash(key: string): string;
/** Bit `i` (0-based, most significant first) of a 16-hex-digit hash. */
export declare function hashBit(hash: string, i: number): "0" | "1";
export declare function nodeName(name: string, bits: string): string;
/** The bits of the object hosting a shard: its bits without trailing zeros. */
export declare function hostBits(bits: string): string;
/** The name of the Durable Object hosting the shard `bits` of `name`. */
export declare function shardObjectName(name: string, bits: string): string;
/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this shard), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses never define `alarm` (the constructor refuses): they override `onAlarm` and schedule
 * it with `scheduleAlarm`.
 */
export declare abstract class ShardedObject<Env = unknown, V = unknown> extends DurableObject<Env> {
    protected splitPolicy: SplitPolicy;
    /** The namespace this class is bound to: to reach siblings. */
    protected abstract nodes(): DurableObjectNamespace<ShardedObject<Env, V>>;
    /** `splitPolicy`, checked once (a subclass sets it after the constructor here runs). */
    private get policy();
    private checkedPolicy;
    private readonly sql;
    private meta;
    private readonly calls;
    private quietUntil;
    /** The split in progress here: writes to `right` wait for `released`. */
    private hold;
    /** The split attempt whose logical copy this object takes (in memory: a restart refuses the rest). */
    private receiving;
    /** An empty shard splitting to the initial depth; calls wait for it, then are moved. */
    private presplitting;
    /** This shard had rows when it could have presplit: it splits by load only. */
    private presplitDeclined;
    constructor(ctx: DurableObjectState, env: Env);
    /** The current rows of these keys (absent keys are missing from the map). */
    protected read(keys: readonly string[]): Promise<Map<string, V>>;
    /**
     * Reads these keys and writes what `decide` returns, in one transaction, if this shard still owns
     * them. `decide` must be synchronous. While the right half is being copied, an update touching
     * it waits; once the copy serves, it answers "moved".
     */
    protected update<R>(keys: readonly string[], decide: (current: Map<string, V>) => UpdateResult<V, R>): Promise<R>;
    /**
     * Runs `fn` in one transaction, fenced like `update`: it waits while any of `keys` is being
     * copied by a split, and runs only if this shard still owns them all. Write the subclass's own
     * tables here, never outside: a write that skips the fence can land after the copy and be lost.
     * List every key whose rows `fn` touches. `fn` must be synchronous.
     */
    protected transact<R>(keys: readonly string[], fn: () => R): Promise<R>;
    /**
     * Up to `limit` of this shard's rows (of `read`/`update`) with keys after `afterKey`, in key
     * order: bounded paging over one shard. Rows of keys it no longer owns are skipped, so a page
     * is right while cleanup is still running.
     */
    protected ownedRowsPage(afterKey: string | null, limit: number): [string, V][];
    /**
     * Runs before this shard copies its storage for a split, while writes to the half that moves
     * wait: a subclass with work queued under rows it is giving away (an outbox) finishes it here,
     * so the new shard continues after it. It may run more than once.
     */
    protected beforeHandOut(): Promise<void>;
    /**
     * Runs in the new object of a split when it claims its copy, inside that transaction. A split
     * copies every table, the subclass's too, and the original keeps its subclass tables and the
     * work queued in them (an outbox it goes on delivering), so the copy drops its share of that
     * work here. `owns(key)` says whether a key is the new shard's. Synchronous. The rows of
     * `read`/`update` are handled by the base class.
     */
    protected afterClone(_owns: (key: string) => boolean): void;
    /**
     * Deletes up to `limit` rows of the subclass's own tables that belong to keys this shard no
     * longer owns, and returns how many it deleted. After a split, the alarm calls it on both
     * shards, in paced batches, until it returns 0, as it does for the rows of `read`/`update`.
     * Until then a subclass that queries its tables in bulk (listings, counts) filters with
     * `owns`. The default deletes nothing: an original that keeps delivering an outbox for keys
     * that moved keeps those rows.
     */
    protected cleanupForeign(_owns: (key: string) => boolean, _limit: number): number;
    /**
     * Throws (and the call answers "moved") unless this shard serves and owns every key. For reads of
     * the subclass's own tables; writes go through `update` or `transact`, which also wait on splits.
     */
    protected assertOwns(keys: readonly string[]): void;
    /** The routing hashes this shard owns, inclusive, as 16-hex-digit strings that sort like numbers. */
    protected ownedHashRange(): readonly [string, string];
    /** Whether `key` is this shard's now. Keys of a split's moving half stay this shard's until it commits. */
    protected owns(key: string): boolean;
    /** This shard's name (`${name}#${bits}`), once it has one. */
    protected nodeLabel(): string;
    /** The shard this one split from and when; null for the root. */
    protected parentNode(): {
        label: string;
        adoptedAt: number;
    } | null;
    /** The name all shards of this object share (what the client was created with). */
    protected objectName(): string;
    private assertOwner;
    /** Waits while a split copies any of these keys, up to `holdWaitMs`; then "not ready". */
    private waitForHold;
    /** What an update of these keys waits for, if any. */
    private holding;
    /** A call routed by the client: `method(keys, payloads, ...args)` if this shard owns the keys. */
    shardedCall(name: string, bits: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<CallAnswer<unknown>>;
    /** A call to every shard: `method(...args)`, with no keys. */
    shardedScan(name: string, bits: string, method: string, args: readonly unknown[]): Promise<CallAnswer<unknown>>;
    /**
     * The subclass method a routed call or scan names. Never one of this base class's own (its
     * reads and writes, the split protocol, the alarm) nor a runtime handler: a call must not reach
     * them with arguments it shaped for something else.
     */
    private routedMethod;
    /** Every row this shard owns, for scans (a subclass's scan method may use it). */
    protected allRows(): Map<string, V>;
    /** "moved" for a call addressed to `bits`: this shard went deeper, so `bits` and more split. */
    private moved;
    /**
     * Null if this object serves `bits` now; otherwise what to answer. Splits to the initial depth
     * first if this shard is above it and empty: every call waits for that.
     */
    private arriveSettled;
    /** Whether this object serves `bits`, initialising the root on its first call. */
    private arrive;
    private presplit;
    private loadMeta;
    private saveMeta;
    private countCall;
    /** Average calls/s over the last full `windowSeconds`. */
    private rate;
    /** Splits now if this object hosts the shard `bits` (also an operator's lever). */
    shardedSplit(name: string, bits: string): Promise<boolean>;
    /**
     * Hands the right half to a new object: holds writes to it, copies this object's storage there,
     * and once the copy is claimed stops owning the right half and activates it. A copy that is not
     * claimed in time is abandoned, and the held writes go ahead here.
     */
    private split;
    /** Copies this object's storage to the right sibling, then claims the copy there. */
    private copyAndClaim;
    private copyTo;
    /**
     * The logical copy, where snapshots are unavailable: every table, as a snapshot would carry
     * them, in byte-bounded batches. Writes to the half that moves are held meanwhile; rows of the
     * other half may change under the copy, which is fine: the copy drops them.
     *
     * Each table is paged by a key that a row keeps while it lives (its rowid, or its primary key
     * in a WITHOUT ROWID table), never by offset: the half that stays, and the cleanup of earlier
     * splits, delete rows during the copy, and an offset would then skip rows of the half that moves.
     */
    private copyRows;
    /**
     * The columns that page a table in a stable order: its rowid (under a name no column shadows),
     * or the primary key of a WITHOUT ROWID table. Returned quoted, ready for SQL.
     */
    private pageOrder;
    /** This object's tables, the subclass's too (not SQLite's or the runtime's). */
    private tables;
    private node;
    private activateSibling;
    /**
     * Runs on a serving shard once the time given to `scheduleAlarm` has come (it may run a little
     * later, and the shard may have split since: check what is due). Never in a split's copy before
     * it serves. If it throws, it runs again a second later. The default does nothing.
     */
    protected onAlarm(): Promise<void>;
    /**
     * Asks for `onAlarm` at `at` (or earlier, if an earlier time is already asked for). Use it, not
     * `ctx.storage.setAlarm`: the alarm is shared with splits, and this never postpones their work
     * nor loses the time asked for. A split's new shard keeps the time too.
     */
    protected scheduleAlarm(at: number | Date): Promise<void>;
    /**
     * Sets the alarm for `time` unless it is set for earlier already. For a time that has come it
     * sets it at once, without reading the alarm first: no alarm can be earlier, and setting it
     * before any await keeps it in the same commit as the writes just before (a split's meta, a
     * claim), so a crash cannot keep one without the other.
     */
    private armAlarm;
    /**
     * While a split's work is pending here (a sibling to activate, rows to clear), makes sure an
     * alarm will do it, at most every ALARM_CHECK_MS: an alarm can be lost, and nothing else would
     * activate the sibling. Called on routed calls, which keep reaching a shard that has split.
     */
    private checkAlarm;
    private alarmCheckedAt;
    /** When the subclass asked for `onAlarm`, if it did. */
    private loadWake;
    private saveWake;
    /**
     * Activates a new sibling, clears what splits gave away, and runs `onAlarm` when it is due. It
     * never throws: the runtime retries a failing alarm only a few times, and the sibling is
     * activated by nothing else, so a failure here reschedules the alarm itself for as long as there
     * is work left. Subclasses cannot override it (the constructor refuses): see `onAlarm`.
     */
    alarm(): Promise<void>;
    /** Deletes one batch of rows this object gave away: the base table's first, then the subclass's. */
    private cleanupBatch;
    /** Schedules this object's storage to be replaced by the snapshot, then restarts to apply it. */
    shardedRestore(snapshot: DurableObjectSnapshot): Promise<void>;
    /**
     * The logical copy's tables and indexes: replaces this object's with them. What is here is
     * not a serving shard's (an unused object, or the copy of an abandoned attempt, maybe made
     * under an older schema), so it all goes: the copy must have the source's schema exactly.
     * From here on this object takes the batches of attempt `epoch` only.
     */
    shardedReceiveSchema(_name: string, _bits: string, epoch: number, objects: readonly {
        readonly name: string;
        readonly sql: string;
    }[]): Promise<void>;
    /**
     * One batch of the logical copy, from attempt `epoch`: rows of one table. The first batch wipes
     * every table. A batch can arrive late, after its attempt was abandoned (and another began, or
     * even finished): it would put back rows as they were then, so only the batches of the attempt
     * whose schema this object took are written, and never on a serving shard.
     */
    shardedReceive(_name: string, _bits: string, epoch: number, first: boolean, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void>;
    /** The logical copy is complete: it holds the source's meta, from this split. */
    shardedReceiveDone(name: string, bits: string, epoch: number): Promise<boolean>;
    /** Storage copied verbatim from a splitting object and not claimed yet: it is not this object's. */
    private isUnclaimedCopy;
    /**
     * A shard that serves and is not splitting. Anything else may be overwritten by a split's copy:
     * an unused object, a copy (claimed or not) from an abandoned attempt.
     */
    private serving;
    /**
     * Claims the copy for the shard `bits`, if it is the copy made by attempt `epoch` of its parent:
     * becomes that shard, not serving yet, and deletes the rows of the half it does not own.
     */
    shardedClaim(name: string, bits: string, parent: string, epoch: number): Promise<boolean>;
    /** The hash ranges outside a shard's. */
    private outside;
    /** Starts serving, if this is the copy of attempt `epoch`. */
    shardedActivate(name: string, bits: string, epoch: number): Promise<boolean>;
    /** The right half of an empty shard's initial split: empty, and serving at once. */
    shardedAdoptEmpty(name: string, bits: string, parent: string): Promise<void>;
    shardedDescribe(): Promise<NodeDescription>;
}
//# sourceMappingURL=index.d.ts.map