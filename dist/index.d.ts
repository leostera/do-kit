import { DurableObject } from "cloudflare:workers";
export { SpansLeaves, SplittableClient, splittable, type LeafResult } from "./client.js";
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
/** When a leaf splits, and how. */
export interface SplitPolicy {
    /** Load, in calls/s, above which a leaf splits (a call weighs 1 + keys / keysPerCall). */
    readonly callsPerSecond: number;
    readonly windowSeconds: number;
    /** No leaf deeper than this (2^maxDepth leaves at most). */
    readonly maxDepth: number;
    /**
     * Keys that cost as much as one call: a batch of many keys is load as well. Workers cannot time
     * synchronous work (clocks move only on I/O), so keys stand in for it. Default 25.
     */
    readonly keysPerCall?: number;
    /**
     * Depth a new object splits to before its first call, while empty: for uniform keys (hashes), so
     * a fresh object starts with 2^initialDepth leaves and nothing to copy.
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
}
/**
 * Split at half of what one object takes: about 800 calls/s on the edge, so 400, sustained for
 * 5 s.
 */
export declare const DEFAULT_SPLIT_POLICY: SplitPolicy;
/** What a node answers a routed call: `moved` names a split leaf (its ancestors split too). */
export type CallAnswer<R> = {
    readonly ok: R;
} | {
    readonly moved: string;
} | {
    readonly notReady: string;
};
/** An object's state, for tests and operators. */
export interface NodeDescription {
    /** The bits of the leaf this object hosts now. */
    readonly bits: string;
    /** "copy": a split's copy that does not serve yet; "new": never used. */
    readonly state: "leaf" | "splitting" | "copy" | "new";
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
/** The bits of the object hosting a leaf: its bits without trailing zeros. */
export declare function hostBits(bits: string): string;
/** The name of the Durable Object hosting the leaf `bits` of `name`. */
export declare function leafObjectName(name: string, bits: string): string;
/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this leaf), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses that define `alarm` must call `super.alarm()`.
 */
export declare abstract class SplittableObject<Env = unknown, V = unknown> extends DurableObject<Env> {
    protected splitPolicy: SplitPolicy;
    /** The namespace this class is bound to: to reach siblings. */
    protected abstract nodes(): DurableObjectNamespace<SplittableObject<Env, V>>;
    private readonly sql;
    private meta;
    private readonly calls;
    private quietUntil;
    /** The split in progress here: writes to `right` wait for `released`. */
    private hold;
    /** An empty leaf splitting to the initial depth; calls wait for it, then are moved. */
    private presplitting;
    /** This leaf had rows when it could have presplit: it splits by load only. */
    private presplitDeclined;
    constructor(ctx: DurableObjectState, env: Env);
    /** The current rows of these keys (absent keys are missing from the map). */
    protected read(keys: readonly string[]): Promise<Map<string, V>>;
    /**
     * Reads these keys and writes what `decide` returns, in one transaction, if this leaf still owns
     * them. `decide` must be synchronous. While the right half is being copied, an update touching
     * it waits; once the copy serves, it answers "moved".
     */
    protected update<R>(keys: readonly string[], decide: (current: Map<string, V>) => UpdateResult<V, R>): Promise<R>;
    /**
     * Runs before this leaf copies its storage for a split, while writes to the half that moves
     * wait: a subclass with work queued under rows it is giving away (an outbox) finishes it here,
     * so the new leaf continues after it. It may run more than once.
     */
    protected beforeHandOut(): Promise<void>;
    /**
     * Runs in the new object of a split when it claims its copy, inside that transaction. A split
     * copies every table, the subclass's too, and the original keeps its subclass tables and the
     * work queued in them (an outbox it goes on delivering), so the copy drops its share of that
     * work here. `owns(key)` says whether a key is the new leaf's. Synchronous. The rows of
     * `read`/`update` are handled by the base class.
     */
    protected afterClone(_owns: (key: string) => boolean): void;
    /**
     * Deletes up to `limit` rows of the subclass's own tables that belong to keys this leaf no
     * longer owns, and returns how many it deleted. After a split, the alarm calls it on both
     * leaves, in paced batches, until it returns 0, as it does for the rows of `read`/`update`.
     * Until then a subclass that queries its tables in bulk (listings, counts) filters with
     * `owns`. The default deletes nothing: an original that keeps delivering an outbox for keys
     * that moved keeps those rows.
     */
    protected cleanupForeign(_owns: (key: string) => boolean, _limit: number): number;
    /** Whether `key` is this leaf's now. Keys of a split's moving half stay this leaf's until it commits. */
    protected owns(key: string): boolean;
    /** This leaf's name (`${name}#${bits}`), once it has one. */
    protected nodeLabel(): string;
    /** The leaf this one split from and when; null for the root. */
    protected parentNode(): {
        label: string;
        adoptedAt: number;
    } | null;
    /** The name all leaves of this object share (what the client was created with). */
    protected objectName(): string;
    private assertOwner;
    /** What an update of these keys waits for, if any. */
    private holding;
    /** A call routed by the client: `method(keys, payloads, ...args)` if this leaf owns the keys. */
    splittableCall(name: string, bits: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<CallAnswer<unknown>>;
    /** A call to every leaf: `method(...args)`, with no keys. */
    splittableScan(name: string, bits: string, method: string, args: readonly unknown[]): Promise<CallAnswer<unknown>>;
    /** Every row this leaf owns, for scans (a subclass's scan method may use it). */
    protected allRows(): Map<string, V>;
    /** "moved" for a call addressed to `bits`: this leaf went deeper, so `bits` and more split. */
    private moved;
    /**
     * Null if this object serves `bits` now; otherwise what to answer. Splits to the initial depth
     * first if this leaf is above it and empty: every call waits for that.
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
    /** Splits now if this object hosts the leaf `bits` (also an operator's lever). */
    splittableSplit(name: string, bits: string): Promise<boolean>;
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
     */
    private copyRows;
    /** This object's tables, the subclass's too (not SQLite's or the runtime's). */
    private tables;
    private node;
    private activateSibling;
    alarm(): Promise<void>;
    /** Schedules this object's storage to be replaced by the snapshot, then restarts to apply it. */
    splittableRestore(snapshot: DurableObjectSnapshot): Promise<void>;
    /** One batch of the logical copy: rows of one table. The first batch wipes every table. */
    splittableReceive(_name: string, _bits: string, first: boolean, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void>;
    /** The logical copy is complete: it holds the source's meta, from this split. */
    splittableReceiveDone(name: string, bits: string, epoch: number): Promise<boolean>;
    /** Storage copied verbatim from a splitting object and not claimed yet: it is not this object's. */
    private isUnclaimedCopy;
    /**
     * A leaf that serves and is not splitting. Anything else may be overwritten by a split's copy:
     * an unused object, a copy (claimed or not) from an abandoned attempt.
     */
    private serving;
    /**
     * Claims the copy for the leaf `bits`, if it is the copy made by attempt `epoch` of its parent:
     * becomes that leaf, not serving yet, and deletes the rows of the half it does not own.
     */
    splittableClaim(name: string, bits: string, parent: string, epoch: number): Promise<boolean>;
    /** The hash ranges outside a leaf's. */
    private outside;
    /** Starts serving, if this is the copy of attempt `epoch`. */
    splittableActivate(name: string, bits: string, epoch: number): Promise<boolean>;
    /** The right half of an empty leaf's initial split: empty, and serving at once. */
    splittableAdoptEmpty(name: string, bits: string, parent: string): Promise<void>;
    splittableDescribe(): Promise<NodeDescription>;
}
//# sourceMappingURL=index.d.ts.map