import { type AtomicOutcome, type Outcome } from "./atomic.js";
import { type NodeDescription, type StubOptions } from "./index.js";
export declare function objectId(namespace: DurableObjectNamespace<any>, objectName: string): {
    readonly id: DurableObjectId;
    readonly text: string;
};
/** One shard's answer: the keys it was given (in the caller's order) and what it returned. */
export interface ShardResult<R> {
    readonly keys: readonly string[];
    /** Positions of `keys` in the caller's list. */
    readonly positions: readonly number[];
    readonly result: R;
}
type AnyMethod = (...args: never[]) => unknown;
/** Methods callable with keys: `(keys, payloads, ...args)`. */
type KeyMethods<T> = {
    [K in keyof T]: K extends `sharded${string}` ? never : T[K] extends (keys: readonly string[], payloads: never, ...args: never[]) => unknown ? K : never;
}[keyof T] & string;
type ScanMethods<T> = {
    [K in keyof T]: K extends `sharded${string}` ? never : T[K] extends AnyMethod ? K : never;
}[keyof T] & string;
type Rest<F> = F extends (keys: never, payloads: never, ...args: infer A) => unknown ? A : never;
type Payload<F> = F extends (keys: never, payloads: infer P, ...args: never[]) => unknown ? (P extends readonly (infer E)[] | null ? E : never) : never;
type Result<F> = F extends (...args: never[]) => infer R ? Awaited<R> : never;
/** Keys that had to go to one shard belong to several (nothing was applied). */
export declare class SpansShards extends Error {
}
/**
 * The client and the object route keys differently (`routeOf`): nothing was called. Calls would
 * otherwise reach shards that don't hold their keys once the tree splits.
 */
export declare class RouteMismatch extends Error {
    readonly key: string;
    readonly clientHash: string;
    readonly objectHash: string;
    constructor(key: string, clientHash: string, objectHash: string);
}
/**
 * Objects whose splits an isolate keeps, the most recently used: a Worker that reaches one object
 * per account would otherwise grow these for the isolate's life. A forgotten object costs a read
 * of the colo cache, or a few hops.
 */
export declare const MAX_KNOWN_OBJECTS = 10000;
/** How a client waits. */
export interface ShardedClientOptions {
    /** How long a call waits for a shard to start serving, at least (default 30 s). */
    readonly notReadyWaitMs?: number;
    /**
     * Options for every stub of this object's shards (`namespace.get(id, stubOptions)`): a version
     * cohort, a location hint. Sent along with each call, so the stubs shards make for each other,
     * when they split, carry them too.
     */
    readonly stubOptions?: StubOptions;
    /**
     * Log a `sharded-wide-hop` JSON line for each hop of a routed call that reaches at least this
     * many shards: shards, keys, the hop's time, its calls' p10/p50/p90/slowest, and retries. A wide
     * fan-out is rare, and these tell queued calls from one slow shard. Off by default.
     */
    readonly logWideHops?: number;
    /**
     * The object's `initialDepth`, if the caller knows it (the class's split policy, usually): the
     * first call from a new isolate goes straight to that depth instead of to the root, which every
     * new isolate's first call would otherwise reach at once. A wrong hint costs one "stale" answer;
     * the client then learns the depth from the root, as without one.
     */
    readonly presplitHint?: number;
    /**
     * The object's `routeOf`: keys with the same route live in the same shard. The same function as
     * the class's (the default, the key itself, if it declares none); a call routed by another is
     * refused with RouteMismatch.
     */
    readonly routeOf?: (key: string) => string;
}
export declare class ShardedClient<T extends Rpc.DurableObjectBranded> {
    private readonly namespace;
    private readonly name;
    /** What known splits are filed under: two classes may share a name, never a root ID. */
    private readonly identity;
    private readonly notReadyWaitMs;
    private readonly stubOptions;
    private readonly logWideHops;
    private readonly presplitHint;
    private readonly routeOf;
    constructor(namespace: DurableObjectNamespace<T>, name: string, options?: ShardedClientOptions);
    private get splits();
    /** Marks this object as just used, so it is the last its isolate forgets. */
    private touch;
    /** The presplit depth known for this object (0: none known). */
    private get presplitTo();
    /** Whether the node `bits` is known to be split: above the presplit depth, or seen splitting. */
    private isSplit;
    /** The hash a key is routed by: of its route, which is the key itself by default. */
    private routeHash;
    /** The deepest known node owning a hash. */
    private shardFor;
    private stub;
    /**
     * Calls a shard through the stub this client keeps for it, making one on first use: a client
     * that calls a shard again does not open it again. A stub whose call throws is dropped, and the
     * next call makes a new one. A client belongs to the request (or the Durable Object) that made
     * it; if another request uses it, its kept stubs are that first request's and cannot be used, so
     * the call is made again on a stub of this request's (nothing was sent on the other).
     */
    private onShard;
    private keep;
    private readonly stubs;
    /**
     * Calls `method(keys, payloads, ...args)` on every shard owning some of the keys, with its share
     * of them (and of `payloads`, aligned with `keys`). One result per shard.
     */
    call<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<ShardResult<Result<T[M]>>[]>;
    /**
     * Like `call`, for keys that must be handled together by one shard (an all-or-nothing update):
     * throws SpansShards, before anything is applied, if they belong to more than one.
     */
    callOneShard<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<Result<T[M]>>;
    /** How `route` calls a shard for a method of the subclass. */
    private method;
    /** How `route` calls a shard for a step of an atomic call. */
    private atomicStep;
    /**
     * Calls `method` on the shards owning `keys` (RFD0008), all or nothing: every shard's share
     * applies, or none does. Keys in one shard: one call, as `callOneShard`. Keys in several: the
     * shard of the smallest key (the primary) records the outcome once every other shard has
     * prepared its share, holding its keys; then each applies or drops it. `method` names one of the
     * class's `atomic` methods. Refused anywhere: nothing is applied, and the refusals come back.
     * Writes to the keys wait while the call holds them; reads see the rows from before.
     */
    atomically<R = unknown, A = unknown>(keys: readonly string[], method: string, payloads: readonly unknown[] | null, ...args: unknown[]): Promise<AtomicOutcome<R, A>>;
    /**
     * An atomic call over several shards, coordinated by the primary (`coordinate`), so a share is
     * held for a round trip between shards, not between this caller and each of them. If the
     * primary's answer is lost, its outcome decides: the primary records an abort if it has none,
     * so a coordination still running then commits nothing. Committed, the results are unknown.
     */
    private atomicAtPrimary;
    /**
     * The coordination of an atomic call over several shards, as the primary runs it (the
     * `coordinate` step): not for callers, who use `atomically`. Nothing applied, to be retried:
     * "again" if a split moved the primary's keys apart before the commit, "contended" if a shard
     * gave way to an older call.
     * @internal
     */
    coordinateAtomic<R, A>(tx: string, keys: readonly string[], method: string, payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicOutcome<R, A> | "again" | "contended">;
    /** The outcome of atomic call `tx`, from its primary (recording an abort if it has none). */
    atomicOutcome(tx: string, primaryKey: string): Promise<Outcome>;
    /** A step at the primary of `tx`. */
    private atomicTo;
    /** Records an abort at the primary, if it can: else the holds end at their deadline, aborted. */
    private decideAbort;
    /** Tells every shard that prepared the outcome; each that applied answers its result. A lost one recovers at its deadline. */
    private resolveAll;
    private route;
    /** Calls `method(...args)` on every shard. */
    scan<M extends ScanMethods<T>>(method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<Result<T[M]>[]>;
    /**
     * Deletes every shard's storage and alarm: the whole sharded object, for one that is done with.
     * Waits out splits in progress (both halves are then wiped). Nothing may write to the object
     * meanwhile: a write after its shard is wiped would make that shard again. The root's object
     * goes last, since it is how the others are found: a wipe that fails partway can run again.
     * Returns how many shards held something.
     */
    wipe(): Promise<number>;
    /** How many shards this client knows of: one more than its known splits. There may be more. */
    knownShardCount(): Promise<number>;
    /** The known splits seen in answers (for tests and operators); the presplit levels are not listed. */
    knownSplits(): string[];
    /** The presplit depth this client knows of the object (0: none known). */
    knownPresplit(): number;
    /** Drops what this client knows of the object's splits, here and in the colo: it was stale. */
    private forget;
    private learn;
    /**
     * Every shard of this tree, as its objects describe themselves (for operators): each shard at
     * the presplit depth (this client's, else the one the root reports), then down every split. A
     * shard never called is described as "new", and the walk goes on past it: in a presplit tree,
     * shard 10 may never have been called while 11 has, and a walk down from the root stops at 10.
     * Asks the objects, not this client's map of splits, so it sees splits no client here has met.
     */
    describeTree(): Promise<ShardDescription[]>;
    private loadCache;
    private saveCache;
}
/** One shard of a tree, as its object describes itself, with the shard's bits and the object's ID. */
export interface ShardDescription extends NodeDescription {
    /** The object's Durable Object ID. */
    readonly object: string;
}
export declare function sharded<T extends Rpc.DurableObjectBranded>(namespace: DurableObjectNamespace<T>, name: string, options?: ShardedClientOptions): ShardedClient<T>;
export {};
//# sourceMappingURL=client.d.ts.map