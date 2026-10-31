import { type StubOptions } from "./index.js";
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
}
export declare class ShardedClient<T extends Rpc.DurableObjectBranded> {
    private readonly namespace;
    private readonly name;
    /** What known splits are filed under: two classes may share a name, never a root ID. */
    private readonly identity;
    private readonly notReadyWaitMs;
    private readonly stubOptions;
    constructor(namespace: DurableObjectNamespace<T>, name: string, options?: ShardedClientOptions);
    private get splits();
    /** Marks this object as just used, so it is the last its isolate forgets. */
    private touch;
    /** The presplit depth known for this object (0: none known). */
    private get presplitTo();
    /** Whether the node `bits` is known to be split: above the presplit depth, or seen splitting. */
    private isSplit;
    /** The deepest known node owning a hash. */
    private shardFor;
    private stub;
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
    private route;
    /** Calls `method(...args)` on every shard. */
    scan<M extends ScanMethods<T>>(method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<Result<T[M]>[]>;
    /** How many shards this client knows of: one more than its known splits. There may be more. */
    knownShardCount(): Promise<number>;
    /** The known splits seen in answers (for tests and operators); the presplit levels are not listed. */
    knownSplits(): string[];
    /** The presplit depth this client knows of the object (0: none known). */
    knownPresplit(): number;
    /** Drops what this client knows of the object's splits, here and in the colo: it was stale. */
    private forget;
    private learn;
    private loadCache;
    private saveCache;
}
export declare function sharded<T extends Rpc.DurableObjectBranded>(namespace: DurableObjectNamespace<T>, name: string, options?: ShardedClientOptions): ShardedClient<T>;
export {};
//# sourceMappingURL=client.d.ts.map