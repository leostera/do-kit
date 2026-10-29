/** One leaf's answer: the keys it was given (in the caller's order) and what it returned. */
export interface LeafResult<R> {
    readonly keys: readonly string[];
    /** Positions of `keys` in the caller's list. */
    readonly positions: readonly number[];
    readonly result: R;
}
type AnyMethod = (...args: never[]) => unknown;
/** Methods callable with keys: `(keys, payloads, ...args)`. */
type KeyMethods<T> = {
    [K in keyof T]: K extends `splittable${string}` ? never : T[K] extends (keys: readonly string[], payloads: never, ...args: never[]) => unknown ? K : never;
}[keyof T] & string;
type ScanMethods<T> = {
    [K in keyof T]: K extends `splittable${string}` ? never : T[K] extends AnyMethod ? K : never;
}[keyof T] & string;
type Rest<F> = F extends (keys: never, payloads: never, ...args: infer A) => unknown ? A : never;
type Payload<F> = F extends (keys: never, payloads: infer P, ...args: never[]) => unknown ? (P extends readonly (infer E)[] | null ? E : never) : never;
type Result<F> = F extends (...args: never[]) => infer R ? Awaited<R> : never;
/** Keys that had to go to one leaf belong to several (nothing was applied). */
export declare class SpansLeaves extends Error {
}
export declare class SplittableClient<T extends Rpc.DurableObjectBranded> {
    private readonly namespace;
    private readonly name;
    /** What known splits are filed under: two classes may share a name, never a root ID. */
    private readonly identity;
    constructor(namespace: DurableObjectNamespace<T>, name: string);
    private get splits();
    /** The deepest known node owning a hash. */
    private leafFor;
    private stub;
    /**
     * Calls `method(keys, payloads, ...args)` on every leaf owning some of the keys, with its share
     * of them (and of `payloads`, aligned with `keys`). One result per leaf.
     */
    call<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<LeafResult<Result<T[M]>>[]>;
    /**
     * Like `call`, for keys that must be handled together by one leaf (an all-or-nothing update):
     * throws SpansLeaves, before anything is applied, if they belong to more than one.
     */
    callOneLeaf<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<Result<T[M]>>;
    private route;
    /** Calls `method(...args)` on every leaf. */
    scan<M extends ScanMethods<T>>(method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<Result<T[M]>[]>;
    /** The known splits (for tests and operators). */
    knownSplits(): string[];
    private learn;
    private loadCache;
    private saveCache;
}
export declare function splittable<T extends Rpc.DurableObjectBranded>(namespace: DurableObjectNamespace<T>, name: string): SplittableClient<T>;
export {};
//# sourceMappingURL=client.d.ts.map