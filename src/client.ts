// The client side of splittable objects (RFD0009): routes keys to leaves through the splits it
// knows, learns new ones from "moved" answers, and shares what it learned in the isolate and the
// colo (Cache API). Splits are final, so stale knowledge costs hops, never correctness.
import { hashBit, keyHash, nodeName, type CallAnswer, type SplittableObject } from "./index.ts";

/** One leaf's answer: the keys it was given (in the caller's order) and what it returned. */
export interface LeafResult<R> {
  readonly keys: readonly string[];
  /** Positions of `keys` in the caller's list. */
  readonly positions: readonly number[];
  readonly result: R;
}

type AnyMethod = (...args: never[]) => unknown;
/** Methods callable with keys: `(keys, payloads, ...args)`. */
type KeyMethods<T> = { [K in keyof T]: K extends `splittable${string}` ? never : T[K] extends (keys: readonly string[], payloads: never, ...args: never[]) => unknown ? K : never }[keyof T] & string;
type ScanMethods<T> = { [K in keyof T]: K extends `splittable${string}` ? never : T[K] extends AnyMethod ? K : never }[keyof T] & string;
type Rest<F> = F extends (keys: never, payloads: never, ...args: infer A) => unknown ? A : never;
type Payload<F> = F extends (keys: never, payloads: infer P, ...args: never[]) => unknown ? (P extends readonly (infer E)[] | null ? E : never) : never;
type Result<F> = F extends (...args: never[]) => infer R ? Awaited<R> : never;

/** Keys that had to go to one leaf belong to several (nothing was applied). */
export class SpansLeaves extends Error {}

const CACHE_ORIGIN = "https://splittable-object.invalid/";
/**
 * Splits known per object (keyed by its root's Durable Object ID, unique per namespace and
 * name), shared by every client in this isolate.
 */
const knownSplits = new Map<string, Set<string>>();
const loadedFromCache = new Set<string>();

/** A hop limit: deeper than any maxDepth, so a loop is a bug, not a slow call. */
const MAX_HOPS = 64;

export class SplittableClient<T extends Rpc.DurableObjectBranded> {
  private readonly namespace: DurableObjectNamespace<T>;
  private readonly name: string;
  /** What known splits are filed under: two classes may share a name, never a root ID. */
  private readonly identity: string;

  constructor(namespace: DurableObjectNamespace<T>, name: string) {
    this.namespace = namespace;
    this.name = name;
    this.identity = namespace.idFromName(nodeName(name, "")).toString();
  }

  private get splits(): Set<string> {
    let set = knownSplits.get(this.identity);
    if (!set) knownSplits.set(this.identity, (set = new Set()));
    return set;
  }

  /** The deepest known node owning a hash. */
  private leafFor(hash: string): string {
    let bits = "";
    while (this.splits.has(bits)) bits += hashBit(hash, bits.length);
    return bits;
  }

  private stub(bits: string): DurableObjectStub<SplittableObject<never, unknown>> {
    const ns = this.namespace as unknown as DurableObjectNamespace<SplittableObject<never, unknown>>;
    return ns.get(ns.idFromName(nodeName(this.name, bits)));
  }

  /**
   * Calls `method(keys, payloads, ...args)` on every leaf owning some of the keys, with its share
   * of them (and of `payloads`, aligned with `keys`). One result per leaf.
   */
  async call<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<LeafResult<Result<T[M]>>[]> {
    return this.route(false, keys, method, payloads, args);
  }

  /**
   * Like `call`, for keys that must be handled together by one leaf (an all-or-nothing update):
   * throws SpansLeaves, before anything is applied, if they belong to more than one.
   */
  async callOneLeaf<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<Result<T[M]>> {
    const [only] = await this.route(true, keys, method, payloads, args);
    return only!.result;
  }

  private async route<M extends KeyMethods<T>>(oneLeaf: boolean, keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, args: Rest<T[M]>): Promise<LeafResult<Result<T[M]>>[]> {
    await this.loadCache();
    const hashes = keys.map(keyHash);
    const out: LeafResult<Result<T[M]>>[] = [];
    let pending = keys.map((_, i) => i);
    for (let hop = 0; pending.length > 0; hop++) {
      if (hop > MAX_HOPS) throw new Error(`splittable ${this.name}: routing did not settle`);
      const groups = new Map<string, number[]>();
      for (const i of pending) {
        const bits = this.leafFor(hashes[i]!);
        let group = groups.get(bits);
        if (!group) groups.set(bits, (group = []));
        group.push(i);
      }
      if (oneLeaf && groups.size > 1) throw new SpansLeaves(`keys of ${this.name} belong to ${groups.size} leaves`);
      const retry: number[] = [];
      await Promise.all(
        [...groups].map(async ([bits, positions]) => {
          const groupKeys = positions.map((i) => keys[i]!);
          const groupPayloads = payloads ? positions.map((i) => payloads[i]) : null;
          const answer = (await this.stub(bits).splittableCall(this.name, bits, method, groupKeys, groupPayloads, args)) as CallAnswer<Result<T[M]>>;
          if ("ok" in answer) out.push({ keys: groupKeys, positions, result: answer.ok });
          else if ("moved" in answer) {
            this.learn(answer.moved);
            retry.push(...positions);
          } else throw new Error(`splittable ${this.name}: ${answer.unknown} was never created`);
        }),
      );
      pending = retry;
    }
    return out;
  }

  /** Calls `method(...args)` on every leaf. */
  async scan<M extends ScanMethods<T>>(method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<Result<T[M]>[]> {
    await this.loadCache();
    const out: Result<T[M]>[] = [];
    const visit = async (bits: string, depth: number): Promise<void> => {
      if (depth > MAX_HOPS) throw new Error(`splittable ${this.name}: scan did not settle`);
      if (this.splits.has(bits)) {
        await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
        return;
      }
      const answer = (await this.stub(bits).splittableScan(this.name, bits, method, args)) as CallAnswer<Result<T[M]>>;
      if ("ok" in answer) out.push(answer.ok);
      else if ("moved" in answer) {
        this.learn(answer.moved);
        await visit(bits, depth + 1);
      } else throw new Error(`splittable ${this.name}: ${answer.unknown} was never created`);
    };
    await visit("", 0);
    return out;
  }

  /** The known splits (for tests and operators). */
  knownSplits(): string[] {
    return [...this.splits].sort();
  }

  private learn(bits: string): void {
    if (this.splits.has(bits)) return;
    // A node that split had split ancestors: all of them are known to be split.
    for (let i = 0; i <= bits.length; i++) this.splits.add(bits.slice(0, i));
    void this.saveCache();
  }

  private async loadCache(): Promise<void> {
    if (loadedFromCache.has(this.identity) || typeof caches === "undefined") return;
    loadedFromCache.add(this.identity);
    try {
      const hit = await (caches as CacheStorage & { default: Cache }).default.match(CACHE_ORIGIN + this.identity);
      if (hit) for (const bits of (await hit.json()) as string[]) this.splits.add(bits);
    } catch {
      // the cache is an optimisation only
    }
  }

  private async saveCache(): Promise<void> {
    if (typeof caches === "undefined") return;
    try {
      const body = JSON.stringify([...this.splits]);
      await (caches as CacheStorage & { default: Cache }).default.put(CACHE_ORIGIN + this.identity, new Response(body, { headers: { "cache-control": "max-age=86400", "content-type": "application/json" } }));
    } catch {
      // the cache is an optimisation only
    }
  }
}

export function splittable<T extends Rpc.DurableObjectBranded>(namespace: DurableObjectNamespace<T>, name: string): SplittableClient<T> {
  return new SplittableClient(namespace, name);
}
