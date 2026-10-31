// The client side of sharded objects: routes keys to shards through the splits it
// knows, learns new ones from "moved" answers, and shares what it learned in the isolate and the
// colo (Cache API). Splits are final, so stale knowledge costs hops, never correctness.
import { hashBit, keyHash, shardObjectName, nodeName, type CallAnswer, type ShardedObject, type StubOptions } from "./index.js";

/** One shard's answer: the keys it was given (in the caller's order) and what it returned. */
export interface ShardResult<R> {
  readonly keys: readonly string[];
  /** Positions of `keys` in the caller's list. */
  readonly positions: readonly number[];
  readonly result: R;
}

type AnyMethod = (...args: never[]) => unknown;
/** Methods callable with keys: `(keys, payloads, ...args)`. */
type KeyMethods<T> = { [K in keyof T]: K extends `sharded${string}` ? never : T[K] extends (keys: readonly string[], payloads: never, ...args: never[]) => unknown ? K : never }[keyof T] & string;
type ScanMethods<T> = { [K in keyof T]: K extends `sharded${string}` ? never : T[K] extends AnyMethod ? K : never }[keyof T] & string;
type Rest<F> = F extends (keys: never, payloads: never, ...args: infer A) => unknown ? A : never;
type Payload<F> = F extends (keys: never, payloads: infer P, ...args: never[]) => unknown ? (P extends readonly (infer E)[] | null ? E : never) : never;
type Result<F> = F extends (...args: never[]) => infer R ? Awaited<R> : never;

/** Keys that had to go to one shard belong to several (nothing was applied). */
export class SpansShards extends Error {}

/** v2: the splits and the presplit depth (v1 entries, splits only, are left to expire). */
const CACHE_ORIGIN = "https://do-kit.invalid/v2/";
/**
 * Splits known per object (keyed by its root's Durable Object ID, unique per namespace and
 * name), shared by every client in this isolate.
 */
const knownSplits = new Map<string, Set<string>>();
/**
 * Each object's presplit depth, once a shard said so: every node above it is split (it never
 * existed), so calls go straight to that depth. Without it, each isolate would ask the shards
 * above, one hop per level, for every path: the top ones are shared by all keys and overload.
 */
const knownPresplit = new Map<string, number>();
const loadedFromCache = new Set<string>();
/**
 * Objects whose splits an isolate keeps, the most recently used: a Worker that reaches one object
 * per account would otherwise grow these for the isolate's life. A forgotten object costs a read
 * of the colo cache, or a few hops.
 */
export const MAX_KNOWN_OBJECTS = 10_000;

/** A hop limit: deeper than any maxDepth, so a loop is a bug, not a slow call. */
const MAX_HOPS = 64;
/**
 * How long a call waits for a shard to serve, by default: a new shard is activated just after a
 * split. A write held by a split in progress waits until that split is done or gives up.
 */
const NOT_READY_WAIT_MS = 30_000;
/** Past a split's deadline, how long a held write still waits for the new shard to be activated. */
const ACTIVATION_SLACK_MS = 5_000;
/** The furthest a shard's "until" can push a call's wait, whatever it says. */
const MAX_HELD_WAIT_MS = 10 * 60_000;

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
/**
 * After waiting this long on shards that are not ready, a call forgets the splits it knows of this
 * object, once, and routes from the root again: an object whose storage was deleted is not ready
 * forever, and only the root can tell. A shard that is only slow to start costs a few hops.
 */
const FORGET_AFTER_MS = 1_000;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Waits a little longer each time a shard is not ready; throws once that has gone on too long. */
class Backoff {
  private wait = 5;
  waited = 0;
  /** A split in progress said it is done or gives up by then: wait at least that long. */
  private until = 0;
  constructor(
    private readonly what: string,
    private readonly limitMs: number,
  ) {}
  /** A shard holding the call for a split that gives up at `until` (its clock). */
  extend(until: number | undefined): void {
    if (until !== undefined) this.until = Math.max(this.until, Math.min(until, Date.now() + MAX_HELD_WAIT_MS));
  }
  async next(): Promise<void> {
    if (this.waited > this.limitMs && Date.now() > this.until + ACTIVATION_SLACK_MS) throw new Error(`${this.what}: a new shard did not start serving`);
    // Jittered: callers held by one split are released together, and must not retry in step.
    const wait = this.wait / 2 + Math.random() * this.wait;
    await sleep(wait);
    this.waited += wait;
    this.wait = Math.min(this.wait * 2, 250);
  }
}

export class ShardedClient<T extends Rpc.DurableObjectBranded> {
  private readonly namespace: DurableObjectNamespace<T>;
  private readonly name: string;
  /** What known splits are filed under: two classes may share a name, never a root ID. */
  private readonly identity: string;

  private readonly notReadyWaitMs: number;
  private readonly stubOptions: StubOptions | null;

  constructor(namespace: DurableObjectNamespace<T>, name: string, options: ShardedClientOptions = {}) {
    this.namespace = namespace;
    this.name = name;
    this.notReadyWaitMs = options.notReadyWaitMs ?? NOT_READY_WAIT_MS;
    this.stubOptions = options.stubOptions ?? null;
    this.identity = namespace.idFromName(nodeName(name, "")).toString();
  }

  private get splits(): Set<string> {
    let set = knownSplits.get(this.identity);
    if (!set) {
      knownSplits.set(this.identity, (set = new Set()));
      if (knownSplits.size > MAX_KNOWN_OBJECTS) {
        const oldest = knownSplits.keys().next().value!;
        knownSplits.delete(oldest);
        knownPresplit.delete(oldest);
        loadedFromCache.delete(oldest);
      }
    }
    return set;
  }

  /** Marks this object as just used, so it is the last its isolate forgets. */
  private touch(): void {
    const set = knownSplits.get(this.identity);
    if (!set) return;
    knownSplits.delete(this.identity);
    knownSplits.set(this.identity, set);
  }

  /** The presplit depth known for this object (0: none known). */
  private get presplitTo(): number {
    return knownPresplit.get(this.identity) ?? 0;
  }

  /** Whether the node `bits` is known to be split: above the presplit depth, or seen splitting. */
  private isSplit(bits: string): boolean {
    return bits.length < this.presplitTo || this.splits.has(bits);
  }

  /** The deepest known node owning a hash. */
  private shardFor(hash: string): string {
    let bits = "";
    while (this.isSplit(bits)) bits += hashBit(hash, bits.length);
    return bits;
  }

  private stub(bits: string): DurableObjectStub<ShardedObject<never, unknown>> {
    const ns = this.namespace as unknown as DurableObjectNamespace<ShardedObject<never, unknown>>;
    const id = ns.idFromName(shardObjectName(this.name, bits));
    return this.stubOptions ? ns.get(id, this.stubOptions) : ns.get(id);
  }

  /**
   * Calls `method(keys, payloads, ...args)` on every shard owning some of the keys, with its share
   * of them (and of `payloads`, aligned with `keys`). One result per shard.
   */
  async call<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<ShardResult<Result<T[M]>>[]> {
    return this.route(false, keys, method, payloads, args);
  }

  /**
   * Like `call`, for keys that must be handled together by one shard (an all-or-nothing update):
   * throws SpansShards, before anything is applied, if they belong to more than one.
   */
  async callOneShard<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<Result<T[M]>> {
    if (keys.length === 0) throw new Error(`sharded ${this.name}: callOneShard needs at least one key to pick its shard`);
    const [only] = await this.route(true, keys, method, payloads, args);
    return only!.result;
  }

  private async route<M extends KeyMethods<T>>(oneShard: boolean, keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, args: Rest<T[M]>): Promise<ShardResult<Result<T[M]>>[]> {
    if (payloads && payloads.length !== keys.length) throw new Error(`sharded ${this.name}: ${payloads.length} payloads for ${keys.length} keys (payloads line up with keys)`);
    await this.loadCache();
    const hashes = keys.map(keyHash);
    const out: ShardResult<Result<T[M]>>[] = [];
    let pending = keys.map((_, i) => i);
    const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
    let forgot = false;
    for (let hop = 0; pending.length > 0; hop++) {
      if (hop > MAX_HOPS) throw new Error(`sharded ${this.name}: routing did not settle`);
      let waiting = false;
      let stale = false;
      const groups = new Map<string, number[]>();
      for (const i of pending) {
        const bits = this.shardFor(hashes[i]!);
        let group = groups.get(bits);
        if (!group) groups.set(bits, (group = []));
        group.push(i);
      }
      if (oneShard && groups.size > 1) throw new SpansShards(`keys of ${this.name} belong to ${groups.size} shards`);
      const retry: number[] = [];
      await Promise.all(
        [...groups].map(async ([bits, positions]) => {
          const groupKeys = positions.map((i) => keys[i]!);
          const groupPayloads = payloads ? positions.map((i) => payloads[i]) : null;
          const answer = (await this.stub(bits).shardedCall(this.name, bits, method, groupKeys, groupPayloads, args, this.stubOptions, this.presplitTo)) as CallAnswer<Result<T[M]>>;
          if ("ok" in answer) out.push({ keys: groupKeys, positions, result: answer.ok });
          else if ("moved" in answer) {
            this.learn(answer.moved, answer.presplit);
            retry.push(...positions);
          } else if ("stale" in answer) {
            stale = true;
            retry.push(...positions);
          } else {
            waiting = true; // not serving yet: the same shard, a little later
            backoff.extend(answer.until);
            retry.push(...positions);
          }
        }),
      );
      if (stale || (waiting && !forgot && backoff.waited >= FORGET_AFTER_MS)) {
        forgot = true;
        this.forget();
      } else if (waiting) {
        await backoff.next();
        hop--;
      }
      pending = retry;
    }
    return out;
  }

  /** Calls `method(...args)` on every shard. */
  async scan<M extends ScanMethods<T>>(method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<Result<T[M]>[]> {
    await this.loadCache();
    for (let attempt = 0; ; attempt++) {
      const out: Result<T[M]>[] = [];
      const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
      let stale = false;
      const visit = async (bits: string, depth: number): Promise<void> => {
        if (depth > MAX_HOPS) throw new Error(`sharded ${this.name}: scan did not settle`);
        if (stale) return;
        if (this.isSplit(bits)) {
          await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
          return;
        }
        const answer = (await this.stub(bits).shardedScan(this.name, bits, method, args, this.stubOptions, this.presplitTo)) as CallAnswer<Result<T[M]>>;
        if ("ok" in answer) out.push(answer.ok);
        else if ("moved" in answer) {
          this.learn(answer.moved, answer.presplit);
          await visit(bits, depth + 1);
        } else if ("stale" in answer) {
          // A scan always reaches the root (its leftmost shard), which says so when the map is
          // stale: start over from it, once the other visits stop.
          stale = true;
        } else {
          await backoff.next();
          await visit(bits, depth);
        }
      };
      await visit("", 0);
      if (!stale) return out;
      if (attempt >= 3) throw new Error(`sharded ${this.name}: scan did not settle`);
      this.forget();
    }
  }

  /** How many shards this client knows of: one more than its known splits. There may be more. */
  async knownShardCount(): Promise<number> {
    await this.loadCache();
    let splits = 0;
    const count = (bits: string): void => {
      if (!this.isSplit(bits)) return;
      splits++;
      count(`${bits}0`);
      count(`${bits}1`);
    };
    count("");
    return splits + 1;
  }

  /** The known splits seen in answers (for tests and operators); the presplit levels are not listed. */
  knownSplits(): string[] {
    return [...this.splits].sort();
  }

  /** The presplit depth this client knows of the object (0: none known). */
  knownPresplit(): number {
    return this.presplitTo;
  }

  /** Drops what this client knows of the object's splits, here and in the colo: it was stale. */
  private forget(): void {
    this.splits.clear();
    knownPresplit.delete(this.identity);
    void this.saveCache();
  }

  private learn(bits: string, presplit: number | undefined): void {
    const newDepth = presplit !== undefined && presplit !== this.presplitTo;
    if (newDepth) knownPresplit.set(this.identity, presplit);
    if (this.isSplit(bits) && !newDepth) return;
    // A node that split had split ancestors: all of them are known to be split. Those above the
    // presplit depth are known without listing them.
    for (let i = this.presplitTo; i <= bits.length; i++) this.splits.add(bits.slice(0, i));
    void this.saveCache();
  }

  private async loadCache(): Promise<void> {
    this.touch();
    if (loadedFromCache.has(this.identity) || typeof caches === "undefined") return;
    loadedFromCache.add(this.identity);
    try {
      const hit = await (caches as CacheStorage & { default: Cache }).default.match(CACHE_ORIGIN + this.identity);
      if (!hit) return;
      const cached = (await hit.json()) as { splits: string[]; presplit: number };
      for (const bits of cached.splits) this.splits.add(bits);
      if (cached.presplit > 0 && !knownPresplit.has(this.identity)) knownPresplit.set(this.identity, cached.presplit);
    } catch {
      // the cache is an optimisation only
    }
  }

  private async saveCache(): Promise<void> {
    if (typeof caches === "undefined") return;
    try {
      const body = JSON.stringify({ splits: [...this.splits], presplit: this.presplitTo });
      await (caches as CacheStorage & { default: Cache }).default.put(CACHE_ORIGIN + this.identity, new Response(body, { headers: { "cache-control": "max-age=86400", "content-type": "application/json" } }));
    } catch {
      // the cache is an optimisation only
    }
  }
}

export function sharded<T extends Rpc.DurableObjectBranded>(namespace: DurableObjectNamespace<T>, name: string, options: ShardedClientOptions = {}): ShardedClient<T> {
  return new ShardedClient(namespace, name, options);
}
