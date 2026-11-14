// The client side of sharded objects: routes keys to shards through the splits it
// knows, learns new ones from "moved" answers, and shares what it learned in the isolate and the
// colo (Cache API). Splits are final, so stale knowledge costs hops, never correctness.
import { ATOMIC_HOLD_MS, atomicTxId, type AtomicAnswer, type AtomicOutcome, type AtomicShardResult, type AtomicStep, type Outcome } from "./atomic.js";
import { assertKey } from "./values.js";
import { hashBit, keyHash } from "./hash.js";
import { hostBits, shardObjectName, nodeName, type CallAnswer, type NodeDescription, type ShardedObject, type StubOptions } from "./index.js";

/**
 * Object ids by name, per namespace binding: computing one from a name is not free, and a client
 * asks for the same shards' ids on every call. Bounded; a namespace made per call (one scoped to a
 * jurisdiction) is simply not remembered.
 */
const knownIds = new WeakMap<object, Map<string, { readonly id: DurableObjectId; readonly text: string }>>();
const MAX_KNOWN_IDS = 20_000;
/** Stubs a client keeps, one per shard it called. */
const MAX_KEPT_STUBS = 256;
export function objectId(namespace: DurableObjectNamespace<any>, objectName: string): { readonly id: DurableObjectId; readonly text: string } {
  let ids = knownIds.get(namespace);
  if (!ids) knownIds.set(namespace, (ids = new Map()));
  let known = ids.get(objectName);
  if (!known) {
    if (ids.size >= MAX_KNOWN_IDS) ids.clear();
    const id = namespace.idFromName(objectName);
    ids.set(objectName, (known = { id, text: id.toString() }));
  }
  return known;
}

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

/** One routed call to one shard, for `route`: a method of the subclass, or a step of an atomic call. */
type Invoke = (stub: DurableObjectStub<ShardedObject<never, unknown>>, bits: string, keys: readonly string[], payloads: readonly unknown[] | null, hashes: readonly string[], deadline: number | null) => Promise<CallAnswer<unknown>>;
/** What resolving an atomic call's shares returned: whether every shard answered, and the results of those that applied. */
type Resolutions = { all: boolean; results: AtomicShardResult<unknown>[] };

/** Keys that had to go to one shard belong to several (nothing was applied). */
export class SpansShards extends Error {}

/**
 * The client and the object route keys differently (`routeOf`): nothing was called. Calls would
 * otherwise reach shards that don't hold their keys once the tree splits.
 */
export class RouteMismatch extends Error {
  constructor(
    readonly key: string,
    readonly clientHash: string,
    readonly objectHash: string,
  ) {
    super(`key ${JSON.stringify(key)}: the client routes it to hash ${clientHash}, the object to ${objectHash}; declare the same routeOf on both`);
    this.name = "RouteMismatch";
  }
}

/**
 * A shard answered something this client does not know: the object runs a newer release than
 * the client, or a bug. An error at once, not a wait: a client that took it for "not ready"
 * would wait out its whole limit for nothing.
 */
export class UnknownAnswer extends Error {
  constructor(
    readonly bits: string,
    readonly answer: unknown,
  ) {
    super(`shard ${bits || "root"} answered ${String(JSON.stringify(answer)).slice(0, 200)}, which this client does not know: deploy the objects' release to clients too`);
    this.name = "UnknownAnswer";
  }
}

/**
 * A shard did not answer within `callTimeoutMs`: its call may have applied or not. The shard
 * refuses it if it gets to it later, past the call's deadline, so it will not apply then.
 */
export class OutcomeUnknown extends Error {
  constructor(
    readonly bits: string,
    readonly keys: readonly string[],
    readonly ms: number,
  ) {
    super(`shard ${bits || "root"} did not answer in ${ms} ms, for ${keys.length} key(s): the outcome is unknown`);
    this.name = "OutcomeUnknown";
  }
}

/** A shard got to a call past its deadline, and refused it: nothing was applied there. */
export class DeadlineExpired extends Error {
  constructor(
    readonly bits: string,
    readonly keys: readonly string[],
  ) {
    super(`shard ${bits || "root"} got to the call past its deadline: not applied`);
    this.name = "DeadlineExpired";
  }
}

/**
 * What kind of failure a shard call's error is: "overloaded" (the runtime shed it; it did not
 * run), "unknown" (the connection or the runtime failed: it may have run), or "app" (the method
 * threw). Reads `overloaded` and `retryable` on the error first, then the runtime's messages,
 * with or without the `jsg.Error: ` or `Error: ` they come with.
 */
export type ShardErrorKind = "overloaded" | "unknown" | "app";
export function classifyShardError(error: unknown): ShardErrorKind {
  const e = error as { overloaded?: unknown; retryable?: unknown; message?: unknown } | null;
  if (e?.overloaded === true) return "overloaded";
  if (error instanceof OutcomeUnknown) return "unknown";
  const message = String(e?.message ?? error).replace(/^(jsg\.)?Error: /, "");
  if (/^Durable Object is overloaded|^too many concurrent storage operations|Too many requests queued/i.test(message)) return "overloaded";
  if (/^Network connection lost|^internal error; reference =/i.test(message)) return "unknown";
  if (e?.retryable === true) return "unknown";
  return "app";
}

/** The runtime's refusal of a stub made in another request: the call was never sent. */
const OTHER_REQUEST = /^Cannot perform I\/O on behalf of a different request/;

/**
 * The failure of a call over several shards: the error of the first shard that failed, with
 * what every shard did. `settled` holds the answers of the shards that returned (their share
 * applied); `unknown` the keys of those that failed, which may have applied as well. Attached to
 * the error itself, so its type and message stay the shard's.
 */
export interface CallOutcomes<R> {
  readonly settled: readonly ShardResult<R>[];
  readonly unknown: readonly { readonly keys: readonly string[]; readonly positions: readonly number[]; readonly error: string }[];
}

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
/** Objects whose root said a client's `presplitHint` was wrong: they learn the depth from the root. */
const hintRejected = new Set<string>();
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
  /**
   * Log a `sharded-wide-hop` JSON line for each hop of a routed call that reaches at least this
   * many shards: shards, keys, the hop's time, its calls' p10/p50/p90/slowest, and retries. A wide
   * fan-out is rare, and these tell queued calls from one slow shard. Off by default.
   */
  readonly logWideHops?: number;
  /**
   * Log a `sharded-shard-error` JSON line (console.error) for each call to a shard that throws:
   * the object, the shard's bits and the error. A call over many shards fails with one shard's
   * error, which does not say which shard: these tell one overloaded shard from many. Off by
   * default.
   */
  readonly logShardErrors?: boolean;
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
  /**
   * How long a call waits for a shard's answer before it throws OutcomeUnknown, naming the shard
   * and its keys. The call carries a deadline a little before that: a shard that gets to it later
   * (a write held by a split, say) refuses it, so a call given up on never applies afterwards.
   * Off by default: a call waits as long as its shard takes.
   */
  readonly callTimeoutMs?: number;
}

/** How long before the caller's own timer a call's deadline is (at most half the timeout): the shard's clock may be behind. */
const DEADLINE_MARGIN_MS = 50;
/**
 * After waiting this long on shards that are not ready, a call forgets the splits it knows of this
 * object, once, and routes from the root again: an object whose storage was deleted is not ready
 * forever, and only the root can tell. A shard that is only slow to start costs a few hops.
 */
const FORGET_AFTER_MS = 1_000;

/** How long a hop's calls took: a few quantiles, which tell queued calls from one slow one. */
function spread(took: number[]): Record<string, number> {
  const sorted = [...took].sort((a, b) => a - b);
  const at = (q: number) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
  return { p10Ms: at(0.1), p50Ms: at(0.5), p90Ms: at(0.9), slowestMs: at(1) };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Calls a scan keeps in flight at most: a scan of a wide tree would otherwise open one per shard at once. */
const SCAN_IN_FLIGHT = 32;

/** At most `size` tasks at once; the rest wait their turn, in order. */
class Slots {
  private busy = 0;
  private readonly waiting: (() => void)[] = [];
  constructor(private readonly size: number) {}
  async run<A>(task: () => Promise<A>): Promise<A> {
    if (this.busy >= this.size) await new Promise<void>((resolve) => this.waiting.push(resolve));
    this.busy++;
    try {
      return await task();
    } finally {
      this.busy--;
      this.waiting.shift()?.();
    }
  }
}

/** Waits a little longer each time a shard is not ready; throws once that has gone on too long. */
class Backoff {
  private wait = 5;
  waited = 0;
  /** A split in progress said it is done or gives up by then: wait at least that long. */
  private until = 0;
  constructor(
    private readonly what: string,
    private readonly limitMs: number,
    private readonly reason = "a new shard did not start serving",
  ) {}
  /** A shard holding the call for a split that gives up at `until` (its clock). */
  extend(until: number | undefined): void {
    if (until !== undefined) this.until = Math.max(this.until, Math.min(until, Date.now() + MAX_HELD_WAIT_MS));
  }
  async next(): Promise<void> {
    if (this.waited > this.limitMs && Date.now() > this.until + ACTIVATION_SLACK_MS) throw new Error(`${this.what}: ${this.reason}`);
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
  private readonly logWideHops: number;
  private readonly logShardErrors: boolean;

  private readonly presplitHint: number;
  private readonly routeOf: ((key: string) => string) | null;
  private readonly callTimeoutMs: number | null;
  constructor(namespace: DurableObjectNamespace<T>, name: string, options: ShardedClientOptions = {}) {
    this.namespace = namespace;
    this.name = name;
    this.notReadyWaitMs = options.notReadyWaitMs ?? NOT_READY_WAIT_MS;
    this.stubOptions = options.stubOptions ?? null;
    this.logWideHops = options.logWideHops ?? 0;
    this.logShardErrors = options.logShardErrors ?? false;
    this.presplitHint = options.presplitHint ?? 0;
    this.routeOf = options.routeOf ?? null;
    if (options.callTimeoutMs !== undefined && !(options.callTimeoutMs > 0 && options.callTimeoutMs <= 2 ** 31 - 1)) throw new Error(`sharded ${name}: callTimeoutMs must be a positive number of ms`);
    this.callTimeoutMs = options.callTimeoutMs ?? null;
    this.identity = objectId(namespace, nodeName(name, "")).text;
  }

  private get splits(): Set<string> {
    let set = knownSplits.get(this.identity);
    if (!set) {
      knownSplits.set(this.identity, (set = new Set()));
      if (knownSplits.size > MAX_KNOWN_OBJECTS) {
        const oldest = knownSplits.keys().next().value!;
        knownSplits.delete(oldest);
        knownPresplit.delete(oldest);
        hintRejected.delete(oldest);
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
    return knownPresplit.get(this.identity) ?? (hintRejected.has(this.identity) ? 0 : this.presplitHint);
  }

  /** Whether the node `bits` is known to be split: above the presplit depth, or seen splitting. */
  private isSplit(bits: string): boolean {
    return bits.length < this.presplitTo || this.splits.has(bits);
  }

  /** The hash a key is routed by: of its route, which is the key itself by default. */
  private routeHash(key: string): string {
    assertKey(key);
    if (!this.routeOf) return keyHash(key);
    const route = this.routeOf(key);
    if (typeof route !== "string") throw new Error(`sharded ${this.name}: routeOf(${JSON.stringify(key)}) is not a string`);
    return keyHash(route);
  }

  /** The deepest known node owning a hash. */
  private shardFor(hash: string): string {
    let bits = "";
    while (this.isSplit(bits)) bits += hashBit(hash, bits.length);
    return bits;
  }

  private stub(bits: string): DurableObjectStub<ShardedObject<never, unknown>> {
    const ns = this.namespace as unknown as DurableObjectNamespace<ShardedObject<never, unknown>>;
    const { id } = objectId(ns, shardObjectName(this.name, bits));
    return this.stubOptions ? ns.get(id, this.stubOptions) : ns.get(id);
  }

  /**
   * Calls a shard through the stub this client keeps for it, making one on first use: a client
   * that calls a shard again does not open it again. A stub whose call throws is dropped, and the
   * next call makes a new one. A client belongs to the request (or the Durable Object) that made
   * it; if another request uses it, its kept stubs are that first request's and cannot be used, so
   * the call is made again on a stub of this request's (nothing was sent on the other).
   */
  private async onShard<A>(bits: string, keys: readonly string[], call: (stub: DurableObjectStub<ShardedObject<never, unknown>>) => Promise<A>, giveUpAt = this.giveUpAt()): Promise<A> {
    const kept = this.stubs.get(bits);
    const stub = kept ?? this.keep(bits);
    try {
      return await this.timed(bits, keys, call(stub), giveUpAt);
    } catch (error) {
      if (this.stubs.get(bits) === stub) this.stubs.delete(bits);
      // Only the runtime's own refusal, never a method's error that says the same: it may have run.
      const refused = kept && (error as { remote?: unknown })?.remote !== true && OTHER_REQUEST.test(String((error as Error)?.message ?? ""));
      if (refused) return this.timed(bits, keys, call(this.keep(bits)), giveUpAt);
      if (this.logShardErrors) console.error(JSON.stringify({ event: "sharded-shard-error", object: this.name, bits, kind: classifyShardError(error), error: String(error).slice(0, 200) }));
      throw error;
    }
  }

  /** `answer`, or OutcomeUnknown at `giveUpAt`. */
  private timed<A>(bits: string, keys: readonly string[], answer: Promise<A>, giveUpAt: number | null): Promise<A> {
    if (giveUpAt === null) return answer;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => (timer = setTimeout(() => reject(new OutcomeUnknown(bits, keys, this.callTimeoutMs!)), Math.max(0, giveUpAt - Date.now()))));
    return Promise.race([answer, timeout]).finally(() => clearTimeout(timer));
  }

  /** When a call that starts now is given up on: callTimeoutMs from now, or never. */
  private giveUpAt(): number | null {
    return this.callTimeoutMs === null ? null : Date.now() + this.callTimeoutMs;
  }

  /** The deadline a call given up on at `giveUpAt` carries: a margin before, for the shard's clock. */
  private deadlineFor(giveUpAt: number | null): number | null {
    if (giveUpAt === null) return null;
    return giveUpAt - Math.min(DEADLINE_MARGIN_MS, this.callTimeoutMs! / 2);
  }
  private keep(bits: string): DurableObjectStub<ShardedObject<never, unknown>> {
    const stub = this.stub(bits);
    if (this.stubs.size >= MAX_KEPT_STUBS) this.stubs.delete(this.stubs.keys().next().value!);
    this.stubs.set(bits, stub);
    return stub;
  }
  private readonly stubs = new Map<string, DurableObjectStub<ShardedObject<never, unknown>>>();

  /**
   * Calls `method(keys, payloads, ...args)` on every shard owning some of the keys, with its share
   * of them (and of `payloads`, aligned with `keys`). One result per shard.
   */
  async call<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<ShardResult<Result<T[M]>>[]> {
    return this.route<Result<T[M]>>(false, keys, method, payloads, this.method(method, args));
  }

  /**
   * Like `call`, for a method that writes nothing: a shard's call that fails with an error of
   * unknown outcome (a lost connection) is sent again, once. Not one shed as overloaded: sending
   * it again adds to the load that shed it.
   */
  async read<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<ShardResult<Result<T[M]>>[]> {
    return this.route<Result<T[M]>>(false, keys, method, payloads, this.method(method, args), true);
  }

  /**
   * Like `call`, for keys that must be handled together by one shard (an all-or-nothing update):
   * throws SpansShards, before anything is applied, if they belong to more than one.
   */
  async callOneShard<M extends KeyMethods<T>>(keys: readonly string[], method: M, payloads: readonly Payload<T[M]>[] | null, ...args: Rest<T[M]>): Promise<Result<T[M]>> {
    if (keys.length === 0) throw new Error(`sharded ${this.name}: callOneShard needs at least one key to pick its shard`);
    const [only] = await this.route<Result<T[M]>>(true, keys, method, payloads, this.method(method, args));
    return only!.result;
  }

  /** How `route` calls a shard for a method of the subclass. */
  private method(method: string, args: readonly unknown[]): Invoke {
    return (stub, bits, keys, payloads, hashes, deadline) => stub.shardedCall(this.name, bits, method, keys, payloads, args, this.stubOptions, this.presplitTo, hashes, deadline) as Promise<CallAnswer<unknown>>;
  }

  /** How `route` calls a shard for a step of an atomic call. */
  private atomicStep(step: AtomicStep, args: readonly unknown[]): Invoke {
    return (stub, bits, keys, payloads, hashes) => stub.shardedAtomic(this.name, bits, step, keys, payloads, args, this.stubOptions, this.presplitTo, hashes) as Promise<CallAnswer<unknown>>;
  }

  /**
   * Calls `method` on the shards owning `keys` (RFD0008), all or nothing: every shard's share
   * applies, or none does. Keys in one shard: one call, as `callOneShard`. Keys in several: the
   * shard of the smallest key (the primary) records the outcome once every other shard has
   * prepared its share, holding its keys; then each applies or drops it. `method` names one of the
   * class's `atomic` methods. Refused anywhere: nothing is applied, and the refusals come back.
   * Writes to the keys wait while the call holds them; reads see the rows from before.
   */
  async atomically<R = unknown, A = unknown>(keys: readonly string[], method: string, payloads: readonly unknown[] | null, ...args: unknown[]): Promise<AtomicOutcome<R, A>> {
    if (keys.length === 0) throw new Error(`sharded ${this.name}: atomically needs at least one key`);
    if (payloads && payloads.length !== keys.length) throw new Error(`sharded ${this.name}: ${payloads.length} payloads for ${keys.length} keys (payloads line up with keys)`);
    await this.loadCache();
    // The call's age, kept across retries: an older call never gives way to a younger one.
    const startedAt = Date.now();
    // A call that gave way waits out the older one, up to notReadyWaitMs over all its retries.
    const contention = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs, "an atomic call kept giving way to older calls");
    for (let splits = 0; ; ) {
      if (splits > MAX_HOPS) throw new Error(`sharded ${this.name}: atomic call did not settle`);
      if (new Set(keys.map((k) => this.shardFor(this.routeHash(k)))).size === 1) {
        try {
          const [only] = await this.route(true, keys, method, payloads, this.atomicStep({ step: "solo", method }, args));
          const answer = only!.result as AtomicAnswer;
          return "refused" in answer ? { committed: false, refusals: [{ keys, result: answer.refused as R }] } : { committed: true, results: [{ keys, result: (answer as { applied: A }).applied }] };
        } catch (error) {
          // A split this client had not heard of: the keys span shards now.
          if (error instanceof SpansShards) {
            splits++;
            continue;
          }
          throw error;
        }
      }
      const outcome = await this.atomicAtPrimary<R, A>(keys, method, payloads, args, startedAt);
      if (outcome === "again") splits++;
      else if (outcome === "contended") await contention.next();
      else return outcome;
    }
  }

  /**
   * An atomic call over several shards, coordinated by the primary (`coordinate`), so a share is
   * held for a round trip between shards, not between this caller and each of them. If the
   * primary's answer is lost, its outcome decides: the primary records an abort if it has none,
   * so a coordination still running then commits nothing. Committed, the results are unknown.
   */
  private async atomicAtPrimary<R, A>(keys: readonly string[], method: string, payloads: readonly unknown[] | null, args: readonly unknown[], startedAt: number): Promise<AtomicOutcome<R, A> | "again" | "contended"> {
    const tx = atomicTxId(startedAt);
    const primaryKey = [...keys].sort()[0]!;
    // Nothing applied: the primary's abort is no longer needed (the coordination resolved every
    // share before it answered; one it could not tell recovers by asking, and finds no outcome:
    // an abort is recorded again).
    const forgetAbort = () => this.atomicTo(primaryKey, { step: "forget", tx }).catch(() => undefined);
    let answer: AtomicAnswer;
    try {
      answer = await this.atomicTo(primaryKey, { step: "coordinate", tx, method, keys, payloads }, args);
    } catch (error) {
      const found = (await this.atomicTo(primaryKey, { step: "outcome", tx }).catch(() => null)) as { outcome: Outcome; recorded?: true } | null;
      if (found?.outcome === "commit") return { committed: true, results: [] };
      // An abort the primary had recorded: nothing applied anywhere. One recorded by this
      // question may be a commit that every share resolved and the primary then forgot: unknown.
      if (found?.outcome === "abort" && !found.recorded) {
        await forgetAbort();
        return { committed: false, refusals: [] };
      }
      throw error;
    }
    const coordinated = (answer as { coordinated: AtomicOutcome<R, A> | "again" | "contended" }).coordinated;
    if (coordinated === "again" || coordinated === "contended" || !coordinated.committed) await forgetAbort();
    return coordinated;
  }

  /**
   * The coordination of an atomic call over several shards, as the primary runs it (the
   * `coordinate` step): not for callers, who use `atomically`. Nothing applied, to be retried:
   * "again" if a split moved the primary's keys apart before the commit, "contended" if a shard
   * gave way to an older call.
   * @internal
   */
  async coordinateAtomic<R, A>(tx: string, keys: readonly string[], method: string, payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicOutcome<R, A> | "again" | "contended"> {
    await this.loadCache();
    const started = Date.now();
    const primaryKey = [...keys].sort()[0]!;
    const primaryBits = this.shardFor(this.routeHash(primaryKey));
    const mine = keys.map((k) => this.shardFor(this.routeHash(k)) === primaryBits);
    const pick = <X>(xs: readonly X[], primary: boolean) => xs.filter((_, i) => mine[i] === primary);
    const deadline = Date.now() + ATOMIC_HOLD_MS;
    const others = pick(keys, false);
    let prepares: ShardResult<unknown>[];
    try {
      prepares = await this.route(false, others, method, payloads && pick(payloads, false), this.atomicStep({ step: "prepare", tx, method, primaryKey, deadline }, args));
    } catch (error) {
      // Some shards may hold: the abort lets them drop at their deadline at the latest.
      await this.decideAbort(tx, primaryKey);
      throw error;
    }
    const prepared = prepares.filter((r) => "prepared" in (r.result as AtomicAnswer));
    const contended = prepares.some((r) => "contended" in (r.result as AtomicAnswer));
    const refusals = prepares.flatMap((r) => ("refused" in (r.result as AtomicAnswer) ? [{ keys: r.keys, result: (r.result as { refused: R }).refused }] : []));
    const preparedAt = Date.now();
    const log = (outcome: string, extra: Record<string, unknown> = {}) => console.log(JSON.stringify({ event: "sharded-atomic", object: this.name, shards: prepares.length + 1, keys: keys.length, outcome, prepareMs: preparedAt - started, ms: Date.now() - started, ...extra }));
    if (refusals.length > 0 || contended) {
      await this.decideAbort(tx, primaryKey);
      await this.resolveAll(prepared, tx, "abort");
      // Refused wins: the call would be refused again.
      if (refusals.length > 0) {
        log("refused");
        return { committed: false, refusals };
      }
      log("contended");
      return "contended";
    }
    let answer: AtomicAnswer;
    let primaryKeys: readonly string[];
    try {
      const [commit] = await this.route(true, pick(keys, true), method, payloads && pick(payloads, true), this.atomicStep({ step: "commit", tx, method }, args));
      answer = commit!.result as AtomicAnswer;
      primaryKeys = commit!.keys;
    } catch (error) {
      if (error instanceof SpansShards) {
        await this.decideAbort(tx, primaryKey);
        await this.resolveAll(prepared, tx, "abort");
        log("again");
        return "again";
      }
      // The commit's answer is lost: the outcome decides, as it would for the shards.
      const outcome = await this.atomicOutcome(tx, primaryKey).catch(() => null);
      if (outcome === null) throw error;
      const resolved = await this.resolveAll(prepared, tx, outcome);
      log(outcome, { lostCommitAnswer: true });
      // Aborted (by the primary's check, or by this question): nothing applied. Only this
      // coordination forgets an outcome, so an abort here is not a forgotten commit.
      if (outcome === "abort") return { committed: false, refusals: [] };
      return { committed: true, results: resolved.results as AtomicShardResult<A>[] };
    }
    if ("contended" in answer) {
      // The primary gave way to an older call, and recorded an abort.
      await this.resolveAll(prepared, tx, "abort");
      log("contended at the primary");
      return "contended";
    }
    if ("refused" in answer || ("outcome" in answer && answer.outcome === "abort")) {
      await this.resolveAll(prepared, tx, "abort");
      log("refused at the primary");
      // Refused by the primary's check, or aborted by a shard's recovery before the commit came.
      return { committed: false, refusals: "refused" in answer ? [{ keys: primaryKeys, result: answer.refused as R }] : [] };
    }
    const results: AtomicShardResult<A>[] = "applied" in answer ? [{ keys: primaryKeys, result: answer.applied as A }] : [];
    const resolved = await this.resolveAll(prepared, tx, "commit");
    if (resolved.all) await this.atomicTo(primaryKey, { step: "forget", tx }).catch(() => undefined);
    log("commit", { resolveMs: Date.now() - started });
    return { committed: true, results: [...results, ...(resolved.results as AtomicShardResult<A>[])] };
  }

  /** The outcome of atomic call `tx`, from its primary (recording an abort if it has none). */
  async atomicOutcome(tx: string, primaryKey: string): Promise<Outcome> {
    const answer = await this.atomicTo(primaryKey, { step: "outcome", tx });
    return (answer as { outcome: Outcome }).outcome;
  }

  /** A step at the primary of `tx`. */
  private async atomicTo(primaryKey: string, step: AtomicStep, args: readonly unknown[] = []): Promise<AtomicAnswer> {
    const [only] = await this.route(true, [primaryKey], step.step, null, this.atomicStep(step, args));
    return only!.result as AtomicAnswer;
  }

  /** Records an abort at the primary, if it can: else the holds end at their deadline, aborted. */
  private async decideAbort(tx: string, primaryKey: string): Promise<void> {
    await this.atomicTo(primaryKey, { step: "abort", tx }).catch((error) => console.error(JSON.stringify({ event: "sharded-atomic-abort-failed", object: this.name, tx, error: String(error) })));
  }

  /** Tells every shard that prepared the outcome; each that applied answers its result. A lost one recovers at its deadline. */
  private async resolveAll(prepared: readonly ShardResult<unknown>[], tx: string, outcome: Outcome): Promise<Resolutions> {
    const results: AtomicShardResult<unknown>[] = [];
    let all = true;
    await Promise.all(
      prepared.map(async (share) => {
        try {
          const [only] = await this.route(true, share.keys, "resolve", null, this.atomicStep({ step: "resolve", tx, outcome }, []));
          const answer = only!.result as AtomicAnswer;
          if ("applied" in answer) results.push({ keys: share.keys, result: answer.applied });
        } catch (error) {
          all = false;
          console.error(JSON.stringify({ event: "sharded-atomic-resolve-failed", object: this.name, tx, outcome, error: String(error) }));
        }
      }),
    );
    return { all, results };
  }

  /**
   * Calls the shards owning `keys`, routing again what comes back "moved", "stale" or "not
   * ready". A shard that fails fails the call, once every other shard of that hop answered: the
   * error carries what each did (CallOutcomes). `idempotent`: a call that writes nothing, sent
   * again once after an error of unknown outcome.
   */
  private async route<R>(oneShard: boolean, keys: readonly string[], method: string, payloads: readonly unknown[] | null, invoke: Invoke, idempotent = false): Promise<ShardResult<R>[]> {
    if (payloads && payloads.length !== keys.length) throw new Error(`sharded ${this.name}: ${payloads.length} payloads for ${keys.length} keys (payloads line up with keys)`);
    await this.loadCache();
    const hashes = keys.map((key) => this.routeHash(key));
    const out: ShardResult<R>[] = [];
    let pending = keys.map((_, i) => i);
    const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
    let forgot = false;
    // One deadline for the whole call, its hops and waits included: a write held by a split
    // that the caller gave up on must not land once the split is done.
    const giveUpAt = this.giveUpAt();
    const deadline = this.deadlineFor(giveUpAt);
    const failed: { keys: string[]; positions: number[]; error: unknown }[] = [];
    for (let hop = 0; pending.length > 0; hop++) {
      if (hop > MAX_HOPS) throw new Error(`sharded ${this.name}: routing did not settle`);
      let waiting = false;
      // A shard that said when it will serve exists and is busy (a split holding writes): no
      // reason to forget the map and ask the root, however long it takes.
      let held = false;
      let stale = false;
      const groups = new Map<string, number[]>();
      for (const i of pending) {
        const bits = this.shardFor(hashes[i]!);
        let group = groups.get(bits);
        if (!group) groups.set(bits, (group = []));
        group.push(i);
      }
      if (oneShard && groups.size > 1) throw new SpansShards(`keys of ${this.name} belong to ${groups.size} shards`);
      const hopStarted = Date.now();
      const took: number[] = [];
      const retry: number[] = [];
      const settled = await Promise.allSettled(
        [...groups].map(async ([bits, positions]) => {
          const groupKeys = positions.map((i) => keys[i]!);
          const groupPayloads = payloads ? positions.map((i) => payloads[i]) : null;
          const called = Date.now();
          const groupHashes = positions.map((i) => hashes[i]!);
          const send = () => this.onShard(bits, groupKeys, (stub) => invoke(stub, bits, groupKeys, groupPayloads, groupHashes, deadline), giveUpAt);
          let answer: CallAnswer<R>;
          try {
            answer = (await send()) as CallAnswer<R>;
          } catch (error) {
            if (!idempotent || classifyShardError(error) !== "unknown") throw error;
            answer = (await send()) as CallAnswer<R>;
          }
          took.push(Date.now() - called);
          if ("ok" in answer) out.push({ keys: groupKeys, positions, result: answer.ok });
          else if ("routeMismatch" in answer) throw new RouteMismatch(answer.routeMismatch.key, answer.routeMismatch.client, answer.routeMismatch.object);
          else if ("expired" in answer) throw new DeadlineExpired(bits, groupKeys);
          else if ("moved" in answer) {
            this.learn(answer.moved, answer.presplit);
            retry.push(...positions);
          } else if ("stale" in answer) {
            stale = true;
            retry.push(...positions);
          } else if ("notReady" in answer) {
            waiting = true; // not serving yet: the same shard, a little later
            if (answer.until !== undefined) held = true;
            backoff.extend(answer.until);
            retry.push(...positions);
          } else throw new UnknownAnswer(bits, answer);
        }),
      );
      [...groups.values()].forEach((positions, g) => {
        const result = settled[g]!;
        if (result.status === "rejected") failed.push({ keys: positions.map((i) => keys[i]!), positions, error: result.reason });
      });
      // Nothing was called for a key the client routes differently from the object.
      const mismatch = failed.find((f) => f.error instanceof RouteMismatch);
      if (mismatch) throw mismatch.error;
      if (failed.length > 0) {
        const error = failed[0]!.error;
        // Keys in neither list were not applied: answered moved, stale or not ready, not sent again.
        const outcomes: CallOutcomes<R> = { settled: out, unknown: failed.map((f) => ({ keys: f.keys, positions: f.positions, error: String(f.error).slice(0, 200) })) };
        if (typeof error === "object" && error !== null) Object.assign(error, outcomes);
        throw error;
      }
      // A routed call that fans out wide: how long its hop took, and its slowest shard.
      if (this.logWideHops > 0 && groups.size >= this.logWideHops) console.log(JSON.stringify({ event: "sharded-wide-hop", object: this.name, method: String(method), hop, shards: groups.size, keys: pending.length, ms: Date.now() - hopStarted, ...spread(took), retries: retry.length }));
      if (stale || (waiting && !held && !forgot && backoff.waited >= FORGET_AFTER_MS)) {
        forgot = true;
        this.forget();
      } else if (waiting) {
        // Out of time: what was held answered "not ready", but a shard that was slow before may
        // still apply its share, so the outcome is unknown, not "not applied".
        if (giveUpAt !== null && Date.now() >= giveUpAt) throw new OutcomeUnknown("", retry.map((i) => keys[i]!), this.callTimeoutMs!);
        await backoff.next();
        if (giveUpAt !== null && Date.now() >= giveUpAt) throw new OutcomeUnknown("", retry.map((i) => keys[i]!), this.callTimeoutMs!);
        hop--;
      }
      pending = retry;
    }
    return out;
  }

  /**
   * Calls `method(...args)` on every shard, at most SCAN_IN_FLIGHT at a time: one result per
   * shard that holds anything. A presplit shard never called holds nothing, and is not made by
   * the scan (its result is left out).
   */
  async scan<M extends ScanMethods<T>>(method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<Result<T[M]>[]> {
    await this.loadCache();
    const slots = new Slots(SCAN_IN_FLIGHT);
    for (let attempt = 0; ; attempt++) {
      const out: Result<T[M]>[] = [];
      let stale = false;
      // Each shard waits on its own: many shards not ready at once wait no longer than one.
      const visit = async (bits: string, depth: number, backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs)): Promise<void> => {
        if (depth > MAX_HOPS) throw new Error(`sharded ${this.name}: scan did not settle`);
        if (stale) return;
        if (this.isSplit(bits)) {
          await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
          return;
        }
        const answer = (await slots.run(() => this.onShard(bits, [], (stub) => stub.shardedScan(this.name, bits, method, args, this.stubOptions, this.presplitTo) as Promise<unknown>))) as CallAnswer<Result<T[M]>>;
        if ("ok" in answer) out.push(answer.ok);
        else if ("absent" in answer) return;
        else if ("moved" in answer) {
          this.learn(answer.moved, answer.presplit);
          await visit(bits, depth + 1);
        } else if ("stale" in answer) {
          // A scan always reaches the root (its leftmost shard), which says so when the map is
          // stale: start over from it, once the other visits stop.
          stale = true;
        } else if ("notReady" in answer) {
          backoff.extend(answer.until);
          await backoff.next();
          await visit(bits, depth, backoff);
        } else throw new UnknownAnswer(bits, answer);
      };
      await visit("", 0);
      if (!stale) return out;
      if (attempt >= 3) throw new Error(`sharded ${this.name}: scan did not settle`);
      this.forget();
    }
  }

  /**
   * Calls `method(...args)` on the one shard `bits`, if it is still a shard: else answers the
   * shards it is known to have split into, in order (one may since have split further: ask them).
   * A walk over shards one at a time uses it, where `scan` would call every shard at once.
   */
  async scanShard<M extends ScanMethods<T>>(bits: string, method: M, ...args: T[M] extends (...a: infer A) => unknown ? A : never): Promise<{ ok: Result<T[M]> } | { split: string[] } | { absent: true }> {
    await this.loadCache();
    const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
    for (let attempt = 0; ; attempt++) {
      if (this.isSplit(bits)) return { split: this.leavesUnder(bits) };
      const answer = (await this.onShard(bits, [], (stub) => stub.shardedScan(this.name, bits, method, args, this.stubOptions, this.presplitTo) as Promise<unknown>)) as CallAnswer<Result<T[M]>>;
      if ("ok" in answer) return { ok: answer.ok };
      // A presplit shard never called: it holds nothing, and is not made to say so.
      if ("absent" in answer) return { absent: true };
      if ("moved" in answer) {
        this.learn(answer.moved, answer.presplit);
        // Moved, yet not known to be split below `bits`: this client's map is behind; the root says.
        if (!this.isSplit(bits)) this.learn(bits, answer.presplit);
      } else if ("stale" in answer) {
        if (attempt >= 3) throw new Error(`sharded ${this.name}: shard ${bits} is not part of the object (was it wiped?)`);
        this.forget();
      } else if ("notReady" in answer) {
        backoff.extend(answer.until);
        await backoff.next();
      } else throw new UnknownAnswer(bits, answer);
    }
  }

  /** The shards this client knows of, in order (by their bits): every key belongs to exactly one. */
  async knownLeaves(): Promise<string[]> {
    await this.loadCache();
    return this.leavesUnder("");
  }

  private leavesUnder(bits: string): string[] {
    if (!this.isSplit(bits)) return [bits];
    return [...this.leavesUnder(`${bits}0`), ...this.leavesUnder(`${bits}1`)];
  }

  /**
   * Deletes every shard's storage and alarm: the whole sharded object, for one that is done with.
   * Waits out splits in progress (both halves are then wiped). Nothing may write to the object
   * meanwhile: a write after its shard is wiped would make that shard again. The root's object
   * goes last, since it is how the others are found: a wipe that fails partway can run again.
   * Returns how many shards held something.
   */
  async wipe(): Promise<number> {
    await this.loadCache();
    for (let attempt = 0; ; attempt++) {
      let wiped = 0;
      let stale = false;
      let root: string | null = null;
      const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
      const visit = async (bits: string, depth: number, last = false): Promise<void> => {
        if (depth > MAX_HOPS) throw new Error(`sharded ${this.name}: wipe did not settle`);
        if (stale) return;
        if (this.isSplit(bits)) {
          await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
          return;
        }
        // The root's object, at the leftmost shard of a tree that has split: after the rest.
        if (bits !== "" && hostBits(bits) === "" && !last) {
          root = bits;
          return;
        }
        const answer = (await this.stub(bits).shardedWipe(this.name, bits)) as CallAnswer<{ wiped: boolean }>;
        if ("ok" in answer) wiped += answer.ok.wiped ? 1 : 0;
        else if ("moved" in answer) {
          this.learn(answer.moved, answer.presplit);
          await visit(bits, depth + 1);
        } else if ("stale" in answer) stale = true;
        else {
          await backoff.next();
          await visit(bits, depth);
        }
      };
      await visit("", 0);
      // A deeper split of the root's object than this client knew defers its leftmost shard again.
      while (!stale && root !== null) {
        const last: string = root;
        root = null;
        await visit(last, 0, true);
      }
      if (!stale) {
        // What this client learned of the object's splits is of no more use.
        this.forget();
        return wiped;
      }
      if (attempt >= 3) throw new Error(`sharded ${this.name}: wipe did not settle`);
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
    if (this.presplitHint > 0) hintRejected.add(this.identity);
    void this.dropCache();
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

  /**
   * Every shard of this tree, as its objects describe themselves (for operators): each shard at
   * the presplit depth (this client's, else the one the root reports), then down every split. A
   * shard never called is described as "new", and the walk goes on past it: in a presplit tree,
   * shard 10 may never have been called while 11 has, and a walk down from the root stops at 10.
   * Asks the objects, not this client's map of splits, so it sees splits no client here has met.
   */
  async describeTree(): Promise<ShardDescription[]> {
    const describe = async (bits: string): Promise<ShardDescription> => {
      const id = this.namespace.idFromName(shardObjectName(this.name, bits));
      const ns = this.namespace as unknown as DurableObjectNamespace<ShardedObject<never, unknown>>;
      const stub = this.stubOptions ? ns.get(id, this.stubOptions) : ns.get(id);
      const node = await stub.shardedDescribe();
      return { ...node, bits: node.state === "new" ? bits : node.bits, object: id.toString() };
    };
    const out: ShardDescription[] = [];
    // The object hosting `bits` hosts a deeper shard once `bits` has split: its left half stayed,
    // and each right half along the way went to an object of its own.
    const visit = async (bits: string): Promise<void> => {
      const node = await describe(bits);
      out.push(node);
      if (node.state === "new") return;
      const rights: string[] = [];
      for (let i = bits.length; i < node.bits.length; i++) rights.push(`${node.bits.slice(0, i)}1`);
      await Promise.all(rights.map(visit));
    };
    let depth = this.presplitTo;
    if (depth === 0) depth = (await describe("")).presplit;
    const leaves = Array.from({ length: 2 ** depth }, (_, i) => (depth === 0 ? "" : i.toString(2).padStart(depth, "0")));
    await Promise.all(leaves.map(visit));
    return out.sort((a, b) => (a.bits < b.bits ? -1 : a.bits > b.bits ? 1 : 0));
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

  /**
   * Saves what this isolate knows of the splits, with what the colo already had: splits are
   * final, so the union is right, and an isolate that knows less must not overwrite one that
   * knows more. (Two saves at once can still lose one's news: it costs that isolate's clients hops.)
   */
  private async saveCache(): Promise<void> {
    if (typeof caches === "undefined") return;
    try {
      const cache = (caches as CacheStorage & { default: Cache }).default;
      const hit = await cache.match(CACHE_ORIGIN + this.identity);
      const cached = hit ? ((await hit.json()) as { splits?: string[] }) : null;
      for (const bits of cached?.splits ?? []) this.splits.add(bits);
      const body = JSON.stringify({ splits: [...this.splits], presplit: this.presplitTo });
      await (caches as CacheStorage & { default: Cache }).default.put(CACHE_ORIGIN + this.identity, new Response(body, { headers: { "cache-control": "max-age=86400", "content-type": "application/json" } }));
    } catch {
      // the cache is an optimisation only
    }
  }

  /** Drops this object's entry from the colo: what it held was stale. */
  private async dropCache(): Promise<void> {
    if (typeof caches === "undefined") return;
    try {
      await (caches as CacheStorage & { default: Cache }).default.delete(CACHE_ORIGIN + this.identity);
    } catch {
      // the cache is an optimisation only
    }
  }
}

/** One shard of a tree, as its object describes itself, with the shard's bits and the object's ID. */
export interface ShardDescription extends NodeDescription {
  /** The object's Durable Object ID. */
  readonly object: string;
}

export function sharded<T extends Rpc.DurableObjectBranded>(namespace: DurableObjectNamespace<T>, name: string, options: ShardedClientOptions = {}): ShardedClient<T> {
  return new ShardedClient(namespace, name, options);
}
