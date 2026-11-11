// Sharded objects: a Durable Object base class whose instances split themselves in two, by the
// next bit of their keys' hash, when their call rate stays high; and a client that routes each key
// to the shard that owns it.
//
// A shard owns the keys whose hash starts with its `bits`. It lives in the Durable Object named
// `${name}#${bits without trailing zeros}`, so when a shard splits its left half (`bits0`) stays in
// the same object and only the right half (`bits1`) moves, to a new object, as one copy of the
// object's storage (`ctx.storage.snapshot()`, or a row copy where snapshots are not available).
// Right-half writes wait while the copy is made; reads and left-half writes go on. Splits are
// final, so a client's map of splits can only lack some: it stops too shallow, and the object it
// reaches answers "moved".
//
// With `initialDepth` N, a tree starts with 2^N empty shards, created lazily: each depth-N
// object makes itself on its first call, and every answer tells the client N (`presplit`), so it
// routes straight to depth N without asking the shards above (which never exist).
//
// The protocol is modelled in specs/snapshot-split.ts and specs/lazy-presplit.ts.
import { DurableObject } from "cloudflare:workers";

import { ATOMIC_OUTCOME_TTL_MS, type AtomicAnswer, type AtomicMethod, type AtomicStep, type Outcome } from "./atomic.js";
import { sharded } from "./client.js";
import { hashBit, keyHash } from "./hash.js";

export { ATOMIC_HOLD_MS, ATOMIC_OUTCOME_TTL_MS, type AtomicMethod, type AtomicOutcome, type AtomicShardResult, type Prepared } from "./atomic.js";
export { hashBit, keyHash } from "./hash.js";
export { RouteMismatch, SpansShards, ShardedClient, sharded, type ShardDescription, type ShardedClientOptions, type ShardResult } from "./client.js";

declare global {
  /** An opaque handle on a point-in-time copy of an object's storage; it can only be sent over RPC. */
  interface DurableObjectSnapshot {}
  interface DurableObjectStorage {
    snapshot?(options?: { bookmark?: string }): Promise<DurableObjectSnapshot>;
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
   * Depth a new tree starts at, for uniform keys (hashes): 2^initialDepth empty shards, each
   * created by its first call, and clients route straight to them. Read when a tree's root is
   * first called, and recorded there: changing it later applies to new names only.
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
export const DEFAULT_SPLIT_POLICY: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25 };

/** The deepest a shard can be: ownership is decided on 64 bits of the key's hash. */
export const MAX_SPLIT_DEPTH = 64;

/** Checks a split policy, so a bad one fails loudly on the first call instead of quietly never splitting. */
export function checkSplitPolicy(policy: SplitPolicy): SplitPolicy {
  const fail = (what: string) => {
    throw new Error(`splitPolicy.${what}`);
  };
  if (!(policy.callsPerSecond > 0)) fail("callsPerSecond must be a positive number");
  if (!Number.isInteger(policy.windowSeconds) || policy.windowSeconds < 1) fail("windowSeconds must be a whole number of seconds, at least 1");
  if (!Number.isInteger(policy.maxDepth) || policy.maxDepth < 0 || policy.maxDepth > MAX_SPLIT_DEPTH) fail(`maxDepth must be an integer from 0 to ${MAX_SPLIT_DEPTH}`);
  if (policy.keysPerCall !== undefined && !(policy.keysPerCall > 0)) fail("keysPerCall must be a positive number");
  const initial = policy.initialDepth ?? 0;
  if (!Number.isInteger(initial) || initial < 0 || initial > policy.maxDepth) fail("initialDepth must be an integer from 0 to maxDepth");
  if (policy.copy !== undefined && !["auto", "native", "logical"].includes(policy.copy)) fail('copy must be "auto", "native" or "logical"');
  if (policy.splitTimeoutMs !== undefined && !(policy.splitTimeoutMs > 0)) fail("splitTimeoutMs must be a positive number");
  if (policy.holdWaitMs !== undefined && !(policy.holdWaitMs >= 0)) fail("holdWaitMs must be zero or more");
  return policy;
}

/** What a node answers a routed call: `moved` names a split shard (its ancestors split too). */
/**
 * What a node answers a routed call: `moved` names a split shard (its ancestors split too);
 * `stale` says the caller's map of splits is deeper than this object, so it outlived the storage
 * it was learned from (deleted, then the name used again): the caller forgets it.
 */
/**
 * Options for a stub of a shard: those of `namespace.get(id, options)`, and a version cohort (in
 * the experimental types only, so spelled out here).
 */
export type StubOptions = DurableObjectNamespaceGetDurableObjectOptions & { readonly version?: { readonly cohort?: string } };

/** `presplit`, on "moved": the tree's initial depth, so the client routes to it directly. */
export type CallAnswer<R> =
  | { readonly ok: R }
  | { readonly moved: string; readonly presplit?: number }
  | { readonly notReady: string; readonly until?: number }
  | { readonly stale: string }
  /** The client routed `key` by another `routeOf` than this object's: nothing was called. */
  | { readonly routeMismatch: { readonly key: string; readonly client: string; readonly object: string } };

/** An object's state, for tests and operators. */
export interface NodeDescription {
  /** The bits of the shard this object hosts now. */
  readonly bits: string;
  /** The tree's initial depth, as this shard knows it (0: none). */
  readonly presplit: number;
  /** "copy": a split's copy that does not serve yet; "new": never used. */
  readonly state: "shard" | "splitting" | "copy" | "new";
  /** Rows of keys this object owns. */
  readonly rows: number;
  /** Rows stored, including ones a split gave away and not deleted yet. */
  readonly storedRows: number;
  readonly callsPerSecond: number;
  /** How this object's last split copied its right half, and how long until the copy was claimed. */
  readonly lastSplit: { readonly copy: "native" | "logical"; readonly ms: number } | null;
  /** Why the last abandoned split attempt was abandoned. */
  readonly lastSplitError: string | null;
  /** This shard's last split attempts, newest last (at most `SPLIT_HISTORY`); a new shard's is empty. */
  readonly splits: readonly SplitRecord[];
  /** What this object answered since it started in this process (counters reset on restart). */
  readonly counters: CallCounters;
}

/** One split attempt of a shard. */
export interface SplitRecord {
  /** When it started (ms since the epoch) and how long until it was claimed or given up. */
  readonly at: number;
  readonly ms: number;
  readonly outcome: "split" | "abandoned";
  /** How the right half was copied; null if abandoned before it was. */
  readonly copy: "native" | "logical" | null;
  /** Rows this shard owned when it started, and of them the right half's (what was copied). */
  readonly rows: number;
  readonly rightRows: number;
  /** Writes that waited on it; of them, the ones answered "not ready" after `holdWaitMs`. */
  readonly held: number;
  readonly notReady: number;
  /** The longest a held write waited, released or not. */
  readonly heldMsMax: number;
  /** Where the time went, in ms: steps not reached are 0. */
  readonly steps: SplitSteps;
  readonly error: string | null;
}

/** The steps of one split attempt, in ms. */
export interface SplitSteps {
  /** `beforeHandOut`. */
  readonly handOut: number;
  /**
   * Before the split holds anything: one call that starts the new object, so starting it is not
   * paid while writes wait.
   */
  readonly warm: number;
  /** Native: taking the snapshot, then the call that hands it over, and of that the sibling's own scheduling of the restore. Logical: copying the rows. */
  readonly snapshot: number;
  readonly restore: number;
  readonly restoreInside: number;
  readonly copyRows: number;
  /** Claiming the copy: the sibling restarting on it, then accepting; and how many claim calls it took. */
  readonly claim: number;
  readonly claimAttempts: number;
  /** Why the last claim call that failed failed (a restart in progress, usually). */
  readonly claimError: string | null;
}

/**
 * Calls this object answered, by answer. Calls the runtime sheds as overloaded never reach the
 * object, so they are not here: compare `calls` with what the caller sent.
 */
export interface CallCounters {
  /** When counting started: when this object was last started. */
  readonly since: number;
  /**
   * Routed calls and scans that reached a method here: `ok` of them returned, the rest answered
   * "moved" or "not ready" from inside it (a write released by a split as "moved", or held past
   * `holdWaitMs`) or threw.
   */
  readonly calls: number;
  readonly ok: number;
  /**
   * Every "moved", "not ready" and "stale" answer, whether given on arrival (the caller's map is
   * out of date; not in `calls`) or from inside a method (in `calls`).
   */
  readonly moved: number;
  readonly notReady: number;
  readonly stale: number;
  /** Writes that waited on a split, and the longest wait. */
  readonly held: number;
  readonly heldMsMax: number;
}

/** The step timings of a split attempt, filled in as it goes. */
type Steps = { -readonly [K in keyof SplitSteps]: SplitSteps[K] };

/** A split in progress: writes to `right` wait for `released`, and are counted for its record. */
type Hold = { epoch: number; right: string; released: Promise<void>; deadline: number; held: number; notReady: number; heldMsMax: number };

/** An atomic call's share prepared here (RFD0008): its keys are held until it is resolved. */
type AtomicHold = { readonly tx: string; readonly primaryKey: string; readonly deadline: number; readonly method: string; readonly keys: readonly string[]; readonly staged: unknown };
/** What a write waits for: a split's hold or an atomic call's (`atomic`), released at the latest around `until`. */
type Waiting = { readonly released: Promise<void>; readonly until: number; readonly atomic: boolean };

/** Writes to a key an atomic call holds wait for `released`. */
type HeldKey = { readonly tx: string; readonly deadline: number; readonly released: Promise<void>; readonly release: () => void };

const ATOMIC_SCHEMA = `
CREATE TABLE IF NOT EXISTS sharded_atomic_holds (tx TEXT PRIMARY KEY, primary_key TEXT NOT NULL, deadline INTEGER NOT NULL, method TEXT NOT NULL, keys TEXT NOT NULL, staged TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sharded_atomic_outcomes (tx TEXT PRIMARY KEY, outcome TEXT NOT NULL, at INTEGER NOT NULL);
`;

/** How many split attempts a shard remembers. */
const SPLIT_HISTORY = 8;

/** The writes of an update (null deletes) and what the method returns. */
export interface UpdateResult<V, R> {
  readonly writes: ReadonlyMap<string, V | null>;
  readonly result: R;
}


function ownsHash(bits: string, hash: string): boolean {
  for (let i = 0; i < bits.length; i++) if (hashBit(hash, i) !== bits[i]) return false;
  return true;
}

/** The hashes a shard owns, inclusive, as 16-hex-digit strings (they sort like the numbers). */
function hashRange(bits: string): readonly [string, string] {
  const lo = BigInt(`0b0${bits}${"0".repeat(64 - bits.length)}`);
  const hi = BigInt(`0b0${bits}${"1".repeat(64 - bits.length)}`);
  return [lo.toString(16).padStart(16, "0"), hi.toString(16).padStart(16, "0")];
}

/** An SQL identifier, quoted (a subclass may name its tables anything). */
function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Tables that are SQLite's (sqlite_), the runtime's (_cf_) or local development's (__miniflare_),
 * never a subclass's. By prefix, not LIKE: in LIKE, "_" matches any character, and "_cf_%" also
 * matched a subclass's table such as "scf_notes", which a split then failed to copy.
 */
function isInternalTable(name: string): boolean {
  return name.startsWith("sqlite_") || name.startsWith("_cf_") || name.startsWith("__miniflare_");
}

function unquoteIdentifier(quoted: string): string {
  return quoted.slice(1, -1).replace(/""/g, '"');
}

export function nodeName(name: string, bits: string): string {
  return `${name}#${bits}`;
}

/** The bits of the object hosting a shard: its bits without trailing zeros. */
export function hostBits(bits: string): string {
  return bits.replace(/0+$/, "");
}

/** The name of the Durable Object hosting the shard `bits` of `name`. */
export function shardObjectName(name: string, bits: string): string {
  return nodeName(name, hostBits(bits));
}

/** Thrown inside a method when its keys left this shard; the dispatcher answers "moved". */
class Moved extends Error {}
/**
 * A write waited on a split for `holdWaitMs`: nothing was applied, the client retries later, until
 * the split is done or gives up at `until`.
 */
class NotReady extends Error {
  constructor(readonly until: number) {
    super("not ready");
  }
}
const HOLD_WAIT_MS = 2_000;
/** Keys whose route hash a shard remembers. */
const ROUTE_HASHES = 4_096;
/**
 * How long a wiped shard refuses calls: long enough for a call that was in flight when it was
 * wiped to arrive and be refused, rather than make the shard again. After that the name is free
 * to use again.
 */
const WIPED_REFUSE_MS = 5_000;
/**
 * How long a split waits for its new object's first call before going ahead anyway. A first call
 * can take seconds, and one that never returns must not leave the shard unable to split again:
 * the split's own calls to the new object retry, or give up.
 */
const WARM_WAIT_MS = 5_000;
/** How often a busy shard logs its load (`shard-load`). */
const LOAD_LOG_S = 10;

const SCHEMA = `
CREATE TABLE IF NOT EXISTS sharded_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sharded_rows (key TEXT PRIMARY KEY, hash TEXT NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS sharded_rows_by_hash ON sharded_rows (hash, key);
`;

/** Bytes of rows per logical-copy call, well under the RPC limit; a call always carries one row. */
const COPY_BATCH_BYTES = 4 * 1024 * 1024;
/** The reason a sibling restarts with after scheduling a restore (its caller may see it). */
const RESTORE_ABORT = "sharded: restart on the snapshot";
/** Rows read per page of the logical copy. */
const COPY_PAGE_ROWS = 1000;
/**
 * Clearing what a split gave away is background work: those rows are already invisible. A delete
 * blocks the object while it runs, for longer the more rows it deletes, so it goes in small
 * batches with a pause between them, leaving the object to its calls.
 */
const DELETE_BATCH_ROWS = 250;
const DELETE_PAUSE_MS = 25;
/** How soon the alarm tries again after activating a sibling or cleaning up failed. */
const ALARM_RETRY_MS = 1000;
/** How often a shard with a split's work pending checks that its alarm is set. */
const ALARM_CHECK_MS = 5_000;

interface Meta {
  name: string;
  /** The shard this object hosts. */
  bits: string;
  /**
   * "shard" serves; "copy" is a split's copy, claimed and waiting to be activated. An object that
   * holds another's storage verbatim (a snapshot not claimed yet) has the other's meta.
   */
  state: "shard" | "copy";
  /** Split attempts so far; a copy is claimed and activated only by the attempt that made it. */
  epoch: number;
  /** The attempt in progress; persisted so the copy carries it. */
  splitting: number | null;
  /** The id of the object that started `splitting`: an object whose own id differs is its copy. */
  from?: string;
  /** A sibling to activate (the alarm retries until it confirms). */
  activate: { bits: string; epoch: number } | null;
  /** Hash ranges this object no longer owns, whose rows the alarm deletes. */
  cleanup: (readonly [string, string])[];
  /** The subclass still has rows for keys it no longer owns: the alarm calls `cleanupForeign`. */
  foreign?: boolean;
  /**
   * The tree's initial depth (`initialDepth` when its root was first called): shards at this
   * depth create themselves. Every shard of the tree records the same one.
   */
  presplit?: number;
  /** The shard this one split from and when (null for the root). */
  parent: string | null;
  splitAt: number;
  lastSplit?: { copy: "native" | "logical"; ms: number };
  /** Why the last abandoned split attempt was abandoned. */
  lastSplitError?: string;
  /** The last split attempts, newest last. */
  splits?: SplitRecord[];
}

/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this shard), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses never define `alarm` (the constructor refuses): they override `onAlarm` and schedule
 * it with `scheduleAlarm`.
 */
export abstract class ShardedObject<Env = unknown, V = unknown> extends DurableObject<Env> {
  protected splitPolicy: SplitPolicy = DEFAULT_SPLIT_POLICY;
  /**
   * The methods `client.atomically` can call (RFD0008), by name: each one's share of an
   * all-or-nothing call over keys in any number of shards. See AtomicMethod.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected atomic: Readonly<Record<string, AtomicMethod<V, any, any, any, any>>> = {};
  /**
   * The share (0 to 1) of new shards that log a `shard-created` JSON line on their first call,
   * with how long the root took to confirm the tree's depth (`rootMs`). Off by default.
   */
  protected shardCreatedLogShare = 0;
  /**
   * The binding of this class, to reach its other shards: `return this.env.COUNTERS`. Typed loosely
   * because a binding's type names the subclass, which a base class cannot.
   */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected abstract nodes(): DurableObjectNamespace<any>;

  /**
   * The route of a key: keys with the same route always live in the same shard, so one
   * transaction (`update`, `transact`) can read and write them all. Shards own routes by the
   * SHA-256 of the route, as they own keys by theirs without this: the default is the key itself.
   * Rows stay keyed by the full key.
   *
   * Declare the same function on the client (`sharded(ns, name, { routeOf })`): a call routed by
   * another is refused with RouteMismatch. It must be pure, and fixed for the life of a tree: a
   * tree written with one routeOf is not readable with another (use a new name). A route's keys
   * can't be split apart, so a route that is hot or large is one key's worth of shard.
   */
  protected routeOf(key: string): string {
    return key;
  }

  /** The hash a key is owned by: `keyHash(routeOf(key))`. */
  protected routeHash(key: string): string {
    // A call asks for a key's hash several times (the route check, ownership, the write): remembered.
    let hash = this.routeHashes.get(key);
    if (hash !== undefined) return hash;
    if (this.routeOf === ShardedObject.prototype.routeOf) hash = keyHash(key);
    else {
      const route = this.routeOf(key);
      if (typeof route !== "string") throw new Error(`routeOf(${JSON.stringify(key)}) is not a string`);
      hash = keyHash(route);
    }
    if (this.routeHashes.size >= ROUTE_HASHES) this.routeHashes.clear();
    this.routeHashes.set(key, hash);
    return hash;
  }
  private readonly routeHashes = new Map<string, string>();

  /** Whether the subclass routes keys by something other than themselves. */
  protected routesByKey(): boolean {
    return this.routeOf === ShardedObject.prototype.routeOf;
  }

  /** `splitPolicy`, checked once (a subclass sets it after the constructor here runs). */
  private get policy(): SplitPolicy {
    if (this.checkedPolicy !== this.splitPolicy) this.checkedPolicy = checkSplitPolicy(this.splitPolicy);
    return this.checkedPolicy;
  }
  private checkedPolicy: SplitPolicy | null = null;
  private readonly sql: SqlStorage;
  private meta: Meta | null;
  private readonly calls = new Map<number, number>();
  private quietUntil = Date.now();
  /** The split in progress here: writes to `right` wait for `released`. */
  private hold: Hold | null = null;
  /** A split is starting its new object, before it holds anything. */
  private warming = false;
  private counters = { since: Date.now(), calls: 0, ok: 0, moved: 0, notReady: 0, stale: 0, held: 0, heldMsMax: 0 };
  /** The split attempt whose logical copy this object takes (in memory: a restart refuses the rest). */
  private receiving: number | null = null;
  /** The stub options of the last routed call (in memory only): see `stubOptions`. */
  private callerStubOptions: StubOptions | null = null;
  /** Whether this object's tables exist: they are made by its first write, not by every visit. */
  private schemaReady: boolean;
  /** When this instance was wiped (shardedWipe): it refuses calls for WIPED_REFUSE_MS after. */
  private wipedAt: number | null = null;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // The alarm is the base class's: it runs splits' work, and a subclass's code after it would
    // also run in a split's copy, on work that is the original's. Subclasses use onAlarm.
    if ((this as { alarm: unknown }).alarm !== ShardedObject.prototype.alarm) {
      throw new Error(`${new.target.name} overrides alarm(): a sharded object's alarm belongs to the base class. Override onAlarm() and schedule it with scheduleAlarm() instead.`);
    }
    this.sql = ctx.storage.sql;
    // An object only visited (a lazy shard asked whether it exists, a wiped one) stays empty.
    this.schemaReady = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'sharded_meta'").one().n > 0;
    this.meta = this.schemaReady ? this.loadMeta() : null;
  }

  private ensureSchema(): void {
    if (this.schemaReady) return;
    this.sql.exec(SCHEMA);
    this.schemaReady = true;
  }

  private assertNotWiped(): void {
    if (this.wipedAt === null) return;
    if (Date.now() - this.wipedAt < WIPED_REFUSE_MS) throw new Error("this shard was wiped");
    // Any call still in flight when it was wiped has long arrived: the name may be used again.
    this.wipedAt = null;
  }
  private get wiped(): boolean {
    return this.wipedAt !== null && Date.now() - this.wipedAt < WIPED_REFUSE_MS;
  }

  // ---- Reads and writes for subclasses ----

  /** The current rows of these keys (absent keys are missing from the map). */
  protected async read(keys: readonly string[]): Promise<Map<string, V>> {
    this.assertOwner(keys);
    const out = new Map<string, V>();
    for (const key of keys) {
      const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_rows WHERE key = ?", key).toArray()[0];
      if (row) out.set(key, JSON.parse(row.value) as V);
    }
    return out;
  }

  /**
   * Reads these keys and writes what `decide` returns, in one transaction, if this shard still owns
   * them. `decide` must be synchronous. While the right half is being copied, an update touching
   * it waits; once the copy serves, it answers "moved".
   */
  protected async update<R>(keys: readonly string[], decide: (current: Map<string, V>) => UpdateResult<V, R>): Promise<R> {
    await this.waitForHold(keys);
    // No await from here on: the check and the transaction see the same split state.
    this.assertOwner(keys);
    return this.ctx.storage.transactionSync(() => {
      const current = new Map<string, V>();
      for (const key of keys) {
        const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_rows WHERE key = ?", key).toArray()[0];
        if (row) current.set(key, JSON.parse(row.value) as V);
      }
      const { writes, result } = decide(current);
      const fenced = new Set(keys);
      for (const [key, value] of writes) {
        // An unlisted key was neither checked as this shard's nor held for a split: its write could be lost.
        if (!fenced.has(key)) throw new Error(`update wrote ${JSON.stringify(key)}, which is not one of its keys: list every key it writes`);
        if (value === null) this.sql.exec("DELETE FROM sharded_rows WHERE key = ?", key);
        else this.sql.exec("INSERT INTO sharded_rows (key, hash, value) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, this.routeHash(key), JSON.stringify(value));
      }
      return result;
    });
  }

  /**
   * Runs `fn` in one transaction, fenced like `update`: it waits while any of `keys` is being
   * copied by a split, and runs only if this shard still owns them all. Write the subclass's own
   * tables here, never outside: a write that skips the fence can land after the copy and be lost.
   * List every key whose rows `fn` touches. `fn` must be synchronous.
   */
  protected async transact<R>(keys: readonly string[], fn: () => R): Promise<R> {
    // As `update`, without reading the keys' rows of `read`/`update`: `fn` reads what it needs.
    await this.waitForHold(keys);
    // No await from here on: the check and the transaction see the same split state.
    this.assertOwner(keys);
    return this.ctx.storage.transactionSync(fn);
  }

  /**
   * Up to `limit` of this shard's rows (of `read`/`update`) with keys after `afterKey`, in key
   * order: bounded paging over one shard. Rows of keys it no longer owns are skipped, so a page
   * is right while cleanup is still running.
   */
  protected ownedRowsPage(afterKey: string | null, limit: number): [string, V][] {
    this.assertOwner([]);
    if (!Number.isInteger(limit) || limit < 1) throw new Error("ownedRowsPage limit must be a positive integer");
    const [lo, hi] = hashRange(this.meta!.bits);
    const out: [string, V][] = [];
    for (const r of this.sql.exec<{ key: string; value: string }>("SELECT key, value FROM sharded_rows WHERE (? IS NULL OR key > ?) AND hash >= ? AND hash <= ? ORDER BY key LIMIT ?", afterKey, afterKey, lo, hi, limit)) out.push([r.key, JSON.parse(r.value) as V]);
    return out;
  }

  /**
   * Runs before this shard copies its storage for a split, while writes to the half that moves
   * wait: a subclass with work queued under rows it is giving away (an outbox) finishes it here,
   * so the new shard continues after it. It may run more than once.
   */
  protected async beforeHandOut(): Promise<void> {}

  /**
   * Runs in the new object of a split when it claims its copy, inside that transaction. A split
   * copies every table, the subclass's too, and the original keeps its subclass tables and the
   * work queued in them (an outbox it goes on delivering), so the copy drops its share of that
   * work here. `owns(key)` says whether a key is the new shard's. Synchronous. The rows of
   * `read`/`update` are handled by the base class.
   */
  protected afterClone(_owns: (key: string) => boolean): void {}

  /**
   * Deletes up to `limit` rows of the subclass's own tables that belong to keys this shard no
   * longer owns, and returns how many it deleted. After a split, the alarm calls it on both
   * shards, in paced batches, until it returns 0, as it does for the rows of `read`/`update`.
   * Until then a subclass that queries its tables in bulk (listings, counts) filters with
   * `owns`. The default deletes nothing: an original that keeps delivering an outbox for keys
   * that moved keeps those rows.
   */
  protected cleanupForeign(_owns: (key: string) => boolean, _limit: number): number {
    return 0;
  }

  /**
   * Throws (and the call answers "moved") unless this shard serves and owns every key. For reads of
   * the subclass's own tables; writes go through `update` or `transact`, which also wait on splits.
   */
  protected assertOwns(keys: readonly string[]): void {
    this.assertOwner(keys);
  }

  /** The routing hashes this shard owns, inclusive, as 16-hex-digit strings that sort like numbers. */
  protected ownedHashRange(): readonly [string, string] {
    if (!this.meta) throw new Error("node has no name yet");
    return hashRange(this.meta.bits);
  }

  /** Whether `key` is this shard's now. Keys of a split's moving half stay this shard's until it commits. */
  protected owns(key: string): boolean {
    if (!this.meta) throw new Error("node has no name yet");
    return ownsHash(this.meta.bits, this.routeHash(key));
  }

  /**
   * Ends the call as "not ready": nothing more is done, and the client backs off and calls again.
   * For work a shard does in slices before it can serve (building an index over its rows): each
   * call does a slice, commits it, and ends here until the work is done.
   */
  protected notReady(): never {
    throw new NotReady(Date.now());
  }

  /** This shard's name (`${name}#${bits}`), once it has one. */
  protected nodeLabel(): string {
    if (!this.meta) throw new Error("node has no name yet");
    return nodeName(this.meta.name, this.meta.bits);
  }

  /** The shard this one split from and when; null for the root. */
  protected parentNode(): { label: string; adoptedAt: number } | null {
    if (!this.meta || this.meta.parent === null) return null;
    return { label: nodeName(this.meta.name, this.meta.parent), adoptedAt: this.meta.splitAt };
  }

  /** The name all shards of this object share (what the client was created with). */
  protected objectName(): string {
    if (!this.meta) throw new Error("node has no name yet");
    return this.meta.name;
  }

  // ---- Atomic calls across shards (RFD0008) ----

  /** The keys atomic calls hold here, loaded from storage on first use (null: not loaded yet). */
  private held: Map<string, HeldKey> | null = null;
  /** Whether the atomic tables exist here. */
  private atomicReady: boolean | null = null;

  private ensureAtomicSchema(): void {
    if (this.atomicReady) return;
    this.ensureSchema();
    this.sql.exec(ATOMIC_SCHEMA);
    this.atomicReady = true;
  }

  private hasAtomicTables(): boolean {
    this.atomicReady ??= this.schemaReady && this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'sharded_atomic_holds'").one().n > 0;
    return this.atomicReady;
  }

  /** Every key an atomic call holds here, by key. */
  private heldKeys(): Map<string, HeldKey> {
    if (this.held) return this.held;
    this.held = new Map();
    if (this.hasAtomicTables()) for (const h of this.loadHolds()) this.holdInMemory(h);
    return this.held;
  }

  private loadHolds(): AtomicHold[] {
    if (!this.hasAtomicTables()) return [];
    return this.sql
      .exec<{ tx: string; primary_key: string; deadline: number; method: string; keys: string; staged: string }>("SELECT * FROM sharded_atomic_holds")
      .toArray()
      .map((r) => ({ tx: r.tx, primaryKey: r.primary_key, deadline: r.deadline, method: r.method, keys: JSON.parse(r.keys) as string[], staged: JSON.parse(r.staged) as unknown }));
  }

  private loadHold(tx: string): AtomicHold | null {
    if (!this.hasAtomicTables()) return null;
    const r = this.sql.exec<{ tx: string; primary_key: string; deadline: number; method: string; keys: string; staged: string }>("SELECT * FROM sharded_atomic_holds WHERE tx = ?", tx).toArray()[0];
    return r ? { tx: r.tx, primaryKey: r.primary_key, deadline: r.deadline, method: r.method, keys: JSON.parse(r.keys) as string[], staged: JSON.parse(r.staged) as unknown } : null;
  }

  private holdInMemory(h: AtomicHold): void {
    let release!: () => void;
    const released = new Promise<void>((resolve) => (release = resolve));
    for (const key of h.keys) this.held!.set(key, { tx: h.tx, deadline: h.deadline, released, release });
  }

  /** Lets the writes that wait on `tx`'s keys go. */
  private releaseInMemory(tx: string): void {
    const held = this.heldKeys();
    for (const [key, h] of held) {
      if (h.tx !== tx) continue;
      held.delete(key);
      h.release();
    }
  }

  /** The earliest deadline of a hold here, or null. */
  private nextHoldDeadline(): number | null {
    let next: number | null = null;
    for (const h of this.heldKeys().values()) if (next === null || h.deadline < next) next = h.deadline;
    return next;
  }

  /** The subclass's atomic method `method`. */
  private atomicMethod(method: string): AtomicMethod<V, unknown, unknown, unknown, unknown> {
    const m = Object.prototype.hasOwnProperty.call(this.atomic, method) ? this.atomic[method] : undefined;
    if (!m) throw new Error(`no atomic method ${method}`);
    return m as AtomicMethod<V, unknown, unknown, unknown, unknown>;
  }

  /** The keys a share holds and reads: its own, then the method's holdKeys, once each. */
  private holdKeysOf(m: AtomicMethod<V, unknown, unknown, unknown, unknown>, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): string[] {
    const extra = m.holdKeys ? (m.holdKeys as (...a: unknown[]) => readonly string[])(keys, payloads, ...args) : [];
    return [...new Set([...keys, ...extra])];
  }

  /** The rows of `keys`, inside a transaction. */
  private rowsOf(keys: readonly string[]): Map<string, V> {
    const current = new Map<string, V>();
    for (const key of keys) {
      const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_rows WHERE key = ?", key).toArray()[0];
      if (row) current.set(key, JSON.parse(row.value) as V);
    }
    return current;
  }

  /** Writes rows, inside a transaction; only `keys` (what was fenced and checked). */
  private writeRows(keys: readonly string[], writes: ReadonlyMap<string, V | null>): void {
    const fenced = new Set(keys);
    for (const [key, value] of writes) {
      if (!fenced.has(key)) throw new Error(`atomic apply wrote ${JSON.stringify(key)}, which it neither holds nor lists in holdKeys`);
      if (value === null) this.sql.exec("DELETE FROM sharded_rows WHERE key = ?", key);
      else this.sql.exec("INSERT INTO sharded_rows (key, hash, value) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, this.routeHash(key), JSON.stringify(value));
    }
  }

  /**
   * Runs `inTransaction` once these keys are free: no split copying them, no other atomic call
   * holding them, this shard owning them. `gather` (I/O) runs first, and again if a hold appeared
   * while it ran, so the transaction checks rows nobody holds.
   */
  private async whenFree<T>(keys: readonly string[], exceptTx: string | undefined, gather: () => Promise<unknown>, inTransaction: (gathered: unknown) => T): Promise<T> {
    for (;;) {
      await this.waitForHold(keys, exceptTx);
      const gathered = await gather();
      // No await from here on: the check and the transaction see the same holds and split state.
      if (this.holding(keys, exceptTx)) continue;
      this.assertOwner(keys);
      return this.ctx.storage.transactionSync(() => inTransaction(gathered));
    }
  }

  private async atomicStep(step: AtomicStep, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicAnswer> {
    switch (step.step) {
      case "solo":
        return this.atomicSolo(step.method, keys, payloads, args);
      case "prepare":
        return this.atomicPrepare(step, keys, payloads, args);
      case "commit":
        return this.atomicCommit(step.tx, step.method, keys, payloads, args);
      case "abort":
      case "outcome":
        this.assertOwner(keys);
        return { outcome: this.recordOutcome(step.tx, "abort") };
      case "forget":
        this.assertOwner(keys);
        if (this.hasAtomicTables()) this.sql.exec("DELETE FROM sharded_atomic_outcomes WHERE tx = ?", step.tx);
        return { gone: true };
      case "resolve":
        return this.atomicResolve(step.tx, step.outcome);
    }
  }

  /** The gather of `m`, bound to this share's arguments. */
  private gatherOf(m: AtomicMethod<V, unknown, unknown, unknown, unknown>, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): () => Promise<unknown> {
    return m.gather ? () => (m.gather as (...a: unknown[]) => Promise<unknown>)(keys, payloads, ...args) : async () => undefined;
  }

  private checkOf(m: AtomicMethod<V, unknown, unknown, unknown, unknown>, current: ReadonlyMap<string, V>, keys: readonly string[], payloads: readonly unknown[] | null, gathered: unknown, args: readonly unknown[]) {
    return (m.check as (...a: unknown[]) => { staged: unknown } | { refused: unknown })(current, keys, payloads, gathered, ...args);
  }

  /** The whole call in this shard: check and apply in one transaction, as `update`. */
  private async atomicSolo(method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicAnswer> {
    const m = this.atomicMethod(method);
    const hold = this.holdKeysOf(m, keys, payloads, args);
    const done = await this.whenFree(hold, undefined, this.gatherOf(m, keys, payloads, args), (gathered) => {
      const current = this.rowsOf(hold);
      const p = this.checkOf(m, current, keys, payloads, gathered, args);
      if ("refused" in p) return { refused: p.refused };
      const { writes, result } = m.apply(current, p.staged);
      this.writeRows(hold, writes);
      return { applied: result };
    });
    return "applied" in done && m.applied ? { applied: await m.applied(keys, done.applied) } : done;
  }

  /** A share that is not the primary's: checked, and if it would apply, kept and held. */
  private async atomicPrepare(step: Extract<AtomicStep, { step: "prepare" }>, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicAnswer> {
    // A prepare delivered twice: the first holds already.
    if (this.loadHold(step.tx)) return { prepared: true };
    const m = this.atomicMethod(step.method);
    const keysHeld = this.holdKeysOf(m, keys, payloads, args);
    const done = await this.whenFree(keysHeld, undefined, this.gatherOf(m, keys, payloads, args), (gathered): AtomicAnswer => {
      const p = this.checkOf(m, this.rowsOf(keysHeld), keys, payloads, gathered, args);
      if ("refused" in p) return { refused: p.refused };
      this.ensureAtomicSchema();
      this.heldKeys();
      this.sql.exec("INSERT INTO sharded_atomic_holds (tx, primary_key, deadline, method, keys, staged) VALUES (?, ?, ?, ?, ?, ?)", step.tx, step.primaryKey, step.deadline, step.method, JSON.stringify(keysHeld), JSON.stringify(p.staged ?? null));
      // Held in memory in the same turn: a write that runs once this transaction is done waits.
      this.holdInMemory({ tx: step.tx, primaryKey: step.primaryKey, deadline: step.deadline, method: step.method, keys: keysHeld, staged: null });
      return { prepared: true };
    });
    if ("prepared" in done) await this.armAlarm(step.deadline);
    return done;
  }

  /**
   * The primary's share: checked, and if it would apply, the outcome "commit" recorded and the
   * share applied, in one transaction; refused, "abort" recorded. An outcome recorded already
   * (a recovery that got here first) is answered as it is.
   */
  private async atomicCommit(tx: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicAnswer> {
    const m = this.atomicMethod(method);
    const hold = this.holdKeysOf(m, keys, payloads, args);
    // Its own shares held here (a client that routed a key here as another shard's) don't block it.
    const done = await this.whenFree(hold, tx, this.gatherOf(m, keys, payloads, args), (gathered): AtomicAnswer => {
      const recorded = this.outcomeOf(tx);
      if (recorded) return { outcome: recorded };
      const current = this.rowsOf(hold);
      const p = this.checkOf(m, current, keys, payloads, gathered, args);
      if ("refused" in p) {
        this.recordOutcome(tx, "abort");
        return { refused: p.refused };
      }
      this.recordOutcome(tx, "commit");
      const { writes, result } = m.apply(current, p.staged);
      this.writeRows(hold, writes);
      return { applied: result };
    });
    return "applied" in done && m.applied ? { applied: await m.applied(keys, done.applied) } : done;
  }

  private outcomeOf(tx: string): Outcome | null {
    if (!this.hasAtomicTables()) return null;
    return (this.sql.exec<{ outcome: Outcome }>("SELECT outcome FROM sharded_atomic_outcomes WHERE tx = ?", tx).toArray()[0]?.outcome as Outcome | undefined) ?? null;
  }

  /** Records `outcome` for `tx` unless one is recorded; returns the one that is. */
  private recordOutcome(tx: string, outcome: Outcome): Outcome {
    const recorded = this.outcomeOf(tx);
    if (recorded) return recorded;
    this.ensureAtomicSchema();
    const now = Date.now();
    this.sql.exec("INSERT INTO sharded_atomic_outcomes (tx, outcome, at) VALUES (?, ?, ?)", tx, outcome, now);
    // Outcomes nobody forgot (a caller that died after the commit) go long after any hold could ask.
    this.sql.exec("DELETE FROM sharded_atomic_outcomes WHERE at < ?", now - ATOMIC_OUTCOME_TTL_MS);
    return outcome;
  }

  /** A share prepared here: applied (commit) or dropped (abort), and its keys released, in one transaction. */
  private async atomicResolve(tx: string, outcome: Outcome): Promise<AtomicAnswer> {
    const h = this.loadHold(tx);
    if (!h) return { gone: true };
    const m = this.atomicMethod(h.method);
    const result = this.ctx.storage.transactionSync(() => {
      let applied: unknown = undefined;
      if (outcome === "commit") {
        const { writes, result } = m.apply(this.rowsOf(h.keys), h.staged);
        this.writeRows(h.keys, writes);
        applied = result;
      }
      this.sql.exec("DELETE FROM sharded_atomic_holds WHERE tx = ?", tx);
      return applied;
    });
    this.releaseInMemory(tx);
    if (outcome === "abort") return { outcome: "abort" };
    return { applied: m.applied ? await m.applied(h.keys, result) : result };
  }

  /** Holds past their deadline: asks each one's primary for the outcome, and resolves it. */
  private recovering: Promise<void> | null = null;
  private recoverHolds(): Promise<void> {
    this.recovering ??= this.recoverDue().finally(() => (this.recovering = null));
    return this.recovering;
  }

  private async recoverDue(): Promise<void> {
    const due = this.loadHolds().filter((h) => h.deadline <= Date.now());
    if (due.length === 0) return;
    const client = sharded(this.nodes() as DurableObjectNamespace<ShardedObject<Env, V>>, this.objectName(), {
      ...(this.routesByKey() ? {} : { routeOf: (key: string) => this.routeOf(key) }),
      ...(this.callerStubOptions ? { stubOptions: this.callerStubOptions } : {}),
    });
    await Promise.all(
      due.map(async (h) => {
        const outcome = await client.atomicOutcome(h.tx, h.primaryKey);
        await this.atomicResolve(h.tx, outcome);
        console.log(JSON.stringify({ event: "sharded-atomic-recovered", node: this.nodeLabel(), tx: h.tx, lateMs: Date.now() - h.deadline, outcome }));
      }),
    );
  }

  private assertOwner(keys: readonly string[]): void {
    const meta = this.meta;
    if (meta?.state !== "shard") throw new Moved();
    for (const key of keys) if (!ownsHash(meta.bits, this.routeHash(key))) throw new Moved();
  }

  /**
   * Waits while a split copies any of these keys, or an atomic call holds them (other than
   * `exceptTx`), up to `holdWaitMs`; then "not ready".
   */
  private async waitForHold(keys: readonly string[], exceptTx?: string): Promise<void> {
    if (!this.holding(keys, exceptTx)) return;
    const start = Date.now();
    const deadline = start + (this.policy.holdWaitMs ?? HOLD_WAIT_MS);
    const counted = new Set<Hold>();
    // Counted on every split this write waits on (one, unless a split follows another).
    const waited = (gaveUp: boolean) => {
      const ms = Date.now() - start;
      this.counters.heldMsMax = Math.max(this.counters.heldMsMax, ms);
      for (const split of counted) {
        split.heldMsMax = Math.max(split.heldMsMax, ms);
        if (gaveUp && split === this.hold) split.notReady++;
      }
    };
    this.counters.held++;
    for (let hold = this.holding(keys, exceptTx); hold; hold = this.holding(keys, exceptTx)) {
      if (this.hold && !counted.has(this.hold)) {
        counted.add(this.hold);
        this.hold.held++;
      }
      // An atomic hold past its deadline: its caller is gone or slow, so ask its primary now
      // rather than at the alarm.
      if (hold.atomic && hold.until <= Date.now()) void this.recoverHolds();
      const left = deadline - Date.now();
      if (left <= 0) {
        waited(true);
        throw new NotReady(hold.until);
      }
      let timer: ReturnType<typeof setTimeout> | undefined;
      const released = await Promise.race([hold.released.then(() => true), new Promise<boolean>((resolve) => (timer = setTimeout(() => resolve(false), left)))]);
      clearTimeout(timer);
      if (!released) {
        waited(true);
        throw new NotReady(hold.until);
      }
    }
    waited(false);
  }

  /** Counts an answer other than "ok". */
  private answered<A extends CallAnswer<unknown>>(answer: A): A {
    if ("moved" in answer) this.counters.moved++;
    else if ("notReady" in answer) this.counters.notReady++;
    else if ("stale" in answer) this.counters.stale++;
    return answer;
  }

  /** What an update of these keys waits for, if any: a split copying them, or an atomic call (not `exceptTx`) holding them. */
  private holding(keys: readonly string[], exceptTx?: string): Waiting | null {
    const hold = this.hold;
    if (hold) for (const key of keys) if (ownsHash(hold.right, this.routeHash(key))) return { released: hold.released, until: hold.deadline, atomic: false };
    const held = this.heldKeys();
    if (held.size > 0) {
      for (const key of keys) {
        const h = held.get(key);
        if (h && h.tx !== exceptTx) return { released: h.released, until: h.deadline, atomic: true };
      }
    }
    return null;
  }

  // ---- The routed call ----

  /** A call routed by the client: `method(keys, payloads, ...args)` if this shard owns the keys. */
  async shardedCall(name: string, bits: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[], stubOptions: StubOptions | null = null, presplitTo = 0, hashes: readonly string[] | null = null): Promise<CallAnswer<unknown>> {
    this.assertNotWiped();
    this.callerStubOptions = stubOptions;
    // Routed by another routeOf: even where it would land here (the root owns every key), the
    // client would look for these keys elsewhere once the tree splits.
    if (hashes) {
      for (let i = 0; i < keys.length; i++) {
        const own = this.routeHash(keys[i]!);
        if (hashes[i] !== own) return { routeMismatch: { key: keys[i]!, client: hashes[i]!, object: own } };
      }
    }
    const answer = await this.arriveOrCreate(name, bits, presplitTo);
    if (answer) return this.answered(answer);
    this.counters.calls++;
    for (const key of keys) if (!ownsHash(bits, this.routeHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);
    this.countCall(keys.length);
    const fn = this.routedMethod(method);
    try {
      const ok = await (fn as (...a: unknown[]) => Promise<unknown>).call(this, keys, payloads, ...args);
      this.counters.ok++;
      return { ok };
    } catch (error) {
      if (error instanceof Moved) return this.answered(this.moved(bits));
      if (error instanceof NotReady) return this.answered({ notReady: bits, until: error.until });
      throw error;
    }
  }

  /** One step of an atomic call (RFD0008), routed by the client as a call is: see AtomicStep. */
  async shardedAtomic(name: string, bits: string, step: AtomicStep, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[], stubOptions: StubOptions | null = null, presplitTo = 0, hashes: readonly string[] | null = null): Promise<CallAnswer<AtomicAnswer>> {
    this.assertNotWiped();
    this.callerStubOptions = stubOptions;
    if (hashes) {
      for (let i = 0; i < keys.length; i++) {
        const own = this.routeHash(keys[i]!);
        if (hashes[i] !== own) return { routeMismatch: { key: keys[i]!, client: hashes[i]!, object: own } };
      }
    }
    const answer = await this.arriveOrCreate(name, bits, presplitTo);
    if (answer) return this.answered(answer);
    this.counters.calls++;
    for (const key of keys) if (!ownsHash(bits, this.routeHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);
    this.countCall(keys.length);
    try {
      const ok = await this.atomicStep(step, keys, payloads, args);
      this.counters.ok++;
      return { ok };
    } catch (error) {
      if (error instanceof Moved) return this.answered(this.moved(bits));
      if (error instanceof NotReady) return this.answered({ notReady: bits, until: error.until });
      throw error;
    }
  }

  /** A call to every shard: `method(...args)`, with no keys. */
  async shardedScan(name: string, bits: string, method: string, args: readonly unknown[], stubOptions: StubOptions | null = null, presplitTo = 0): Promise<CallAnswer<unknown>> {
    this.assertNotWiped();
    this.callerStubOptions = stubOptions;
    const answer = await this.arriveOrCreate(name, bits, presplitTo);
    if (answer) return this.answered(answer);
    this.counters.calls++;
    const fn = this.routedMethod(method);
    try {
      const ok = await (fn as (...a: unknown[]) => Promise<unknown>).call(this, ...args);
      this.counters.ok++;
      return { ok };
    } catch (error) {
      if (error instanceof Moved) return this.answered(this.moved(bits));
      // A scan method that writes waits on a split as a call does; the client backs off and visits again.
      if (error instanceof NotReady) return this.answered({ notReady: bits, until: error.until });
      throw error;
    }
  }

  /**
   * The subclass method a routed call or scan names. Never one of this base class's own (its
   * reads and writes, the split protocol, the alarm) nor a runtime handler: a call must not reach
   * them with arguments it shaped for something else.
   */
  private routedMethod(method: string): (...args: unknown[]) => unknown {
    const fn = (this as unknown as Record<string, unknown>)[method];
    if (typeof fn !== "function" || method.startsWith("sharded") || RESERVED_METHODS.has(method)) throw new Error(`no method ${method}`);
    return fn as (...args: unknown[]) => unknown;
  }

  /** Every row this shard owns, for scans (a subclass's scan method may use it). */
  protected allRows(): Map<string, V> {
    this.assertOwner([]);
    const [lo, hi] = hashRange(this.meta!.bits);
    const out = new Map<string, V>();
    for (const r of this.sql.exec<{ key: string; value: string }>("SELECT key, value FROM sharded_rows WHERE hash >= ? AND hash <= ?", lo, hi)) out.set(r.key, JSON.parse(r.value) as V);
    return out;
  }

  /** "moved" for a call addressed to `bits`: this shard went deeper, so `bits` and more split. */
  private moved(bits: string): CallAnswer<never> {
    const meta = this.meta!;
    const moved = meta.bits.length > bits.length ? meta.bits.slice(0, -1) : bits;
    return meta.presplit ? { moved, presplit: meta.presplit } : { moved };
  }

  /**
   * `arrive`, for an object reached with the depth the client learned (`presplitTo`): a new
   * object at that depth, or above it on its all-zeros path (the same object, by its name),
   * creates itself empty, if the tree's root says the tree has that depth. One call to the root
   * per shard, ever: the root fixed the depth when it was first called, so a client that
   * remembers another (the name's storage was deleted, and `initialDepth` changed) is told its map
   * is stale, and learns the depth again (specs/lazy-presplit.ts). Safe because no shard above the
   * initial depth ever holds rows: none is created.
   */
  private async arriveOrCreate(name: string, bits: string, presplitTo: number): Promise<CallAnswer<never> | null> {
    // (The root's own object, reached at its all-zeros bits, creates itself in `arrive`.)
    if (!this.meta && hostBits(bits) !== "" && presplitTo > 0 && bits.length <= presplitTo) {
      const started = Date.now();
      const depth = await this.node("", name).shardedTreeDepth(name);
      // A sample of what a new shard's first call costs: the root's answer is on its path.
      if (this.shardCreatedLogShare > 0 && Math.random() < this.shardCreatedLogShare) console.log(JSON.stringify({ event: "shard-created", node: nodeName(name, bits), rootMs: Date.now() - started }));
      // Another first call may have created this object meanwhile.
      if (!this.meta) {
        if (depth !== presplitTo) return { stale: bits };
        this.create(name, bits.padEnd(depth, "0"));
      }
    }
    return this.arrive(name, bits);
  }

  /** The root's answer to a new shard: the tree's presplit depth, creating the root if it is new. */
  async shardedTreeDepth(name: string): Promise<number> {
    this.assertNotWiped();
    if (!this.meta) this.create(name, "0".repeat(this.policy.initialDepth ?? 0));
    if (this.meta!.name !== name || hostBits(this.meta!.bits) !== "") throw new Error(`${nodeName(name, "")} reached ${nodeName(this.meta!.name, this.meta!.bits)}`);
    return this.meta!.presplit ?? 0;
  }

  /** A new shard, empty and serving: the root (at the initial depth), or one at the tree's depth. */
  private create(name: string, bits: string): void {
    const presplit = bits.length;
    this.saveMeta({ name, bits, state: "shard", epoch: 0, splitting: null, activate: null, cleanup: [], ...(presplit > 0 ? { presplit } : {}), parent: presplit > 0 ? bits.slice(0, -1) : null, splitAt: Date.now() });
  }

  /**
   * Whether this object serves `bits`; null if it does, else what to answer. A new root object
   * creates itself, as the all-zeros shard at this class's initial depth, which fixes the tree's
   * (a caller that expected another depth is then moved, or told its map is stale); any other new
   * object is a split's copy that has not arrived, or a name whose storage is gone.
   */
  private arrive(name: string, bits: string): CallAnswer<never> | null {
    if (!this.meta) {
      if (hostBits(bits) !== "") return { notReady: bits };
      this.create(name, "0".repeat(this.policy.initialDepth ?? 0));
    }
    const meta = this.meta!;
    // Storage copied from another object and not claimed yet: it keeps that object's meta.
    if (this.isUnclaimedCopy()) return { notReady: bits };
    if (meta.name !== name || hostBits(meta.bits) !== hostBits(bits)) throw new Error(`${nodeName(name, bits)} reached ${nodeName(meta.name, meta.bits)}`);
    if (meta.state === "copy") return { notReady: bits };
    // A split interrupted by a restart: abandoned (its copy is never activated).
    if (meta.splitting !== null && !this.hold) {
      this.saveMeta({ ...meta, splitting: null });
      if (meta.activate || meta.cleanup.length > 0 || meta.foreign) void this.armAlarm(Date.now());
    }
    this.checkAlarm();
    if (meta.bits === bits) return null;
    if (meta.bits.startsWith(bits)) return this.moved(bits);
    // Splits are final: a caller that knows this shard split, when it has not, learned that from
    // storage that is gone.
    return { stale: bits };
  }

  private loadMeta(): Meta | null {
    const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_meta WHERE key = 'meta'").toArray()[0];
    return row ? (JSON.parse(row.value) as Meta) : null;
  }

  private saveMeta(meta: Meta): void {
    this.ensureSchema();
    this.meta = meta;
    this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
  }

  // ---- Load and splitting ----

  private countCall(keys: number): void {
    const second = Math.floor(Date.now() / 1000);
    this.calls.set(second, (this.calls.get(second) ?? 0) + 1 + keys / (this.policy.keysPerCall ?? 25));
    if (this.calls.size > this.policy.windowSeconds + 2) {
      for (const s of this.calls.keys()) if (s < second - this.policy.windowSeconds - 1) this.calls.delete(s);
    }
    if (Date.now() - this.quietUntil > this.policy.windowSeconds * 1000 && this.rate() > this.policy.callsPerSecond && this.splittable()) void this.split();
    this.logLoad(second);
  }

  /**
   * Every LOAD_LOG_S seconds, a shard above half its split rate says how busy it is and what a
   * split would wait on: why a busy shard does not split shows in the logs, not only in its callers'
   * latency.
   */
  private logLoad(second: number): void {
    if (second - this.loadLoggedAt < LOAD_LOG_S) return;
    const rate = this.rate();
    if (rate < this.policy.callsPerSecond / 2) return;
    this.loadLoggedAt = second;
    const meta = this.meta;
    console.log(JSON.stringify({ event: "shard-load", node: meta ? nodeName(meta.name, meta.bits) : "", rate: Math.round(rate), splitAt: this.policy.callsPerSecond, warming: this.warming ? 1 : 0, holding: this.hold ? 1 : 0, state: meta?.state ?? "", quietMs: Date.now() - this.quietUntil }));
  }

  private loadLoggedAt = 0;

  /**
   * Whether a split would spread this shard's load (asked only once the load calls for one). A
   * split halves the keys, not the load of any one key: a subclass whose load is one hot key,
   * which it serves some other way, answers false, and the shard stays put rather than handing
   * that key to a new object that knows nothing of it. True by default.
   */
  protected splittable(): boolean {
    return true;
  }

  /** Average calls/s over the last full `windowSeconds`. */
  private rate(): number {
    const now = Math.floor(Date.now() / 1000);
    let sum = 0;
    for (let s = now - this.policy.windowSeconds; s < now; s++) sum += this.calls.get(s) ?? 0;
    return sum / this.policy.windowSeconds;
  }

  /** Splits now if this object hosts the shard `bits` (also an operator's lever). */
  async shardedSplit(name: string, bits: string): Promise<boolean> {
    this.assertNotWiped();
    if (this.arrive(name, bits)) return false;
    return this.split();
  }

  /**
   * Hands the right half to a new object: holds writes to it, copies this object's storage there,
   * and once the copy is claimed stops owning the right half and activates it. A copy that is not
   * claimed in time is abandoned, and the held writes go ahead here.
   */
  private async split(): Promise<boolean> {
    const before = this.meta;
    // A shard holding keys for atomic calls stays whole until they end (milliseconds, or a hold's
    // deadline): a hold and the share it keeps belong to one object.
    if (!before || this.hold || this.warming || before.state !== "shard" || before.bits.length >= this.policy.maxDepth || this.heldKeys().size > 0) return false;
    const right = `${before.bits}1`;
    // Start the new object before holding writes: a first call to an object can take seconds.
    this.warming = true;
    const warmStarted = Date.now();
    let warmTimer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([this.node(right).shardedWarm(), new Promise<void>((resolve) => (warmTimer = setTimeout(resolve, WARM_WAIT_MS)))]);
    } catch {
      // The split's own calls retry, or give up.
    } finally {
      clearTimeout(warmTimer);
      this.warming = false;
    }
    const warm = Date.now() - warmStarted;
    // Meanwhile the meta may have been saved (cleanup progress): split from the current one.
    const meta = this.meta;
    if (!meta || meta.bits !== before.bits || meta.state !== "shard" || meta.epoch !== before.epoch || this.hold || this.heldKeys().size > 0) return false;
    const epoch = meta.epoch + 1;
    let release!: () => void;
    const started = Date.now();
    const timeout = this.policy.splitTimeoutMs ?? 60_000;
    const hold = { epoch, right, released: new Promise<void>((resolve) => (release = resolve)), deadline: started + timeout, held: 0, notReady: 0, heldMsMax: 0 };
    this.hold = hold;
    const [lo, hi] = hashRange(meta.bits);
    const [rightLo] = hashRange(right);
    const counted = this.sql.exec<{ rows: number; rightRows: number }>("SELECT COUNT(*) AS rows, COALESCE(SUM(hash >= ?), 0) AS rightRows FROM sharded_rows WHERE hash >= ? AND hash <= ?", rightLo, lo, hi).one();
    let copy: "native" | "logical" | null = null;
    const steps = { warm, handOut: 0, snapshot: 0, restore: 0, restoreInside: 0, copyRows: 0, claim: 0, claimAttempts: 0, claimError: null as string | null };
    const record = (outcome: SplitRecord["outcome"], error: string | null): SplitRecord[] => [
      ...(this.meta?.splits ?? []),
      { at: started, ms: Date.now() - started, outcome, copy, rows: counted.rows, rightRows: counted.rightRows, held: hold.held, notReady: hold.notReady, heldMsMax: hold.heldMsMax, steps: { ...steps }, error },
    ].slice(-SPLIT_HISTORY);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      this.saveMeta({ ...meta, epoch, splitting: epoch, from: this.ctx.id.toString() });
      const live = () => this.hold === hold && Date.now() - started < timeout;
      const copied = this.copyAndClaim(right, epoch, live, steps, (method) => (copy = method));
      copied.catch(() => {}); // after a timeout nobody waits for it
      const expired = new Promise<never>((_, reject) => (timer = setTimeout(() => reject(new Error(`split did not finish in ${timeout} ms`)), timeout)));
      const method = await Promise.race([copied, expired]);
      // The copy is claimed: from this transaction on, the right half is the sibling's.
      const now = this.meta!;
      this.saveMeta({ ...now, bits: `${meta.bits}0`, splitting: null, activate: { bits: right, epoch }, cleanup: [...now.cleanup, hashRange(right)], foreign: true, parent: meta.bits, splitAt: Date.now(), lastSplit: { copy: method, ms: Date.now() - started }, splits: record("split", null) });
      this.calls.clear();
      this.quietUntil = Date.now();
      console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), copy: method, ms: Date.now() - started }));
      this.hold = null;
      release();
      await this.armAlarm(Date.now());
      await this.activateSibling().catch(() => {}); // the alarm retries
      return true;
    } catch (error) {
      const now = this.meta;
      if (now) this.saveMeta({ ...now, splitting: now.splitting === epoch ? null : now.splitting, lastSplitError: String(error).slice(0, 500), splits: record("abandoned", String(error).slice(0, 500)) });
      this.quietUntil = Date.now();
      console.error(JSON.stringify({ event: "split-abandoned", node: nodeName(meta.name, meta.bits), error: String(error), ms: Date.now() - started }));
      return false;
    } finally {
      clearTimeout(timer);
      if (this.hold === hold) {
        this.hold = null;
        release();
      }
    }
  }

  /** Copies this object's storage to the right sibling, then claims the copy there. */
  private async copyAndClaim(right: string, epoch: number, live: () => boolean, steps: Steps, copied: (method: "native" | "logical") => void): Promise<"native" | "logical"> {
    let t = Date.now();
    await this.beforeHandOut();
    steps.handOut = Date.now() - t;
    const method = await this.copyTo(right, epoch, live, steps);
    copied(method);
    const meta = this.meta!;
    let lastError: unknown = null;
    t = Date.now();
    for (let wait = 10; live(); wait = Math.min(wait * 2, 500)) {
      steps.claimAttempts++;
      try {
        // A fresh stub each time: the sibling restarts to apply the restore, which breaks the old one.
        const claimed = await this.node(right).shardedClaim(meta.name, right, meta.bits, epoch);
        steps.claim = Date.now() - t;
        if (claimed) return method;
      } catch (error) {
        // The sibling restarting (or still starting on the copy). Claiming is idempotent.
        lastError = error;
        steps.claim = Date.now() - t;
        steps.claimError = String(error).slice(0, 200);
      }
      await new Promise((resolve) => setTimeout(resolve, wait));
    }
    throw new Error(`split timed out claiming its copy${lastError ? `: ${String(lastError)}` : ""}`);
  }

  private async copyTo(right: string, epoch: number, live: () => boolean, steps: Steps): Promise<"native" | "logical"> {
    const mode = this.policy.copy ?? "auto";
    if (mode !== "logical" && typeof this.ctx.storage.snapshot === "function") {
      let snapshot: DurableObjectSnapshot | null = null;
      let t = Date.now();
      try {
        snapshot = await this.ctx.storage.snapshot();
      } catch (error) {
        if (mode === "native") throw error; // auto: no snapshots here (local development)
      }
      steps.snapshot = Date.now() - t;
      if (snapshot) {
        t = Date.now();
        try {
          steps.restoreInside = (await this.node(right).shardedRestore(snapshot, epoch)).scheduleMs;
        } catch (error) {
          // The sibling restarts right after replying; under load the restart can overtake the
          // reply. Claiming tells whether the restore took.
          if (!String(error).includes(RESTORE_ABORT)) throw error;
        } finally {
          steps.restore = Date.now() - t;
        }
        return "native";
      }
    } else if (mode === "native") {
      throw new Error("storage snapshots need the durable_object_snapshots compatibility flag");
    }
    const t = Date.now();
    try {
      await this.copyRows(right, epoch, live);
    } finally {
      steps.copyRows = Date.now() - t;
    }
    return "logical";
  }

  /**
   * The logical copy, where snapshots are unavailable: every table, as a snapshot would carry
   * them, in byte-bounded batches. Writes to the half that moves are held meanwhile; rows of the
   * other half may change under the copy, which is fine: the copy drops them.
   *
   * Each table is paged by a key that a row keeps while it lives (its rowid, or its primary key
   * in a WITHOUT ROWID table), never by offset: the half that stays, and the cleanup of earlier
   * splits, delete rows during the copy, and an offset would then skip rows of the half that moves.
   */
  private async copyRows(right: string, epoch: number, live: () => boolean): Promise<void> {
    const meta = this.meta!;
    const target = this.node(right);
    // Tables and indexes first, as a snapshot would carry them: the copy may not have created a
    // subclass's yet (a sharded table creates its own on first use).
    // A virtual table's own (shadow) tables are not sent: creating it creates them, and its rows
    // are copied through it.
    const tables = this.tableList();
    const names = new Set(tables.map((t) => t.name));
    const schema = this.sql
      .exec<{ type: string; name: string; tbl_name: string; sql: string | null }>("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('table', 'index') ORDER BY type DESC, name")
      .toArray()
      .filter((o): o is { type: string; name: string; tbl_name: string; sql: string } => o.sql !== null && names.has(o.type === "table" ? o.name : o.tbl_name))
      .map(({ name, sql }) => ({ name, sql }));
    await target.shardedReceiveSchema(meta.name, right, epoch, schema);
    let first = true;
    for (const { name: table, withoutRowid } of tables) {
      const order = this.pageOrder(table, withoutRowid);
      const columns = order.columns.join(", ");
      const select = `SELECT ${order.rowid ? `${columns}, ` : ""}* FROM ${quoteIdentifier(table)}`;
      let after: SqlStorageValue[] | null = null;
      for (;;) {
        if (!live()) throw new Error("split timed out copying rows");
        const where = after ? ` WHERE (${columns}) > (${after.map(() => "?").join(", ")})` : "";
        const cursor: SqlStorageCursor<Record<string, SqlStorageValue>> = this.sql.exec(`${select}${where} ORDER BY ${columns} LIMIT ?`, ...(after ?? []), COPY_PAGE_ROWS);
        const names: string[] = cursor.columnNames;
        const keyAt: number[] = order.rowid ? [0] : order.columns.map((c) => names.indexOf(unquoteIdentifier(c)));
        const rows: SqlStorageValue[][] = [];
        let bytes = 0;
        for (const row of cursor.raw()) {
          after = keyAt.map((i): SqlStorageValue => row[i]!);
          rows.push(order.rowid ? row.slice(1) : row);
          for (const value of row) bytes += value instanceof ArrayBuffer ? value.byteLength : String(value).length;
          if (bytes >= COPY_BATCH_BYTES) break;
        }
        if (rows.length === 0 && !first) break;
        await target.shardedReceive(meta.name, right, epoch, first, table, order.rowid ? names.slice(1) : names, rows);
        first = false;
        if (rows.length === 0 || (rows.length < COPY_PAGE_ROWS && bytes < COPY_BATCH_BYTES)) break;
      }
    }
    if (!(await target.shardedReceiveDone(meta.name, right, epoch))) throw new Error("the copy did not take this split's meta");
  }

  /**
   * The columns that page a table in a stable order: its rowid (under a name no column shadows),
   * or the primary key of a WITHOUT ROWID table. Returned quoted, ready for SQL.
   */
  private pageOrder(table: string, withoutRowid: boolean): { columns: string[]; rowid: boolean } {
    const info = this.sql.exec<{ name: string; pk: number }>(`PRAGMA table_info(${quoteIdentifier(table)})`).toArray();
    if (withoutRowid) {
      const key = info.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk);
      if (key.length === 0) throw new Error(`table ${table} has no primary key to copy it by`);
      return { columns: key.map((c) => quoteIdentifier(c.name)), rowid: false };
    }
    const names = new Set(info.map((c) => c.name.toLowerCase()));
    const alias = ["rowid", "_rowid_", "oid"].find((a) => !names.has(a));
    if (!alias) throw new Error(`table ${table} shadows every name of its rowid: it cannot be copied in order`);
    return { columns: [alias], rowid: true };
  }

  /**
   * This object's tables, the subclass's too, virtual ones included: not SQLite's, the runtime's,
   * nor a virtual table's own (shadow) tables, which it manages itself.
   */
  private tableList(): { name: string; withoutRowid: boolean }[] {
    return this.sql
      .exec<{ schema: string; name: string; type: string; wr: number }>("PRAGMA table_list")
      .toArray()
      .filter((t) => t.schema === "main" && (t.type === "table" || t.type === "virtual") && !isInternalTable(t.name))
      .map((t) => ({ name: t.name, withoutRowid: t.wr === 1 }))
      .sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
  }

  private tables(): string[] {
    return this.tableList().map((t) => t.name);
  }

  /**
   * Options for the stubs this shard makes for others of its object (when it splits, presplits or
   * activates a sibling). By default, those of the last routed call: the client's `stubOptions`,
   * such as a version cohort, so a split's new shard runs the version its callers asked for. In
   * memory only: work an alarm resumes after a restart uses none until the next call. Override to
   * compute them here instead.
   */
  protected stubOptions(): StubOptions | null {
    return this.callerStubOptions;
  }

  /**
   * A stub of another shard of this object, in this shard's jurisdiction: a shard of an object
   * reached through `namespace.jurisdiction("eu")` makes its shards there too, never through the
   * plain binding (the same name there is another object, outside the jurisdiction).
   */
  private node(bits: string, name: string = this.meta!.name): DurableObjectStub<ShardedObject<Env, V>> {
    let nodes = this.nodes() as DurableObjectNamespace<ShardedObject<Env, V>>;
    const jurisdiction = this.ctx.id.jurisdiction as DurableObjectJurisdiction | undefined;
    if (jurisdiction) nodes = nodes.jurisdiction(jurisdiction);
    const id = nodes.idFromName(shardObjectName(name, bits));
    const options = this.stubOptions();
    return options ? nodes.get(id, options) : nodes.get(id);
  }

  private async activateSibling(): Promise<void> {
    const pending = this.meta?.activate;
    if (!pending) return;
    if (!(await this.node(pending.bits).shardedActivate(this.meta!.name, pending.bits, pending.epoch))) {
      console.error(JSON.stringify({ event: "split-activate-refused", node: this.nodeLabel(), sibling: pending.bits }));
      return;
    }
    const now = this.meta!;
    if (now.activate?.epoch === pending.epoch && now.activate.bits === pending.bits) this.saveMeta({ ...now, activate: null });
  }

  // ---- Alarms ----

  /**
   * Runs on a serving shard once the time given to `scheduleAlarm` has come (it may run a little
   * later, and the shard may have split since: check what is due). Never in a split's copy before
   * it serves. If it throws, it runs again a second later. The default does nothing.
   */
  protected async onAlarm(): Promise<void> {}

  /**
   * Asks for `onAlarm` at `at` (or earlier, if an earlier time is already asked for). Use it, not
   * `ctx.storage.setAlarm`: the alarm is shared with splits, and this never postpones their work
   * nor loses the time asked for. A split's new shard keeps the time too.
   */
  protected async scheduleAlarm(at: number | Date): Promise<void> {
    const time = typeof at === "number" ? at : at.getTime();
    // During onAlarm the stored wake is its retry (see alarm): what is asked for meanwhile is kept
    // aside, and stored once onAlarm returns.
    if (this.onAlarmAsked !== undefined) {
      this.onAlarmAsked = this.onAlarmAsked === null ? time : Math.min(this.onAlarmAsked, time);
      return;
    }
    const wake = this.loadWake();
    if (wake === null || time < wake) this.saveWake(time);
    if (this.meta?.state === "shard" || !this.meta) await this.armAlarm(time);
  }

  /**
   * Sets the alarm for `time` unless it is set for earlier already. For a time that has come it
   * sets it at once, without reading the alarm first: no alarm can be earlier, and setting it
   * before any await keeps it in the same commit as the writes just before (a split's meta, a
   * claim), so a crash cannot keep one without the other.
   */
  private async armAlarm(time: number): Promise<void> {
    if (time <= Date.now()) return this.ctx.storage.setAlarm(time);
    const current = await this.ctx.storage.getAlarm();
    if (current === null || time < current) await this.ctx.storage.setAlarm(time);
  }

  /**
   * While a split's work is pending here (a sibling to activate, rows to clear), makes sure an
   * alarm will do it, at most every ALARM_CHECK_MS: an alarm can be lost, and nothing else would
   * activate the sibling. Called on routed calls, which keep reaching a shard that has split.
   */
  private checkAlarm(): void {
    const meta = this.meta;
    if (!meta || !(meta.activate || meta.cleanup.length > 0 || meta.foreign) || Date.now() - this.alarmCheckedAt < ALARM_CHECK_MS) return;
    this.alarmCheckedAt = Date.now();
    void this.ctx.storage.getAlarm().then((at) => (at === null ? this.ctx.storage.setAlarm(Date.now()) : undefined));
  }
  private alarmCheckedAt = 0;

  /** While onAlarm runs: the earliest time it (or a call meanwhile) asked for, or null. Else undefined. */
  private onAlarmAsked: number | null | undefined = undefined;

  /** When the subclass asked for `onAlarm`, if it did. */
  private loadWake(): number | null {
    if (!this.schemaReady) return null;
    const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_meta WHERE key = 'wake'").toArray()[0];
    return row ? Number(row.value) : null;
  }

  private saveWake(time: number | null): void {
    this.ensureSchema();
    if (time === null) this.sql.exec("DELETE FROM sharded_meta WHERE key = 'wake'");
    else this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('wake', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", String(time));
  }

  /**
   * Activates a new sibling, clears what splits gave away, and runs `onAlarm` when it is due. It
   * never throws: the runtime retries a failing alarm only a few times, and the sibling is
   * activated by nothing else, so a failure here reschedules the alarm itself for as long as there
   * is work left. Subclasses cannot override it (the constructor refuses): see `onAlarm`.
   */
  override async alarm(): Promise<void> {
    if (this.wiped) return;
    const meta = this.meta;
    // A verbatim copy not claimed yet carries the source's alarm: it is not this object's.
    if (!meta || this.isUnclaimedCopy()) return;
    let failed = false;
    if (meta.activate) {
      try {
        await this.activateSibling();
      } catch (error) {
        failed = true;
        console.error(JSON.stringify({ event: "split-activate-failed", node: this.nodeLabel(), sibling: meta.activate.bits, error: String(error) }));
      }
    }
    try {
      this.cleanupBatch();
    } catch (error) {
      failed = true;
      console.error(JSON.stringify({ event: "split-cleanup-failed", node: this.nodeLabel(), error: String(error) }));
    }
    // Atomic holds past their deadline ask their primary (RFD0008).
    try {
      await this.recoverHolds();
    } catch (error) {
      failed = true;
      console.error(JSON.stringify({ event: "sharded-atomic-recover-failed", node: this.nodeLabel(), error: String(error) }));
    }
    // The subclass's turn, on a serving shard only: a copy's is armed when it is activated.
    const serving = this.meta!.state === "shard";
    const wake = this.loadWake();
    let ran = false;
    if (serving && wake !== null && wake <= Date.now()) {
      ran = true;
      // Until onAlarm returns, the wake stays, as a retry: an object restarted partway through
      // (evicted, redeployed, out of CPU) runs it again, instead of forgetting it was due.
      const retry = Date.now() + ALARM_RETRY_MS;
      this.saveWake(retry);
      await this.ctx.storage.setAlarm(retry);
      this.onAlarmAsked = null;
      try {
        await this.onAlarm();
        this.saveWake(this.onAlarmAsked);
      } catch (error) {
        console.error(JSON.stringify({ event: "on-alarm-failed", node: this.nodeLabel(), error: String(error) }));
        this.saveWake(Date.now() + ALARM_RETRY_MS);
      } finally {
        this.onAlarmAsked = undefined;
      }
    }
    const now = this.meta!;
    const times: number[] = [];
    if (now.activate || failed) times.push(Date.now() + ALARM_RETRY_MS);
    else if (now.cleanup.length > 0 || now.foreign) times.push(Date.now() + DELETE_PAUSE_MS);
    const next = this.loadWake();
    if (serving && next !== null) times.push(next);
    const holdDue = this.nextHoldDeadline();
    if (holdDue !== null) times.push(holdDue <= Date.now() ? Date.now() + ALARM_RETRY_MS : holdDue);
    // After onAlarm the alarm is its retry's: set it to what is due next, or clear it.
    if (ran) return times.length > 0 ? this.ctx.storage.setAlarm(Math.min(...times)) : this.ctx.storage.deleteAlarm();
    if (times.length > 0) await this.armAlarm(Math.min(...times));
  }

  /** Deletes one batch of rows this object gave away: the base table's first, then the subclass's. */
  private cleanupBatch(): void {
    const meta = this.meta!;
    const range = meta.cleanup[0];
    if (range) {
      const deleted = this.sql.exec("DELETE FROM sharded_rows WHERE key IN (SELECT key FROM sharded_rows WHERE hash >= ? AND hash <= ? LIMIT ?)", range[0], range[1], DELETE_BATCH_ROWS).rowsWritten;
      if (deleted === 0) this.saveMeta({ ...this.meta!, cleanup: this.meta!.cleanup.slice(1) });
    } else if (meta.foreign) {
      const bits = meta.bits;
      const deleted = this.ctx.storage.transactionSync(() => this.cleanupForeign((key) => ownsHash(bits, this.routeHash(key)), DELETE_BATCH_ROWS));
      if (deleted === 0) this.saveMeta({ ...this.meta!, foreign: false });
    }
  }

  // ---- The sibling's side of a split ----

  /** Schedules this object's storage to be replaced by the snapshot, then restarts to apply it. */
  /** Nothing: a split calls it first to start this object, before it holds writes. */
  async shardedWarm(): Promise<void> {}

  async shardedRestore(snapshot: DurableObjectSnapshot, epoch?: number): Promise<{ scheduleMs: number }> {
    this.assertNotWiped();
    if (this.serving()) throw new Error("refusing to overwrite a serving shard");
    this.refuseOlderAttempt(epoch);
    const started = Date.now();
    try {
      await this.ctx.storage.onNextSessionRestore!(snapshot);
    } catch (error) {
      // One pending restore per object: an earlier attempt's. Claiming tells which it was.
      if (!String(error).includes("already been scheduled")) throw error;
    }
    // Reply first, then restart: the next session starts on the copy.
    this.ctx.waitUntil(
      new Promise<void>((resolve) =>
        setTimeout(() => {
          resolve();
          try {
            this.ctx.abort(RESTORE_ABORT);
          } catch {
            // aborting throws in the caller too
          }
        }, 50),
      ),
    );
    return { scheduleMs: Date.now() - started };
  }

  /**
   * The logical copy's tables and indexes: replaces this object's with them. What is here is
   * not a serving shard's (an unused object, or the copy of an abandoned attempt, maybe made
   * under an older schema), so it all goes: the copy must have the source's schema exactly.
   * From here on this object takes the batches of attempt `epoch` only.
   */
  async shardedReceiveSchema(_name: string, _bits: string, epoch: number, objects: readonly { readonly name: string; readonly sql: string }[]): Promise<void> {
    this.assertNotWiped();
    if (this.serving()) throw new Error("refusing to change a serving shard");
    this.refuseOlderAttempt(epoch);
    this.receiving = epoch;
    this.ctx.storage.transactionSync(() => {
      for (const t of this.tables()) this.sql.exec(`DROP TABLE IF EXISTS ${quoteIdentifier(t)}`);
      this.meta = null;
      for (const o of objects) this.sql.exec(o.sql);
    });
    this.schemaReady = true;
  }

  /**
   * One batch of the logical copy, from attempt `epoch`: rows of one table. The first batch wipes
   * every table. A batch can arrive late, after its attempt was abandoned (and another began, or
   * even finished): it would put back rows as they were then, so only the batches of the attempt
   * whose schema this object took are written, and never on a serving shard.
   */
  async shardedReceive(_name: string, _bits: string, epoch: number, first: boolean, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void> {
    if (this.serving()) throw new Error("refusing to overwrite a serving shard");
    if (this.receiving !== epoch) throw new Error(`refusing a batch of split attempt ${epoch}: this object takes attempt ${this.receiving ?? "none"}`);
    this.ctx.storage.transactionSync(() => {
      if (first) {
        for (const t of this.tables()) this.sql.exec(`DELETE FROM ${quoteIdentifier(t)}`);
        this.meta = null;
      }
      if (rows.length === 0) return;
      // shardedReceiveSchema created every table first.
      if (!this.tables().includes(table)) throw new Error(`table ${table} does not exist in the copy`);
      const sql = `INSERT OR REPLACE INTO ${quoteIdentifier(table)} (${columns.map(quoteIdentifier).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
      for (const row of rows) this.sql.exec(sql, ...row);
    });
  }

  /** The logical copy is complete: it holds the source's meta, from this split. */
  async shardedReceiveDone(name: string, bits: string, epoch: number): Promise<boolean> {
    if (this.receiving !== epoch) return false;
    this.meta = this.loadMeta();
    const meta = this.meta;
    return meta !== null && meta.name === name && meta.splitting === epoch && `${meta.bits}1` === bits && this.isUnclaimedCopy();
  }

  /**
   * Refuses the copy of split attempt `epoch` if this object holds one at least as new. A claimed
   * copy may be all there is of its half (its parent stops owning the half once the claim
   * answers), so a message of an older attempt that arrives late must not replace it; nor may it
   * replace a newer attempt's copy that is about to be claimed. (`epoch` undefined: a caller from
   * before attempts were sent.)
   */
  private refuseOlderAttempt(epoch: number | undefined): void {
    const meta = this.meta;
    if (epoch === undefined || !meta) return;
    if (meta.state === "copy" && meta.epoch >= epoch) throw new Error(`refusing the copy of split attempt ${epoch}: this object holds the claimed copy of attempt ${meta.epoch}`);
    if (this.isUnclaimedCopy() && meta.splitting! > epoch) throw new Error(`refusing the copy of split attempt ${epoch}: this object holds the copy of attempt ${meta.splitting}`);
  }

  /** Storage copied verbatim from a splitting object and not claimed yet: it is not this object's. */
  private isUnclaimedCopy(): boolean {
    const meta = this.meta;
    return meta !== null && meta.splitting !== null && meta.from !== this.ctx.id.toString();
  }

  /**
   * A shard that serves and is not splitting. Anything else may be overwritten by a split's copy:
   * an unused object, a copy (claimed or not) from an abandoned attempt.
   */
  private serving(): boolean {
    return this.meta?.state === "shard" && !this.isUnclaimedCopy();
  }

  /**
   * Claims the copy for the shard `bits`, if it is the copy made by attempt `epoch` of its parent:
   * becomes that shard, not serving yet, and deletes the rows of the half it does not own.
   */
  async shardedClaim(name: string, bits: string, parent: string, epoch: number): Promise<boolean> {
    this.assertNotWiped();
    const meta = this.meta;
    if (!meta || meta.name !== name) return false;
    if (meta.bits === bits) return meta.epoch === epoch && meta.state === "copy"; // claimed already
    if (!this.isUnclaimedCopy() || meta.bits !== parent || meta.splitting !== epoch) return false; // not this copy
    this.receiving = null; // claimed: no batch may change it now
    this.ctx.storage.transactionSync(() => {
      this.afterClone((key) => ownsHash(bits, this.routeHash(key)));
      // The parent's meta (copied with its storage) gives the tree's presplit depth.
      this.saveMeta({ name, bits, state: "copy", epoch, splitting: null, activate: null, cleanup: this.outside(bits), foreign: true, ...(meta.presplit ? { presplit: meta.presplit } : {}), parent, splitAt: Date.now() });
    });
    await this.armAlarm(Date.now());
    return true;
  }

  /** The hash ranges outside a shard's. */
  private outside(bits: string): (readonly [string, string])[] {
    const [lo, hi] = hashRange(bits);
    const out: (readonly [string, string])[] = [];
    if (lo !== "0000000000000000") out.push(["0000000000000000", (BigInt(`0x${lo}`) - 1n).toString(16).padStart(16, "0")]);
    if (hi !== "ffffffffffffffff") out.push([(BigInt(`0x${hi}`) + 1n).toString(16).padStart(16, "0"), "ffffffffffffffff"]);
    return out;
  }

  /** Starts serving, if this is the copy of attempt `epoch`. */
  async shardedActivate(name: string, bits: string, epoch: number): Promise<boolean> {
    this.assertNotWiped();
    const meta = this.meta;
    if (!meta || meta.name !== name) return false;
    if (meta.bits !== bits) return meta.state === "shard" && meta.splitting === null && meta.bits.startsWith(bits); // serving, and split since
    if (meta.epoch !== epoch) return false;
    if (meta.state === "copy") {
      this.saveMeta({ ...meta, state: "shard" });
      // The subclass's alarm, copied from the original, was held until now.
      const wake = this.loadWake();
      if (wake !== null) await this.armAlarm(wake);
      this.checkAlarm(); // its cleanup's alarm, in case it was lost while it waited
    }
    return true;
  }

  /**
   * Deletes this shard's storage and alarm, if it serves `bits` (the client's `wipe` visits every
   * shard). A shard that is splitting, or a copy not yet serving, answers "not ready", and the
   * client comes back once the split is over and visits both halves. An object that holds nothing
   * stays empty: this creates nothing. From then on this instance refuses every call.
   */
  async shardedWipe(name: string, bits: string): Promise<CallAnswer<{ wiped: boolean }>> {
    const meta = this.meta;
    if (this.wiped || !meta) {
      // Nothing here (a shard never made, or wiped already), unless an unclaimed copy sits here.
      if (!this.schemaReady || this.wiped) return { ok: { wiped: false } };
    }
    if (meta) {
      if (this.isUnclaimedCopy() || meta.state === "copy" || this.hold || this.warming || meta.splitting !== null || meta.activate !== null) return this.answered({ notReady: bits });
      if (meta.name !== name || hostBits(meta.bits) !== hostBits(bits)) throw new Error(`${nodeName(name, bits)} reached ${nodeName(meta.name, meta.bits)}`);
      if (meta.bits !== bits) return this.answered(meta.bits.startsWith(bits) ? this.moved(bits) : { stale: bits });
    }
    this.wipedAt = Date.now();
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
    this.meta = null;
    this.schemaReady = false;
    this.calls.clear();
    return { ok: { wiped: true } };
  }

  /** The right half of an empty shard's initial split: empty, and serving at once. */
  async shardedDescribe(): Promise<NodeDescription> {
    const meta = this.meta;
    if (!this.schemaReady) return { bits: "", presplit: 0, state: "new", rows: 0, storedRows: 0, callsPerSecond: 0, lastSplit: null, lastSplitError: null, splits: [], counters: { ...this.counters } };
    const storedRows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_rows").one().n;
    let rows = storedRows;
    if (meta) {
      const [lo, hi] = hashRange(meta.bits);
      rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_rows WHERE hash >= ? AND hash <= ?", lo, hi).one().n;
    }
    const state = !meta ? "new" : this.hold ? "splitting" : meta.state === "copy" || this.isUnclaimedCopy() ? "copy" : "shard";
    return { bits: meta?.bits ?? "", presplit: meta?.presplit ?? 0, state, rows, storedRows, callsPerSecond: this.rate(), lastSplit: meta?.lastSplit ?? null, lastSplitError: meta?.lastSplitError ?? null, splits: meta?.splits ?? [], counters: { ...this.counters } };
  }
}

/** Names a routed call may not reach: the base class's own methods, and the runtime's handlers. */
const RESERVED_METHODS: ReadonlySet<string> = new Set([
  ...Object.getOwnPropertyNames(ShardedObject.prototype),
  ...Object.getOwnPropertyNames(Object.prototype),
  "fetch",
  "alarm",
  "webSocketMessage",
  "webSocketClose",
  "webSocketError",
]);
