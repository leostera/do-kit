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
import { AsyncLocalStorage } from "node:async_hooks";
import { DurableObject } from "cloudflare:workers";

import { ATOMIC_OUTCOME_TTL_MS, olderTx, type AtomicAnswer, type AtomicMethod, type AtomicStep, type Outcome } from "./atomic.js";
import { sharded, type ShardedClient } from "./client.js";
import { hashBit, keyHash } from "./hash.js";
import { pendingAlarm } from "./alarms.js";
import { compareText, storedJson } from "./values.js";

export { ATOMIC_HOLD_MS, ATOMIC_OUTCOME_TTL_MS, type AtomicMethod, type AtomicOutcome, type AtomicShardResult, type Prepared } from "./atomic.js";
export { hashBit, keyHash } from "./hash.js";
export { pendingAlarm, STRANDED_ALARM_MS } from "./alarms.js";
export { RouteMismatch, ShardFull, SpansShards, ShardedClient, sharded, type ShardDescription, type ShardedClientOptions, type ShardResult } from "./client.js";

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
  /**
   * Bytes of database (`ctx.storage.sql.databaseSize`) at which a shard splits, whatever its load:
   * checked after every write, and by the alarm. Default 1 GiB, where a split's copy and cleanup
   * still take seconds.
   */
  readonly splitAtBytes?: number;
  /**
   * Bytes of database at which a write that grows it is refused (ShardFull), and one that does not
   * (a delete, a change in place) is served: the platform refuses every write at its cap, deletes
   * too, and a shard must still be able to drain. Default 1.1 × `splitAtBytes`: a shard that can't
   * split (its rows share one route, or it is at maxDepth) stops growing just past where it would
   * have split. Raise it for a table whose routes hold more.
   */
  readonly refuseAtBytes?: number;
}

export const DEFAULT_SPLIT_AT_BYTES = 1024 ** 3;
/** The default `refuseAtBytes`, as a multiple of `splitAtBytes`. */
export const REFUSE_MARGIN = 1.1;

/**
 * Split at half of what one object takes: about 800 calls/s on the edge, so 400, sustained for
 * 5 s.
 */
export const DEFAULT_SPLIT_POLICY: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25 };

/** The longest delay setTimeout keeps: past it, a timer fires at once. */
export const MAX_TIMER_MS = 2 ** 31 - 1;

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
  // setTimeout fires at once for a delay past 2^31 - 1 ms (about 24.8 days).
  if (policy.splitTimeoutMs !== undefined && !(policy.splitTimeoutMs > 0 && policy.splitTimeoutMs <= MAX_TIMER_MS)) fail(`splitTimeoutMs must be a positive number of ms, at most ${MAX_TIMER_MS}`);
  if (policy.holdWaitMs !== undefined && !(policy.holdWaitMs >= 0 && policy.holdWaitMs <= MAX_TIMER_MS)) fail(`holdWaitMs must be from 0 to ${MAX_TIMER_MS} ms`);
  if (policy.splitAtBytes !== undefined && !(Number.isSafeInteger(policy.splitAtBytes) && policy.splitAtBytes > 0)) fail("splitAtBytes must be a positive whole number of bytes");
  if (policy.refuseAtBytes !== undefined && !(Number.isSafeInteger(policy.refuseAtBytes) && policy.refuseAtBytes > 0)) fail("refuseAtBytes must be a positive whole number of bytes");
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
  /** The shard got to the call past its deadline (the caller gave up on it): nothing was applied. */
  | { readonly expired: string }
  /** A scan of a presplit shard never called: it holds nothing, and was not made to answer. */
  | { readonly absent: string }
  /** The client routed `key` by another `routeOf` than this object's: nothing was called. */
  | { readonly routeMismatch: { readonly key: string; readonly client: string; readonly object: string } }
  /** The write would grow the shard past `refuseAtBytes`: refused, nothing applied. */
  | { readonly full: string; readonly bytes: number; readonly cap: number };

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
  /** Bytes of this object's database (`ctx.storage.sql.databaseSize`); null before it has one. */
  readonly databaseSize: number | null;
  /** How long a sibling a split made has waited to be activated; null if none waits. */
  readonly activatePendingMs: number | null;
  /** What the cleanup of what splits gave away has left: ranges (and other tables) to go, and rows deleted since the last split. */
  readonly cleanup: { readonly pendingRanges: number; readonly rows: number };
  /** The alarm set, and how far past due it is (more than 5 min: stranded, see RFD0001); null if none. */
  readonly alarm: { readonly at: number; readonly overdueMs: number } | null;
}

/** Why a split ran. */
export type SplitTrigger = "operator" | "load" | "size";

/** One split attempt of a shard. */
export interface SplitRecord {
  /** When it started (ms since the epoch) and how long until it was claimed or given up. */
  readonly at: number;
  readonly ms: number;
  /** "interrupted": the object restarted partway through (the copy is never activated). */
  readonly outcome: "split" | "abandoned" | "interrupted";
  /** Why it ran: an operator's `shardedSplit`, the call rate, or the database's size. */
  readonly trigger: SplitTrigger;
  /** How the right half was copied; null if abandoned before it was. */
  readonly copy: "native" | "logical" | null;
  /**
   * Rows this shard owned when it started, and of them the right half's (what was copied); null
   * where the layer keeps no count (counting would read every row): ShardedObject's own rows.
   */
  readonly rows: number | null;
  readonly rightRows: number | null;
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
  /**
   * The wait from a client's send to the method's start, and the method's own time, in ms: the
   * median and 99th percentile of the last CALL_SAMPLES calls; null before any call. Clocks across
   * machines differ by milliseconds: the waits are approximate.
   */
  readonly waitedMs: number | null;
  readonly waitedMsP99: number | null;
  readonly handlerMsP99: number | null;
  /** Per routed method: calls, and the rows its SQL statements read and wrote. */
  readonly methods: Readonly<Record<string, { readonly calls: number; readonly rowsRead: number; readonly rowsWritten: number; readonly statements: number }>>;
  /** A table's listing pages, the rows they scanned, and the pages cut by their budget (ShardedTable only). */
  readonly table?: { readonly pages: number; readonly scanned: number; readonly budgetStops: number };
}

/** The step timings of a split attempt, filled in as it goes. */
type Steps = { -readonly [K in keyof SplitSteps]: SplitSteps[K] };

/** A split in progress: writes to `right` wait for `released`, and are counted for its record. */
type Hold = { epoch: number; right: string; released: Promise<void>; deadline: number; held: number; notReady: number; heldMsMax: number };

/** An atomic call's share prepared here (RFD0008): its keys are held until it is resolved. */
type AtomicHold = { readonly tx: string; readonly primaryKey: string; readonly deadline: number; readonly method: string; readonly keys: readonly string[]; readonly staged: unknown };
/** What a write waits for: a split's hold or an atomic call's (`atomic`), released at the latest around `until`. */
type Waiting = { readonly released: Promise<void>; readonly until: number; readonly atomic: boolean; readonly tx?: string };

/** Writes to a key an atomic call holds wait for `released`. */
type HeldKey = { readonly tx: string; readonly deadline: number; readonly released: Promise<void>; readonly release: () => void };

/** Holds `h`'s keys in `held`: writes to them wait for `released`. */
function addHold(held: Map<string, HeldKey>, h: AtomicHold): void {
  let release!: () => void;
  const released = new Promise<void>((resolve) => (release = resolve));
  for (const key of h.keys) held.set(key, { tx: h.tx, deadline: h.deadline, released, release });
}

const ATOMIC_SCHEMA = `
CREATE TABLE IF NOT EXISTS sharded_atomic_holds (tx TEXT PRIMARY KEY, primary_key TEXT NOT NULL, deadline INTEGER NOT NULL, method TEXT NOT NULL, keys TEXT NOT NULL, staged TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sharded_atomic_outcomes (tx TEXT PRIMARY KEY, outcome TEXT NOT NULL, at INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS sharded_atomic_outcomes_at ON sharded_atomic_outcomes (at);
`;

/**
 * Outcomes one alarm purges at most: a backlog goes over several, each a bounded transaction.
 * A deleted outcome costs about five rows (four read through the table and its index, one written).
 */
const PURGE_BATCH = 250;

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
 * The tree was written with another routeOf than this object's: its rows are where the old one
 * put them, so a call routed by the new one would read them as missing, or write them twice.
 */
export class RouteChanged extends Error {
  constructor(
    readonly key: string,
    readonly was: string,
    readonly now: string,
  ) {
    super(`routeOf changed: key ${JSON.stringify(key)} was routed by ${was} when written, and is ${now} now. A tree is readable only with the routeOf it was written with: deploy the old one, or move the rows to a tree of a new name`);
    this.name = "RouteChanged";
  }
}

/** Keys a shard keeps with their route hash, to tell a changed routeOf. */
const ROUTE_SAMPLE = 16;
/** A call reached its write past its caller's deadline: refused, nothing applied. */
class Expired extends Error {}
/** A write would grow the database past `refuseAtBytes`: refused, nothing applied. */
class Full extends Error {
  constructor(
    readonly bytes: number,
    readonly cap: number,
  ) {
    super(`shard full: ${bytes} bytes, at its cap of ${cap}`);
  }
}
/** The routed call being served: its caller's deadline, checked just before its transaction. */
const callContext = new AsyncLocalStorage<{ readonly deadline: number | null; readonly cursors?: SqlStorageCursor<Record<string, SqlStorageValue>>[] }>();
/** Calls whose wait and time a shard keeps, for describe's percentiles. */
const CALL_SAMPLES = 256;
/** Marks the storage's exec once it records each call's statements (a new instance may get the same storage). */
const MEASURED = Symbol("do-kit measured exec");

/** The `q` quantile (0 to 1) of `samples`, or null if there are none. */
function quantile(samples: readonly number[], q: number): number | null {
  if (samples.length === 0) return null;
  const sorted = [...samples].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))]!;
}
/**
 * A write waited on a split for `holdWaitMs`: nothing was applied, the client retries later, until
 * the split is done or gives up at `until`.
 */
class NotReady extends Error {
  constructor(readonly until: number) {
    super("not ready");
  }
}
/** An atomic step met an older call's hold: it gives way rather than wait (wait-die, RFD0008). */
class Contended extends Error {}
const HOLD_WAIT_MS = 2_000;
/** The grid shared hold-wait timers fall on. */
const TICK_MS = 50;
/** How often a copy waiting to be activated nudges its parent, at most. */
const NUDGE_EVERY_MS = 1_000;
/** Keys whose route hash a shard remembers. */
const ROUTE_HASHES = 4_096;
/**
 * How long a wiped shard refuses calls: long enough for a call that was in flight when it was
 * wiped to arrive and be refused, rather than make the shard again. After that the name is free
 * to use again.
 */
const WIPED_REFUSE_MS = 5_000;
/** The one table a wiped object keeps for WIPED_REFUSE_MS: when it was wiped. */
const WIPED_TABLE = "sharded_wiped";
/**
 * How long a split waits for its new object's first call before going ahead anyway. A first call
 * can take seconds, and one that never returns must not leave the shard unable to split again:
 * the split's own calls to the new object retry, or give up.
 */
const WARM_WAIT_MS = 5_000;
/** How often a busy shard logs its load (`shard-load`). */
const LOAD_LOG_S = 10;

/**
 * Rows live in a rowid table: in a WITHOUT ROWID one a row over about a kilobyte spills to a page
 * of its own, so a 1 KB value took 4.7 KB of disk; here it takes 1.4 KB. The first layout's table
 * is renamed at wake (LEGACY_ROWS), read until empty, and moved over by the alarm.
 */
const SCHEMA = `
CREATE TABLE IF NOT EXISTS sharded_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sharded_rows (key TEXT PRIMARY KEY, hash TEXT NOT NULL, value TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS sharded_rows_hash ON sharded_rows (hash, key);
`;
/** The rows of the first layout (WITHOUT ROWID), being moved to sharded_rows. */
const LEGACY_ROWS = "sharded_rows_legacy";
/** Rows of the first layout the alarm moves a turn. */
const LEGACY_MOVE_ROWS = 250;
/** A shard refused at its cap takes writes that grow it again under this share of the size it was refused at. */
const FULL_UNTIL = 0.95;
/** The longest automatic splits wait after attempts given up, one after another. */
const MAX_SPLIT_BACKOFF_MS = 30 * 60_000;

/** Rowids of one 32-bit route-hash prefix: the slot a key's row goes in (see clusteredRowid). */
const ROWID_SLOT = 2 ** 21;

/**
 * The rowid that lays a row out on disk in the order of its route hash: the hash's first 32 bits,
 * then 21 bits of the key's own hash (2^53 in all, exact as a number). The rows a split gives away
 * then lie together, and deleting them frees whole pages: rows in the order they were written
 * leave every page half full, and the database no smaller.
 */
function clusteredRowid(routeHash: string, key: string): number {
  return Number.parseInt(routeHash.slice(0, 8), 16) * ROWID_SLOT + (Number.parseInt(keyHash(key).slice(8, 14), 16) % ROWID_SLOT);
}

/** What a value takes on the heap: two bytes a character of text, its length for bytes, 8 otherwise. */
function heapBytes(value: SqlStorageValue): number {
  if (typeof value === "string") return 2 * value.length;
  if (value instanceof ArrayBuffer) return value.byteLength;
  return 8;
}

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
/** The longest the ask to activate a sibling waits after refusals. */
const MAX_ACTIVATE_BACKOFF_MS = 3_000;
/** The longest a retry of failing alarm work waits. */
const MAX_BACKOFF_MS = 60_000;
/** The wait before retry `n` (1, 2, …) of work that keeps failing: 1 s, doubled each time, up to a minute. */
const backoffMs = (n: number): number => Math.min(MAX_BACKOFF_MS, ALARM_RETRY_MS * 2 ** Math.max(0, n - 1));
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
  activate: { bits: string; epoch: number; at?: number } | null;
  /** The split in progress, as its record would start: kept so a restart records it interrupted. */
  splitStart?: { at: number; trigger: SplitTrigger; rows: number | null; rightRows: number | null } | null;
  /** The copy of a split given up, which the alarm tells the sibling to delete until it has. */
  discard?: { bits: string; epoch: number } | null;
  /** Rows the cleanup since the last split deleted. */
  cleanedRows?: number;
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
  /**
   * Keys this tree was written with, and the route hash each had then (up to ROUTE_SAMPLE): the
   * tree's routeOf, as far as a check can tell. Copied to a split's new shard.
   */
  routes?: [string, string][];
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
   * How long a primary keeps an atomic call's outcome that nobody forgot (a caller gone after the
   * commit), and a split half its copies: far longer than any hold lives. The alarm purges them.
   */
  protected atomicOutcomeTtlMs = ATOMIC_OUTCOME_TTL_MS;
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
  /** When this instance's last split attempt ended, or the floor stopped one: size splits wait a window after. */
  private splitEndedAt = 0;
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
    // Every statement a routed call runs is counted for its method (describe's counters.methods).
    const sql = ctx.storage.sql as SqlStorage & { [MEASURED]?: true };
    if (!sql[MEASURED]) {
      const exec = sql.exec;
      sql.exec = function (this: SqlStorage, query: string, ...bindings: unknown[]) {
        const cursor = exec.call(sql, query, ...bindings);
        callContext.getStore()?.cursors?.push(cursor as SqlStorageCursor<Record<string, SqlStorageValue>>);
        return cursor;
      } as SqlStorage["exec"];
      sql[MEASURED] = true;
    }
    // An object only visited (a lazy shard asked whether it exists, a wiped one) stays empty.
    // One read: whether the schema is there, and in which layout (see SCHEMA).
    const tables = this.sql.exec<{ name: string; sql: string }>("SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name IN ('sharded_meta', 'sharded_rows', ?)", LEGACY_ROWS).toArray();
    const firstLayout = tables.some((t) => t.name === "sharded_rows" && /WITHOUT ROWID/i.test(t.sql));
    this.legacyRows = firstLayout || tables.some((t) => t.name === LEGACY_ROWS);
    // The first layout's table is renamed (no row moves), and the new one made, at once.
    if (firstLayout) {
      this.sql.exec(`ALTER TABLE sharded_rows RENAME TO ${LEGACY_ROWS}`);
      this.sql.exec(SCHEMA);
    }
    this.schemaReady = tables.some((t) => t.name === "sharded_meta");
    this.meta = this.schemaReady ? this.loadMeta() : null;
    // Wiped, then evicted: the refusal outlives the instance.
    const tombstone = !this.schemaReady && this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = ?", WIPED_TABLE).one().n > 0;
    if (tombstone) this.wipedAt = this.sql.exec<{ at: number }>(`SELECT at FROM ${WIPED_TABLE}`).toArray()[0]?.at ?? null;
  }

  private ensureSchema(): void {
    if (this.schemaReady) return;
    this.sql.exec(SCHEMA);
    this.schemaReady = true;
  }

  /** Rows of the first layout are still to move (see SCHEMA). */
  private legacyRows = false;
  private legacyAsked = false;
  /** The attempt whose first batch of the logical copy this instance took. */
  private receivedFirst: number | null = null;
  /** A restore is scheduled: the next start replaces the storage. */
  private restoring = false;

  /** A key's stored value: in sharded_rows, or still in the first layout's table. */
  private storedValue(key: string): string | undefined {
    const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_rows WHERE key = ?", key).toArray()[0];
    if (row || !this.legacyRows) return row?.value;
    return this.sql.exec<{ value: string }>(`SELECT value FROM ${LEGACY_ROWS} WHERE key = ?`, key).toArray()[0]?.value;
  }

  /** Writes a key's value (null deletes it), moving it out of the first layout's table. A new key is growth. */
  private storeValue(key: string, value: V | null, existed: boolean): void {
    if (value !== null && !existed) this.markGrowth();
    // A key still in the first layout's table is moved: deleted there, inserted here.
    const moved = this.legacyRows && this.sql.exec(`DELETE FROM ${LEGACY_ROWS} WHERE key = ? RETURNING 1`, key).toArray().length > 0;
    if (value === null) return void this.sql.exec("DELETE FROM sharded_rows WHERE key = ?", key);
    const json = storedJson(value, `the value of ${JSON.stringify(key)}`);
    if (existed && !moved) this.sql.exec("UPDATE sharded_rows SET value = ? WHERE key = ?", json, key);
    else this.insertClustered("sharded_rows", ["key", "hash", "value"], [key, this.routeHash(key), json], key);
  }

  /**
   * Inserts a new row of `key` at its clustered rowid (see clusteredRowid). Taken (two keys of
   * one route that share their 21 bits): the next free rowid of the route's slot; the slot full,
   * any rowid. The table's columns must not include `_rowid_`.
   */
  protected insertClustered(table: string, columns: readonly string[], values: readonly SqlStorageValue[], key: string): void {
    const insert = (or: string) => `INSERT ${or}INTO ${table} (_rowid_, ${columns.join(", ")}) VALUES (?, ${columns.map(() => "?").join(", ")})`;
    const at = clusteredRowid(this.routeHash(key), key);
    const first = this.sql.exec(insert("OR IGNORE "), at, ...values);
    first.toArray();
    if (first.rowsWritten > 0) return;
    const slot = at - (at % ROWID_SLOT);
    const last = this.sql.exec<{ n: number | null }>(`SELECT max(_rowid_) AS n FROM ${table} WHERE _rowid_ >= ? AND _rowid_ < ?`, slot, slot + ROWID_SLOT).one().n;
    const free = last === null ? slot : last + 1;
    if (free < slot + ROWID_SLOT) this.sql.exec(insert(""), free, ...values);
    else this.sql.exec(`INSERT INTO ${table} (${columns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`, ...values);
  }

  /** Moves a batch of the first layout's rows over; drops its table once empty. True once done. */
  private moveLegacyRows(): boolean {
    if (!this.legacyRows) return true;
    const rows = this.sql.exec<{ key: string; hash: string; value: string }>(`SELECT key, hash, value FROM ${LEGACY_ROWS} LIMIT ?`, LEGACY_MOVE_ROWS).toArray();
    this.ctx.storage.transactionSync(() => {
      // A key is in one table or the other (a write moves it): each goes in at its clustered rowid.
      for (const r of rows) {
        this.sql.exec(`DELETE FROM ${LEGACY_ROWS} WHERE key = ?`, r.key);
        this.insertClustered("sharded_rows", ["key", "hash", "value"], [r.key, r.hash, r.value], r.key);
      }
      if (rows.length === 0) this.sql.exec(`DROP TABLE ${LEGACY_ROWS}`);
    });
    if (rows.length === 0) this.legacyRows = false;
    return !this.legacyRows;
  }

  private assertNotWiped(): void {
    if (this.wipedAt === null) return;
    if (Date.now() - this.wipedAt < WIPED_REFUSE_MS) throw new Error("this shard was wiped");
    // Any call still in flight when it was wiped has long arrived: the name may be used again.
    this.endWipe();
  }

  /** The refusal window is over: the tombstone goes, and the object is empty. */
  private endWipe(): void {
    this.sql.exec(`DROP TABLE IF EXISTS ${WIPED_TABLE}`);
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
      const value = this.storedValue(key);
      if (value !== undefined) out.set(key, JSON.parse(value) as V);
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
    // No await from here on: the checks and the transaction see the same split state and time.
    this.assertBeforeDeadline();
    this.assertOwner(keys);
    return this.writeTransaction(() => {
      const current = new Map<string, V>();
      for (const key of keys) {
        const value = this.storedValue(key);
        if (value !== undefined) current.set(key, JSON.parse(value) as V);
      }
      const { writes, result } = decide(current);
      const fenced = new Set(keys);
      for (const [key, value] of writes) {
        // An unlisted key was neither checked as this shard's nor held for a split: its write could be lost.
        if (!fenced.has(key)) throw new Error(`update wrote ${JSON.stringify(key)}, which is not one of its keys: list every key it writes`);
        this.storeValue(key, value, current.has(key));
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
    // No await from here on: the checks and the transaction see the same split state and time.
    this.assertBeforeDeadline();
    this.assertOwner(keys);
    return this.writeTransaction(fn);
  }

  /**
   * A transaction of the subclass's writes: if it throws, it is rolled back, and `afterRollback`
   * tells the layers to drop what they kept in memory of it.
   */
  private writeTransaction<R>(fn: () => R, admit = true): R {
    let result: R;
    try {
      result = this.ctx.storage.transactionSync(() => {
        this.grew = false;
        const before = this.sql.databaseSize;
        const out = fn();
        // At the cap, refused if it added a row or grew the database (the size moves inside the
        // transaction, a page at a time): a delete, or a change in place that fits, which a shard
        // needs to drain, is served. Once refused, the shard stays full until deletes take it 5%
        // under the size it was refused at: the write refused rolled back, and the next must not
        // take the room it left.
        const after = this.sql.databaseSize;
        const cap = this.refuseAtBytes();
        if (this.fullAt !== null && after < this.fullAt * FULL_UNTIL) this.fullAt = null;
        if (admit && (after > before || this.grew) && (after >= cap || this.fullAt !== null)) {
          this.fullAt ??= before;
          throw new Full(after, cap);
        }
        return out;
      });
    } catch (error) {
      this.afterRollback();
      // Refused at the cap: the split decision runs here too. Only after a write that applies, a
      // shard at its cap would refuse every write and never split.
      if (error instanceof Full) this.checkSize();
      throw error;
    }
    this.checkSize();
    return result;
  }

  private refuseAtBytes(): number {
    return this.policy.refuseAtBytes ?? Math.floor(REFUSE_MARGIN * (this.policy.splitAtBytes ?? DEFAULT_SPLIT_AT_BYTES));
  }

  /**
   * After a write: a shard at `splitAtBytes` splits (level-triggered: every write past it asks,
   * and the alarm does, until one split takes). Not while it still deletes what it gave away
   * (its size counts those rows), nor sooner than an earlier split's quiet window, or the wait
   * after splits given up.
   */
  private checkSize(): void {
    const meta = this.meta;
    if (!meta || meta.state !== "shard" || this.hold || this.warming) return;
    if (this.sql.databaseSize < (this.policy.splitAtBytes ?? DEFAULT_SPLIT_AT_BYTES)) return;
    if (meta.cleanup.length > 0 || meta.foreign || meta.bits.length >= this.policy.maxDepth) return;
    if (Date.now() - this.splitEndedAt <= this.policy.windowSeconds * 1000 || Date.now() < this.splitAllowedAt()) return;
    if (this.splittable()) void this.split("size");
  }

  /**
   * When an automatic split may start again: after n attempts given up one after another, no
   * sooner than 2^n split timeouts after the last one started (capped at 30 min). A copy that keeps
   * failing is not tried again every few seconds; an operator's split does not wait.
   */
  private splitAllowedAt(): number {
    const splits = this.meta?.splits ?? [];
    let failed = 0;
    for (let i = splits.length - 1; i >= 0 && splits[i]!.outcome !== "split"; i--) failed++;
    if (failed === 0) return 0;
    return splits.at(-1)!.at + Math.min(MAX_SPLIT_BACKOFF_MS, (this.policy.splitTimeoutMs ?? 60_000) * 2 ** failed);
  }

  /** The write transaction under way adds a row: at `refuseAtBytes` it is refused. A layer that stores rows of its own calls it. */
  protected markGrowth(): void {
    this.grew = true;
  }
  private grew = false;
  /** The size a write was refused at, at the cap; null once deletes took the shard 5% under it. */
  private fullAt: number | null = null;

  /** After a write transaction rolled back: a layer that keeps what it wrote in memory (a table's row count) drops it. */
  protected afterRollback(): void {}

  /**
   * Throws Expired past the deadline of the routed call being served: its caller has given up on
   * it (OutcomeUnknown), and a write applied now would land after that.
   */
  private assertBeforeDeadline(): void {
    const deadline = callContext.getStore()?.deadline;
    if (deadline !== null && deadline !== undefined && Date.now() > deadline) throw new Expired(`past the caller's deadline by ${Date.now() - deadline} ms`);
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
    // In key order, seeking to `afterKey`: by the hash's index SQLite would sort the whole range.
    // `+hash` keeps SQLite off the hash's index, on the key's.
    const page = (table: string) =>
      (afterKey === null
        ? this.sql.exec<{ key: string; value: string }>(`SELECT key, value FROM ${table} WHERE +hash >= ? AND +hash <= ? ORDER BY key LIMIT ?`, lo, hi, limit)
        : this.sql.exec<{ key: string; value: string }>(`SELECT key, value FROM ${table} WHERE key > ? AND +hash >= ? AND +hash <= ? ORDER BY key LIMIT ?`, afterKey, lo, hi, limit)
      ).toArray();
    // A key is in one table or the other: the first `limit` of both, merged.
    const rows = this.legacyRows ? [...page("sharded_rows"), ...page(LEGACY_ROWS)].sort((a, b) => compareText(a.key, b.key)).slice(0, limit) : page("sharded_rows");
    for (const r of rows) out.push([r.key, JSON.parse(r.value) as V]);
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
  /**
   * After a wipe deleted every table: forget what this instance knew of its storage, as an
   * instance that never saw it would. The object may be used again once the wipe's refusal ends,
   * in this same instance. A subclass that caches anything read from storage (a migration done,
   * a table made) overrides this, resets it, and calls super.
   */
  protected forgetStorage(): void {
    this.held = null;
    this.atomicReady = null;
    this.receiving = null;
    this.calls.clear();
    this.alarmCheckedAt = 0;
    this.routesChecked = false;
    this.storedWake = undefined;
  }

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
  protected notReady(until: number = Date.now()): never {
    throw new NotReady(until);
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
    if (this.atomicReady === null) {
      const exists = this.schemaReady && this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'sharded_atomic_holds'").one().n > 0;
      // Tables made by an earlier release: brought up to this one's (the outcomes' index on time).
      if (exists) this.sql.exec(ATOMIC_SCHEMA);
      this.atomicReady = exists;
    }
    return this.atomicReady;
  }

  /** Every key an atomic call holds here, by key. */
  private heldKeys(): Map<string, HeldKey> {
    if (this.held) return this.held;
    // Kept only once loaded: a load that throws (a storage error) must not leave an empty map
    // behind, which would let writes through on keys a stored hold covers.
    const held = new Map<string, HeldKey>();
    if (this.hasAtomicTables()) for (const h of this.loadHolds()) addHold(held, h);
    this.held = held;
    return held;
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
    addHold(this.heldKeys(), h);
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
      const value = this.storedValue(key);
      if (value !== undefined) current.set(key, JSON.parse(value) as V);
    }
    return current;
  }

  /** Writes rows, inside a transaction; only `keys` (what was fenced and checked). */
  private writeRows(keys: readonly string[], writes: ReadonlyMap<string, V | null>, current: ReadonlyMap<string, V>): void {
    const fenced = new Set(keys);
    for (const [key, value] of writes) {
      if (!fenced.has(key)) throw new Error(`atomic apply wrote ${JSON.stringify(key)}, which it neither holds nor lists in holdKeys`);
      this.storeValue(key, value, current.has(key));
    }
  }

  /**
   * Runs `inTransaction` once these keys are free: no split copying them, no other atomic call
   * holding them, this shard owning them. `gather` (I/O) runs first, and again if a hold appeared
   * while it ran, so the transaction checks rows nobody holds.
   */
  private async whenFree<T>(keys: readonly string[], exceptTx: string | undefined, gather: () => Promise<unknown>, inTransaction: (gathered: unknown) => T, requester?: string): Promise<T> {
    for (;;) {
      // Wait-die: a step of an atomic call waits only on the holds of younger calls, and gives way
      // to an older one's. Waits then only go from older to younger calls, so no ring of calls
      // can wait on each other's holds, each holding what the next one needs.
      this.giveWayToOlder(keys, exceptTx, requester);
      await this.waitForHold(keys, exceptTx);
      const gathered = await gather();
      // No await from here on: the check and the transaction see the same holds and split state.
      if (this.holding(keys, exceptTx)) continue;
      this.assertOwner(keys);
      return this.writeTransaction(() => inTransaction(gathered));
    }
  }

  /** Throws Contended if `requester` (an atomic call) meets the hold of an older call on these keys. */
  private giveWayToOlder(keys: readonly string[], exceptTx: string | undefined, requester: string | undefined): void {
    if (requester === undefined) return;
    const held = this.heldKeys();
    for (const key of keys) {
      const h = held.get(key);
      if (h && h.tx !== exceptTx && olderTx(h.tx, requester)) throw new Contended();
    }
  }

  private async atomicStep(step: AtomicStep, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicAnswer> {
    switch (step.step) {
      case "solo":
        return this.atomicSolo(step.method, keys, payloads, args);
      case "coordinate":
        // Routed by the primary key: this shard records the outcome, and drives the other shards.
        this.assertOwner(keys);
        return { coordinated: await this.atomicClient().coordinateAtomic(step.tx, step.keys, step.method, step.payloads, args) };
      case "prepare":
        return this.atomicPrepare(step, keys, payloads, args);
      case "commit": {
        const answer = await this.atomicCommit(step.tx, step.method, keys, payloads, args);
        await this.armOutcomeExpiry();
        return answer;
      }
      case "abort":
      case "outcome": {
        this.assertOwner(keys);
        const found = this.outcomeOf(step.tx);
        if (found) return { outcome: found };
        const outcome = this.recordOutcome(step.tx, "abort");
        await this.armOutcomeExpiry();
        return { outcome, recorded: true };
      }
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
      this.writeRows(hold, writes, current);
      return { applied: result };
    });
    return "applied" in done && m.applied ? { applied: await m.applied(keys, done.applied) } : done;
  }

  /** A share that is not the primary's: checked, and if it would apply, kept and held. */
  private async atomicPrepare(step: Extract<AtomicStep, { step: "prepare" }>, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<AtomicAnswer> {
    // A prepare delivered twice: the first holds already.
    if (this.loadHold(step.tx)) return { prepared: true };
    // Past its deadline the hold would be recovered as soon as it is stored: hold nothing, and
    // let the caller start again, as it does after meeting another call's hold.
    if (Date.now() >= step.deadline) return { contended: true };
    const m = this.atomicMethod(step.method);
    const keysHeld = this.holdKeysOf(m, keys, payloads, args);
    // The alarm first, the hold it ends after: an object stopped in between has an alarm and no
    // hold (a wasted wake), never a hold and no alarm (held until a call happens by).
    await this.armAlarm(step.deadline);
    let done: AtomicAnswer;
    try {
      done = await this.whenFree(keysHeld, undefined, this.gatherOf(m, keys, payloads, args), (gathered): AtomicAnswer => this.prepareShare(step, m, keysHeld, keys, payloads, gathered, args), step.tx);
    } catch (error) {
      if (error instanceof Contended) return { contended: true };
      throw error;
    }
    return done;
  }

  /** In the transaction that holds them: checks the share, and if it would apply, keeps it and holds its keys. */
  private prepareShare(step: Extract<AtomicStep, { step: "prepare" }>, m: AtomicMethod<V, unknown, unknown, unknown, unknown>, keysHeld: readonly string[], keys: readonly string[], payloads: readonly unknown[] | null, gathered: unknown, args: readonly unknown[]): AtomicAnswer {
    {
      const p = this.checkOf(m, this.rowsOf(keysHeld), keys, payloads, gathered, args);
      if ("refused" in p) return { refused: p.refused };
      this.ensureAtomicSchema();
      this.heldKeys();
      this.sql.exec("INSERT INTO sharded_atomic_holds (tx, primary_key, deadline, method, keys, staged) VALUES (?, ?, ?, ?, ?, ?)", step.tx, step.primaryKey, step.deadline, step.method, JSON.stringify(keysHeld), JSON.stringify(p.staged ?? null));
      // Held in memory in the same turn: a write that runs once this transaction is done waits.
      this.holdInMemory({ tx: step.tx, primaryKey: step.primaryKey, deadline: step.deadline, method: step.method, keys: keysHeld, staged: null });
      return { prepared: true };
    }
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
    let done: AtomicAnswer;
    try {
      done = await this.whenFree(hold, tx, this.gatherOf(m, keys, payloads, args), (gathered): AtomicAnswer => this.commitShare(tx, m, hold, keys, payloads, gathered, args), tx);
    } catch (error) {
      if (!(error instanceof Contended)) throw error;
      // Given way to an older call: the outcome is abort, so the shares that hold drop theirs.
      const outcome = this.recordOutcome(tx, "abort");
      return outcome === "commit" ? { outcome } : { contended: true };
    }
    return "applied" in done && m.applied ? { applied: await m.applied(keys, done.applied) } : done;
  }

  /** In one transaction: the outcome recorded and the primary's share applied, or refused and aborted. */
  private commitShare(tx: string, m: AtomicMethod<V, unknown, unknown, unknown, unknown>, hold: readonly string[], keys: readonly string[], payloads: readonly unknown[] | null, gathered: unknown, args: readonly unknown[]): AtomicAnswer {
    {
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
      this.writeRows(hold, writes, current);
      return { applied: result };
    }
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
    return outcome;
  }

  /** When the oldest outcome kept here expires, or null if none is kept. */
  private outcomeExpiry(): number | null {
    if (!this.hasAtomicTables()) return null;
    const oldest = this.sql.exec<{ at: number | null }>("SELECT min(at) AS at FROM sharded_atomic_outcomes").one().at;
    return oldest === null ? null : oldest + this.atomicOutcomeTtlMs;
  }

  /** Outcomes nobody forgot go once their TTL passes, by the alarm: asks for it then. */
  private async armOutcomeExpiry(): Promise<void> {
    const expiry = this.outcomeExpiry();
    if (expiry !== null) await this.armAlarm(expiry);
  }

  /** Deletes up to PURGE_BATCH of the outcomes past their TTL, oldest first, through their index on time. */
  private purgeOutcomes(): void {
    if (!this.hasAtomicTables()) return;
    this.sql.exec("DELETE FROM sharded_atomic_outcomes WHERE rowid IN (SELECT rowid FROM sharded_atomic_outcomes WHERE at <= ? ORDER BY at LIMIT ?)", Date.now() - this.atomicOutcomeTtlMs, PURGE_BATCH);
  }

  /** A share prepared here: applied (commit) or dropped (abort), and its keys released, in one transaction. */
  private async atomicResolve(tx: string, outcome: Outcome): Promise<AtomicAnswer> {
    const h = this.loadHold(tx);
    if (!h) return { gone: true };
    const m = this.atomicMethod(h.method);
    // A decided outcome is applied whatever the size: refusing it would keep its keys held.
    const result = this.writeTransaction(() => {
      let applied: unknown = undefined;
      if (outcome === "commit") {
        const current = this.rowsOf(h.keys);
        const { writes, result } = m.apply(current, h.staged);
        this.writeRows(h.keys, writes, current);
        applied = result;
      }
      this.sql.exec("DELETE FROM sharded_atomic_holds WHERE tx = ?", tx);
      return applied;
    }, false);
    this.releaseInMemory(tx);
    if (outcome === "abort") return { outcome: "abort" };
    return { applied: m.applied ? await m.applied(h.keys, result) : result };
  }

  /** A client of this sharded object, from a shard: for recovery, and for the calls a primary coordinates. */
  private atomicClient(): ShardedClient<ShardedObject<Env, V>> {
    return sharded(this.nodes() as DurableObjectNamespace<ShardedObject<Env, V>>, this.objectName(), {
      ...(this.routesByKey() ? {} : { routeOf: (key: string) => this.routeOf(key) }),
      ...(this.callerStubOptions ? { stubOptions: this.callerStubOptions } : {}),
    });
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
    const client = this.atomicClient();
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
      const released = await Promise.race([hold.released.then(() => true), this.tick(deadline).then(() => false)]);
      if (!released) {
        waited(true);
        throw new NotReady(hold.until);
      }
    }
    waited(false);
  }

  /**
   * A promise that resolves at `at`, or a little after: one timer per TICK_MS slot, shared by every
   * write waiting until then. A split holding thousands of writes keeps a handful of timers, not
   * one each.
   */
  private tick(at: number): Promise<void> {
    const slot = Math.ceil(at / TICK_MS) * TICK_MS;
    let due = this.ticks.get(slot);
    if (!due) {
      due = new Promise<void>((resolve) =>
        setTimeout(() => {
          this.ticks.delete(slot);
          resolve();
        }, Math.max(0, slot - Date.now())),
      );
      this.ticks.set(slot, due);
    }
    return due;
  }
  private readonly ticks = new Map<number, Promise<void>>();

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
  async shardedCall(name: string, bits: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[], stubOptions: StubOptions | null = null, presplitTo = 0, hashes: readonly string[] | null = null, deadline: number | null = null, sentAt: number | null = null): Promise<CallAnswer<unknown>> {
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
    this.sampleRoutes(keys);
    this.countCall(keys.length);
    const fn = this.routedMethod(method);
    try {
      const ok = await this.measured(method, deadline, sentAt, () => (fn as (...a: unknown[]) => Promise<unknown>).call(this, keys, payloads, ...args));
      this.counters.ok++;
      return { ok };
    } catch (error) {
      if (error instanceof Moved) return this.answered(this.moved(bits));
      if (error instanceof NotReady) return this.answered({ notReady: bits, until: error.until });
      if (error instanceof Full) return { full: bits, bytes: error.bytes, cap: error.cap };
      if (error instanceof Expired) return { expired: bits };
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
      if (error instanceof Full) return { full: bits, bytes: error.bytes, cap: error.cap };
      throw error;
    }
  }

  /** A call to every shard: `method(...args)`, with no keys. */
  async shardedScan(name: string, bits: string, method: string, args: readonly unknown[], stubOptions: StubOptions | null = null, presplitTo = 0, sentAt: number | null = null): Promise<CallAnswer<unknown>> {
    this.assertNotWiped();
    this.callerStubOptions = stubOptions;
    // A presplit shard never called holds nothing: say so, rather than make it (and its schema).
    if (!this.meta && hostBits(bits) !== "" && presplitTo > 0 && bits.length <= presplitTo) return { absent: bits };
    const answer = await this.arriveOrCreate(name, bits, presplitTo);
    if (answer) return this.answered(answer);
    this.counters.calls++;
    const fn = this.routedMethod(method);
    try {
      const ok = await this.measured(method, null, sentAt, () => (fn as (...a: unknown[]) => Promise<unknown>).call(this, ...args));
      this.counters.ok++;
      return { ok };
    } catch (error) {
      if (error instanceof Moved) return this.answered(this.moved(bits));
      // A scan method that writes waits on a split as a call does; the client backs off and visits again.
      if (error instanceof NotReady) return this.answered({ notReady: bits, until: error.until });
      if (error instanceof Full) return { full: bits, bytes: error.bytes, cap: error.cap };
      throw error;
    }
  }

  /** Runs a routed method, counting what its statements read and wrote, and sampling its wait and time. */
  private async measured<T>(method: string, deadline: number | null, sentAt: number | null, run: () => Promise<T>): Promise<T> {
    const cursors: SqlStorageCursor<Record<string, SqlStorageValue>>[] = [];
    const started = Date.now();
    if (typeof sentAt === "number" && Number.isFinite(sentAt)) this.sample(this.waits, Math.max(0, started - sentAt));
    try {
      return await callContext.run({ deadline, cursors }, run);
    } finally {
      this.sample(this.handlerTimes, Date.now() - started);
      const m = (this.methodCounters[method] ??= { calls: 0, rowsRead: 0, rowsWritten: 0, statements: 0 });
      m.calls++;
      m.statements += cursors.length;
      for (const c of cursors) {
        m.rowsRead += c.rowsRead;
        m.rowsWritten += c.rowsWritten;
      }
    }
  }
  private sample(into: number[], value: number): void {
    into.push(value);
    if (into.length > CALL_SAMPLES) into.shift();
  }
  private readonly waits: number[] = [];
  private readonly handlerTimes: number[] = [];
  private readonly methodCounters: Record<string, { calls: number; rowsRead: number; rowsWritten: number; statements: number }> = {};

  /** For `cleanupForeign`: rows (not other entries) it deleted, for describe's cleanup progress. */
  protected countCleaned(rows: number): void {
    this.cleanedForeign += rows;
  }
  private cleanedForeign = 0;

  /** A layer's own counters for describe (a table's listings). */
  protected layerCounters(): Pick<CallCounters, "table"> {
    return {};
  }

  /**
   * Rows this object owns, and stores (with those of keys given away), for describe; and of the
   * owned ones, those in the right half `rightLo` up, for a split's record. A layer that keeps its
   * rows elsewhere (a table) counts them there.
   */
  protected rowCounts(rightLo: string | null): { rows: number; storedRows: number; rightRows: number } {
    const [lo, hi] = this.meta ? hashRange(this.meta.bits) : ["", "\uffff"];
    const out = { rows: 0, storedRows: 0, rightRows: 0 };
    for (const table of this.rowTables()) {
      const owned = this.sql.exec<{ rows: number; rightRows: number }>(`SELECT COUNT(*) AS rows, COALESCE(SUM(hash >= ?), 0) AS rightRows FROM ${table} WHERE hash >= ? AND hash <= ?`, rightLo ?? "\uffff", lo, hi).one();
      out.rows += owned.rows;
      out.rightRows += owned.rightRows;
      out.storedRows += this.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${table}`).one().n;
    }
    return out;
  }

  /** The tables of ShardedObject's own rows: one, and the first layout's while it is moved. */
  private rowTables(): string[] {
    return this.legacyRows ? ["sharded_rows", LEGACY_ROWS] : ["sharded_rows"];
  }

  /**
   * What a split at `rightLo` would leave and move: whether each half holds a row, and the counts
   * where the layer keeps them (a table does). ShardedObject's own rows are not counted, which would
   * read them all: a row of each half is looked for, by the hash's index.
   */
  protected splitHalves(rightLo: string): { rows: number | null; rightRows: number | null; left: boolean; right: boolean } {
    const [lo, hi] = hashRange(this.meta!.bits);
    const any = (from: string, to: string) => this.rowTables().some((t) => this.sql.exec(`SELECT 1 FROM ${t} WHERE hash >= ? AND hash < ? LIMIT 1`, from, to).toArray().length > 0);
    const right = this.rowTables().some((t) => this.sql.exec(`SELECT 1 FROM ${t} WHERE hash >= ? AND hash <= ? LIMIT 1`, rightLo, hi).toArray().length > 0);
    return { rows: null, rightRows: null, left: any(lo, rightLo), right };
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
    // At most allRowsCap: past it one call would read, hold and return more than it should.
    for (const table of this.rowTables()) {
      for (const r of this.sql.exec<{ key: string; value: string }>(`SELECT key, value FROM ${table} WHERE hash >= ? AND hash <= ? LIMIT ?`, lo, hi, this.allRowsCap + 1 - out.size)) out.set(r.key, JSON.parse(r.value) as V);
      if (out.size > this.allRowsCap) break;
    }
    if (out.size > this.allRowsCap) throw new Error(`allRows: this shard owns more than ${this.allRowsCap} rows; page them with ownedRowsPage(afterKey, limit)`);
    return out;
  }

  /** The most rows `allRows` returns: a shard with more throws, naming `ownedRowsPage`. */
  protected allRowsCap = 10_000;

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
    // Rows of the first layout to move: the alarm moves them, once asked (see SCHEMA).
    if (this.legacyRows && !this.legacyAsked) {
      this.legacyAsked = true;
      void this.armAlarm(Date.now() + DELETE_PAUSE_MS);
    }
    if (meta.name !== name || hostBits(meta.bits) !== hostBits(bits)) throw new Error(`${nodeName(name, bits)} reached ${nodeName(meta.name, meta.bits)}`);
    this.checkRoutes(meta);
    if (meta.state === "copy") {
      // Calls may reach only this half: they are the backstop for a parent whose alarm was lost.
      this.nudgeParent(meta);
      return { notReady: bits };
    }
    // A split interrupted by a restart: abandoned (its copy is never activated).
    if (meta.splitting !== null && !this.hold) {
      const start = meta.splitStart;
      const interrupted: SplitRecord | null = start
        ? { at: start.at, ms: Date.now() - start.at, outcome: "interrupted", trigger: start.trigger, copy: null, rows: start.rows, rightRows: start.rightRows, held: 0, notReady: 0, heldMsMax: 0, steps: { warm: 0, handOut: 0, snapshot: 0, restore: 0, restoreInside: 0, copyRows: 0, claim: 0, claimAttempts: 0, claimError: null }, error: "the object restarted during the split" }
        : null;
      this.saveMeta({ ...meta, splitting: null, splitStart: null, ...(interrupted ? { splits: [...(meta.splits ?? []), interrupted].slice(-SPLIT_HISTORY) } : {}) });
      if (meta.activate || meta.cleanup.length > 0 || meta.foreign) void this.armAlarm(Date.now());
    }
    this.checkAlarm();
    if (meta.bits === bits) return null;
    if (meta.bits.startsWith(bits)) return this.moved(bits);
    // Splits are final: a caller that knows this shard split, when it has not, learned that from
    // storage that is gone.
    return { stale: bits };
  }

  /** Keeps the first ROUTE_SAMPLE keys called here with their route hash (see Meta.routes). */
  private sampleRoutes(keys: readonly string[]): void {
    const meta = this.meta!;
    const kept = meta.routes ?? [];
    if (kept.length >= ROUTE_SAMPLE || keys.length === 0) return;
    const known = new Set(kept.map(([k]) => k));
    const added = [...new Set(keys)].filter((k) => !known.has(k)).slice(0, ROUTE_SAMPLE - kept.length);
    if (added.length > 0) this.saveMeta({ ...meta, routes: [...kept, ...added.map((k): [string, string] => [k, this.routeHash(k)])] });
  }

  /** Once an instance: refuses a tree whose sampled keys route elsewhere now (RouteChanged). */
  private checkRoutes(meta: Meta): void {
    if (this.routesChecked) return;
    for (const [key, was] of meta.routes ?? []) {
      const now = this.routeHash(key);
      if (now !== was) throw new RouteChanged(key, was, now);
    }
    this.routesChecked = true;
  }
  private routesChecked = false;

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
    if (Date.now() - this.quietUntil > this.policy.windowSeconds * 1000 && this.rate() > this.policy.callsPerSecond && Date.now() >= this.splitAllowedAt() && this.splittable()) void this.split("load");
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

  /** Whether an atomic call holds a key of the shard `bits`. */
  private holdsIn(bits: string): boolean {
    for (const key of this.heldKeys().keys()) if (ownsHash(bits, this.routeHash(key))) return true;
    return false;
  }

  /** Tells the sibling of a split given up to delete its copy; forgets it once the sibling has. */
  private async discardCopy(): Promise<void> {
    const meta = this.meta;
    const discard = meta?.discard;
    if (!meta || !discard) return;
    const done = await this.node(discard.bits).shardedDiscard(meta.name, discard.bits, discard.epoch);
    const now = this.meta!;
    if (done && now.discard?.epoch === discard.epoch) this.saveMeta({ ...now, discard: null });
  }

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
    return this.split("operator");
  }

  /**
   * Hands the right half to a new object: holds writes to it, copies this object's storage there,
   * and once the copy is claimed stops owning the right half and activates it. A copy that is not
   * claimed in time is abandoned, and the held writes go ahead here.
   */
  private async split(trigger: SplitTrigger): Promise<boolean> {
    const before = this.meta;
    if (!before || this.hold || this.warming || before.state !== "shard" || before.bits.length >= this.policy.maxDepth) return false;
    const right = `${before.bits}1`;
    // A key of the half that moves held for an atomic call keeps the shard whole until the call
    // ends (milliseconds, or a hold's deadline): a hold and the share it keeps belong to one
    // object. Holds of the half that stays stay here, and the copy drops them.
    if (this.holdsIn(right)) return false;
    const [rightLo] = hashRange(right);
    const counted = this.splitHalves(rightLo);
    // The floor: an automatic split that would move none of the rows, or all of them, frees
    // nothing (rows that share one route never part). An operator's split goes ahead.
    if (trigger !== "operator" && !(counted.left && counted.right)) {
      this.quietUntil = this.splitEndedAt = Date.now();
      return false;
    }
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
    if (!meta || meta.bits !== before.bits || meta.state !== "shard" || meta.epoch !== before.epoch || this.hold || this.holdsIn(right)) return false;
    const epoch = meta.epoch + 1;
    let release!: () => void;
    const started = Date.now();
    const timeout = this.policy.splitTimeoutMs ?? 60_000;
    // Only the `finally` below releases the hold: nothing that can throw goes between the two.
    const hold = { epoch, right, released: new Promise<void>((resolve) => (release = resolve)), deadline: started + timeout, held: 0, notReady: 0, heldMsMax: 0 };
    this.hold = hold;
    let copy: "native" | "logical" | null = null;
    const steps = { warm, handOut: 0, snapshot: 0, restore: 0, restoreInside: 0, copyRows: 0, claim: 0, claimAttempts: 0, claimError: null as string | null };
    const record = (outcome: SplitRecord["outcome"], error: string | null): SplitRecord[] => [
      ...(this.meta?.splits ?? []),
      { at: started, ms: Date.now() - started, outcome, trigger, copy, rows: counted.rows, rightRows: counted.rightRows, held: hold.held, notReady: hold.notReady, heldMsMax: hold.heldMsMax, steps: { ...steps }, error },
    ].slice(-SPLIT_HISTORY);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      this.saveMeta({ ...meta, epoch, splitting: epoch, from: this.ctx.id.toString(), splitStart: { at: started, trigger, rows: counted.rows, rightRows: counted.rightRows } });
      const live = () => this.hold === hold && Date.now() - started < timeout;
      const copied = this.copyAndClaim(right, epoch, live, steps, (method) => (copy = method));
      copied.catch(() => {}); // after a timeout nobody waits for it
      const expired = new Promise<never>((_, reject) => (timer = setTimeout(() => reject(new Error(`split did not finish in ${timeout} ms`)), timeout)));
      const method = await Promise.race([copied, expired]);
      // The copy is claimed: from this transaction on, the right half is the sibling's.
      const now = this.meta!;
      this.saveMeta({ ...now, bits: `${meta.bits}0`, splitting: null, splitStart: null, activate: { bits: right, epoch, at: Date.now() }, cleanup: [...now.cleanup, hashRange(right)], cleanedRows: 0, foreign: true, parent: meta.bits, splitAt: Date.now(), lastSplit: { copy: method, ms: Date.now() - started }, splits: record("split", null) });
      this.calls.clear();
      this.quietUntil = this.splitEndedAt = Date.now();
      console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), copy: method, ms: Date.now() - started }));
      this.hold = null;
      release();
      await this.armAlarm(Date.now());
      await this.activateSibling().catch(() => {}); // the alarm retries
      return true;
    } catch (error) {
      const now = this.meta;
      // The copy goes: the alarm tells the sibling until it has (a copy left behind is billed,
      // in an object nothing routes to).
      if (now) this.saveMeta({ ...now, splitting: now.splitting === epoch ? null : now.splitting, splitStart: null, discard: { bits: right, epoch }, lastSplitError: String(error).slice(0, 500), splits: record("abandoned", String(error).slice(0, 500)) });
      this.quietUntil = this.splitEndedAt = Date.now();
      console.error(JSON.stringify({ event: "split-abandoned", node: nodeName(meta.name, meta.bits), error: String(error), ms: Date.now() - started }));
      await this.discardCopy().catch(() => {}); // the alarm retries
      // A size split is tried again once its wait is over, with or without writes.
      if (this.sql.databaseSize >= (this.policy.splitAtBytes ?? DEFAULT_SPLIT_AT_BYTES)) await this.armAlarm(this.splitAllowedAt());
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
    const [rightLo, rightHi] = hashRange(right);
    for (const { name: table, withoutRowid } of tables) {
      const order = this.pageOrder(table, withoutRowid);
      const columns = order.columns.join(", ");
      const select = `SELECT ${order.rowid ? `${columns}, ` : ""}* FROM ${quoteIdentifier(table)}`;
      // A table whose rows carry their key's hash sends the right half's only: the rest would be
      // sent, written, and deleted again by the copy's cleanup.
      const hash = this.copiedHash(table);
      const half = hash === null ? [] : [`+${quoteIdentifier(hash)} >= ?`, `+${quoteIdentifier(hash)} <= ?`];
      let after: SqlStorageValue[] | null = null;
      for (;;) {
        if (!live()) throw new Error("split timed out copying rows");
        const conditions = [...(after ? [`(${columns}) > (${after.map(() => "?").join(", ")})`] : []), ...half];
        const where = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
        const cursor: SqlStorageCursor<Record<string, SqlStorageValue>> = this.sql.exec(`${select}${where} ORDER BY ${columns} LIMIT ?`, ...(after ?? []), ...(hash === null ? [] : [rightLo, rightHi]), COPY_PAGE_ROWS);
        const names: string[] = cursor.columnNames;
        const keyAt: number[] = order.rowid ? [0] : order.columns.map((c) => names.indexOf(unquoteIdentifier(c)));
        const rows: SqlStorageValue[][] = [];
        let bytes = 0;
        let cut = false;
        for (const row of cursor.raw()) {
          // Bytes as the batch takes them on the heap (two a character): a batch carries at least
          // one row, and stops before the row that would take it past COPY_BATCH_BYTES.
          const size = row.reduce<number>((n, value) => n + heapBytes(value), 0);
          if (rows.length > 0 && bytes + size > COPY_BATCH_BYTES) {
            cut = true;
            break;
          }
          after = keyAt.map((i): SqlStorageValue => row[i]!);
          rows.push(order.rowid ? row.slice(1) : row);
          bytes += size;
        }
        if (rows.length === 0 && !first) break;
        await target.shardedReceive(meta.name, right, epoch, first, table, order.rowid ? names.slice(1) : names, rows);
        first = false;
        if (rows.length === 0 || (rows.length < COPY_PAGE_ROWS && !cut)) break;
      }
    }
    // Views and triggers last, once the rows are in: a trigger made first would fire on every
    // copied row. Views first of the two, as an INSTEAD OF trigger is on a view.
    const late = this.sql
      .exec<{ type: string; name: string; tbl_name: string; sql: string | null }>("SELECT type, name, tbl_name, sql FROM sqlite_master WHERE type IN ('view', 'trigger') ORDER BY type DESC, name")
      .toArray()
      .filter((o): o is { type: string; name: string; tbl_name: string; sql: string } => o.sql !== null && !o.name.startsWith("_cf_") && !o.name.startsWith("sqlite_"))
      .map(({ name, sql }) => ({ name, sql }));
    if (!(await target.shardedReceiveDone(meta.name, right, epoch, late))) throw new Error("the copy did not take this split's meta");
  }

  /**
   * The column holding the route hash of each row's key, in a table whose rows a split's copy
   * sends for the half that moves only; null sends every row. ShardedObject's own rows by default;
   * a layer adds its tables, and calls super.
   */
  protected copiedHash(table: string): string | null {
    return table === "sharded_rows" || table === LEGACY_ROWS ? "hash" : null;
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

  /** Activates the sibling a split made, if one waits: true once it serves (or none waits). */
  private async activateSibling(): Promise<boolean> {
    const pending = this.meta?.activate;
    if (!pending) return true;
    if (!(await this.node(pending.bits).shardedActivate(this.meta!.name, pending.bits, pending.epoch))) {
      console.error(JSON.stringify({ event: "split-activate-refused", node: this.nodeLabel(), sibling: pending.bits }));
      return false;
    }
    const now = this.meta!;
    if (now.activate?.epoch === pending.epoch && now.activate.bits === pending.bits) this.saveMeta({ ...now, activate: null });
    return true;
  }
  /** Activations refused or failed in a row, and when to ask again (in memory: a restart asks at once). */
  private activateFailures = 0;
  private activateAgainAt = 0;


  // ---- Alarms ----

  /**
   * Runs on a serving shard once the time given to `scheduleAlarm` has come (it may run a little
   * later, and the shard may have split since: check what is due). Never in a split's copy before
   * it serves. If it throws, it runs again a second later. The default does nothing.
   */
  protected async onAlarm(): Promise<void> {}

  /**
   * For layers built on this class (a table's index builds, an indexed table's deliveries): their
   * work at the time asked for with `scheduleAlarm`, run before `onAlarm` and apart from it, so
   * app code that throws in `onAlarm` does not stop it. A layer overrides it and calls super;
   * apps override `onAlarm`.
   */
  protected async layerAlarm(): Promise<void> {}

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
    const current = await pendingAlarm(this.ctx.storage);
    if (current === null || time < current) await this.ctx.storage.setAlarm(time);
  }

  /**
   * While a split's work is pending here (a sibling to activate, rows to clear), an atomic call
   * holds keys, outcomes wait for their TTL, or the subclass asked for `onAlarm`, makes sure an
   * alarm will come for it, at most every ALARM_CHECK_MS: an alarm can be lost, and nothing else
   * would activate the sibling, end a hold (reads do not wait on holds), purge the outcomes or run
   * the subclass's alarm. Called on routed calls, which keep reaching the shard.
   */
  private checkAlarm(): void {
    const meta = this.meta;
    if (!meta || Date.now() - this.alarmCheckedAt < ALARM_CHECK_MS) return;
    const wake = meta.state === "shard" && this.onAlarmAsked === undefined ? this.loadWake() : null;
    const later = [this.nextHoldDeadline(), this.outcomeExpiry(), wake].filter((t): t is number => t !== null);
    const due = meta.activate || meta.cleanup.length > 0 || meta.foreign ? Date.now() : later.length > 0 ? Math.max(Date.now(), Math.min(...later)) : null;
    if (due === null) return;
    this.alarmCheckedAt = Date.now();
    void pendingAlarm(this.ctx.storage).then((at) => (at === null || at > due ? this.ctx.storage.setAlarm(due) : undefined));
  }
  private alarmCheckedAt = 0;

  /** While onAlarm runs: the earliest time it (or a call meanwhile) asked for, or null. Else undefined. */
  private onAlarmAsked: number | null | undefined = undefined;

  /** When the subclass asked for `onAlarm`, if it did. */
  private loadWake(): number | null {
    if (!this.schemaReady) return null;
    // Read once an instance: only saveWake changes it, and keeps this in step.
    if (this.storedWake === undefined) {
      const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_meta WHERE key = 'wake'").toArray()[0];
      this.storedWake = row ? Number(row.value) : null;
    }
    return this.storedWake;
  }
  private storedWake: number | null | undefined = undefined;

  /** Alarms in a row whose work threw (layers' or the app's). */
  private loadWakeFailures(): number {
    const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_meta WHERE key = 'wake_failures'").toArray()[0];
    return row ? Number(row.value) : 0;
  }

  private saveWakeFailures(n: number): void {
    if (n === 0) this.sql.exec("DELETE FROM sharded_meta WHERE key = 'wake_failures'");
    else this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('wake_failures', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", String(n));
  }

  private saveWake(time: number | null): void {
    this.ensureSchema();
    // Unknown until read again if the statement fails (inside a transaction that rolls back, say).
    this.storedWake = undefined;
    if (time === null) this.sql.exec("DELETE FROM sharded_meta WHERE key = 'wake'");
    else this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('wake', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", String(time));
    this.storedWake = time;
  }

  /**
   * Activates a new sibling, clears what splits gave away, and runs `onAlarm` when it is due. It
   * never throws: the runtime retries a failing alarm only a few times, and the sibling is
   * activated by nothing else, so a failure here reschedules the alarm itself for as long as there
   * is work left. Subclasses cannot override it (the constructor refuses): see `onAlarm`.
   */
  override async alarm(): Promise<void> {
    if (this.wiped) return;
    if (this.wipedAt !== null) return this.endWipe();
    const meta = this.meta;
    // A verbatim copy not claimed yet carries the source's alarm: it is not this object's.
    if (!meta || this.isUnclaimedCopy()) return;
    let failed = false;
    // A sibling that keeps refusing (or failing) activation is asked less and less often.
    if (meta.activate && Date.now() >= this.activateAgainAt) {
      let activated = false;
      try {
        activated = await this.activateSibling();
      } catch (error) {
        console.error(JSON.stringify({ event: "split-activate-failed", node: this.nodeLabel(), sibling: meta.activate.bits, error: String(error) }));
      }
      this.activateFailures = activated ? 0 : this.activateFailures + 1;
      // Capped low: until the sibling serves, its rows wait (and calls to it nudge at once anyway).
      this.activateAgainAt = activated ? 0 : Date.now() + Math.min(MAX_ACTIVATE_BACKOFF_MS, backoffMs(this.activateFailures));
    }
    try {
      this.cleanupBatch();
    } catch (error) {
      failed = true;
      console.error(JSON.stringify({ event: "split-cleanup-failed", node: this.nodeLabel(), error: String(error) }));
    }
    try {
      this.moveLegacyRows();
    } catch (error) {
      failed = true;
      console.error(JSON.stringify({ event: "legacy-rows-move-failed", node: this.nodeLabel(), error: String(error) }));
    }
    if (meta.discard) {
      try {
        await this.discardCopy();
      } catch (error) {
        failed = true;
        console.error(JSON.stringify({ event: "split-discard-failed", node: this.nodeLabel(), sibling: meta.discard.bits, error: String(error).slice(0, 200) }));
      }
    }
    // Atomic holds past their deadline ask their primary, and outcomes past their TTL go (RFD0008).
    // Each on its own: a purge that fails must not keep holds past their deadline from recovering.
    try {
      this.purgeOutcomes();
    } catch (error) {
      failed = true;
      console.error(JSON.stringify({ event: "sharded-atomic-purge-failed", node: this.nodeLabel(), error: String(error) }));
    }
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
      // The layers' work (index deliveries, builds) and the app's, each on its own: app code that
      // throws, before it calls super or not, does not stop the library's.
      let threw = false;
      try {
        await this.layerAlarm();
      } catch (error) {
        threw = true;
        console.error(JSON.stringify({ event: "layer-alarm-failed", node: this.nodeLabel(), error: String(error) }));
      }
      try {
        await this.onAlarm();
      } catch (error) {
        threw = true;
        console.error(JSON.stringify({ event: "on-alarm-failed", node: this.nodeLabel(), error: String(error) }));
      }
      // Failing again and again: retried less and less often, from 1 s up to a minute. The count is
      // stored: a shard idle between retries is evicted, and would start again from a second.
      const failures = threw ? this.loadWakeFailures() + 1 : 0;
      this.saveWakeFailures(failures);
      this.saveWake(threw ? Date.now() + backoffMs(failures) : this.onAlarmAsked);
      this.onAlarmAsked = undefined;
    }
    const now = this.meta!;
    const times: number[] = [];
    if (now.activate) times.push(Math.max(this.activateAgainAt, Date.now() + ALARM_RETRY_MS));
    if (failed) times.push(Date.now() + ALARM_RETRY_MS);
    if (now.cleanup.length > 0 || now.foreign || this.legacyRows) times.push(Date.now() + DELETE_PAUSE_MS);
    if (now.discard) times.push(Date.now() + ALARM_RETRY_MS);
    // A size split given up is tried again once its wait is over, with or without writes.
    if (serving) {
      this.checkSize();
      const again = this.splitAllowedAt();
      if (again > Date.now() && this.sql.databaseSize >= (this.policy.splitAtBytes ?? DEFAULT_SPLIT_AT_BYTES)) times.push(again);
    }
    const next = this.loadWake();
    if (serving && next !== null) times.push(next);
    const holdDue = this.nextHoldDeadline();
    if (holdDue !== null) times.push(holdDue <= Date.now() ? Date.now() + ALARM_RETRY_MS : holdDue);
    const outcomesDue = this.outcomeExpiry();
    if (outcomesDue !== null) times.push(Math.max(outcomesDue, Date.now() + ALARM_RETRY_MS));
    // After onAlarm the alarm is its retry's: set it to what is due next, or clear it.
    if (ran) return times.length > 0 ? this.ctx.storage.setAlarm(Math.min(...times)) : this.ctx.storage.deleteAlarm();
    if (times.length > 0) await this.armAlarm(Math.min(...times));
  }

  /** Deletes one batch of rows this object gave away: the base table's first, then the subclass's. */
  private cleanupBatch(): void {
    const meta = this.meta!;
    const range = meta.cleanup[0];
    if (range) {
      // Rows, not rows written (each row's entry in the hash index is written too).
      let deleted = 0;
      for (const t of this.rowTables()) if (deleted === 0) deleted = this.sql.exec(`DELETE FROM ${t} WHERE key IN (SELECT key FROM ${t} WHERE hash >= ? AND hash <= ? LIMIT ?) RETURNING 1`, range[0], range[1], DELETE_BATCH_ROWS).toArray().length;
      const now = this.meta!;
      if (deleted === 0) this.saveMeta({ ...now, cleanup: now.cleanup.slice(1) });
      else this.saveMeta({ ...now, cleanedRows: (now.cleanedRows ?? 0) + deleted });
    } else if (meta.foreign) {
      const bits = meta.bits;
      const deleted = this.ctx.storage.transactionSync(() => this.cleanupForeign((key) => ownsHash(bits, this.routeHash(key)), DELETE_BATCH_ROWS));
      const cleaned = this.cleanedForeign;
      this.cleanedForeign = 0;
      if (deleted === 0) this.saveMeta({ ...this.meta!, foreign: false, cleanedRows: (this.meta!.cleanedRows ?? 0) + cleaned });
      else if (cleaned > 0) this.saveMeta({ ...this.meta!, cleanedRows: (this.meta!.cleanedRows ?? 0) + cleaned });
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
      this.restoring = true;
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
      // Views and triggers of an earlier attempt too: a view outlives the tables it reads.
      for (const o of this.sql.exec<{ type: string; name: string }>("SELECT type, name FROM sqlite_master WHERE type IN ('view', 'trigger')").toArray()) {
        if (!o.name.startsWith("_cf_") && !o.name.startsWith("sqlite_")) this.sql.exec(`DROP ${o.type === "view" ? "VIEW" : "TRIGGER"} IF EXISTS ${quoteIdentifier(o.name)}`);
      }
      for (const t of this.tables()) this.sql.exec(`DROP TABLE IF EXISTS ${quoteIdentifier(t)}`);
      this.meta = null;
      for (const o of objects) this.sql.exec(o.sql);
    });
    this.schemaReady = true;
    // The source's rows may still be partly in the first layout's table.
    this.legacyRows = objects.some((o) => o.name === LEGACY_ROWS);
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
    // A first batch sent again after later ones (a retry overtaken) would wipe what they wrote.
    if (first && this.receivedFirst === epoch) throw new Error(`refusing the first batch of split attempt ${epoch} again: it would wipe the batches after it`);
    if (first) this.receivedFirst = epoch;
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
  async shardedReceiveDone(name: string, bits: string, epoch: number, late: readonly { readonly name: string; readonly sql: string }[] = []): Promise<boolean> {
    if (this.receiving !== epoch) return false;
    // The views and triggers, now that every row is in (see copyRows).
    if (late.length > 0) this.ctx.storage.transactionSync(() => { for (const o of late) this.sql.exec(o.sql); });
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

  /**
   * Deletes the copy of split attempt `epoch`, given up by its parent, or any older one's: claimed
   * or not, it never serves (only a parent whose split took activates its copy). A newer attempt's
   * copy stays, as does a serving shard. True once nothing of that attempt is left here.
   */
  async shardedDiscard(name: string, bits: string, epoch: number): Promise<boolean> {
    const meta = this.meta;
    if (this.restoring) return false; // a restore still to apply would bring the copy back
    if (this.receiving !== null && this.receiving > epoch) return true;
    if (meta) {
      if (meta.name !== name) return true;
      if (this.serving()) return true;
      if (meta.state === "copy" && (meta.bits !== bits || meta.epoch > epoch)) return true;
      if (this.isUnclaimedCopy() && meta.splitting! > epoch) return true;
    } else if (this.receiving === null && this.tables().length === 0) {
      return true;
    }
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
    this.meta = null;
    this.schemaReady = false;
    this.legacyRows = false;
    this.forgetStorage();
    return true;
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
      this.dropForeignHolds(bits);
      // The parent's meta (copied with its storage) gives the tree's presplit depth.
      this.saveMeta({ name, bits, state: "copy", epoch, splitting: null, activate: null, cleanup: this.outside(bits), foreign: true, ...(meta.presplit ? { presplit: meta.presplit } : {}), ...(meta.routes ? { routes: meta.routes } : {}), parent, splitAt: Date.now() });
    });
    await this.armAlarm(Date.now());
    return true;
  }

  /**
   * Deletes the atomic holds copied from the parent that hold none of `bits`' keys: they hold the
   * half the parent kept, which the parent resolves. (A split waits out holds before it starts, so
   * these are prepares that arrived while it ran, on the half that stays.)
   */
  private dropForeignHolds(bits: string): void {
    this.atomicReady = null;
    if (!this.hasAtomicTables()) return;
    for (const h of this.loadHolds()) if (!h.keys.some((key) => ownsHash(bits, this.routeHash(key)))) this.sql.exec("DELETE FROM sharded_atomic_holds WHERE tx = ?", h.tx);
    this.held = null;
  }

  /** The hash ranges outside a shard's. */
  private outside(bits: string): (readonly [string, string])[] {
    const [lo, hi] = hashRange(bits);
    const out: (readonly [string, string])[] = [];
    if (lo !== "0000000000000000") out.push(["0000000000000000", (BigInt(`0x${lo}`) - 1n).toString(16).padStart(16, "0")]);
    if (hi !== "ffffffffffffffff") out.push([(BigInt(`0x${hi}`) + 1n).toString(16).padStart(16, "0"), "ffffffffffffffff"]);
    return out;
  }

  /**
   * A claimed copy, called before it serves, asks its parent to activate it: the parent's alarm
   * may be lost, and its calls may have stopped (clients route this half here). One nudge in
   * flight at a time, at most every NUDGE_EVERY_MS.
   */
  private nudgeParent(meta: Meta): void {
    const parent = meta.bits.slice(0, -1);
    if (meta.bits === "" || this.nudging || Date.now() - this.nudgedAt < NUDGE_EVERY_MS) return;
    this.nudging = true;
    this.nudgedAt = Date.now();
    void this.node(`${parent}0`)
      .shardedNudge(meta.name, meta.bits, meta.epoch)
      .catch((error: unknown) => console.error(JSON.stringify({ event: "split-nudge-failed", node: this.nodeLabel(), error: String(error).slice(0, 200) })))
      .finally(() => (this.nudging = false));
  }
  private nudging = false;
  private nudgedAt = 0;

  /**
   * A copy of `bits` at `epoch` waiting to be activated: if this shard still has to activate it,
   * asks for the alarm that does now (as a routed call's check would). Not a call: not counted.
   */
  async shardedNudge(name: string, bits: string, epoch: number): Promise<void> {
    if (this.wiped) return;
    const meta = this.meta;
    if (!meta || meta.name !== name || meta.activate?.bits !== bits || meta.activate.epoch !== epoch) return;
    this.activateAgainAt = 0; // a call waits on the sibling: ask it now, backoff or not
    await this.armAlarm(Date.now());
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
    this.forgetStorage();
    // A tombstone, so the refusal survives an eviction, and an alarm to drop it once it is over.
    this.sql.exec(`CREATE TABLE ${WIPED_TABLE} (at INTEGER NOT NULL)`);
    this.sql.exec(`INSERT INTO ${WIPED_TABLE} (at) VALUES (?)`, this.wipedAt);
    await this.ctx.storage.setAlarm(this.wipedAt + WIPED_REFUSE_MS);
    return { ok: { wiped: true } };
  }

  /** The right half of an empty shard's initial split: empty, and serving at once. */
  async shardedDescribe(): Promise<NodeDescription> {
    const meta = this.meta;
    const counters: CallCounters = {
      ...this.counters,
      waitedMs: quantile(this.waits, 0.5),
      waitedMsP99: quantile(this.waits, 0.99),
      handlerMsP99: quantile(this.handlerTimes, 0.99),
      methods: structuredClone(this.methodCounters),
      ...(this.schemaReady ? this.layerCounters() : {}),
    };
    const alarmAt = await this.ctx.storage.getAlarm();
    const alarm = alarmAt === null ? null : { at: alarmAt, overdueMs: Math.max(0, Date.now() - alarmAt) };
    const databaseSize = this.ctx.storage.sql.databaseSize ?? null;
    if (!this.schemaReady) return { bits: "", presplit: 0, state: "new", rows: 0, storedRows: 0, callsPerSecond: 0, lastSplit: null, lastSplitError: null, splits: [], counters, databaseSize, activatePendingMs: null, cleanup: { pendingRanges: 0, rows: 0 }, alarm };
    const { rows, storedRows } = this.rowCounts(null);
    const state = !meta ? "new" : this.hold ? "splitting" : meta.state === "copy" || this.isUnclaimedCopy() ? "copy" : "shard";
    const activatePendingMs = meta?.activate ? Date.now() - (meta.activate.at ?? meta.splitAt) : null;
    const cleanup = { pendingRanges: (meta?.cleanup.length ?? 0) + (meta?.foreign ? 1 : 0), rows: meta?.cleanedRows ?? 0 };
    return { bits: meta?.bits ?? "", presplit: meta?.presplit ?? 0, state, rows, storedRows, callsPerSecond: this.rate(), lastSplit: meta?.lastSplit ?? null, lastSplitError: meta?.lastSplitError ?? null, splits: meta?.splits ?? [], counters, databaseSize, activatePendingMs, cleanup, alarm };
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
