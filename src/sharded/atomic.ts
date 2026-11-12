// Atomic calls across shards (RFD0008): the shapes a subclass gives and a caller gets, and the
// protocol between the client and the shards. The protocol is modelled in
// specs/atomic-across-shards.ts.

/** A shard's answer to its share of an atomic call: refused, or what it will write if all agree. */
export type Prepared<S, R> = { readonly staged: S } | { readonly refused: R };

/**
 * An atomic method of a sharded object: its share of an all-or-nothing call, on each shard
 * owning some of its keys. `gather` does the call's I/O (lookups elsewhere) and writes nothing;
 * `check` decides, in the transaction that holds the keys, from their rows then; `apply` writes,
 * in the transaction that releases them. `check` and `apply` are synchronous, as `update`'s decide.
 */
export interface AtomicMethod<V, G = undefined, S = unknown, R = unknown, A = unknown> {
  /**
   * The keys a share holds and reads, besides its own: keys of the same route whose writes would
   * change the check (the folders of a git ref, say). They must belong to the shard: same route.
   */
  holdKeys?(keys: readonly string[], payloads: readonly unknown[] | null, ...args: never[]): readonly string[];
  gather?(keys: readonly string[], payloads: readonly unknown[] | null, ...args: never[]): Promise<G>;
  check(current: ReadonlyMap<string, V>, keys: readonly string[], payloads: readonly unknown[] | null, gathered: G, ...args: never[]): Prepared<S, R>;
  apply(current: ReadonlyMap<string, V>, staged: S): { readonly writes: ReadonlyMap<string, V | null>; readonly result: A };
  /** After `apply`'s transaction, for work that waits on other objects. Its answer is the caller's. */
  applied?(keys: readonly string[], result: A): Promise<A>;
}

/** What an atomic call did. A shard that applied late (by recovery) has no result. */
export type AtomicOutcome<R, A> =
  | { readonly committed: true; readonly results: readonly AtomicShardResult<A>[] }
  | { readonly committed: false; readonly refusals: readonly AtomicShardResult<R>[] };

export interface AtomicShardResult<T> {
  readonly keys: readonly string[];
  readonly result: T;
}

/** One step of the protocol, as the client sends it to a shard. */
export type AtomicStep =
  /** The whole call in one shard: check and apply in one transaction. */
  | { readonly step: "solo"; readonly method: string }
  /** A share that is not the primary's: check, and hold the keys until the outcome. */
  | { readonly step: "prepare"; readonly tx: string; readonly method: string; readonly primaryKey: string; readonly deadline: number }
  /** The primary's share: check, record the outcome, apply, in one transaction. */
  | { readonly step: "commit"; readonly tx: string; readonly method: string }
  /** At the primary: record an abort unless an outcome is recorded. */
  | { readonly step: "abort"; readonly tx: string }
  /** At the primary: the outcome, recording an abort if there is none. */
  | { readonly step: "outcome"; readonly tx: string }
  /** At the primary: the call is over everywhere, its outcome may go. */
  | { readonly step: "forget"; readonly tx: string }
  /** At a share that prepared: apply or drop, and release its keys. */
  | { readonly step: "resolve"; readonly tx: string; readonly outcome: Outcome };

export type Outcome = "commit" | "abort";

/** A shard's answer to a step. */
export type AtomicAnswer =
  | { readonly refused: unknown }
  | { readonly applied: unknown }
  | { readonly prepared: true }
  | { readonly outcome: Outcome }
  /** A resolve for a share that is not held here (already resolved, or never prepared). */
  | { readonly gone: true }
  /**
   * A prepare or commit that met an older call's hold on its keys: nothing is held or applied for
   * it (a commit records an abort), and the client aborts and retries the call (wait-die).
   */
  | { readonly contended: true };

/**
 * A call's id, which orders calls by age: its first attempt's start (fixed width, so ids sort as
 * the times do) and a random part. Retries keep the start, so a call that keeps meeting holds
 * becomes the oldest, and the oldest call never gives way (wait-die).
 */
export function atomicTxId(startedAt: number): string {
  return `${String(startedAt).padStart(15, "0")}-${crypto.randomUUID()}`;
}

/** Whether call `a` is older than call `b`, by their ids: only an older call waits on another's hold. */
export function olderTx(a: string, b: string): boolean {
  return a < b;
}

/**
 * How long a share holds its keys before it asks the primary for the outcome. The whole call
 * takes a few round trips: a hold reaches this only if its caller is gone or the primary is slow.
 */
export const ATOMIC_HOLD_MS = 5_000;
/** How long the primary keeps an outcome nobody said it may forget: far past any hold's life. */
export const ATOMIC_OUTCOME_TTL_MS = 24 * 60 * 60_000;
