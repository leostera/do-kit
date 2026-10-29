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
// The protocol is modelled in specs/snapshot-split.ts.
import { DurableObject } from "cloudflare:workers";
import { createHash } from "node:crypto";

export { SpansShards, ShardedClient, sharded, type ShardResult } from "./client.js";

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
   * Depth a new object splits to before its first call, while empty: for uniform keys (hashes), so
   * a fresh object starts with 2^initialDepth shards and nothing to copy.
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

/** What a node answers a routed call: `moved` names a split shard (its ancestors split too). */
export type CallAnswer<R> = { readonly ok: R } | { readonly moved: string } | { readonly notReady: string };

/** An object's state, for tests and operators. */
export interface NodeDescription {
  /** The bits of the shard this object hosts now. */
  readonly bits: string;
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
}

/** The writes of an update (null deletes) and what the method returns. */
export interface UpdateResult<V, R> {
  readonly writes: ReadonlyMap<string, V | null>;
  readonly result: R;
}

/** The first 64 bits of the key's SHA-256, as 16 hex digits: what ownership is decided on. */
export function keyHash(key: string): string {
  return createHash("sha256").update(key).digest("hex").slice(0, 16);
}

/** Bit `i` (0-based, most significant first) of a 16-hex-digit hash. */
export function hashBit(hash: string, i: number): "0" | "1" {
  const nibble = Number.parseInt(hash[i >> 2]!, 16);
  return (nibble >> (3 - (i & 3))) & 1 ? "1" : "0";
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
/** A write waited on a split for `holdWaitMs`: nothing was applied, the client retries later. */
class NotReady extends Error {}
const HOLD_WAIT_MS = 2_000;

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
  /** The shard this one split from and when (null for the root). */
  parent: string | null;
  splitAt: number;
  lastSplit?: { copy: "native" | "logical"; ms: number };
  /** Why the last abandoned split attempt was abandoned. */
  lastSplitError?: string;
}

/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this shard), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses that define `alarm` must call `super.alarm()`.
 */
export abstract class ShardedObject<Env = unknown, V = unknown> extends DurableObject<Env> {
  protected splitPolicy: SplitPolicy = DEFAULT_SPLIT_POLICY;
  /** The namespace this class is bound to: to reach siblings. */
  protected abstract nodes(): DurableObjectNamespace<ShardedObject<Env, V>>;

  private readonly sql: SqlStorage;
  private meta: Meta | null;
  private readonly calls = new Map<number, number>();
  private quietUntil = Date.now();
  /** The split in progress here: writes to `right` wait for `released`. */
  private hold: { epoch: number; right: string; released: Promise<void> } | null = null;
  /** An empty shard splitting to the initial depth; calls wait for it, then are moved. */
  private presplitting: Promise<void> | null = null;
  /** This shard had rows when it could have presplit: it splits by load only. */
  private presplitDeclined = false;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(SCHEMA);
    this.meta = this.loadMeta();
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
      for (const [key, value] of writes) {
        if (value === null) this.sql.exec("DELETE FROM sharded_rows WHERE key = ?", key);
        else this.sql.exec("INSERT INTO sharded_rows (key, hash, value) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, keyHash(key), JSON.stringify(value));
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
  protected transact<R>(keys: readonly string[], fn: () => R): Promise<R> {
    return this.update(keys, () => ({ writes: new Map(), result: fn() }));
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
    for (const r of this.sql.exec<{ key: string; value: string }>("SELECT key, value FROM sharded_rows WHERE key > ? AND hash >= ? AND hash <= ? ORDER BY key LIMIT ?", afterKey ?? "", lo, hi, limit)) out.push([r.key, JSON.parse(r.value) as V]);
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

  /** Whether `key` is this shard's now. Keys of a split's moving half stay this shard's until it commits. */
  protected owns(key: string): boolean {
    if (!this.meta) throw new Error("node has no name yet");
    return ownsHash(this.meta.bits, keyHash(key));
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

  private assertOwner(keys: readonly string[]): void {
    const meta = this.meta;
    if (meta?.state !== "shard") throw new Moved();
    for (const key of keys) if (!ownsHash(meta.bits, keyHash(key))) throw new Moved();
  }

  /** Waits while a split copies any of these keys, up to `holdWaitMs`; then "not ready". */
  private async waitForHold(keys: readonly string[]): Promise<void> {
    const deadline = Date.now() + (this.splitPolicy.holdWaitMs ?? HOLD_WAIT_MS);
    for (let hold = this.holding(keys); hold; hold = this.holding(keys)) {
      const left = deadline - Date.now();
      if (left <= 0) throw new NotReady();
      let timer: ReturnType<typeof setTimeout> | undefined;
      const released = await Promise.race([hold.then(() => true), new Promise<boolean>((resolve) => (timer = setTimeout(() => resolve(false), left)))]);
      clearTimeout(timer);
      if (!released) throw new NotReady();
    }
  }

  /** What an update of these keys waits for, if any. */
  private holding(keys: readonly string[]): Promise<void> | null {
    const hold = this.hold;
    if (!hold) return null;
    for (const key of keys) if (ownsHash(hold.right, keyHash(key))) return hold.released;
    return null;
  }

  // ---- The routed call ----

  /** A call routed by the client: `method(keys, payloads, ...args)` if this shard owns the keys. */
  async shardedCall(name: string, bits: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<CallAnswer<unknown>> {
    const answer = await this.arriveSettled(name, bits);
    if (answer) return answer;
    for (const key of keys) if (!ownsHash(bits, keyHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);
    this.countCall(keys.length);
    const fn = (this as unknown as Record<string, unknown>)[method];
    if (typeof fn !== "function" || method.startsWith("sharded")) throw new Error(`no method ${method}`);
    try {
      return { ok: await (fn as (...a: unknown[]) => Promise<unknown>).call(this, keys, payloads, ...args) };
    } catch (error) {
      if (error instanceof Moved) return this.moved(bits);
      if (error instanceof NotReady) return { notReady: bits };
      throw error;
    }
  }

  /** A call to every shard: `method(...args)`, with no keys. */
  async shardedScan(name: string, bits: string, method: string, args: readonly unknown[]): Promise<CallAnswer<unknown>> {
    const answer = await this.arriveSettled(name, bits);
    if (answer) return answer;
    const fn = (this as unknown as Record<string, unknown>)[method];
    if (typeof fn !== "function" || method.startsWith("sharded")) throw new Error(`no method ${method}`);
    try {
      return { ok: await (fn as (...a: unknown[]) => Promise<unknown>).call(this, ...args) };
    } catch (error) {
      if (error instanceof Moved) return this.moved(bits);
      throw error;
    }
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
    const now = this.meta!.bits;
    return { moved: now.length > bits.length ? now.slice(0, -1) : bits };
  }

  /**
   * Null if this object serves `bits` now; otherwise what to answer. Splits to the initial depth
   * first if this shard is above it and empty: every call waits for that.
   */
  private async arriveSettled(name: string, bits: string): Promise<CallAnswer<never> | null> {
    const answer = this.arrive(name, bits);
    if (answer) return answer;
    const meta = this.meta!;
    if (this.presplitDeclined || meta.bits.length >= (this.splitPolicy.initialDepth ?? 0)) return null;
    this.presplitting ??= this.presplit().finally(() => (this.presplitting = null));
    await this.presplitting;
    return this.arrive(name, bits);
  }

  /** Whether this object serves `bits`, initialising the root on its first call. */
  private arrive(name: string, bits: string): CallAnswer<never> | null {
    if (!this.meta) {
      if (bits !== "") return { notReady: bits }; // a sibling whose copy has not arrived
      this.saveMeta({ name, bits: "", state: "shard", epoch: 0, splitting: null, activate: null, cleanup: [], parent: null, splitAt: 0 });
    }
    const meta = this.meta!;
    // Storage copied from another object and not claimed yet: it keeps that object's meta.
    if (this.isUnclaimedCopy()) return { notReady: bits };
    if (meta.name !== name || hostBits(meta.bits) !== hostBits(bits)) throw new Error(`${nodeName(name, bits)} reached ${nodeName(meta.name, meta.bits)}`);
    if (meta.state === "copy") return { notReady: bits };
    // A split interrupted by a restart: abandoned (its copy is never activated).
    if (meta.splitting !== null && !this.hold) {
      this.saveMeta({ ...meta, splitting: null });
      if (meta.activate || meta.cleanup.length > 0 || meta.foreign) void this.ctx.storage.setAlarm(Date.now());
    }
    if (meta.bits === bits) return null;
    if (meta.bits.startsWith(bits)) return this.moved(bits);
    throw new Error(`${nodeName(name, bits)} is not split, ${nodeName(meta.name, meta.bits)} is the shard`);
  }

  private async presplit(): Promise<void> {
    const meta = this.meta!;
    const rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_rows").one().n;
    if (rows > 0 || meta.state !== "shard" || this.hold) {
      this.presplitDeclined = true; // not empty: splits by load only
      return;
    }
    // Empty: the right halves of every level start empty and serving; they split further on
    // their own first call.
    const depth = this.splitPolicy.initialDepth ?? 0;
    let bits = meta.bits;
    for (; bits.length < depth; bits += "0") await this.node(`${bits}1`).shardedAdoptEmpty(meta.name, `${bits}1`, bits);
    this.saveMeta({ ...this.meta!, bits, parent: meta.bits, splitAt: Date.now() });
  }

  private loadMeta(): Meta | null {
    const row = this.sql.exec<{ value: string }>("SELECT value FROM sharded_meta WHERE key = 'meta'").toArray()[0];
    return row ? (JSON.parse(row.value) as Meta) : null;
  }

  private saveMeta(meta: Meta): void {
    this.meta = meta;
    this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
  }

  // ---- Load and splitting ----

  private countCall(keys: number): void {
    const second = Math.floor(Date.now() / 1000);
    this.calls.set(second, (this.calls.get(second) ?? 0) + 1 + keys / (this.splitPolicy.keysPerCall ?? 25));
    if (this.calls.size > this.splitPolicy.windowSeconds + 2) {
      for (const s of this.calls.keys()) if (s < second - this.splitPolicy.windowSeconds - 1) this.calls.delete(s);
    }
    if (Date.now() - this.quietUntil > this.splitPolicy.windowSeconds * 1000 && this.rate() > this.splitPolicy.callsPerSecond) void this.split();
  }

  /** Average calls/s over the last full `windowSeconds`. */
  private rate(): number {
    const now = Math.floor(Date.now() / 1000);
    let sum = 0;
    for (let s = now - this.splitPolicy.windowSeconds; s < now; s++) sum += this.calls.get(s) ?? 0;
    return sum / this.splitPolicy.windowSeconds;
  }

  /** Splits now if this object hosts the shard `bits` (also an operator's lever). */
  async shardedSplit(name: string, bits: string): Promise<boolean> {
    if (this.arrive(name, bits)) return false;
    return this.split();
  }

  /**
   * Hands the right half to a new object: holds writes to it, copies this object's storage there,
   * and once the copy is claimed stops owning the right half and activates it. A copy that is not
   * claimed in time is abandoned, and the held writes go ahead here.
   */
  private async split(): Promise<boolean> {
    const meta = this.meta;
    if (!meta || this.hold || meta.state !== "shard" || meta.bits.length >= this.splitPolicy.maxDepth) return false;
    const epoch = meta.epoch + 1;
    const right = `${meta.bits}1`;
    let release!: () => void;
    const hold = { epoch, right, released: new Promise<void>((resolve) => (release = resolve)) };
    this.hold = hold;
    const started = Date.now();
    const timeout = this.splitPolicy.splitTimeoutMs ?? 60_000;
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      this.saveMeta({ ...meta, epoch, splitting: epoch, from: this.ctx.id.toString() });
      const live = () => this.hold === hold && Date.now() - started < timeout;
      const copied = this.copyAndClaim(right, epoch, live);
      copied.catch(() => {}); // after a timeout nobody waits for it
      const expired = new Promise<never>((_, reject) => (timer = setTimeout(() => reject(new Error(`split did not finish in ${timeout} ms`)), timeout)));
      const method = await Promise.race([copied, expired]);
      // The copy is claimed: from this transaction on, the right half is the sibling's.
      const now = this.meta!;
      this.saveMeta({ ...now, bits: `${meta.bits}0`, splitting: null, activate: { bits: right, epoch }, cleanup: [...now.cleanup, hashRange(right)], foreign: true, parent: meta.bits, splitAt: Date.now(), lastSplit: { copy: method, ms: Date.now() - started } });
      this.calls.clear();
      this.quietUntil = Date.now();
      console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), copy: method, ms: Date.now() - started }));
      this.hold = null;
      release();
      await this.ctx.storage.setAlarm(Date.now());
      await this.activateSibling().catch(() => {}); // the alarm retries
      return true;
    } catch (error) {
      const now = this.meta;
      if (now) this.saveMeta({ ...now, splitting: now.splitting === epoch ? null : now.splitting, lastSplitError: String(error).slice(0, 500) });
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
  private async copyAndClaim(right: string, epoch: number, live: () => boolean): Promise<"native" | "logical"> {
    await this.beforeHandOut();
    const method = await this.copyTo(right, epoch, live);
    const meta = this.meta!;
    let lastError: unknown = null;
    for (let wait = 10; live(); wait = Math.min(wait * 2, 500)) {
      try {
        // A fresh stub each time: the sibling restarts to apply the restore, which breaks the old one.
        if (await this.node(right).shardedClaim(meta.name, right, meta.bits, epoch)) return method;
      } catch (error) {
        // The sibling restarting (or still starting on the copy). Claiming is idempotent.
        lastError = error;
      }
      await new Promise((resolve) => setTimeout(resolve, wait));
    }
    throw new Error(`split timed out claiming its copy${lastError ? `: ${String(lastError)}` : ""}`);
  }

  private async copyTo(right: string, epoch: number, live: () => boolean): Promise<"native" | "logical"> {
    const mode = this.splitPolicy.copy ?? "auto";
    if (mode !== "logical" && typeof this.ctx.storage.snapshot === "function") {
      let snapshot: DurableObjectSnapshot | null = null;
      try {
        snapshot = await this.ctx.storage.snapshot();
      } catch (error) {
        if (mode === "native") throw error; // auto: no snapshots here (local development)
      }
      if (snapshot) {
        try {
          await this.node(right).shardedRestore(snapshot);
        } catch (error) {
          // The sibling restarts right after replying; under load the restart can overtake the
          // reply. Claiming tells whether the restore took.
          if (!String(error).includes(RESTORE_ABORT)) throw error;
        }
        return "native";
      }
    } else if (mode === "native") {
      throw new Error("storage snapshots need the durable_object_snapshots compatibility flag");
    }
    await this.copyRows(right, epoch, live);
    return "logical";
  }

  /**
   * The logical copy, where snapshots are unavailable: every table, as a snapshot would carry
   * them, in byte-bounded batches. Writes to the half that moves are held meanwhile; rows of the
   * other half may change under the copy, which is fine: the copy drops them.
   */
  private async copyRows(right: string, epoch: number, live: () => boolean): Promise<void> {
    const meta = this.meta!;
    const target = this.node(right);
    let first = true;
    for (const table of this.tables()) {
      for (let offset = 0; ; ) {
        if (!live()) throw new Error("split timed out copying rows");
        const cursor = this.sql.exec(`SELECT * FROM "${table}" LIMIT ? OFFSET ?`, COPY_PAGE_ROWS, offset);
        const rows: SqlStorageValue[][] = [];
        let bytes = 0;
        for (const row of cursor.raw()) {
          rows.push(row);
          for (const value of row) bytes += value instanceof ArrayBuffer ? value.byteLength : String(value).length;
          if (bytes >= COPY_BATCH_BYTES) break;
        }
        if (rows.length === 0 && !first) break;
        await target.shardedReceive(meta.name, right, first, table, cursor.columnNames, rows);
        first = false;
        offset += rows.length;
        if (rows.length === 0 || (rows.length < COPY_PAGE_ROWS && bytes < COPY_BATCH_BYTES)) break;
      }
    }
    if (!(await target.shardedReceiveDone(meta.name, right, epoch))) throw new Error("the copy did not take this split's meta");
  }

  /** This object's tables, the subclass's too (not SQLite's or the runtime's). */
  private tables(): string[] {
    return this.sql
      .exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name")
      .toArray()
      .map((t) => t.name);
  }

  private node(bits: string): DurableObjectStub<ShardedObject<Env, V>> {
    const nodes = this.nodes();
    return nodes.get(nodes.idFromName(shardObjectName(this.meta!.name, bits)));
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

  override async alarm(): Promise<void> {
    const meta = this.meta;
    // A verbatim copy not claimed yet carries the source's alarm: it is not this object's.
    if (!meta || this.isUnclaimedCopy()) return;
    if (meta.activate) await this.activateSibling();
    const range = meta.cleanup[0];
    if (range) {
      const deleted = this.sql.exec("DELETE FROM sharded_rows WHERE key IN (SELECT key FROM sharded_rows WHERE hash >= ? AND hash <= ? LIMIT ?)", range[0], range[1], DELETE_BATCH_ROWS).rowsWritten;
      if (deleted === 0) this.saveMeta({ ...this.meta!, cleanup: this.meta!.cleanup.slice(1) });
    } else if (meta.foreign) {
      const bits = meta.bits;
      const deleted = this.ctx.storage.transactionSync(() => this.cleanupForeign((key) => ownsHash(bits, keyHash(key)), DELETE_BATCH_ROWS));
      if (deleted === 0) this.saveMeta({ ...this.meta!, foreign: false });
    }
    if (this.meta!.activate) await this.ctx.storage.setAlarm(Date.now() + 1000);
    else if (this.meta!.cleanup.length > 0 || this.meta!.foreign) await this.ctx.storage.setAlarm(Date.now() + DELETE_PAUSE_MS);
  }

  // ---- The sibling's side of a split ----

  /** Schedules this object's storage to be replaced by the snapshot, then restarts to apply it. */
  async shardedRestore(snapshot: DurableObjectSnapshot): Promise<void> {
    if (this.serving()) throw new Error("refusing to overwrite a serving shard");
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
  }

  /** One batch of the logical copy: rows of one table. The first batch wipes every table. */
  async shardedReceive(_name: string, _bits: string, first: boolean, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void> {
    if (first && this.serving()) throw new Error("refusing to overwrite a serving shard");
    this.ctx.storage.transactionSync(() => {
      if (first) {
        for (const t of this.tables()) this.sql.exec(`DELETE FROM "${t}"`);
        this.meta = null;
      }
      if (rows.length === 0) return;
      // A subclass table the copy has not created yet: its constructor creates it on a restart,
      // but copies need it now.
      if (!this.tables().includes(table)) throw new Error(`table ${table} does not exist in the copy`);
      const sql = `INSERT OR REPLACE INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
      for (const row of rows) this.sql.exec(sql, ...row);
    });
  }

  /** The logical copy is complete: it holds the source's meta, from this split. */
  async shardedReceiveDone(name: string, bits: string, epoch: number): Promise<boolean> {
    this.meta = this.loadMeta();
    const meta = this.meta;
    return meta !== null && meta.name === name && meta.splitting === epoch && `${meta.bits}1` === bits && this.isUnclaimedCopy();
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
    const meta = this.meta;
    if (!meta || meta.name !== name) return false;
    if (meta.bits === bits) return meta.epoch === epoch && meta.state === "copy"; // claimed already
    if (!this.isUnclaimedCopy() || meta.bits !== parent || meta.splitting !== epoch) return false; // not this copy
    this.ctx.storage.transactionSync(() => {
      this.afterClone((key) => ownsHash(bits, keyHash(key)));
      this.saveMeta({ name, bits, state: "copy", epoch, splitting: null, activate: null, cleanup: this.outside(bits), foreign: true, parent, splitAt: Date.now() });
    });
    await this.ctx.storage.setAlarm(Date.now());
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
    const meta = this.meta;
    if (!meta || meta.name !== name) return false;
    if (meta.bits !== bits) return meta.state === "shard" && meta.splitting === null && meta.bits.startsWith(bits); // serving, and split since
    if (meta.epoch !== epoch) return false;
    if (meta.state === "copy") this.saveMeta({ ...meta, state: "shard" });
    return true;
  }

  /** The right half of an empty shard's initial split: empty, and serving at once. */
  async shardedAdoptEmpty(name: string, bits: string, parent: string): Promise<void> {
    if (this.meta) return;
    this.saveMeta({ name, bits, state: "shard", epoch: 0, splitting: null, activate: null, cleanup: [], parent, splitAt: Date.now() });
  }

  async shardedDescribe(): Promise<NodeDescription> {
    const meta = this.meta;
    const storedRows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_rows").one().n;
    let rows = storedRows;
    if (meta) {
      const [lo, hi] = hashRange(meta.bits);
      rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_rows WHERE hash >= ? AND hash <= ?", lo, hi).one().n;
    }
    const state = !meta ? "new" : this.hold ? "splitting" : meta.state === "copy" || this.isUnclaimedCopy() ? "copy" : "shard";
    return { bits: meta?.bits ?? "", state, rows, storedRows, callsPerSecond: this.rate(), lastSplit: meta?.lastSplit ?? null, lastSplitError: meta?.lastSplitError ?? null };
  }
}
