// Splittable objects (RFD0009): a Durable Object base class whose instances split themselves in
// two, by the next bit of their keys' hash, when their call rate stays high; and a client that
// routes each key to the leaf that owns it.
//
// A node named `${name}#${bits}` owns the keys whose hash starts with `bits`. Splits are final, so
// a client's map of splits can only lack some; it never names a wrong owner, only stops too
// shallow, and the node it reaches answers "moved".
import { DurableObject } from "cloudflare:workers";
import { createHash } from "node:crypto";

export { SpansLeaves, SplittableClient, splittable, type LeafResult } from "./client.ts";

/** When a node splits: its average calls/s over `windowSeconds` above `callsPerSecond`. */
export interface SplitPolicy {
  /** Load, in calls/s, above which a leaf splits (a call weighs 1 + keys / keysPerCall). */
  readonly callsPerSecond: number;
  readonly windowSeconds: number;
  /** No node deeper than this (2^maxDepth leaves at most). */
  readonly maxDepth: number;
  /**
   * Keys that cost as much as one call (RFD0013): a batch of many keys is load as well. Workers
   * cannot time synchronous work (clocks move only on I/O), so keys stand in for it. Default 25.
   */
  readonly keysPerCall?: number;
  /**
   * Depth a new object splits to before its first call, while empty (RFD0015): for uniform keys
   * (hashes), so a fresh object starts with 2^initialDepth leaves and no rows to hand out.
   */
  readonly initialDepth?: number;
}

/**
 * Split at half of what one object takes: about 800 calls/s on the edge (RFD0008), so 400,
 * sustained for 5 s.
 */
export const DEFAULT_SPLIT_POLICY: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25 };

/** What a node answers a routed call. */
export type CallAnswer<R> = { readonly ok: R } | { readonly moved: string } | { readonly unknown: string };

/** A node's state, for tests and operators. */
export interface NodeDescription {
  readonly bits: string;
  readonly state: "leaf" | "split" | "new";
  readonly inherited: boolean;
  readonly rows: number;
  readonly callsPerSecond: number;
}

/** A row as handed from a parent to a child. */
export type HandedRow = {
  readonly key: string;
  readonly hash: string;
  readonly value: string;
};

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

/** The hashes a node owns, inclusive, as 16-hex-digit strings (they sort like the numbers). */
function hashRange(bits: string): readonly [string, string] {
  const lo = BigInt(`0b0${bits}${"0".repeat(64 - bits.length)}`);
  const hi = BigInt(`0b0${bits}${"1".repeat(64 - bits.length)}`);
  return [lo.toString(16).padStart(16, "0"), hi.toString(16).padStart(16, "0")];
}

export function nodeName(name: string, bits: string): string {
  return `${name}#${bits}`;
}

/** Thrown inside a method when its node split under it; the dispatcher answers "moved". */
class Moved extends Error {}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS splittable_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS splittable_rows (key TEXT PRIMARY KEY, hash TEXT NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS splittable_rows_by_hash ON splittable_rows (hash, key);
CREATE TABLE IF NOT EXISTS splittable_decided (key TEXT PRIMARY KEY) WITHOUT ROWID;
`;

/** Rows per background inheriting call. */
const HAND_OUT_ROWS = 1000;

interface Meta {
  name: string;
  bits: string;
  parent: string | null;
  state: "leaf" | "split";
  inherited: boolean;
  /** Background copy position: the last (hash, key) copied. */
  cursorHash: string;
  cursorKey: string;
  /** Released to the parent (after inheriting). */
  released: boolean;
  /** Children that have finished inheriting (a split parent). */
  releasedChildren: string[];
  /** When the parent adopted this node (0 for the root). */
  adoptedAt?: number;
}

/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this node), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses that define `alarm` must call `super.alarm()`.
 */
export abstract class SplittableObject<Env = unknown, V = unknown> extends DurableObject<Env> {
  protected splitPolicy: SplitPolicy = DEFAULT_SPLIT_POLICY;
  /** The namespace this class is bound to: to reach parent and children. */
  protected abstract nodes(): DurableObjectNamespace<SplittableObject<Env, V>>;

  private readonly sql: SqlStorage;
  private meta: Meta | null;
  private readonly calls = new Map<number, number>();
  private readonly bornAt = Date.now();
  private splitting = false;
  /** An empty node splitting to the initial depth; calls wait for it, then are moved. */
  private presplitting: Promise<void> | null = null;
  /** This node had rows when it could have presplit: it splits by load only. */
  private presplitDeclined = false;
  private readonly pulls = new Map<string, Promise<void>>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(SCHEMA);
    const row = this.sql.exec<{ value: string }>("SELECT value FROM splittable_meta WHERE key = 'meta'").toArray()[0];
    this.meta = row ? (JSON.parse(row.value) as Meta) : null;
  }

  // ---- Reads and writes for subclasses ----

  /** The current rows of these keys (absent keys are missing from the map). */
  protected async read(keys: readonly string[]): Promise<Map<string, V>> {
    await this.ensure(keys);
    this.assertOwner();
    const out = new Map<string, V>();
    for (const key of keys) {
      const row = this.sql.exec<{ value: string }>("SELECT value FROM splittable_rows WHERE key = ?", key).toArray()[0];
      if (row) out.set(key, JSON.parse(row.value) as V);
    }
    return out;
  }

  /**
   * Reads these keys and writes what `decide` returns, in one transaction, if this node still owns
   * them. `decide` must be synchronous.
   */
  protected async update<R>(keys: readonly string[], decide: (current: Map<string, V>) => UpdateResult<V, R>): Promise<R> {
    await this.ensure(keys);
    this.assertOwner();
    return this.ctx.storage.transactionSync(() => {
      const current = new Map<string, V>();
      for (const key of keys) {
        const row = this.sql.exec<{ value: string }>("SELECT value FROM splittable_rows WHERE key = ?", key).toArray()[0];
        if (row) current.set(key, JSON.parse(row.value) as V);
      }
      const { writes, result } = decide(current);
      for (const [key, value] of writes) {
        if (value === null) this.sql.exec("DELETE FROM splittable_rows WHERE key = ?", key);
        else this.sql.exec("INSERT INTO splittable_rows (key, hash, value) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, keyHash(key), JSON.stringify(value));
        if (!this.meta!.inherited) this.sql.exec("INSERT OR IGNORE INTO splittable_decided (key) VALUES (?)", key);
      }
      return result;
    });
  }

  /**
   * Runs before a split node hands rows to its children: a subclass with work queued under the
   * rows it is giving away (an outbox) finishes it here, so a child continues after it.
   */
  protected async beforeHandOut(): Promise<void> {}

  /** This node's name (`${name}#${bits}`), once it has one. */
  protected nodeLabel(): string {
    if (!this.meta) throw new Error("node has no name yet");
    return nodeName(this.meta.name, this.meta.bits);
  }

  /** The parent's node name and when it adopted this node; null for the root. */
  protected parentNode(): { label: string; adoptedAt: number } | null {
    if (!this.meta || this.meta.parent === null) return null;
    return { label: nodeName(this.meta.name, this.meta.parent), adoptedAt: this.meta.adoptedAt ?? 0 };
  }

  /** The name all nodes of this object share (what the client was created with). */
  protected objectName(): string {
    if (!this.meta) throw new Error("node has no name yet");
    return this.meta.name;
  }

  private assertOwner(): void {
    if (this.meta?.state !== "leaf") throw new Moved();
  }

  // ---- The routed call ----

  /** A call routed by the client: `method(keys, payloads, ...args)` if this node owns the keys. */
  async splittableCall(name: string, bits: string, method: string, keys: readonly string[], payloads: readonly unknown[] | null, args: readonly unknown[]): Promise<CallAnswer<unknown>> {
    const meta = await this.arriveSettled(name, bits);
    if (!meta) return { unknown: bits };
    if (meta.state === "split") return { moved: bits };
    for (const key of keys) if (!ownsHash(bits, keyHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);
    this.countCall(keys.length);
    const fn = (this as unknown as Record<string, unknown>)[method];
    if (typeof fn !== "function" || method.startsWith("splittable")) throw new Error(`no method ${method}`);
    try {
      return { ok: await (fn as (...a: unknown[]) => Promise<unknown>).call(this, keys, payloads, ...args) };
    } catch (error) {
      if (error instanceof Moved) return { moved: bits };
      throw error;
    }
  }

  /** A call to every leaf: `method(...args)`, with no keys. */
  async splittableScan(name: string, bits: string, method: string, args: readonly unknown[]): Promise<CallAnswer<unknown>> {
    const meta = await this.arriveSettled(name, bits);
    if (!meta) return { unknown: bits };
    if (meta.state === "split") return { moved: bits };
    const fn = (this as unknown as Record<string, unknown>)[method];
    if (typeof fn !== "function" || method.startsWith("splittable")) throw new Error(`no method ${method}`);
    if (!meta.inherited) await this.inheritAll();
    try {
      return { ok: await (fn as (...a: unknown[]) => Promise<unknown>).call(this, ...args) };
    } catch (error) {
      if (error instanceof Moved) return { moved: bits };
      throw error;
    }
  }

  /** Every row this node owns, for scans (a subclass's scan method may use it). */
  protected allRows(): Map<string, V> {
    this.assertOwner();
    const out = new Map<string, V>();
    for (const r of this.sql.exec<{ key: string; value: string }>("SELECT key, value FROM splittable_rows")) out.set(r.key, JSON.parse(r.value) as V);
    return out;
  }

  /**
   * `arrive`, then the split to the initial depth if this node is above it and empty. Every call
   * waits for that split, so nothing is written to a node that is about to hand out nothing.
   */
  private async arriveSettled(name: string, bits: string): Promise<Meta | null> {
    const meta = this.arrive(name, bits);
    if (!meta || meta.state !== "leaf" || this.presplitDeclined || meta.bits.length >= (this.splitPolicy.initialDepth ?? 0)) return meta;
    this.presplitting ??= this.presplit().finally(() => (this.presplitting = null));
    await this.presplitting;
    return this.meta;
  }

  private async presplit(): Promise<void> {
    const meta = this.meta!;
    const rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM splittable_rows").one().n;
    if (rows > 0 || !meta.inherited || meta.state !== "leaf") {
      this.presplitDeclined = true; // not empty: splits by load only
      return;
    }
    // Children of an empty node start with everything inherited: there is nothing to hand out.
    for (const child of [`${meta.bits}0`, `${meta.bits}1`]) await this.node(child).splittableAdopt(meta.name, child, meta.bits, true);
    this.ctx.storage.transactionSync(() => this.saveMeta({ ...meta, state: "split", releasedChildren: [`${meta.bits}0`, `${meta.bits}1`] }));
  }

  /** The node's meta, initialising the root on its first call; null if never adopted. */
  private arrive(name: string, bits: string): Meta | null {
    if (this.meta) {
      if (this.meta.bits !== bits || this.meta.name !== name) throw new Error(`${nodeName(name, bits)} reached ${nodeName(this.meta.name, this.meta.bits)}`);
      return this.meta;
    }
    if (bits !== "") return null;
    this.saveMeta({ name, bits: "", parent: null, state: "leaf", inherited: true, cursorHash: "", cursorKey: "", released: true, releasedChildren: [] });
    return this.meta;
  }

  private saveMeta(meta: Meta): void {
    this.meta = meta;
    this.sql.exec("INSERT INTO splittable_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
  }

  // ---- Load and splitting ----

  private countCall(keys: number): void {
    const second = Math.floor(Date.now() / 1000);
    this.calls.set(second, (this.calls.get(second) ?? 0) + 1 + keys / (this.splitPolicy.keysPerCall ?? 25));
    if (this.calls.size > this.splitPolicy.windowSeconds + 2) {
      for (const s of this.calls.keys()) if (s < second - this.splitPolicy.windowSeconds - 1) this.calls.delete(s);
    }
    if (this.rate() > this.splitPolicy.callsPerSecond && Date.now() - this.bornAt > this.splitPolicy.windowSeconds * 1000) void this.split();
  }

  /** Average calls/s over the last full `windowSeconds`. */
  private rate(): number {
    const now = Math.floor(Date.now() / 1000);
    let sum = 0;
    for (let s = now - this.splitPolicy.windowSeconds; s < now; s++) sum += this.calls.get(s) ?? 0;
    return sum / this.splitPolicy.windowSeconds;
  }

  /** Splits now if this is a leaf that has finished inheriting (also an operator's lever). */
  async splittableSplit(name: string, bits: string): Promise<boolean> {
    const meta = this.arrive(name, bits);
    if (!meta) return false;
    return this.split();
  }

  private async split(): Promise<boolean> {
    const meta = this.meta;
    if (!meta || this.splitting || meta.state !== "leaf" || !meta.inherited || meta.bits.length >= this.splitPolicy.maxDepth) return false;
    this.splitting = true;
    try {
      // 1. Both children exist and know their parent before anything moves to them.
      for (const child of [`${meta.bits}0`, `${meta.bits}1`]) await this.node(child).splittableAdopt(meta.name, child, meta.bits);
      // 2. Frozen: no more calls here; rows wait for the children to take them.
      this.ctx.storage.transactionSync(() => this.saveMeta({ ...meta, state: "split", releasedChildren: [] }));
      console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), callsPerSecond: this.rate() }));
      return true;
    } finally {
      this.splitting = false;
    }
  }

  private node(bits: string): DurableObjectStub<SplittableObject<Env, V>> {
    const nodes = this.nodes();
    return nodes.get(nodes.idFromName(nodeName(this.meta!.name, bits)));
  }

  // ---- Inheriting (child side) ----

  /** Called by the parent when it splits. */
  async splittableAdopt(name: string, bits: string, parent: string, empty = false): Promise<void> {
    if (this.meta) return;
    if (empty) {
      // The initial split (RFD0015): the parent had no rows, so there is nothing to inherit.
      this.saveMeta({ name, bits, parent, state: "leaf", inherited: true, cursorHash: "", cursorKey: "", released: true, releasedChildren: [], adoptedAt: 0 });
      return;
    }
    this.saveMeta({ name, bits, parent, state: "leaf", inherited: false, cursorHash: "", cursorKey: "", released: false, releasedChildren: [], adoptedAt: Date.now() });
    await this.ctx.storage.setAlarm(Date.now());
  }

  /** Keys not decided yet are fetched from the parent first. */
  private async ensure(keys: readonly string[]): Promise<void> {
    if (!this.meta || this.meta.inherited) return;
    const waits: Promise<void>[] = [];
    const fresh: string[] = [];
    for (const key of new Set(keys)) {
      const pending = this.pulls.get(key);
      if (pending) waits.push(pending);
      else if (this.sql.exec("SELECT 1 FROM splittable_decided WHERE key = ?", key).toArray().length === 0) fresh.push(key);
    }
    if (fresh.length > 0) {
      const pull = this.pull(fresh);
      for (const key of fresh) this.pulls.set(key, pull);
      waits.push(pull.finally(() => fresh.forEach((key) => this.pulls.delete(key))));
    }
    await Promise.all(waits);
  }

  private async pull(keys: readonly string[]): Promise<void> {
    const meta = this.meta!;
    const rows = await this.waitForParent(() => this.node(meta.parent!).splittableHandOut(meta.name, meta.parent!, keys));
    if (this.meta!.inherited) return; // the background copy finished meanwhile and has these rows
    this.ctx.storage.transactionSync(() => this.takeRows(keys, rows));
  }

  /** Stores handed rows for keys not decided yet, and marks the keys decided. */
  private takeRows(keys: readonly string[], rows: readonly HandedRow[]): void {
    for (const row of rows) {
      if (this.sql.exec("SELECT 1 FROM splittable_decided WHERE key = ?", row.key).toArray().length > 0) continue;
      this.sql.exec("INSERT OR IGNORE INTO splittable_rows (key, hash, value) VALUES (?, ?, ?)", row.key, row.hash, row.value);
    }
    for (const key of keys) this.sql.exec("INSERT OR IGNORE INTO splittable_decided (key) VALUES (?)", key);
  }

  /** The parent hands out rows only once split; adopt comes just before that. */
  private async waitForParent<T>(ask: () => Promise<T | "not split">): Promise<T> {
    for (let attempt = 0; ; attempt++) {
      const answer = await ask();
      if (answer !== "not split") return answer;
      if (attempt > 100) throw new Error("parent did not split");
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }

  /** Copies the rest of the range from the parent (the alarm's job), then releases the parent. */
  private async inheritAll(): Promise<void> {
    while (this.meta && !this.meta.inherited) await this.inheritStep();
  }

  private async inheritStep(): Promise<void> {
    const meta = this.meta!;
    const rows = await this.waitForParent(() => this.node(meta.parent!).splittableHandOutRange(meta.name, meta.parent!, meta.bits, meta.cursorHash, meta.cursorKey, HAND_OUT_ROWS));
    const now = this.meta!;
    if (now.inherited || now.cursorHash !== meta.cursorHash || now.cursorKey !== meta.cursorKey) return; // another step got here first
    this.ctx.storage.transactionSync(() => {
      this.takeRows(rows.map((r) => r.key), rows);
      const last = rows[rows.length - 1];
      if (rows.length < HAND_OUT_ROWS) {
        this.sql.exec("DELETE FROM splittable_decided");
        this.saveMeta({ ...now, inherited: true, cursorHash: "", cursorKey: "" });
      } else {
        this.saveMeta({ ...now, cursorHash: last!.hash, cursorKey: last!.key });
      }
    });
  }

  override async alarm(): Promise<void> {
    const meta = this.meta;
    if (!meta || meta.parent === null) return;
    if (!meta.inherited) {
      await this.inheritStep();
      await this.ctx.storage.setAlarm(Date.now());
      return;
    }
    if (!meta.released) {
      await this.node(meta.parent).splittableRelease(meta.name, meta.parent, meta.bits);
      this.saveMeta({ ...this.meta!, released: true });
    }
  }

  // ---- Handing out (parent side) ----

  async splittableHandOut(name: string, bits: string, keys: readonly string[]): Promise<HandedRow[] | "not split"> {
    const meta = this.arrive(name, bits);
    if (meta?.state !== "split") return "not split";
    await this.beforeHandOut();
    const out: HandedRow[] = [];
    for (const key of keys) {
      const row = this.sql.exec<HandedRow>("SELECT key, hash, value FROM splittable_rows WHERE key = ?", key).toArray()[0];
      if (row) out.push(row);
    }
    // A parent that is itself still inheriting would have to ask its own parent; it cannot be:
    // a node splits only once it has inherited everything.
    return out;
  }

  async splittableHandOutRange(name: string, bits: string, child: string, afterHash: string, afterKey: string, limit: number): Promise<HandedRow[] | "not split"> {
    const meta = this.arrive(name, bits);
    if (meta?.state !== "split") return "not split";
    await this.beforeHandOut();
    const [lo, hi] = hashRange(child);
    return this.sql
      .exec<HandedRow>(
        "SELECT key, hash, value FROM splittable_rows WHERE hash >= ? AND hash <= ? AND (hash > ? OR (hash = ? AND key > ?)) ORDER BY hash, key LIMIT ?",
        lo,
        hi,
        afterHash,
        afterHash,
        afterKey,
        limit,
      )
      .toArray();
  }

  /** A child has all its rows: this node deletes them. */
  async splittableRelease(name: string, bits: string, child: string): Promise<void> {
    const meta = this.arrive(name, bits);
    if (meta?.state !== "split" || meta.releasedChildren.includes(child)) return;
    const [lo, hi] = hashRange(child);
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM splittable_rows WHERE hash >= ? AND hash <= ?", lo, hi);
      this.saveMeta({ ...meta, releasedChildren: [...meta.releasedChildren, child] });
    });
  }

  async splittableDescribe(): Promise<NodeDescription> {
    const rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM splittable_rows").one().n;
    return { bits: this.meta?.bits ?? "", state: this.meta?.state ?? "new", inherited: this.meta?.inherited ?? false, rows, callsPerSecond: this.rate() };
  }
}
