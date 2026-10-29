// An alternative snapshot split, kept for comparison only: the splitting object freezes and
// restores its storage into two new children, which drop the half they do not own, and it
// stays as a routing node. Not part of the package.
// Splittable objects: a Durable Object base class whose instances split themselves in
// two, by the next bit of their keys' hash, when their call rate stays high; and a client that
// routes each key to the leaf that owns it.
//
// A node named `${name}#${bits}` owns the keys whose hash starts with `bits`. Splits are final, so
// a client's map of splits can only lack some; it never names a wrong owner, only stops too
// shallow, and the node it reaches answers "moved".
//
// A split copies the node's whole database into both children: with the storage
// snapshot API where it works, by a logical copy of every table elsewhere (local workerd). Each
// child drops what it does not own, and only then does the parent send anyone to it.
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
   * Keys that cost as much as one call: a batch of many keys is load as well. Workers
   * cannot time synchronous work (clocks move only on I/O), so keys stand in for it. Default 25.
   */
  readonly keysPerCall?: number;
  /**
   * Depth a new object splits to before its first call, while empty: for uniform keys
   * (hashes), so a fresh object starts with 2^initialDepth leaves and no rows to hand out.
   */
  readonly initialDepth?: number;
}

/**
 * Split at half of what one object takes: about 800 calls/s on the edge, so 400,
 * sustained for 5 s.
 */
export const DEFAULT_SPLIT_POLICY: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25 };

/** What a node answers a routed call. */
export type CallAnswer<R> = { readonly ok: R } | { readonly moved: string } | { readonly unknown: string };

/** A node's state, for tests and operators. */
export interface NodeDescription {
  readonly bits: string;
  readonly state: "leaf" | "split" | "new";
  readonly rows: number;
  readonly callsPerSecond: number;
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

export function ownsHash(bits: string, hash: string): boolean {
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
`;

/** Rows per page of a logical copy. */
const COPY_ROWS = 500;
/** The abort a child restoring a snapshot ends its session with (its caller expects it). */
const RESTORE_ABORT = "splittable: restart on the copy";

/** A split in progress, persisted in the parent before the copy, so the copies carry it too. */
interface Cloning {
  readonly children: readonly [string, string];
  /** The parent's id: a node whose own id differs is a copy. */
  readonly from: string;
  /** This attempt; a child copied by it records it, so a retry tells stale copies apart. */
  readonly attempt: string;
}

interface Meta {
  name: string;
  bits: string;
  parent: string | null;
  state: "leaf" | "split";
  /** Set in a splitting parent, and in the copies until they fix themselves up. */
  cloning?: Cloning;
  /** The split attempt this node was copied by (a child). */
  copiedFrom?: string;
}

/** The storage snapshot API, not in workers-types yet; needs `durable_object_snapshots`. */
interface SnapshotStorage {
  snapshot?(): Promise<unknown>;
  onNextSessionRestore?(snapshot: unknown): Promise<string>;
}

/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this node), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * A split copies every table, the subclass's too; a child then calls `afterClone` to drop or
 * rewrite subclass state that belongs to the parent (work it queued, rows it does not own).
 * Subclasses that define `alarm` must call `super.alarm()` first.
 */
export abstract class SplittableObject<Env = unknown, V = unknown> extends DurableObject<Env> {
  protected splitPolicy: SplitPolicy = DEFAULT_SPLIT_POLICY;
  /** The namespace this class is bound to: to reach children. */
  protected abstract nodes(): DurableObjectNamespace<SplittableObject<Env, V>>;

  private readonly sql: SqlStorage;
  private meta: Meta | null;
  private readonly calls = new Map<number, number>();
  private readonly bornAt = Date.now();
  /** A split in progress: calls wait for it, then are moved (or run, if it failed). */
  private frozen: Promise<void> | null = null;
  /** An empty node splitting to the initial depth; calls wait for it, then are moved. */
  private presplitting: Promise<void> | null = null;
  /** This node had rows when it could have presplit: it splits by load only. */
  private presplitDeclined = false;
  /** False once the snapshot API failed here: copies are logical from then on. */
  private nativeSnapshots = true;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(SCHEMA);
    this.meta = this.loadMeta();
  }

  // ---- Reads and writes for subclasses ----

  /** The current rows of these keys (absent keys are missing from the map). */
  protected async read(keys: readonly string[]): Promise<Map<string, V>> {
    await this.assertOwner();
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
    await this.assertOwner();
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
      }
      return result;
    });
  }

  /**
   * Runs in a child copied from its parent, inside the transaction that makes it its own node:
   * the parent keeps its subclass tables and the work queued in them, so a child drops its copy
   * of that work here. `owns(key)` says whether a key is this child's. Synchronous.
   */
  protected afterClone(_owns: (key: string) => boolean): void {}

  /** This node's name (`${name}#${bits}`), once it has one. */
  protected nodeLabel(): string {
    if (!this.meta) throw new Error("node has no name yet");
    return nodeName(this.meta.name, this.meta.bits);
  }

  /** The name all nodes of this object share (what the client was created with). */
  protected objectName(): string {
    if (!this.meta) throw new Error("node has no name yet");
    return this.meta.name;
  }

  /** Waits out a split in progress; throws Moved if this node is no longer a leaf. */
  private async assertOwner(): Promise<void> {
    while (this.frozen) await this.frozen;
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
    try {
      return { ok: await (fn as (...a: unknown[]) => Promise<unknown>).call(this, ...args) };
    } catch (error) {
      if (error instanceof Moved) return { moved: bits };
      throw error;
    }
  }

  /** Every row this node owns, for scans (a subclass's scan method may use it). */
  protected allRows(): Map<string, V> {
    if (this.meta?.state !== "leaf" || this.frozen) throw new Moved();
    const out = new Map<string, V>();
    for (const r of this.sql.exec<{ key: string; value: string }>("SELECT key, value FROM splittable_rows")) out.set(r.key, JSON.parse(r.value) as V);
    return out;
  }

  /**
   * `arrive`, then waits out a split in progress (resuming one a restart interrupted), and splits
   * to the initial depth if this node is above it and empty. Every call waits for those, so
   * nothing is written to a node that is being copied or is about to hand out nothing.
   */
  private async arriveSettled(name: string, bits: string): Promise<Meta | null> {
    let meta = this.arrive(name, bits);
    if (meta?.cloning && meta.state === "leaf" && !this.frozen) void this.split(); // interrupted by a restart
    while (this.frozen) await this.frozen;
    meta = this.meta;
    if (!meta || meta.state !== "leaf" || this.presplitDeclined || meta.bits.length >= (this.splitPolicy.initialDepth ?? 0)) return meta;
    this.presplitting ??= this.presplit().finally(() => (this.presplitting = null));
    await this.presplitting;
    return this.meta;
  }

  private async presplit(): Promise<void> {
    const meta = this.meta!;
    const rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM splittable_rows").one().n;
    if (rows > 0 || meta.state !== "leaf") {
      this.presplitDeclined = true; // not empty: splits by load only
      return;
    }
    // An empty node has nothing to copy: its children start empty.
    for (const child of [`${meta.bits}0`, `${meta.bits}1`]) await this.node(child).splittableAdoptEmpty(meta.name, child, meta.bits);
    this.ctx.storage.transactionSync(() => this.saveMeta({ name: meta.name, bits: meta.bits, parent: meta.parent, state: "split" }));
  }

  /** The node's meta, initialising the root on its first call; null if never created. */
  private arrive(name: string, bits: string): Meta | null {
    this.fixUpIfCopy();
    if (this.meta) {
      if (this.meta.bits !== bits || this.meta.name !== name) throw new Error(`${nodeName(name, bits)} reached ${nodeName(this.meta.name, this.meta.bits)}`);
      return this.meta;
    }
    if (bits !== "") return null;
    this.saveMeta({ name, bits: "", parent: null, state: "leaf" });
    return this.meta;
  }

  private loadMeta(): Meta | null {
    const row = this.sql.exec<{ value: string }>("SELECT value FROM splittable_meta WHERE key = 'meta'").toArray()[0];
    return row ? (JSON.parse(row.value) as Meta) : null;
  }

  private saveMeta(meta: Meta): void {
    this.meta = meta;
    this.sql.exec("INSERT INTO splittable_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
  }

  private node(bits: string): DurableObjectStub<SplittableObject<Env, V>> {
    const nodes = this.nodes();
    return nodes.get(nodes.idFromName(nodeName(this.meta!.name, bits)));
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

  /** Splits now if this is a leaf (also an operator's lever). */
  async splittableSplit(name: string, bits: string): Promise<boolean> {
    const meta = this.arrive(name, bits);
    if (!meta) return false;
    while (this.frozen) await this.frozen;
    return this.split();
  }

  /**
   * Freezes this node, copies it into both children, and marks it split once both have started
   * on their copy. Calls arriving meanwhile wait (about a second), then are moved. If the copy
   * fails the node thaws as the leaf it was and splits again later.
   */
  private split(): Promise<boolean> {
    const meta = this.meta;
    if (!meta || this.frozen || meta.state !== "leaf" || meta.bits.length >= this.splitPolicy.maxDepth) return Promise.resolve(false);
    const started = Date.now();
    const done = this.copyToChildren(meta);
    this.frozen = done.then(
      () => undefined,
      () => undefined,
    );
    void this.frozen.finally(() => (this.frozen = null));
    return done.then(
      ({ copy, phases }) => {
        console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), copy, ms: Date.now() - started, ...phases, callsPerSecond: this.rate() }));
        return true;
      },
      (error: unknown) => {
        console.error(JSON.stringify({ event: "split-failed", node: nodeName(meta.name, meta.bits), error: String(error) }));
        return false;
      },
    );
  }

  private async copyToChildren(meta: Meta): Promise<{ copy: "native" | "logical"; phases: Record<string, number> }> {
    let at = Date.now();
    const phases: Record<string, number> = {};
    const phase = (name: string) => {
      phases[`${name}Ms`] = Date.now() - at;
      at = Date.now();
    };
    const children = [`${meta.bits}0`, `${meta.bits}1`] as const;
    // 1. Persisted first: a restart resumes the split instead of serving, and the copies carry it.
    const cloning: Cloning = meta.cloning ?? { children, from: this.ctx.id.toString(), attempt: crypto.randomUUID() };
    if (!meta.cloning) this.ctx.storage.transactionSync(() => this.saveMeta({ ...meta, cloning }));
    // 2. The copy: a snapshot each child restarts on, or every table, page by page.
    const snapshot = await this.takeSnapshot();
    phase("snapshot");
    await Promise.all(children.map((child) => (snapshot !== null ? this.restoreChild(meta.name, child, cloning.attempt, snapshot) : this.copyLogically(meta.name, child, cloning.attempt))));
    phase("restore");
    // 3. Each child starts on its copy, makes itself its own node, and confirms it.
    await Promise.all(
      children.map(async (child) => {
        if (!(await this.node(child).splittableSettle(meta.name, child, cloning.attempt))) throw new Error(`${nodeName(meta.name, child)} did not start on attempt ${cloning.attempt}`);
      }),
    );
    phase("settle");
    // 4. Split: the children own every row now.
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM splittable_rows");
      this.saveMeta({ name: meta.name, bits: meta.bits, parent: meta.parent, state: "split" });
    });
    return { copy: snapshot !== null ? "native" : "logical", phases };
  }

  /** A storage snapshot, or null where the API is missing or fails (copy logically then). */
  private async takeSnapshot(): Promise<unknown> {
    const storage = this.ctx.storage as unknown as SnapshotStorage;
    if (!this.nativeSnapshots || typeof storage.snapshot !== "function") return null;
    try {
      return await storage.snapshot();
    } catch (error) {
      this.nativeSnapshots = false;
      console.warn(JSON.stringify({ event: "split-snapshot-unavailable", node: this.nodeLabel(), error: String(error) }));
      return null;
    }
  }

  private async restoreChild(name: string, child: string, attempt: string, snapshot: unknown): Promise<void> {
    try {
      await this.node(child).splittableRestore(name, child, attempt, snapshot);
    } catch (error) {
      if (!String(error).includes(RESTORE_ABORT)) throw error; // the child restarting, as expected
    }
  }

  /** Every table, page by page, into the child (the child wipes its tables first). */
  private async copyLogically(name: string, child: string, attempt: string): Promise<void> {
    const stub = this.node(child);
    if (!(await stub.splittableLoadBegin(name, child, attempt))) return; // already copied by this attempt
    const tables = this.sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'").toArray();
    for (const { name: table } of tables) {
      for (let offset = 0; ; offset += COPY_ROWS) {
        const cursor = this.sql.exec(`SELECT * FROM "${table}" LIMIT ? OFFSET ?`, COPY_ROWS, offset);
        const rows = [...cursor.raw()];
        if (rows.length === 0) break;
        await stub.splittableLoadRows(name, child, attempt, table, cursor.columnNames, rows);
        if (rows.length < COPY_ROWS) break;
      }
    }
  }

  // ---- Copies (child side) ----

  /** Children of an empty node: nothing to copy. */
  async splittableAdoptEmpty(name: string, bits: string, parent: string): Promise<void> {
    if (this.meta) return;
    this.saveMeta({ name, bits, parent, state: "leaf" });
  }

  /** Restarts on the parent's snapshot (the caller sees this session abort). */
  async splittableRestore(name: string, bits: string, attempt: string, snapshot: unknown): Promise<void> {
    this.fixUpIfCopy();
    if (this.meta?.copiedFrom === attempt && this.meta.bits === bits && this.meta.name === name) return;
    try {
      await (this.ctx.storage as unknown as Required<SnapshotStorage>).onNextSessionRestore(snapshot);
    } catch (error) {
      if (!String(error).includes("already been scheduled")) throw error;
    }
    this.ctx.abort(RESTORE_ABORT);
  }

  /** Starts a logical copy: false if this node already holds this attempt's copy. */
  async splittableLoadBegin(name: string, bits: string, attempt: string): Promise<boolean> {
    this.fixUpIfCopy();
    if (this.meta?.copiedFrom === attempt && this.meta.bits === bits && this.meta.name === name) return false;
    const tables = this.sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%'").toArray();
    this.ctx.storage.transactionSync(() => {
      for (const { name: table } of tables) this.sql.exec(`DELETE FROM "${table}"`);
    });
    this.meta = null;
    return true;
  }

  async splittableLoadRows(_name: string, _bits: string, _attempt: string, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void> {
    const sql = `INSERT OR REPLACE INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
    this.ctx.storage.transactionSync(() => {
      for (const row of rows) this.sql.exec(sql, ...row);
    });
    if (table === "splittable_meta") this.meta = this.loadMeta();
  }

  /** The copy has started and fixed itself up as this child, for this attempt. */
  async splittableSettle(name: string, bits: string, attempt: string): Promise<boolean> {
    this.fixUpIfCopy();
    return this.meta?.name === name && this.meta.bits === bits && this.meta.copiedFrom === attempt;
  }

  /**
   * A copy of a splitting parent becomes the child it was copied for, before it serves a call or
   * runs an alarm: drops the rows it does not own, lets the subclass drop the parent's work, and
   * writes its own meta, in one transaction.
   */
  private fixUpIfCopy(): void {
    const meta = this.meta;
    if (!meta?.cloning || meta.cloning.from === this.ctx.id.toString()) return;
    const nodes = this.nodes();
    const bits = meta.cloning.children.find((child) => nodes.idFromName(nodeName(meta.name, child)).equals(this.ctx.id));
    if (bits === undefined) throw new Error(`a copy of ${nodeName(meta.name, meta.bits)} that is neither of its children`);
    const [lo, hi] = hashRange(bits);
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("DELETE FROM splittable_rows WHERE hash < ? OR hash > ?", lo, hi);
      this.afterClone((key) => ownsHash(bits, keyHash(key)));
      this.saveMeta({ name: meta.name, bits, parent: meta.bits, state: "leaf", copiedFrom: meta.cloning!.attempt });
    });
  }

  /** Subclasses that define `alarm` call this first (a copied alarm may be a copy's first event). */
  override async alarm(): Promise<void> {
    this.fixUpIfCopy();
  }

  async splittableDescribe(): Promise<NodeDescription> {
    this.fixUpIfCopy();
    const rows = this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM splittable_rows").one().n;
    return { bits: this.meta?.bits ?? "", state: this.meta?.state ?? "new", rows, callsPerSecond: this.rate() };
  }
}
