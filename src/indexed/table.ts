// The row side of an IndexedTable (RFD0003, specs/indexed-write.ts): a sharded table that writes the
// index changes of every row write to an outbox in the same transaction, delivers them to the index
// shards before the write returns, and fences the reservation tokens of unique values.
import { keyHash, sharded } from "../sharded/index.js";
import type { Untyped } from "../table/client.js";
import { ShardedTable } from "../table/index.js";
import type { RowShape, WriteResult } from "../table/schema.js";
import { addressKey, entriesOf, entryOf, globalIndexes, type GlobalIndexInfo, type IndexOp, type ResolveAnswer } from "./entries.js";

const OUTBOX = "indexed_outbox";
const FENCES = "indexed_fences";
/** How soon the alarm delivers what a write could not, after it started. */
const DELIVERY_RETRY_MS = 2_000;
/** Outbox entries delivered per call to the index shards. */
const DELIVERY_BATCH = 200;
const CLEANUP_BATCH = 250;

/** What the client sends with a write: a reservation token per unique index it reserved. */
export interface WriteTokens {
  readonly tokens?: Readonly<Record<string, number>>;
}

/** A write's result on the row shard: `indexed` false when its entries were not all delivered in time. */
export type IndexedShardResult = WriteResult & { readonly indexed?: boolean };

/** The write in progress on this shard (synchronous: one at a time). */
interface Batch {
  readonly tokens: ReadonlyMap<string, Readonly<Record<string, number>>>;
  /** Outbox ids written, per key. */
  readonly ops: Map<string, number[]>;
}

/**
 * Extend with `protected table = defineTable({ ..., globalIndexes })`, `nodes()` (this class's
 * binding) and `indexNodes()` (the binding of its GlobalIndex class). The client
 * (`indexedTable()`) reserves unique values, writes, looks up and lists.
 *
 * Custom transactional methods that write rows use `indexedTransact`, not `transact`: it delivers
 * the writes' index changes before returning. A custom write that changes a unique column's value
 * is refused (`refused: index`): those go through the client, which reserves first.
 *
 * Subclasses that override `onAlarm`, `afterClone` or `cleanupForeign` must call `super`.
 */
export abstract class IndexedTable<Env, Row extends RowShape> extends ShardedTable<Env, Row> {
  /** The binding of the GlobalIndex class holding this table's global indexes. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  protected abstract indexNodes(): DurableObjectNamespace<any>;
  /** How long a write waits for its index entries before it returns `IndexPending`. */
  protected indexWaitMs = 5_000;

  private indexMigrated = false;
  private batch: Batch | null = null;
  private indexClient: Untyped | null = null;
  /** Outbox entries being delivered now. */
  private readonly delivering = new Set<number>();
  private wakeAt: number | null = null;

  // ---- Called by GlobalIndex ----

  /**
   * Whether row `key` has an entry a reservation is pending for. If not, fences `token`, so no
   * write carrying it (or an older one) lands afterwards.
   */
  async indexResolve(keys: readonly string[], payloads: readonly { readonly route: string, readonly entry: string, readonly token: number }[]): Promise<ResolveAnswer[]> {
    this.migrate();
    return this.transact(keys, () =>
      keys.map((key, i) => {
        const { route, entry, token } = payloads[i]!;
        const current = this.getRow(key);
        if (current && entriesOf(this.table, current.row).has(addressKey({ route, entry }))) return { committed: true, version: current.version, row: current.row };
        this.raiseFence(key, route, entry, token);
        return { committed: false, version: current?.version ?? 0 };
      }),
    );
  }

  // ---- Writes ----

  /**
   * Like `transact`, for custom methods that write rows: delivers their index changes before it
   * returns, and throws `IndexPending` if that takes longer than `indexWaitMs` (the rows are
   * written; the alarm goes on delivering).
   */
  protected async indexedTransact<R>(keys: readonly string[], fn: () => R): Promise<R> {
    this.migrate();
    await this.wake(Date.now() + DELIVERY_RETRY_MS);
    const { result, ops } = await this.transact(keys, () => this.inBatch(new Map(), fn));
    const ids = [...ops.values()].flat();
    if (ids.length && !(await this.deliverWithin(ids, this.indexWaitMs))) throw new Error("IndexPending: rows written, index entries still being delivered");
    return result;
  }

  protected override async writeBatch(keys: readonly string[], payloads: readonly unknown[] | null, write: () => WriteResult[]): Promise<IndexedShardResult[]> {
    this.migrate();
    // Before the transaction: an outbox left by a crash right after it is delivered by the alarm.
    await this.wake(Date.now() + DELIVERY_RETRY_MS);
    const tokens = new Map<string, Readonly<Record<string, number>>>();
    keys.forEach((key, i) => tokens.set(key, (payloads?.[i] as WriteTokens | undefined)?.tokens ?? {}));
    const { result, ops } = await this.transact(keys, () => this.inBatch(tokens, write));
    const ids = [...ops.values()].flat();
    const delivered = ids.length === 0 || (await this.deliverWithin(ids, this.indexWaitMs));
    return result.map((r, i) => (r.ok && ops.has(keys[i]!) ? { ...r, indexed: delivered } : r));
  }

  private inBatch<R>(tokens: ReadonlyMap<string, Readonly<Record<string, number>>>, fn: () => R): { result: R; ops: Map<string, number[]> } {
    this.batch = { tokens, ops: new Map() };
    try {
      return { result: fn(), ops: this.batch.ops };
    } finally {
      this.batch = null;
    }
  }

  protected override watchesRows(): boolean {
    return true;
  }

  protected override keepsGlobalIndexes(): boolean {
    return true;
  }

  /** A unique value the row takes needs a reservation token above the fence. */
  protected override refuseRowWrite(key: string, before: Row | null, after: Row | null): string | null {
    if (!after) return null;
    for (const ix of this.uniqueIndexes()) {
      const next = entryOf(this.table, ix, after);
      if (!next) continue;
      const prev = before ? entryOf(this.table, ix, before) : null;
      if (prev && addressKey(prev) === addressKey(next)) continue;
      const token = this.batch?.tokens.get(key)?.[ix.name];
      if (token === undefined || token <= this.fence(key, next.route, next.entry)) return ix.name;
    }
    return null;
  }

  /** Appends the row's index changes to the outbox: a put for each entry it has, a remove for each it lost. */
  protected override rowWritten(key: string, before: Row | null, after: Row | null, version: number): void {
    const had = entriesOf(this.table, before);
    const has = entriesOf(this.table, after);
    const tokens = this.batch?.tokens.get(key) ?? {};
    const unique = new Set(this.uniqueIndexes().map((ix) => ix.name));
    const ops: IndexOp[] = [];
    for (const [id, e] of has) {
      let token: number | null = null;
      const given = tokens[e.index];
      if (unique.has(e.index) && given !== undefined && given > this.fence(key, e.route, e.entry)) {
        token = given;
        // Used: an older write carrying it, or an older token, cannot land after this one.
        this.raiseFence(key, e.route, e.entry, given);
      }
      ops.push({ kind: "put", route: e.route, entry: e.entry, owner: key, version, row: after as RowShape, token });
      had.delete(id);
    }
    for (const e of had.values()) ops.push({ kind: "remove", route: e.route, entry: e.entry, owner: key, version });
    const now = Date.now();
    const ids = ops.map((op) => this.store.exec<{ id: number }>(`INSERT INTO ${OUTBOX} (key, op, at) VALUES (?, ?, ?) RETURNING id`, key, JSON.stringify(op), now).one().id);
    if (this.batch && ids.length) this.batch.ops.set(key, [...(this.batch.ops.get(key) ?? []), ...ids]);
  }

  // ---- Delivery ----

  /** Delivers these outbox entries; true if they all were within `ms` (they go on being delivered either way). */
  private async deliverWithin(ids: readonly number[], ms: number): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const delivery = this.deliver(ids).then(
      () => true,
      (error) => {
        console.error(JSON.stringify({ event: "index-delivery-failed", node: this.nodeLabel(), error: String(error) }));
        return false;
      },
    );
    const timeout = new Promise<boolean>((resolve) => (timer = setTimeout(() => resolve(false), ms)));
    const done = await Promise.race([delivery, timeout]);
    clearTimeout(timer);
    return done;
  }

  /**
   * Sends outbox entries to their index shards, and deletes them once applied. Entries being sent
   * by another delivery are waited for, not sent twice.
   */
  private async deliver(ids: readonly number[]): Promise<void> {
    const mine = ids.filter((id) => !this.delivering.has(id));
    if (mine.length === 0) return this.waitDelivered(ids);
    for (const id of mine) this.delivering.add(id);
    try {
      const rows = mine.length ? this.store.exec<{ id: number; op: string }>(`SELECT id, op FROM ${OUTBOX} WHERE id IN (${mine.map(() => "?").join(", ")})`, ...mine).toArray() : [];
      if (rows.length) {
        const ops = rows.map((r) => JSON.parse(r.op) as IndexOp);
        await this.indexShards().call(ops.map((op) => op.route), "indexApply", ops);
        // Not fenced by a split: a copy drops the outbox it gets (see afterClone), so a delete
        // the copy misses changes nothing.
        this.store.exec(`DELETE FROM ${OUTBOX} WHERE id IN (${rows.map(() => "?").join(", ")})`, ...rows.map((r) => r.id));
      }
    } finally {
      for (const id of mine) this.delivering.delete(id);
    }
    await this.waitDelivered(ids);
  }

  /** Waits while another delivery sends some of these entries; throws if they are still in the outbox after. */
  private async waitDelivered(ids: readonly number[]): Promise<void> {
    while (ids.some((id) => this.delivering.has(id))) await new Promise((resolve) => setTimeout(resolve, 5));
    const left = this.store.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM ${OUTBOX} WHERE id IN (${ids.map(() => "?").join(", ")})`, ...ids).one().n;
    if (left > 0) throw new Error(`${left} index change(s) not delivered`);
  }

  /** Delivers what is left in the outbox. Call `super.onAlarm()`. */
  protected override async onAlarm(): Promise<void> {
    this.migrate();
    this.wakeAt = null;
    for (;;) {
      const ids = this.store.exec<{ id: number }>(`SELECT id FROM ${OUTBOX} ORDER BY id LIMIT ?`, DELIVERY_BATCH).toArray().map((r) => r.id);
      if (ids.length === 0) return;
      try {
        await this.deliver(ids);
      } catch (error) {
        console.error(JSON.stringify({ event: "index-delivery-failed", node: this.nodeLabel(), error: String(error) }));
        await this.wake(Date.now() + DELIVERY_RETRY_MS);
        return;
      }
    }
  }

  // ---- Splits ----

  /** The copy drops the outbox: the original goes on delivering all of it. Call `super.afterClone`. */
  protected override afterClone(owns: (key: string) => boolean): void {
    this.migrate();
    this.store.exec(`DELETE FROM ${OUTBOX}`);
    super.afterClone(owns);
  }

  /** Drops the fences of keys given away (never the outbox: see afterClone). Call `super.cleanupForeign`. */
  protected override cleanupForeign(owns: (key: string) => boolean, limit: number): number {
    this.migrate();
    const [lo, hi] = this.ownedHashRange();
    const deleted = this.store.exec(`DELETE FROM ${FENCES} WHERE rowid IN (SELECT rowid FROM ${FENCES} WHERE hash < ? OR hash > ? LIMIT ?)`, lo, hi, Math.min(limit, CLEANUP_BATCH)).rowsWritten;
    return deleted + super.cleanupForeign(owns, limit);
  }

  // ---- Storage ----

  protected override migrate(): void {
    super.migrate();
    if (this.indexMigrated) return;
    this.store.exec(`CREATE TABLE IF NOT EXISTS ${OUTBOX} (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, op TEXT NOT NULL, at INTEGER NOT NULL)`);
    this.store.exec(`CREATE TABLE IF NOT EXISTS ${FENCES} (key TEXT NOT NULL, route TEXT NOT NULL, entry TEXT NOT NULL, hash TEXT NOT NULL, token INTEGER NOT NULL, PRIMARY KEY (key, route, entry))`);
    this.store.exec(`CREATE INDEX IF NOT EXISTS ${FENCES}_hash ON ${FENCES} (hash)`);
    this.indexMigrated = true;
  }

  private fence(key: string, route: string, entry: string): number {
    return this.store.exec<{ token: number }>(`SELECT token FROM ${FENCES} WHERE key = ? AND route = ? AND entry = ?`, key, route, entry).toArray()[0]?.token ?? 0;
  }

  private raiseFence(key: string, route: string, entry: string, token: number): void {
    this.store.exec(`INSERT INTO ${FENCES} (key, route, entry, hash, token) VALUES (?, ?, ?, ?, ?) ON CONFLICT (key, route, entry) DO UPDATE SET token = max(token, excluded.token)`, key, route, entry, keyHash(key), token);
  }

  private uniqueIndexes(): GlobalIndexInfo[] {
    return globalIndexes(this.table).filter((ix) => ix.unique);
  }

  private async wake(time: number): Promise<void> {
    if (this.wakeAt !== null && this.wakeAt <= time && this.wakeAt > Date.now()) return;
    this.wakeAt = time;
    await this.scheduleAlarm(time);
  }

  private indexShards(): Untyped {
    if (!this.indexClient) {
      let ns = this.indexNodes() as DurableObjectNamespace<Rpc.DurableObjectBranded>;
      const jurisdiction = this.ctx.id.jurisdiction as DurableObjectJurisdiction | undefined;
      if (jurisdiction) ns = ns.jurisdiction(jurisdiction);
      const options = this.stubOptions();
      this.indexClient = sharded(ns, this.objectName(), options ? { stubOptions: options } : {}) as unknown as Untyped;
    }
    return this.indexClient;
  }

  private get store(): SqlStorage {
    return this.ctx.storage.sql;
  }
}
