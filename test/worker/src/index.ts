// Test Worker: counters keyed by name, on sharded objects. Routes:
//   POST /call {cls, name, method, keys, payloads}  POST /scan {cls, name, method}
//   POST /split {cls, name, bits}                    POST /describe {cls, name, bits}
//   POST /splits {cls, name}  (the splits this isolate's client knows)
//   POST /tree {cls, name, presplitHint?}  (every shard, as its objects describe themselves)
import { MAX_KNOWN_OBJECTS, RouteMismatch } from "../../../src/sharded/client.ts";
import { checkSplitPolicy, DEFAULT_SPLIT_POLICY, keyHash, shardObjectName, sharded, ShardedObject, type AtomicMethod, type SplitPolicy, type StubOptions, type UpdateResult } from "../../../src/sharded/index.ts";
import type { AtomicStep } from "../../../src/sharded/atomic.ts";
import { defineTable, ShardedTable, table, type ListQuery, type TableSchema } from "../../../src/table/index.ts";
import { GlobalIndex, IndexedTable, indexedTable, type IndexedListQuery } from "../../../src/indexed/index.ts";
import { entryOf, globalIndexes } from "../../../src/indexed/entries.ts";
import { storageReport, type StorageReport } from "./storage-report.ts";
import { graphRoute, type GraphEnv, type GraphRequest } from "./workgraph.ts";
export { GraphCoordinators, GraphResults, GraphRunners } from "./workgraph.ts";

interface Env extends GraphEnv {
  readonly COUNTERS: DurableObjectNamespace<Counters>;
  readonly AUTO_COUNTERS: DurableObjectNamespace<AutoCounters>;
  readonly PRESPLIT_COUNTERS: DurableObjectNamespace<PresplitCounters>;
  readonly ROUTED_COUNTERS: DurableObjectNamespace<RoutedCounters>;
  readonly ROUTED_DOCS: DurableObjectNamespace<RoutedDocs>;
  readonly DOCS: DurableObjectNamespace<Docs>;
  readonly EVOLVING: DurableObjectNamespace<EvolvingDocs>;
  readonly FLAKY: DurableObjectNamespace<FlakyDocs>;
  readonly OLD_ALARM: DurableObjectNamespace<OldAlarm>;
  readonly USERS: DurableObjectNamespace<Users>;
  readonly USER_INDEXES: DurableObjectNamespace<UserIndexes>;
  readonly EVOLVING_USERS: DurableObjectNamespace<EvolvingUsers>;
  readonly EVOLVING_USER_INDEXES: DurableObjectNamespace<EvolvingUserIndexes>;
  /** Set when deployed: every request carries it as a bearer token (the routes can delete data). */
  readonly TEST_TOKEN?: string;
}

// ---- A sharded table: documents, split by hand ----

export type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
export const docs = defineTable<Doc>({
  primaryKey: "name",
  columns: { name: "text", id: "text", createdAt: "integer", lastEditAt: "integer", deleted: "boolean", meta: "json" },
  indexes: { byCreated: ["createdAt"], byEdit: ["lastEditAt"], byDeletedCreated: ["deleted", "createdAt"] },
  counts: { byDeleted: "deleted" },
});

export class Docs extends ShardedTable<Env, Doc> {
  protected table = docs;
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6, holdWaitMs: 0 };
  /** Tests hold cleanup back, to list while shards still store rows they gave away. */
  protected override cleanupForeign(owns: (key: string) => boolean, limit: number): number {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_flags (paused INTEGER NOT NULL)");
    if (this.ctx.storage.sql.exec("SELECT paused FROM test_flags WHERE paused = 1").toArray().length) return 1; // not done: try again
    return super.cleanupForeign(owns, limit);
  }
  async pauseCleanup(paused: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_flags (paused INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_flags");
    this.ctx.storage.sql.exec("INSERT INTO test_flags (paused) VALUES (?)", paused ? 1 : 0);
  }
  /** Rows stored here, owned or not. */
  async storedRows(): Promise<number> {
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_table").one().n;
  }
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.DOCS as never;
  }
}

/** Git refs that can conflict (refs/heads/a and refs/heads/a/b) share their first 3 components. */
export const refRoute = (key: string): string => key.split("/").slice(0, 3).join("/");

/** Docs routed like refs: every doc under refs/x/y lives in one shard. */
export class RoutedDocs extends Docs {
  protected override routeOf(key: string): string {
    return refRoute(key);
  }
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.ROUTED_DOCS as never;
  }
}

// ---- An indexed table: users, unique by email, listed by team ----

export type User = { id: string; email: string | null; name: string; team: string; createdAt: number; active: boolean };
export const users = defineTable<User>({
  primaryKey: "id",
  columns: { id: "text", email: "text", name: "text", team: "text", createdAt: "integer", active: "boolean" },
  indexes: { byName: ["name"] },
  globalIndexes: {
    byEmail: { columns: ["email"], unique: true },
    byTeam: { columns: ["team", "createdAt"], partition: 1 },
  },
});

export class Users extends IndexedTable<Env, User> {
  protected table = users;
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected override indexWaitMs = 1_500;
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USERS as never;
  }
  protected indexNodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USER_INDEXES as never;
  }
  /** The keys with index changes not delivered yet, on this object, and whether it owns each. */
  async outboxKeys(): Promise<{ key: string; owned: boolean }[]> {
    return this.ctx.storage.sql.exec<{ key: string }>("SELECT DISTINCT key FROM indexed_outbox ORDER BY key").toArray().map(({ key }) => ({ key, owned: this.owns(key) }));
  }
  /** Index changes not delivered yet, on this object (owned or not). */
  async outboxSize(): Promise<number> {
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM indexed_outbox").one().n;
  }
  /** A custom write: renames users in one transaction (no unique column changes). */
  async rename(keys: readonly string[], payloads: readonly { name: string }[]): Promise<number[]> {
    return this.indexedTransact(keys, () => keys.map((key, i) => this.patchRow(key, { name: payloads[i]!.name }).version));
  }
  /** A custom write that tries to change a unique column: refused. */
  async setEmail(keys: readonly string[], payloads: readonly { email: string }[]): Promise<(string | undefined)[]> {
    return this.indexedTransact(keys, () => keys.map((key, i) => this.patchRow(key, { email: payloads[i]!.email }).refused));
  }
}

export class UserIndexes extends GlobalIndex<Env> {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected override reservationTimeoutMs = 1_000;
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USER_INDEXES as never;
  }
  protected rowNodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USERS as never;
  }
  /** While set, deliveries to this object fail (the flag is copied with the rest). */
  async failApply(fail: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_apply (fail INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_apply");
    this.ctx.storage.sql.exec("INSERT INTO test_apply (fail) VALUES (?)", fail ? 1 : 0);
  }
  override async indexApply(...args: Parameters<GlobalIndex<Env>["indexApply"]>): ReturnType<GlobalIndex<Env>["indexApply"]> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_apply (fail INTEGER NOT NULL)");
    if (this.ctx.storage.sql.exec("SELECT fail FROM test_apply WHERE fail = 1").toArray().length) throw new Error("test: apply failed");
    return super.indexApply(...args);
  }
}

/** Users listed by team, then (after `upgrade`) also unique by email: an index added to a table that has rows. */
const usersV1 = defineTable<User>({ primaryKey: "id", columns: users.columns, globalIndexes: { byTeam: { columns: ["team", "createdAt"], partition: 1 } } });
/** After a second upgrade: byEmail dropped, and byTeam redefined (ordered by name). */
export const usersV3 = defineTable<User>({ primaryKey: "id", columns: users.columns, globalIndexes: { byTeam: { columns: ["team", "name"], partition: 1 } } });

export class EvolvingUsers extends IndexedTable<Env, User> {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected get table(): TableSchema<User> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
    const version = this.ctx.storage.sql.exec<{ version: number }>("SELECT version FROM test_schema").toArray()[0]?.version ?? 1;
    return version === 3 ? usersV3 : version === 2 ? users : usersV1;
  }
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.EVOLVING_USERS as never;
  }
  protected indexNodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.EVOLVING_USER_INDEXES as never;
  }
  /** A deploy of the new schema: records it and restarts, so the next call migrates. */
  async upgrade(version = 2): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_schema");
    this.ctx.storage.sql.exec("INSERT INTO test_schema (version) VALUES (?)", version);
    await this.ctx.storage.sync();
    this.ctx.abort("upgrade");
  }
}

export class EvolvingUserIndexes extends GlobalIndex<Env> {
  // Small, so dropping an index takes several alarms.
  protected override dropBatch = 50;
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.EVOLVING_USER_INDEXES as never;
  }
  protected rowNodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.EVOLVING_USERS as never;
  }
}

/** The docs table, then (after `upgrade`) with a `stars` column and a `byStars` index instead of `byEdit`. */
type DocV2 = Doc & { stars: number | null };
const docsV2 = defineTable<DocV2>({
  primaryKey: "name",
  columns: { ...docs.columns, stars: "integer" },
  indexes: { byCreated: ["createdAt"], byStars: ["stars"], byDeletedCreated: ["deleted", "createdAt"] },
  // byDeleted dropped, and the same column counted again under a new name: counted from the rows stored.
  counts: { deletedAgain: "deleted", byStars: "stars" },
});

export class EvolvingDocs extends ShardedTable<Env, DocV2> {
  /** A new index is built over a few hundred rows a call, so tests see it take several. */
  protected override indexBuildRows = 300;
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected get table(): TableSchema<DocV2> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
    const version = this.ctx.storage.sql.exec<{ version: number }>("SELECT version FROM test_schema").toArray()[0]?.version ?? 1;
    return (version === 2 ? docsV2 : docs) as TableSchema<DocV2>;
  }
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.EVOLVING as never;
  }
  /** A deploy of the new schema: records it and restarts, so the next call migrates. */
  async upgrade(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_schema");
    this.ctx.storage.sql.exec("INSERT INTO test_schema (version) VALUES (2)");
    await this.ctx.storage.sync();
    this.ctx.abort("upgrade");
  }
  async indexNames(): Promise<string[]> {
    return this.ctx.storage.sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'sharded_table' AND name LIKE 'sharded_table_ix_%' ORDER BY name").toArray().map((r) => r.name);
  }
}

/**
 * EvolvingDocs whose split copies can be made to fail to claim (`failClones`), so a split is
 * abandoned after its copy was made, and with a short split timeout.
 */
export class FlakyDocs extends EvolvingDocs {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6, splitTimeoutMs: 1500 };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.FLAKY as never;
  }
  /** While set, a copy of this object refuses to be claimed (the flag is copied with the rest). */
  async failClones(fail: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_clone (fail INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_clone");
    this.ctx.storage.sql.exec("INSERT INTO test_clone (fail) VALUES (?)", fail ? 1 : 0);
  }
  protected override afterClone(owns: (key: string) => boolean): void {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_clone (fail INTEGER NOT NULL)");
    if (this.ctx.storage.sql.exec("SELECT fail FROM test_clone WHERE fail = 1").toArray().length) throw new Error("test: clone refused");
    super.afterClone(owns);
  }
  /** The next SQL statement containing `text` throws, once, as a storage error would. */
  async failStatementOnce(text: string): Promise<void> {
    const sql = this.ctx.storage.sql;
    const exec = sql.exec.bind(sql);
    sql.exec = ((query: string, ...bindings: unknown[]) => {
      if (!query.includes(text)) return exec(query, ...bindings);
      sql.exec = exec;
      throw new Error("test: storage error");
    }) as typeof sql.exec;
  }
  /** Asks for onAlarm in `ms`. */
  async wakeIn(ms: number): Promise<void> {
    await this.scheduleAlarm(Date.now() + ms);
  }
  /** Loses this object's alarm (as a crash between a write and its alarm would). */
  async dropAlarm(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }
  /** Fires this object's alarm now, as a snapshot's copied alarm would. */
  async alarmNow(): Promise<void> {
    await this.ctx.storage.setAlarm(Date.now());
  }
  /** The next onAlarm restarts this object partway through, once (as an eviction or a deploy would). */
  async crashAlarmOnce(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_crash (n INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("INSERT INTO test_crash (n) VALUES (1)");
  }
  /** A message of split attempt `epoch`'s row copy, arriving now: "accepted", or why it was refused. */
  async lateSchema(epoch: number): Promise<string> {
    try {
      await this.shardedReceiveSchema("", "", epoch, []);
      return "accepted";
    } catch (error) {
      return `refused: ${String(error)}`;
    }
  }
  protected override async onAlarm(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_crash (n INTEGER NOT NULL)");
    if (this.ctx.storage.sql.exec("SELECT n FROM test_crash").toArray().length) {
      this.ctx.storage.sql.exec("DELETE FROM test_crash");
      await this.ctx.storage.sync(); // as any I/O in onAlarm does: the writes so far are durable
      this.ctx.abort("test: restarted partway through onAlarm");
    }
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_alarm_ran (id TEXT NOT NULL)");
    this.ctx.storage.sql.exec("INSERT INTO test_alarm_ran (id) VALUES (?)", this.ctx.id.toString());
  }
  /** The objects whose onAlarm wrote to this object's storage (it is copied with the rest). */
  async alarmRan(): Promise<{ self: string; ran: string[] }> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_alarm_ran (id TEXT NOT NULL)");
    return { self: this.ctx.id.toString(), ran: this.ctx.storage.sql.exec<{ id: string }>("SELECT id FROM test_alarm_ran").toArray().map((r) => r.id) };
  }
  /** While set, this object refuses to be activated (the flag is copied with the rest). */
  async failActivations(fail: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_activate (fail INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_activate");
    this.ctx.storage.sql.exec("INSERT INTO test_activate (fail) VALUES (?)", fail ? 1 : 0);
  }
  override async shardedActivate(name: string, bits: string, epoch: number): Promise<boolean> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_activate (fail INTEGER NOT NULL)");
    if (this.ctx.storage.sql.exec("SELECT fail FROM test_activate WHERE fail = 1").toArray().length) throw new Error("test: activation failed");
    return super.shardedActivate(name, bits, epoch);
  }
}

/** Overrides alarm() the way the docs once said to: refused. */
export class OldAlarm extends ShardedObject<Env, number> {
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.OLD_ALARM as never;
  }
  async get(keys: readonly string[], _payloads: null): Promise<null[]> {
    return keys.map(() => null);
  }
  override async alarm(): Promise<void> {
    await super.alarm();
  }
}

class CounterBase extends ShardedObject<Env, number> {
  /** What this object holds (tables, keys, alarm): to check a wipe left nothing. */
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }

  /** Loses this shard's alarm, as a failed setAlarm or an alarm the runtime gave up on would. */
  async loseAlarm(keys: readonly string[], _payloads: null): Promise<null[]> {
    await this.ctx.storage.deleteAlarm();
    return keys.map(() => null);
  }

  /** The stub options this shard would make its stubs with now (see `stubOptions`). */
  async seenStubOptions(keys: readonly string[], _payloads: null): Promise<unknown[]> {
    return keys.map(() => this.stubOptions());
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // A subclass table: which keys were added to here (a stand-in for an outbox).
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS history (key TEXT PRIMARY KEY)");
    // A subclass table that both shards clean after a split (a stand-in for a listing).
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS listing (key TEXT PRIMARY KEY)");
    // transferSlow: transfer, whose gather takes `ms` (its call's argument): a hold may appear meanwhile.
    this.atomic = {
      ...this.atomic,
      transferSlow: {
        ...this.atomic["transfer"]!,
        gather: async (_keys: readonly string[], _payloads: readonly unknown[] | null, ms: number) => {
          this.gathers++;
          await new Promise((resolve) => setTimeout(resolve, ms));
          return null;
        },
      } as never,
    };
  }

  protected override cleanupForeign(owns: (key: string) => boolean, limit: number): number {
    const foreign = this.ctx.storage.sql
      .exec<{ key: string }>("SELECT key FROM listing")
      .toArray()
      .filter(({ key }) => !owns(key))
      .slice(0, limit);
    for (const { key } of foreign) this.ctx.storage.sql.exec("DELETE FROM listing WHERE key = ?", key);
    return foreign.length;
  }

  /** Every key of this shard, read with ownedRowsPage `limit` at a time. */
  async pagedKeys(limit: number): Promise<string[]> {
    const keys: string[] = [];
    for (let page = this.ownedRowsPage(null, limit); page.length > 0; page = this.ownedRowsPage(page.at(-1)![0], limit)) keys.push(...page.map(([k]) => k));
    return keys;
  }

  /** The keys in this object's listing table, and whether this shard owns each. */
  async listing(): Promise<{ key: string; owned: boolean }[]> {
    return this.ctx.storage.sql
      .exec<{ key: string }>("SELECT key FROM listing")
      .toArray()
      .map(({ key }) => ({ key, owned: this.owns(key) }));
  }

  protected override afterClone(owns: (key: string) => boolean): void {
    for (const { key } of this.ctx.storage.sql.exec<{ key: string }>("SELECT key FROM history").toArray()) if (!owns(key)) this.ctx.storage.sql.exec("DELETE FROM history WHERE key = ?", key);
  }

  /** The keys in this object's history table, and whether this shard owns each. */
  async history(): Promise<{ key: string; owned: boolean }[]> {
    const owned = this.allRows();
    return this.ctx.storage.sql
      .exec<{ key: string }>("SELECT key FROM history")
      .toArray()
      .map(({ key }) => ({ key, owned: owned.has(key) }));
  }

  protected nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.COUNTERS as never;
  }

  /**
   * RFD0008: moves amounts between counters, all or nothing; a counter may not go below zero.
   * `gather` counts its calls (in memory), to see it runs before the check.
   */
  protected override atomic: Record<string, AtomicMethod<number, any, any, any, any>> = {
    transfer: {
      gather: async () => {
        this.gathers++;
        return null;
      },
      check: (current: ReadonlyMap<string, number>, keys: readonly string[], deltas: readonly unknown[] | null) => {
        const after = keys.map((k, i) => (current.get(k) ?? 0) + (deltas![i] as number));
        const low = keys.filter((_, i) => after[i]! < 0);
        if (low.length > 0) return { refused: `below zero: ${low.join(", ")}` };
        return { staged: keys.map((k, i) => [k, after[i]!] as [string, number]) };
      },
      apply: (_current: ReadonlyMap<string, number>, staged: [string, number][]) => ({ writes: new Map(staged), result: staged.map(([, v]) => v) }),
    } satisfies AtomicMethod<number, null, [string, number][], string, number[]>,
  };
  private gathers = 0;

  /** This object's atomic holds and outcomes (RFD0008), as stored. */
  async atomicTables(): Promise<{ holds: { tx: string; keys: string; deadline: number }[]; outcomes: { tx: string; outcome: string }[]; gathers: number }> {
    const has = (t: string) => this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = ?", t).one().n > 0;
    return {
      holds: has("sharded_atomic_holds") ? this.ctx.storage.sql.exec<{ tx: string; keys: string; deadline: number }>("SELECT tx, keys, deadline FROM sharded_atomic_holds").toArray() : [],
      outcomes: has("sharded_atomic_outcomes") ? this.ctx.storage.sql.exec<{ tx: string; outcome: string }>("SELECT tx, outcome FROM sharded_atomic_outcomes").toArray() : [],
      gathers: this.gathers,
    };
  }

  async get(keys: readonly string[], _payloads: null): Promise<(number | null)[]> {
    const rows = await this.read(keys);
    return keys.map((k) => rows.get(k) ?? null);
  }

  /** Adds each payload to its key's counter; returns the new values. */
  async add(keys: readonly string[], payloads: readonly number[]): Promise<number[]> {
    return this.update(keys, (current): UpdateResult<number, number[]> => {
      for (const k of keys) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO history (key) VALUES (?)", k);
      for (const k of keys) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO listing (key) VALUES (?)", k);
      const writes = new Map<string, number>();
      const result = keys.map((k, i) => {
        const next = (writes.get(k) ?? current.get(k) ?? 0) + payloads[i]!;
        writes.set(k, next);
        return next;
      });
      return { writes, result };
    });
  }

  /** Per key: sets `new` if the value is `old` (null: absent); true if it did. */
  async cas(keys: readonly string[], payloads: readonly { old: number | null; new: number | null }[]): Promise<boolean[]> {
    return this.update(keys, (current) => {
      const writes = new Map<string, number | null>();
      const result = keys.map((k, i) => {
        const now = writes.has(k) ? writes.get(k)! : (current.get(k) ?? null);
        if (now !== payloads[i]!.old) return false;
        writes.set(k, payloads[i]!.new);
        return true;
      });
      return { writes, result };
    });
  }

  /** Writes a key it did not list (payloads[0]): refused. */
  async sneak(keys: readonly string[], payloads: readonly string[]): Promise<number> {
    return this.update(keys, () => ({ writes: new Map([[payloads[0]!, 1]]), result: 1 }));
  }

  /** Sets each key to its payload (any JSON, for large rows). */
  async put(keys: readonly string[], payloads: readonly unknown[]): Promise<number> {
    return this.update(keys, () => ({ writes: new Map(keys.map((k, i) => [k, payloads[i] as number])), result: keys.length }));
  }

  /** A scan that writes: adds `by` to every counter this shard owns; how many it changed. */
  async bumpAll(by: number): Promise<number> {
    const keys = [...this.allRows().keys()];
    if (keys.length === 0) return 0;
    return this.update(keys, (current) => ({ writes: new Map(keys.map((k) => [k, (current.get(k) ?? 0) + by])), result: keys.length }));
  }

  /** Deletes these keys. */
  async remove(keys: readonly string[], _payloads: null): Promise<number> {
    return this.update(keys, () => ({ writes: new Map(keys.map((k) => [k, null])), result: keys.length }));
  }

  /** Makes this shard's next split slow to start copying (beforeHandOut waits `ms`). */
  async slowHandOut(ms: number): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_slow (ms INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_slow");
    this.ctx.storage.sql.exec("INSERT INTO test_slow (ms) VALUES (?)", ms);
  }

  protected override async beforeHandOut(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_slow (ms INTEGER NOT NULL)");
    const ms = this.ctx.storage.sql.exec<{ ms: number }>("SELECT ms FROM test_slow").toArray()[0]?.ms ?? 0;
    if (ms > 0) await new Promise((resolve) => setTimeout(resolve, ms));
  }

  /** Schedules work on this key's shard, due in `delayMs` (a stand-in for an outbox retry). */
  async scheduleWork(keys: readonly string[], payloads: readonly number[]): Promise<null[]> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS work (key TEXT PRIMARY KEY, due INTEGER NOT NULL, fired INTEGER)");
    const due = Date.now() + payloads[0]!;
    await this.transact(keys, () => this.ctx.storage.sql.exec("INSERT OR REPLACE INTO work (key, due, fired) VALUES (?, ?, NULL)", keys[0]!, due));
    await this.scheduleAlarm(due);
    return keys.map(() => null);
  }

  protected override async onAlarm(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS work (key TEXT PRIMARY KEY, due INTEGER NOT NULL, fired INTEGER)");
    // Both shards of a split hold the work table: each does the work of its own keys.
    for (const { key } of this.ctx.storage.sql.exec<{ key: string }>("SELECT key FROM work WHERE fired IS NULL AND due <= ?", Date.now()).toArray()) {
      if (this.owns(key)) this.ctx.storage.sql.exec("UPDATE work SET fired = ? WHERE key = ?", Date.now(), key);
    }
    // A shard with recurring work asks for its next turn much later; that must not hold back splits.
    const recurring = this.ctx.storage.sql.exec("SELECT 1 FROM work WHERE key = '__recurring'").toArray().length > 0;
    if (recurring) await this.scheduleAlarm(Date.now() + 60_000);
  }

  /** Work this shard has done. */
  async workFired(): Promise<string[]> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS work (key TEXT PRIMARY KEY, due INTEGER NOT NULL, fired INTEGER)");
    return this.ctx.storage.sql.exec<{ key: string }>("SELECT key FROM work WHERE fired IS NOT NULL").toArray().map((r) => r.key);
  }

  /** Adds a full-text index of these keys to this shard (an FTS5 table, written in a transaction). */
  async index(keys: readonly string[], payloads: readonly string[]): Promise<null[]> {
    this.ctx.storage.sql.exec("CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(key, body)");
    await this.transact(keys, () => keys.forEach((k, i) => this.ctx.storage.sql.exec("INSERT INTO search (key, body) VALUES (?, ?)", k, payloads[i]!)));
    return keys.map(() => null);
  }

  /** Notes on keys, in a table whose name looks like a runtime's to a careless LIKE ("_cf_%"). */
  async note(keys: readonly string[], payloads: readonly string[]): Promise<null[]> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS scf_notes (key TEXT PRIMARY KEY, note TEXT NOT NULL)");
    await this.transact(keys, () => keys.forEach((k, i) => this.ctx.storage.sql.exec("INSERT OR REPLACE INTO scf_notes (key, note) VALUES (?, ?)", k, payloads[i]!)));
    return keys.map(() => null);
  }

  /** The notes of the keys this shard owns. */
  async notes(): Promise<[string, string][]> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS scf_notes (key TEXT PRIMARY KEY, note TEXT NOT NULL)");
    return this.ctx.storage.sql
      .exec<{ key: string; note: string }>("SELECT key, note FROM scf_notes")
      .toArray()
      .filter((r) => this.owns(r.key))
      .map((r): [string, string] => [r.key, r.note]);
  }

  /** Keys this shard owns whose text matches. */
  async search(query: string): Promise<string[]> {
    return this.ctx.storage.sql
      .exec<{ key: string }>("SELECT key FROM search WHERE search MATCH ?", query)
      .toArray()
      .map((r) => r.key)
      .filter((k) => this.owns(k));
  }

  /** Deletes this object's storage and restarts it, as if it had never been used. */
  async wipe(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
    await this.ctx.storage.sync(); // durable before the restart, which drops what is not
    this.ctx.abort("wiped");
  }

  /** Every row's key and the hash it is stored under. */
  async rowHashes(): Promise<{ key: string; hash: string }[]> {
    return this.ctx.storage.sql.exec<{ key: string; hash: string }>("SELECT key, hash FROM sharded_rows ORDER BY key").toArray();
  }

  async entries(): Promise<[string, number][]> {
    return [...this.allRows()];
  }
}

export class Counters extends CounterBase {
  // Writes to a half being copied answer "not ready" at once, so every split test also covers
  // the client backing off and retrying.
  protected override splitPolicy: SplitPolicy = { ...DEFAULT_SPLIT_POLICY, holdWaitMs: 0 };
}

export class AutoCounters extends CounterBase {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 20, windowSeconds: 2, maxDepth: 3 };
  /** A shard holding only the key "pinned" does not split: its load is that one key. */
  protected override splittable(): boolean {
    const rows = this.ownedRowsPage(null, 2);
    return !(rows.length === 1 && rows[0]![0] === "pinned");
  }
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.AUTO_COUNTERS as never;
  }
}

/** Counters routed like git refs, with refs created the way git refuses a file/folder conflict. */
export class RoutedCounters extends CounterBase {
  protected override splitPolicy: SplitPolicy = { ...DEFAULT_SPLIT_POLICY, holdWaitMs: 0 };
  protected override routeOf(key: string): string {
    return refRoute(key);
  }
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.ROUTED_COUNTERS as never;
  }
  /**
   * Creates each ref unless one exists that it would be a file or folder of: refs/heads/a and
   * refs/heads/a/b can't both exist. One transaction per ref, over the ref and its folders (each
   * of at least 3 components, so in its route), and a look at what is below it, all in this shard.
   */
  async createRef(keys: readonly string[], payloads: readonly number[]): Promise<("created" | "conflict")[]> {
    const out: ("created" | "conflict")[] = [];
    for (const [i, key] of keys.entries()) {
      const parts = key.split("/");
      const folders = parts.slice(3).map((_, j) => parts.slice(0, 3 + j).join("/"));
      out.push(
        await this.update([key, ...folders], (current) => {
          // Keys under `key/` sort between "key/" and "key0" ("0" follows "/").
          const below = this.ctx.storage.sql.exec("SELECT key FROM sharded_rows WHERE key > ? AND key < ? LIMIT 1", `${key}/`, `${key}0`).toArray();
          if (current.size > 0 || below.length > 0) return { writes: new Map(), result: "conflict" as const };
          return { writes: new Map([[key, payloads[i]!]]), result: "created" as const };
        }),
      );
    }
    return out;
  }
}

/** Splits to depth 2 while empty, never by load. */
export class PresplitCounters extends CounterBase {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 4, initialDepth: 2 };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.PRESPLIT_COUNTERS as never;
  }
}

const keptClients = new Map<string, ReturnType<typeof sharded<CounterBase>>>();

function namespace(env: Env, cls: string): DurableObjectNamespace<CounterBase> {
  return (cls === "users" ? env.USERS : cls === "userIndexes" ? env.USER_INDEXES : cls === "auto" ? env.AUTO_COUNTERS : cls === "presplit" ? env.PRESPLIT_COUNTERS : cls === "routed" ? env.ROUTED_COUNTERS : cls === "routedTable" ? env.ROUTED_DOCS : cls === "table" ? env.DOCS : cls === "evolving" ? env.EVOLVING : cls === "flaky" ? env.FLAKY : cls === "oldAlarm" ? env.OLD_ALARM : env.COUNTERS) as never;
}

/** POST /indexed {name, op, args}: one call of the indexed table client, or a test hook. */
async function indexedOp(env: Env, body: { name: string; op: string; args: unknown[]; cls?: "evolving"; version?: number }): Promise<unknown> {
  // "evolving": the table that gains its unique index after `upgrade` (args: the shards' bits),
  // and (`version` 3) drops it and redefines the other. `version` is also the client's schema.
  if (body.cls === "evolving" && body.op === "upgrade") {
    for (const bits of body.args as string[]) await env.EVOLVING_USERS.get(env.EVOLVING_USERS.idFromName(shardObjectName(body.name, bits))).upgrade(body.version ?? 2).catch(() => {});
    return null;
  }
  const [rowNs, indexNs] = body.cls === "evolving" ? [env.EVOLVING_USERS as unknown as typeof env.USERS, env.EVOLVING_USER_INDEXES as unknown as typeof env.USER_INDEXES] : [env.USERS, env.USER_INDEXES];
  const t = indexedTable(rowNs, indexNs, body.name, body.version === 3 ? usersV3 : users);
  const rows = sharded(rowNs, body.name);
  const indexes = sharded(indexNs, body.name);
  const untyped = (client: unknown) => client as { call(keys: readonly string[], method: string, payloads: readonly unknown[] | null): Promise<{ positions: number[]; result: unknown[] }[]>; scan(method: string, ...a: unknown[]): Promise<unknown[]> };
  const flat = async (groups: Promise<{ positions: number[]; result: unknown[] }[]>) => {
    const out: unknown[] = [];
    for (const g of await groups) g.positions.forEach((p, i) => (out[p] = g.result[i]));
    return out;
  };
  switch (body.op) {
    case "withRouteOf":
      try {
        indexedTable(env.USERS, env.USER_INDEXES, body.name, users, { routeOf: refRoute });
        return "made";
      } catch (error) {
        return (error as Error).message;
      }
    case "all": {
      const out: unknown[] = [];
      for await (const row of t.all(body.args[0] as ListQuery)) out.push(row);
      return out;
    }
    case "listAll": {
      // Every page of a listing through a global index.
      const out: unknown[] = [];
      let cursor: string | null = null;
      do {
        const page = await t.list({ ...(body.args[0] as IndexedListQuery), cursor });
        out.push(...page.rows);
        cursor = page.cursor;
      } while (cursor !== null);
      return out;
    }
    case "rawReserve": // [routes, payloads]
      return flat(untyped(indexes).call(body.args[0] as string[], "indexReserve", body.args[1] as unknown[]));
    case "rawPut": // [keys, payloads]
      return flat(untyped(rows).call(body.args[0] as string[], "tablePut", body.args[1] as unknown[]));
    case "rawCall": // [method, keys, payloads]: a custom method of Users
      return flat(untyped(rows).call(body.args[1] as string[], body.args[0] as string, body.args[2] as unknown[]));
    case "indexStats":
      return untyped(indexes).scan("indexStats");
    case "lateOldPut": { // [row]: a put of byEmail (the v2 schema's), as a row shard of an earlier deploy would send it
      const ix = globalIndexes(users).find((g) => g.name === "byEmail")!;
      const row = body.args[0] as User;
      const e = entryOf(users, ix, row)!;
      return flat(untyped(indexes).call([e.route], "indexApply", [{ kind: "put", route: e.route, entry: e.entry, owner: row.id, version: 99, row, token: null }]));
    }
    case "outboxKeys":
      return untyped(rows).scan("outboxKeys");
    case "outbox":
      return (await untyped(rows).scan("outboxSize")).reduce((a: number, b) => a + (b as number), 0);
    case "split": // [side, bits]
      return (body.args[0] === "rows" ? rowNs : indexNs).get((body.args[0] === "rows" ? rowNs : indexNs).idFromName(shardObjectName(body.name, body.args[1] as string))).shardedSplit(body.name, body.args[1] as string);
    case "failApply": // [fail, bits]
      return env.USER_INDEXES.get(env.USER_INDEXES.idFromName(shardObjectName(body.name, (body.args[1] as string) ?? ""))).failApply(body.args[0] as boolean);
  }
  try {
    return { ok: await (t as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>)[body.op]!.call(t, ...body.args) };
  } catch (error) {
    const e = error as Error & { key?: string; version?: number; index?: string };
    return { error: e.name, message: e.message, ...(e.version === undefined ? {} : { version: e.version }), ...(e.index === undefined ? {} : { index: e.index }) };
  }
}

/** POST /table {cls, name, op, args}: one call of the table client. */
async function tableOp(env: Env, body: { cls: string; name: string; op: string; args: unknown[] }): Promise<unknown> {
  const t = table(namespace(env, body.cls) as never, body.name, (body.cls === "evolving" || body.cls === "flaky" ? docsV2 : docs) as never, body.cls === "routedTable" ? { routeOf: refRoute } : {}) as unknown as Record<string, (...a: unknown[]) => unknown>;
  if (body.op === "all") {
    const rows: unknown[] = [];
    for await (const row of (t.all as (q: ListQuery) => AsyncGenerator<unknown>).call(t, body.args[0] as ListQuery)) rows.push(row);
    return rows;
  }
  if (body.op === "upgrade") {
    const ns = namespace(env, body.cls) as unknown as DurableObjectNamespace<EvolvingDocs>;
    await ns.get(ns.idFromName(shardObjectName(body.name, ""))).upgrade().catch(() => {});
    return null;
  }
  if (body.op === "failActivations") {
    const ns = env.FLAKY;
    await ns.get(ns.idFromName(shardObjectName(body.name, body.args[1] as string))).failActivations(body.args[0] as boolean);
    return null;
  }
  if (body.op === "wakeIn" || body.op === "failStatementOnce" || body.op === "alarmNow" || body.op === "alarmRan" || body.op === "dropAlarm" || body.op === "crashAlarmOnce" || body.op === "lateSchema") {
    const ns = env.FLAKY;
    const stub = ns.get(ns.idFromName(shardObjectName(body.name, (body.args[1] as string) ?? ""))) as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
    return stub[body.op]!(body.args[0]);
  }
  if (body.op === "failClones") {
    const ns = env.FLAKY;
    await ns.get(ns.idFromName(shardObjectName(body.name, ""))).failClones(body.args[0] as boolean);
    return null;
  }
  if (body.op === "indexNames") {
    const ns = env.EVOLVING;
    return ns.get(ns.idFromName(shardObjectName(body.name, ""))).indexNames();
  }
  if (body.op === "scan") return (sharded(namespace(env, body.cls) as never, body.name, body.cls === "routedTable" ? { routeOf: refRoute } : {}).scan as (m: string, ...a: unknown[]) => Promise<unknown>)(...(body.args as [string, ...unknown[]]));
  return t[body.op]!.call(t, ...body.args);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (env.TEST_TOKEN && request.headers.get("authorization") !== `Bearer ${env.TEST_TOKEN}`) return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    const body = (await request.json().catch(() => ({}))) as { cls: string; name: string; method: string; keys: string[]; payloads: unknown[] | null; bits: string; args?: unknown[]; jurisdiction?: DurableObjectJurisdiction; stubOptions?: StubOptions };
    // In a jurisdiction, if asked: the same names there are other objects.
    const ns = body.jurisdiction ? namespace(env, body.cls).jurisdiction(body.jurisdiction) : namespace(env, body.cls);
    try {
      switch (url.pathname) {
        case "/call": {
          const wait = (body as { notReadyWaitMs?: number }).notReadyWaitMs;
          const hint = (body as { presplitHint?: number }).presplitHint;
          const routed = (body as { routeOf?: "refs" }).routeOf === "refs";
          const options = { ...(wait === undefined ? {} : { notReadyWaitMs: wait }), ...(body.stubOptions ? { stubOptions: body.stubOptions } : {}), ...(hint === undefined ? {} : { presplitHint: hint }), ...(routed ? { routeOf: refRoute } : {}) };
          const results = await sharded(ns, body.name, options).call(body.keys, body.method as "get", (body.payloads ?? null) as null);
          // Back in the caller's order.
          const out: unknown[] = new Array(body.keys.length);
          for (const shard of results) shard.positions.forEach((p, j) => (out[p] = (shard.result as unknown[])[j]));
          return Response.json(out);
        }
        case "/callKept": {
          // A client kept by the isolate and used by one request after another (an app's mistake:
          // a client belongs to one request). Its kept stubs are the first request's.
          let client = keptClients.get(body.name);
          if (!client) keptClients.set(body.name, (client = sharded(ns, body.name)));
          const results = await client.call(body.keys, body.method as "get", (body.payloads ?? null) as null);
          return Response.json(results.flatMap((r) => r.result as unknown[]));
        }
        case "/callOne": {
          const routed = (body as { routeOf?: "refs" }).routeOf === "refs";
          return Response.json(await sharded(ns, body.name, routed ? { routeOf: refRoute } : {}).callOneShard(body.keys, body.method as "get", (body.payloads ?? null) as null));
        }
        case "/checkPolicy":
          try {
            checkSplitPolicy((body as unknown as { policy: SplitPolicy }).policy);
            return Response.json(null);
          } catch (error) {
            return Response.json(String(error));
          }
        case "/scan":
          return Response.json(await (sharded(ns, body.name).scan as (m: string, ...a: unknown[]) => Promise<unknown>)(body.method, ...((body.args as unknown[]) ?? [])));
        case "/split":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedSplit(body.name, body.bits));
        case "/atomically": {
          const routed = (body as { routeOf?: "refs" }).routeOf === "refs";
          return Response.json(await sharded(ns, body.name, routed ? { routeOf: refRoute } : {}).atomically(body.keys, body.method, body.payloads ?? null, ...((body.args as unknown[]) ?? [])));
        }
        case "/atomicStep":
          // One step of the protocol, as a client would send it: a caller that stops partway.
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedAtomic(body.name, body.bits, (body as unknown as { step: AtomicStep }).step, body.keys, body.payloads ?? null, []));
        case "/atomicTables":
          return Response.json(await (ns.get(ns.idFromName(shardObjectName(body.name, body.bits))) as unknown as { atomicTables(): Promise<unknown> }).atomicTables());
        case "/describe":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedDescribe());
        case "/workgraph":
          return Response.json(await graphRoute(env, body as unknown as GraphRequest));
        case "/indexed":
          return Response.json((await indexedOp(env, body as never)) ?? null);
        case "/table":
          return Response.json((await tableOp(env, body as never)) ?? null);
        case "/wipeAll":
          return Response.json(await sharded(ns, body.name).wipe());
        case "/storage":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).storage());
        case "/wipe":
          await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).wipe().catch(() => {});
          return Response.json(null);
        case "/useManyObjects": {
          // Clients of many other objects, each used once: the isolate keeps at most MAX_KNOWN_OBJECTS.
          for (let i = 0; i <= MAX_KNOWN_OBJECTS; i++) sharded(ns, `${body.name}-other-${i}`).knownSplits();
          return Response.json(MAX_KNOWN_OBJECTS);
        }
        case "/lateBatch": {
          // A batch of a logical copy that arrives late, setting `keys[0]` to `payloads[0]`.
          const b = body as unknown as { bits: string; keys: string[]; payloads: unknown[]; epoch: number };
          const stub = ns.get(ns.idFromName(shardObjectName(body.name, b.bits))) as unknown as { shardedReceive(...a: unknown[]): Promise<void> };
          await stub.shardedReceive(body.name, b.bits, b.epoch, false, "sharded_rows", ["key", "hash", "value"], [[b.keys[0], keyHash(b.keys[0]!), JSON.stringify(b.payloads[0])]]);
          return Response.json(null);
        }
        case "/slowHandOut":
          await (ns.get(ns.idFromName(shardObjectName(body.name, body.bits))) as unknown as { slowHandOut(ms: number): Promise<void> }).slowHandOut((body as unknown as { ms: number }).ms);
          return Response.json(null);
        case "/splits":
          return Response.json(sharded(ns, body.name).knownSplits());
        case "/presplit":
          return Response.json(sharded(ns, body.name).knownPresplit());
        case "/tree": {
          const hint = (body as { presplitHint?: number }).presplitHint;
          return Response.json(await sharded(ns, body.name, hint === undefined ? {} : { presplitHint: hint }).describeTree());
        }
      }
      return new Response("not found", { status: 404 });
    } catch (error) {
      if (error instanceof RouteMismatch) return Response.json({ error: error.name, key: error.key }, { status: 409 });
      return new Response(String(error instanceof Error ? (error.stack ?? error.message) : error), { status: 500 });
    }
  },
};
