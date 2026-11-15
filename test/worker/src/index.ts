// Test Worker: counters keyed by name, on sharded objects. Routes:
//   POST /call {cls, name, method, keys, payloads}  POST /scan {cls, name, method}
//   POST /split {cls, name, bits}                    POST /describe {cls, name, bits}
//   POST /splits {cls, name}  (the splits this isolate's client knows)
//   POST /tree {cls, name, presplitHint?}  (every shard, as its objects describe themselves)
import { MAX_KNOWN_OBJECTS, RouteMismatch } from "../../../src/sharded/client.ts";
import { checkSplitPolicy, DEFAULT_SPLIT_POLICY, keyHash, shardObjectName, sharded, ShardedObject, type AtomicMethod, type CallStats, type SplitPolicy, type StubOptions, type UpdateResult } from "../../../src/sharded/index.ts";
import type { AtomicStep } from "../../../src/sharded/atomic.ts";
import { defineCommands, defineTable, ShardedTable, table, TableClient, type Command, type Decision, type ListQuery, type TableSchema } from "../../../src/table/index.ts";
import type { ShardPage, ShardPageRequest } from "../../../src/table/schema.ts";
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
  readonly DOCS_DEFAULT_HOLD: DurableObjectNamespace<DocsDefaultHold>;
  readonly PRESPLIT_DOCS: DurableObjectNamespace<PresplitDocs>;
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
  version: 1,
  primaryKey: "name",
  columns: { name: "text", id: "text", createdAt: "integer", lastEditAt: "integer", deleted: "boolean", meta: "json" },
  indexes: { byCreated: ["createdAt"], byEdit: ["lastEditAt"], byDeletedCreated: ["deleted", "createdAt"] },
});

/** Commands of the docs table, for `apply`: each typed by its own arguments and result. */
export const docCommands = defineCommands<Doc>()({
  /** Adds `by` to the row's lastEditAt; nothing for an absent row. */
  bump: (current, args: { by: number }): Decision<Doc, number | null> => {
    if (!current) return { result: null };
    const lastEditAt = (current.row.lastEditAt ?? 0) + args.by;
    return { result: lastEditAt, write: { ...current.row, lastEditAt } };
  },
  /** Creates the row if absent; a no-op otherwise. */
  ensure: (current, args: { row: Doc }): Decision<Doc, "created" | "exists"> => (current ? { result: "exists" } : { result: "created", write: args.row }),
  /** Deletes the row: whether there was one. */
  remove: (current, _args: null): Decision<Doc, boolean> => ({ result: current !== null, write: "delete" }),
  /** Throws after deciding a write: nothing may be written. */
  boom: (current, _args: null): Decision<Doc, never> => {
    if (current) current.row.deleted = true;
    throw new Error("test: the command failed");
  },
  /** Not synchronous: refused. */
  later: ((_current: unknown, _args: null) => Promise.resolve({ result: 1 })) as unknown as Command<Doc, null, number>,
});

/** Compile-time checks of `apply`'s types (never called). */
export async function applyTypes(t: TableClient<Doc, typeof docCommands>): Promise<void> {
  const bumped = await t.apply("a", "bump", { by: 1 });
  if (bumped.ok) {
    const n: number | null = bumped.result;
    void n;
  }
  // @ts-expect-error: bump takes { by: number }
  await t.apply("a", "bump", { by: "1" });
  // @ts-expect-error: no such command
  await t.apply("a", "nope", null);
  const ensured = await t.apply("a", "ensure", { row: { name: "a", id: "a", createdAt: 0, lastEditAt: null, deleted: false, meta: null } });
  // @ts-expect-error: ensure answers "created" | "exists", not a number
  if (ensured.ok) ensured.result satisfies number;
}

export class Docs extends ShardedTable<Env, Doc> {
  /** This shard's rows, from its counts: the tests' `count` op sums them over the shards. */
  async testCount(): Promise<number> {
    return this.ownedRowCount();
  }
  protected table = docs;
  /** The logical-copy batches this instance took: their table, rows, and bytes on the heap (2 a UTF-16 unit). */
  private batches: { table: string; rows: number; heapBytes: number }[] = [];
  override async shardedReceive(name: string, bits: string, epoch: number, first: boolean, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void> {
    const heapBytes = rows.reduce((sum, row) => sum + row.reduce<number>((n, v) => n + (typeof v === "string" ? 2 * v.length : v instanceof ArrayBuffer ? v.byteLength : 8), 0), 0);
    this.batches.push({ table, rows: rows.length, heapBytes });
    return super.shardedReceive(name, bits, epoch, first, table, columns, rows);
  }
  async receivedBatches(): Promise<{ table: string; rows: number; heapBytes: number }[]> {
    return this.batches;
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    usePolicyOverride(ctx, () => this.splitPolicy, (p) => (this.splitPolicy = p));
  }
  /** Changes this object's split policy (merged over the class's), as a deploy would; kept across restarts. */
  async setPolicy(change: Record<string, unknown>): Promise<void> {
    savePolicyOverride(this.ctx.storage, change);
    this.splitPolicy = { ...this.splitPolicy, ...change } as SplitPolicy;
  }
  /** This object's database size, as the runtime counts it. */
  /** This object's tables and their rows, its keys and its alarm. */
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }
  async databaseSize(): Promise<number> {
    return this.ctx.storage.sql.databaseSize;
  }

  protected override commands = docCommands;
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6, holdWaitMs: 0 };
  async sqlRun(statements: readonly { query: string; bindings?: readonly unknown[] }[]): Promise<unknown[][]> {
    return sqlRun(this.ctx.storage, statements);
  }
  /** When this object's alarm is set for (null: none). */
  async alarmAt(): Promise<number | null> {
    return this.ctx.storage.getAlarm();
  }
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
  /** Makes this shard's next split slow to start copying, writes to the moving half held meanwhile. */
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
  /** Page calls this instance answered: what a listing costs. */
  private pages = 0;
  /** Each page call waits this long first (in memory): a slow shard. */
  private pageDelayMs = 0;
  override async tablePage(request: ShardPageRequest): Promise<ShardPage> {
    this.pages++;
    if (this.pageDelayMs > 0) await new Promise((resolve) => setTimeout(resolve, this.pageDelayMs));
    return super.tablePage(request);
  }
  async slowPages(ms: number): Promise<void> {
    this.pageDelayMs = ms;
  }
  async pageCalls(): Promise<number> {
    return this.pages;
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

/** The docs table with the default hold wait: a write held by a split waits for it, not refused at once. */
export class DocsDefaultHold extends Docs {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.DOCS_DEFAULT_HOLD as never;
  }
}

/** The docs table, presplit into 4 shards. */
export class PresplitDocs extends Docs {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6, holdWaitMs: 0, initialDepth: 2 };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.PRESPLIT_DOCS as never;
  }
}

/** A later deploy's routeOf: by the first 2 components, so refs/heads/a and refs/heads/b share a route. */
export const refRouteV2 = (key: string): string => key.split("/").slice(0, 2).join("/");

/** Docs routed like refs: every doc under refs/x/y lives in one shard. */
export class RoutedDocs extends Docs {
  /** Which deploy's routeOf this instance runs: 1, or 2 once `deployRoute` restarted it on it. */
  private readonly routeVersion: number;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_route (version INTEGER NOT NULL)");
    this.routeVersion = ctx.storage.sql.exec<{ version: number }>("SELECT version FROM test_route").toArray()[0]?.version ?? 1;
  }
  protected override routeOf(key: string): string {
    return this.routeVersion === 2 ? refRouteV2(key) : refRoute(key);
  }
  /** A deploy that changes routeOf: this shard restarts running `version`'s. */
  async deployRoute(version: number): Promise<void> {
    this.ctx.storage.sql.exec("DELETE FROM test_route");
    this.ctx.storage.sql.exec("INSERT INTO test_route (version) VALUES (?)", version);
    await this.ctx.storage.sync();
    this.ctx.abort("test: deployed another routeOf");
  }
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.ROUTED_DOCS as never;
  }
}

// ---- An indexed table: users, unique by email, listed by team ----

export type User = { id: string; email: string | null; name: string; team: string; createdAt: number; active: boolean };
export const users = defineTable<User>({
  version: 2,
  primaryKey: "id",
  columns: { id: "text", email: "text", name: "text", team: "text", createdAt: "integer", active: "boolean" },
  indexes: { byName: ["name"] },
  globalIndexes: {
    byEmail: { columns: ["email"], unique: true },
    byTeam: { columns: ["team", "createdAt"], partition: 1 },
  },
});

/** `target[prop]`, bound to `target` if it is a method (the runtime's objects check `this`). */
function bound(target: object, prop: string | symbol): unknown {
  const value = Reflect.get(target, prop);
  return typeof value === "function" ? value.bind(target) : value;
}

/** `ns`, whose stubs lose the answer to each `coordinate` step once it has come back. */
function losingCoordinateAnswers<T extends Rpc.DurableObjectBranded>(ns: DurableObjectNamespace<T>): DurableObjectNamespace<T> {
  return new Proxy(ns, {
    get(target, prop) {
      if (prop !== "get") return bound(target, prop);
      return (id: DurableObjectId, options?: DurableObjectNamespaceGetDurableObjectOptions) => {
        const stub = target.get(id, options) as unknown as { shardedAtomic(...a: unknown[]): Promise<unknown> };
        return new Proxy(stub, {
          get(s, p) {
            if (p !== "shardedAtomic") return bound(s, p);
            return async (...a: unknown[]) => {
              const answer = await s.shardedAtomic(...a);
              if ((a[2] as AtomicStep).step === "coordinate") throw new Error("test: the answer was lost");
              return answer;
            };
          },
        });
      };
    },
  });
}

/** Timers set and not fired or cleared yet, in this isolate: to count what a split's held writes cost. */
const liveTimers = new Set<unknown>();
{
  const set = globalThis.setTimeout;
  const clear = globalThis.clearTimeout;
  globalThis.setTimeout = ((fn: (...a: unknown[]) => void, ms?: number, ...rest: unknown[]) => {
    const id: unknown = set(
      (...a: unknown[]) => {
        liveTimers.delete(id);
        fn(...a);
      },
      ms,
      ...rest,
    );
    liveTimers.add(id);
    return id;
  }) as typeof setTimeout;
  globalThis.clearTimeout = ((id: unknown) => {
    liveTimers.delete(id);
    clear(id as never);
  }) as typeof clearTimeout;
}

/** A statement an object ran: its text, its bindings, and what its cursor read and wrote. */
export interface StatementCost {
  readonly query: string;
  readonly rowsRead: number;
  readonly rowsWritten: number;
  /** EXPLAIN QUERY PLAN of it, with its bindings, run when asked (empty for DDL and errors). */
  readonly plan: string[];
}

/**
 * Logs every statement an object runs (the last 20,000), with its cursor, so a test can ask what
 * each cost and how SQLite planned it. Installed first, under any fault a test adds later.
 */
/** Marks a fixture's own statement: StatementLog leaves it out. */
const FIXTURE_SQL = "/* fixture */ ";

class StatementLog {
  private entries: { query: string; bindings: unknown[]; cursor: { rowsRead: number; rowsWritten: number } }[] = [];
  private readonly exec: SqlStorage["exec"];
  constructor(private readonly storage: DurableObjectStorage) {
    const sql = storage.sql;
    const exec = sql.exec.bind(sql);
    this.exec = exec;
    sql.exec = ((query: string, ...bindings: unknown[]) => {
      const cursor = exec(query, ...bindings);
      // The fixtures' own bookkeeping (marked so) is not the library's cost.
      if (query.startsWith(FIXTURE_SQL)) return cursor;
      if (this.entries.length >= 20_000) this.entries.shift();
      this.entries.push({ query, bindings, cursor });
      return cursor;
    }) as typeof sql.exec;
  }
  /** Rows read by every statement since the last reset. */
  rowsRead(): number {
    return this.entries.reduce((sum, e) => sum + e.cursor.rowsRead, 0);
  }
  /** The statements since the last reset whose text contains `filter` (all, for ""), and starts again if `reset`. */
  report(filter: string, reset: boolean): StatementCost[] {
    const out = this.entries
      .filter((e) => e.query.includes(filter))
      .map((e) => ({ query: e.query, rowsRead: e.cursor.rowsRead, rowsWritten: e.cursor.rowsWritten, plan: this.plan(e.query, e.bindings) }));
    if (reset) this.entries = [];
    return out;
  }
  private plan(query: string, bindings: unknown[]): string[] {
    if (!/^\s*(SELECT|UPDATE|DELETE|INSERT|WITH)/i.test(query)) return [];
    try {
      return this.exec<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, ...bindings).toArray().map((r) => r.detail);
    } catch {
      return [];
    }
  }
}

/**
 * A split policy a test set on this object (`setPolicy`), stored so it outlasts a restart, and
 * merged over the class's once the instance is built (after the subclass's own fields are set).
 * It may name fields the library does not have yet, such as size limits.
 */
function usePolicyOverride(ctx: DurableObjectState, policy: () => SplitPolicy, apply: (policy: SplitPolicy) => void): void {
  void ctx.blockConcurrencyWhile(async () => {
    await null;
    const stored = loadPolicyOverride(ctx.storage);
    if (stored) apply({ ...policy(), ...stored });
  });
}
function loadPolicyOverride(storage: DurableObjectStorage): Record<string, unknown> | null {
  storage.sql.exec("CREATE TABLE IF NOT EXISTS test_policy (policy TEXT NOT NULL)");
  const row = storage.sql.exec<{ policy: string }>("SELECT policy FROM test_policy").toArray()[0];
  return row ? (JSON.parse(row.policy) as Record<string, unknown>) : null;
}
function savePolicyOverride(storage: DurableObjectStorage, change: Record<string, unknown>): void {
  const merged = { ...(loadPolicyOverride(storage) ?? {}), ...change };
  storage.sql.exec("DELETE FROM test_policy");
  storage.sql.exec("INSERT INTO test_policy (policy) VALUES (?)", JSON.stringify(merged));
}

/** The schema version `upgrade` deployed to this object (1 before any). */
function deployedVersion(storage: DurableObjectStorage): number {
  storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
  return storage.sql.exec<{ version: number }>("SELECT version FROM test_schema").toArray()[0]?.version ?? 1;
}

/** Plain SQL statements on `storage`: the rows of each. For references, ANALYZE, and corrupting state on purpose. */
function sqlRun(storage: DurableObjectStorage, statements: readonly { query: string; bindings?: readonly unknown[] }[]): unknown[][] {
  return statements.map(({ query, bindings }) => storage.sql.exec(query, ...((bindings ?? []) as never[])).toArray());
}

/** Counts `storage`'s setAlarm calls from now on. */
function countSetAlarm(storage: DurableObjectStorage): { sets: number } {
  const count = { sets: 0 };
  const set = storage.setAlarm.bind(storage);
  storage.setAlarm = ((time: number | Date, options?: DurableObjectSetAlarmOptions) => {
    count.sets++;
    return set(time, options);
  }) as typeof storage.setAlarm;
  return count;
}

/**
 * The next SQL statement on `storage` containing `text` throws, once, as a storage error would;
 * with `binding`, only one of whose bound values contains that too.
 */
function failStatementOnce(storage: DurableObjectStorage, text: string, binding?: string): void {
  const sql = storage.sql;
  const exec = sql.exec.bind(sql);
  sql.exec = ((query: string, ...bindings: unknown[]) => {
    if (!query.includes(text) || (binding !== undefined && !bindings.some((b) => typeof b === "string" && b.includes(binding)))) return exec(query, ...bindings);
    sql.exec = exec;
    throw new Error("test: storage error");
  }) as typeof sql.exec;
}

/**
 * Strands `storage`'s alarm, as the platform can: `getAlarm()` answers a time 10 minutes past, but
 * no alarm will fire. Lasts until something sets an alarm (or the object restarts).
 */
async function strandAlarm(storage: DurableObjectStorage): Promise<void> {
  await storage.deleteAlarm();
  const stranded = Date.now() - 10 * 60_000;
  const getAlarm = storage.getAlarm.bind(storage);
  const setAlarm = storage.setAlarm.bind(storage);
  let on = true;
  storage.getAlarm = (async (options?: DurableObjectGetAlarmOptions) => (on ? stranded : getAlarm(options))) as typeof storage.getAlarm;
  storage.setAlarm = ((time: number | Date, options?: DurableObjectSetAlarmOptions) => {
    on = false;
    return setAlarm(time, options);
  }) as typeof storage.setAlarm;
}

/** While set, deliveries to this index object fail. */
function setFailApply(storage: DurableObjectStorage, fail: boolean): void {
  storage.sql.exec("CREATE TABLE IF NOT EXISTS test_apply (fail INTEGER NOT NULL)");
  storage.sql.exec("DELETE FROM test_apply");
  storage.sql.exec("INSERT INTO test_apply (fail) VALUES (?)", fail ? 1 : 0);
}
function assertApplyAllowed(storage: DurableObjectStorage): void {
  storage.sql.exec("CREATE TABLE IF NOT EXISTS test_apply (fail INTEGER NOT NULL)");
  if (storage.sql.exec("SELECT fail FROM test_apply WHERE fail = 1").toArray().length) throw new Error("test: apply failed");
}

export class Users extends IndexedTable<Env, User> {
  /** This shard's rows, from its counts: the tests' `count` op sums them over the shards. */
  async testCount(): Promise<number> {
    return this.ownedRowCount();
  }
  protected table = users;
  private readonly log = new StatementLog(this.ctx.storage);
  private readonly alarms = countSetAlarm(this.ctx.storage);
  /** setAlarm calls of this instance since the last reset. */
  async alarmSets(reset: boolean): Promise<number> {
    const n = this.alarms.sets;
    if (reset) this.alarms.sets = 0;
    return n;
  }
  /** The statements this instance ran whose text contains `filter`, since the last reset. */
  async statements(filter: string, reset: boolean): Promise<StatementCost[]> {
    return this.log.report(filter, reset);
  }

  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected override indexWaitMs = 1_500;
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USERS as never;
  }
  protected indexNodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USER_INDEXES as never;
  }
  /** The next SQL statement containing `text` throws, once. */
  async failStatementOnce(text: string): Promise<void> {
    failStatementOnce(this.ctx.storage, text);
  }
  /** Loses this object's alarm, as a failed setAlarm or an alarm the runtime gave up on would. */
  async loseAlarm(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }
  /** The keys with index changes not delivered yet, on this object, and whether it owns each. */
  async outboxKeys(): Promise<{ key: string; owned: boolean }[]> {
    return this.ctx.storage.sql.exec<{ key: string }>("SELECT DISTINCT key FROM indexed_outbox ORDER BY key").toArray().map(({ key }) => ({ key, owned: this.owns(key) }));
  }
  /** While set, this subclass's onAlarm throws before it calls super (app code that fails). */
  async throwOnAlarm(on: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_throw_alarm (x INTEGER)");
    this.ctx.storage.sql.exec("DELETE FROM test_throw_alarm");
    if (on) this.ctx.storage.sql.exec("INSERT INTO test_throw_alarm (x) VALUES (1)");
  }
  /** Index changes each alarm delivered, in order (this instance's). */
  private delivered: number[] = [];
  async deliveredPerAlarm(): Promise<number[]> {
    return this.delivered;
  }
  protected override async onAlarm(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_throw_alarm (x INTEGER)");
    if (this.ctx.storage.sql.exec("SELECT x FROM test_throw_alarm").toArray().length) throw new Error("test: the app's onAlarm failed");
    await super.onAlarm();
  }
  /** Around the library's deliveries: how many each alarm made, once asked to count (it reads the outbox). */
  private countDeliveries = false;
  async countDeliveriesPerAlarm(): Promise<void> {
    this.countDeliveries = true;
  }
  protected override async layerAlarm(): Promise<void> {
    if (!this.countDeliveries) return super.layerAlarm();
    const size = () => (this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'indexed_outbox'").one().n ? this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM indexed_outbox").one().n : 0);
    const before = size();
    try {
      await super.layerAlarm();
    } finally {
      this.delivered.push(before - size());
    }
  }
  /** Ends this instance: what it kept in memory (sends in flight, retries) is gone. */
  async restart(): Promise<void> {
    this.ctx.abort("test: restart");
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
  async sqlRun(statements: readonly { query: string; bindings?: readonly unknown[] }[]): Promise<unknown[][]> {
    return sqlRun(this.ctx.storage, statements);
  }
  private readonly log = new StatementLog(this.ctx.storage);
  /** The statements this instance ran whose text contains `filter`, since the last reset. */
  async statements(filter: string, reset: boolean): Promise<StatementCost[]> {
    return this.log.report(filter, reset);
  }
  /** Runs this index shard's own alarm work now (its sweep of reservations and tombstones). */
  /** The work of an alarm due now, as the alarm runs it: the layers', then the app's. */
  async alarmNow(): Promise<void> {
    await this.layerAlarm();
    await this.onAlarm();
  }

  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected override reservationTimeoutMs = 1_000;
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USER_INDEXES as never;
  }
  protected rowNodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.USERS as never;
  }
  /** The next SQL statement containing `text` throws, once. */
  async failStatementOnce(text: string): Promise<void> {
    failStatementOnce(this.ctx.storage, text);
  }
  /** The next setAlarm throws, once, as a storage error would. */
  async failSetAlarmOnce(): Promise<void> {
    const storage = this.ctx.storage;
    const setAlarm = storage.setAlarm.bind(storage);
    storage.setAlarm = (async () => {
      storage.setAlarm = setAlarm;
      throw new Error("test: setAlarm failed");
    }) as typeof storage.setAlarm;
  }
  /** While set, deliveries to this object fail (the flag is copied with the rest). */
  async failApply(fail: boolean): Promise<void> {
    setFailApply(this.ctx.storage, fail);
  }
  /** The next `n` deliveries to this instance never answer (an index shard that hangs). */
  private hangs = 0;
  async hangApplies(n: number): Promise<void> {
    this.hangs = n;
  }
  override async indexApply(...args: Parameters<GlobalIndex<Env>["indexApply"]>): ReturnType<GlobalIndex<Env>["indexApply"]> {
    if (this.hangs > 0) {
      this.hangs--;
      return new Promise(() => {});
    }
    assertApplyAllowed(this.ctx.storage);
    return super.indexApply(...args);
  }
}

/** Users listed by team, then (after `upgrade`) also unique by email: an index added to a table that has rows. */
const usersV1 = defineTable<User>({ version: 1, primaryKey: "id", columns: users.columns, globalIndexes: { byTeam: { columns: ["team", "createdAt"], partition: 1 } } });
/** After a second upgrade: byEmail dropped, and byTeam redefined (ordered by name). */
export const usersV3 = defineTable<User>({ version: 3, primaryKey: "id", columns: users.columns, globalIndexes: { byTeam: { columns: ["team", "name"], partition: 1 } } });

export class EvolvingUsers extends IndexedTable<Env, User> {
  /** This shard's rows, from its counts: the tests' `count` op sums them over the shards. */
  async testCount(): Promise<number> {
    return this.ownedRowCount();
  }
  private readonly log = new StatementLog(this.ctx.storage);
  /** The statements this instance ran whose text contains `filter`, since the last reset. */
  async statements(filter: string, reset: boolean): Promise<StatementCost[]> {
    return this.log.report(filter, reset);
  }
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  /** The deployed schema's version: read once an instance, as a deploy restarts it (see upgrade). */
  private deployed: number | null = null;
  protected get table(): TableSchema<User> {
    this.deployed ??= deployedVersion(this.ctx.storage);
    return this.deployed === 3 ? usersV3 : this.deployed === 2 ? users : usersV1;
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
  /** While set, deliveries to this object fail (the flag is copied with the rest). */
  async failApply(fail: boolean): Promise<void> {
    setFailApply(this.ctx.storage, fail);
  }
  override async indexApply(...args: Parameters<GlobalIndex<Env>["indexApply"]>): ReturnType<GlobalIndex<Env>["indexApply"]> {
    assertApplyAllowed(this.ctx.storage);
    return super.indexApply(...args);
  }
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
  version: 2,
  primaryKey: "name",
  columns: { ...docs.columns, stars: "integer" },
  indexes: { byCreated: ["createdAt"], byStars: ["stars"], byDeletedCreated: ["deleted", "createdAt"] },
});

/**
 * TextEncoder.encode calls on text longer than 100 characters made in this isolate (the Worker's
 * and its objects'), for tests that check a hot path copies no values to UTF-8 (keys it hashes are
 * shorter).
 */
let encodes = 0;
const encode = TextEncoder.prototype.encode;
TextEncoder.prototype.encode = function (this: TextEncoder, input?: string) {
  if ((input?.length ?? 0) > 100) encodes++;
  return encode.call(this, input);
};

export class EvolvingDocs extends ShardedTable<Env, DocV2> {
  /** This shard's rows, from its counts: the tests' `count` op sums them over the shards. */
  async testCount(): Promise<number> {
    return this.ownedRowCount();
  }
  /** encodes in this object's isolate: reset with `reset`. */
  async encodes(reset: boolean): Promise<number> {
    const n = encodes;
    if (reset) encodes = 0;
    return n;
  }
  /** A new index is built over a few hundred rows a call, so tests see it take several. */
  protected override indexBuildRows = 300;
  /** Flooded at 20,000 rows a second: a few full-budget listings in a second, however fast the machine. */
  protected override scanRowsPerSecond = 20_000;
  protected override commands = docCommands as never;
  async sqlRun(statements: readonly { query: string; bindings?: readonly unknown[] }[]): Promise<unknown[][]> {
    return sqlRun(this.ctx.storage, statements);
  }
  private readonly log: StatementLog;
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.log = new StatementLog(ctx.storage);
    usePolicyOverride(ctx, () => this.splitPolicy, (p) => (this.splitPolicy = p));
  }
  /** Changes this object's split policy (merged over the class's), as a deploy would; kept across restarts. */
  async setPolicy(change: Record<string, unknown>): Promise<void> {
    savePolicyOverride(this.ctx.storage, change);
    this.splitPolicy = { ...this.splitPolicy, ...change } as SplitPolicy;
  }
  /** This object's database size, as the runtime counts it. */
  async databaseSize(): Promise<number> {
    return this.ctx.storage.sql.databaseSize;
  }

  /** Rows this instance read since the last reset, and starts counting again. */
  async rowsRead(): Promise<number> {
    const n = this.log.rowsRead();
    this.log.report("\u0000", true);
    return n;
  }
  /** The statements this instance ran whose text contains `filter`, since the last reset. */
  async statements(filter: string, reset: boolean): Promise<StatementCost[]> {
    return this.log.report(filter, reset);
  }
  /** A page of owned rows (ownedRowsAfter), as a subclass's own walk would read it. */
  async ownedPage(after: string | null, limit: number): Promise<string[]> {
    return this.ownedRowsAfter(after, limit).map((r) => String((r.row as { name: string }).name));
  }
  /** While set, the alarm takes no build steps: only calls do (kept across restarts). */
  async pauseAlarmBuilds(paused: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_alarm_builds (paused INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_alarm_builds");
    this.ctx.storage.sql.exec("INSERT INTO test_alarm_builds (paused) VALUES (?)", paused ? 1 : 0);
  }
  protected override async layerAlarm(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_alarm_builds (paused INTEGER NOT NULL)");
    if (this.ctx.storage.sql.exec("SELECT 1 FROM test_alarm_builds WHERE paused = 1").toArray().length) return;
    await super.layerAlarm();
  }
  /** The migration, and one build step, as one call's would be: true once nothing is left to build. */
  async migrateOnce(): Promise<boolean> {
    this.migrate();
    return !this.buildStep();
  }
  /** What this object holds (tables, keys, alarm). */
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }
  /** The local index builds not finished yet, read without migrating. */
  async buildsLeft(): Promise<number> {
    const t = this.ctx.storage.sql.exec<{ n: number }>("SELECT count(*) AS n FROM sqlite_master WHERE name = 'sharded_table_builds'").one().n;
    return t === 0 ? 0 : this.ctx.storage.sql.exec<{ n: number }>("SELECT count(*) AS n FROM sharded_table_builds").one().n;
  }
  /** Ends this instance, as an eviction would: the next call starts a new one. */
  async restart(): Promise<void> {
    this.ctx.abort("test: restart");
  }
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  /** The deployed schema's version: read once an instance, as a deploy restarts it (see upgrade). */
  private deployed: number | null = null;
  protected get table(): TableSchema<DocV2> {
    this.deployed ??= deployedVersion(this.ctx.storage);
    return (this.deployed === 2 ? docsV2 : docs) as TableSchema<DocV2>;
  }
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.EVOLVING as never;
  }
  /** A deploy of schema `version` (2: the new one; 1: a rollback): records it and restarts, so the next call migrates. */
  async upgrade(version = 2): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("DELETE FROM test_schema");
    this.ctx.storage.sql.exec("INSERT INTO test_schema (version) VALUES (?)", version);
    await this.ctx.storage.sync();
    this.ctx.abort("upgrade");
  }
  /**
   * Puts this shard's byEdit index back in the earlier layout, as a release before key tables
   * left it: a hidden key column on the table, filled, with an SQL index, no key table, and no
   * record that a migration finished. Ends the instance, so the next call migrates.
   */
  async legacyLayout(): Promise<void> {
    const sql = this.ctx.storage.sql;
    const keys = sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'sharded\\_table\\_keys\\_byEdit%' ESCAPE '\\'").toArray()[0]!.name;
    const id = keys.slice("sharded_table_keys_".length);
    sql.exec(`ALTER TABLE sharded_table ADD COLUMN "_ix_${id}" TEXT`);
    sql.exec(`UPDATE sharded_table SET "_ix_${id}" = (SELECT _key FROM "${keys}" WHERE _pk = sharded_table.name)`);
    sql.exec(`CREATE INDEX "sharded_table_ix_${id}" ON sharded_table ("_ix_${id}")`);
    sql.exec(`DROP TABLE "${keys}"`);
    sql.exec("DROP TABLE IF EXISTS sharded_table_ready");
    await this.ctx.storage.sync();
    this.ctx.abort("legacy layout");
  }
  /** Rows whose earlier layout's key column still holds a key, and that layout's SQL indexes. */
  async legacyLeft(): Promise<{ keyed: number; indexes: number }> {
    const sql = this.ctx.storage.sql;
    const column = sql.exec<{ name: string }>("PRAGMA table_info(sharded_table)").toArray().find((c) => c.name.startsWith("_ix_"))?.name;
    const keyed = column ? sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM sharded_table WHERE "${column}" IS NOT NULL`).one().n : 0;
    const indexes = sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'index' AND name LIKE 'sharded\\_table\\_ix\\_%' ESCAPE '\\'").one().n;
    return { keyed, indexes };
  }
  async indexNames(): Promise<string[]> {
    return this.ctx.storage.sql.exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'sharded\\_table\\_keys\\_%' ESCAPE '\\' ORDER BY name").toArray().map((r) => r.name);
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
    failStatementOnce(this.ctx.storage, text);
  }
  /** Asks for onAlarm in `ms`. */
  async wakeIn(ms: number): Promise<void> {
    await this.scheduleAlarm(Date.now() + ms);
  }
  /** Loses this object's alarm (as a crash between a write and its alarm would). */
  async dropAlarm(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }
  /** Strands this object's alarm: getAlarm() answers a past time that will never fire. */
  async strandAlarm(): Promise<void> {
    await strandAlarm(this.ctx.storage);
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
  /** onAlarm runs this object made (stored: an instance idle between them is evicted), and whether they throw. */
  async onAlarmRuns(): Promise<number> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_alarm_runs (n INTEGER NOT NULL)");
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM test_alarm_runs").one().n;
  }
  async failOnAlarm(on: boolean): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_fail_alarm (x INTEGER)");
    this.ctx.storage.sql.exec("DELETE FROM test_fail_alarm");
    if (on) this.ctx.storage.sql.exec("INSERT INTO test_fail_alarm (x) VALUES (1)");
  }
  protected override async onAlarm(): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_alarm_runs (n INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("INSERT INTO test_alarm_runs (n) VALUES (1)");
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_fail_alarm (x INTEGER)");
    if (this.ctx.storage.sql.exec("SELECT x FROM test_fail_alarm").toArray().length) throw new Error("test: onAlarm failed");
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
  /** Activations this instance was asked for. */
  private activations = 0;
  async activationAttempts(): Promise<number> {
    return this.activations;
  }
  override async shardedActivate(name: string, bits: string, epoch: number): Promise<boolean> {
    this.activations++;
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
  /** The next statement containing `text`, with a bound value containing `binding`, throws once. */
  async failStatementWithOnce(text: string, binding: string): Promise<void> {
    failStatementOnce(this.ctx.storage, text, binding);
  }
  /** The next setAlarm throws, once, as a storage error would. */
  async failSetAlarmOnce(): Promise<void> {
    const storage = this.ctx.storage;
    const setAlarm = storage.setAlarm.bind(storage);
    storage.setAlarm = (async () => {
      storage.setAlarm = setAlarm;
      throw new Error("test: setAlarm failed");
    }) as typeof storage.setAlarm;
  }
  /** The logical-copy batches this instance took: their table, rows, and bytes on the heap (2 a UTF-16 unit). */
  private batches: { table: string; rows: number; heapBytes: number }[] = [];
  override async shardedReceive(name: string, bits: string, epoch: number, first: boolean, table: string, columns: readonly string[], rows: readonly (readonly SqlStorageValue[])[]): Promise<void> {
    const heapBytes = rows.reduce((sum, row) => sum + row.reduce<number>((n, v) => n + (typeof v === "string" ? 2 * v.length : v instanceof ArrayBuffer ? v.byteLength : 8), 0), 0);
    this.batches.push({ table, rows: rows.length, heapBytes });
    return super.shardedReceive(name, bits, epoch, first, table, columns, rows);
  }
  async receivedBatches(): Promise<{ table: string; rows: number; heapBytes: number }[]> {
    return this.batches;
  }

  /** Timers live in this isolate now. */
  async liveTimers(): Promise<number> {
    return liveTimers.size;
  }
  /** Inserts `n` atomic outcomes recorded at `at` (as old calls' would be). */
  async seedOutcomes(n: number, at: number): Promise<void> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS sharded_atomic_outcomes (tx TEXT PRIMARY KEY, outcome TEXT NOT NULL, at INTEGER NOT NULL)");
    for (let i = 0; i < n; i++) this.ctx.storage.sql.exec("INSERT OR REPLACE INTO sharded_atomic_outcomes (tx, outcome, at) VALUES (?, 'abort', ?)", `seed-${i}`, at);
  }
  async outcomeCount(): Promise<number> {
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'sharded_atomic_outcomes'").one().n ? this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sharded_atomic_outcomes").one().n : 0;
  }
  /** Runs this shard's alarm now. */
  async alarmNow(): Promise<void> {
    await this.alarm();
  }
  /** The next getAlarm() ends the instance, as an eviction there would. */
  async abortOnNextGetAlarm(): Promise<void> {
    const storage = this.ctx.storage;
    const getAlarm = storage.getAlarm.bind(storage);
    storage.getAlarm = (async (options?: DurableObjectGetAlarmOptions) => {
      storage.getAlarm = getAlarm;
      await storage.sync(); // what was written so far is durable, as it would be by then
      this.ctx.abort("test: evicted at getAlarm");
      return getAlarm(options);
    }) as typeof storage.getAlarm;
  }
  /**
   * Runs plain SQL statements here, outside any sharded state: the rows of each. An object used
   * this way is a reference (a plain SQLite Durable Object), never a shard.
   */
  async sqlRun(statements: readonly { query: string; bindings?: readonly unknown[] }[]): Promise<unknown[][]> {
    return sqlRun(this.ctx.storage, statements);
  }
  /** When this object's alarm is set for (null: none). */
  async alarmAt(): Promise<number | null> {
    return this.ctx.storage.getAlarm();
  }
  /** Rows each alarm run here deleted (a split's cleanup), in order: those that deleted any. */
  private readonly deletedByAlarm: number[] = [];
  async cleanupPerAlarm(): Promise<number[]> {
    return this.deletedByAlarm;
  }
  // Wrapped on the instance (the class may not override alarm): what each run deleted.
  private readonly countsAlarmDeletes = (() => {
    const alarm = this.alarm.bind(this);
    const stored = () => (this.ctx.storage.sql.exec<{ n: number }>(`${FIXTURE_SQL}SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'sharded_rows'`).one().n ? this.ctx.storage.sql.exec<{ n: number }>(`${FIXTURE_SQL}SELECT COUNT(*) AS n FROM sharded_rows`).one().n : 0);
    this.alarm = async (...args: Parameters<typeof alarm>) => {
      const before = stored();
      try {
        await alarm(...args);
      } finally {
        const deleted = before - stored();
        if (deleted > 0) this.deletedByAlarm.push(deleted);
      }
    };
    return true;
  })();
  /** How many rows allRows() returns here (it loads them all). */
  async allRowsCount(): Promise<number> {
    return this.allRows().size;
  }
  /** Writes a value that is not JSON (NaN, undefined): what update does with it. */
  async putOdd(keys: readonly string[], payloads: readonly ("nan" | "undefined")[]): Promise<null[]> {
    const odd = payloads[0] === "nan" ? Number.NaN : undefined;
    return this.update(keys, () => ({ writes: new Map(keys.map((k) => [k, odd as unknown as number])), result: keys.map(() => null) }));
  }
  /** Stores text values (the class's values are numbers; the base table takes any JSON). */
  async putText(keys: readonly string[], payloads: readonly string[]): Promise<null[]> {
    return this.update(keys, () => ({ writes: new Map(keys.map((k, i) => [k, payloads[i] as unknown as number])), result: keys.map(() => null) }));
  }

  /** Changes this object's split policy (merged over the class's), as a deploy would; kept across restarts. */
  async setPolicy(change: Record<string, unknown>): Promise<void> {
    savePolicyOverride(this.ctx.storage, change);
    this.splitPolicy = { ...this.splitPolicy, ...change } as SplitPolicy;
  }
  /** This object's database size, as the runtime counts it. */
  async databaseSize(): Promise<number> {
    return this.ctx.storage.sql.databaseSize;
  }

  private readonly log = new StatementLog(this.ctx.storage);
  /** The statements this instance ran whose text contains `filter`, since the last reset. */
  async statements(filter: string, reset: boolean): Promise<StatementCost[]> {
    return this.log.report(filter, reset);
  }
  /** A page of owned rows (ownedRowsPage), as a subclass's own walk would read it. */
  async ownedPage(after: string | null, limit: number): Promise<string[]> {
    return this.ownedRowsPage(after, limit).map(([key]) => key);
  }

  /** What this object holds (tables, keys, alarm): to check a wipe left nothing. */
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }

  /** Loses this shard's alarm, as a failed setAlarm or an alarm the runtime gave up on would. */
  async loseAlarm(keys: readonly string[], _payloads: null): Promise<null[]> {
    await this.ctx.storage.deleteAlarm();
    return keys.map(() => null);
  }

  /** Strands this shard's alarm: getAlarm() answers a past time that will never fire. */
  async strandAlarm(keys: readonly string[], _payloads: null): Promise<null[]> {
    await strandAlarm(this.ctx.storage);
    return keys.map(() => null);
  }

  /** A subclass's own schema: rows, a count kept by a trigger, and a view over them. */
  async keepCounted(keys: readonly string[], _payloads: null): Promise<null[]> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counted (key TEXT PRIMARY KEY)");
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS counted_n (n INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("INSERT INTO counted_n (n) SELECT 0 WHERE NOT EXISTS (SELECT 1 FROM counted_n)");
    this.ctx.storage.sql.exec("CREATE TRIGGER IF NOT EXISTS counted_bump AFTER INSERT ON counted BEGIN UPDATE counted_n SET n = n + 1; END");
    this.ctx.storage.sql.exec("CREATE VIEW IF NOT EXISTS counted_view AS SELECT key FROM counted");
    return keys.map(() => null);
  }
  /** Adds a note per key, and answers the trigger's count. */
  async addCounted(keys: readonly string[], _payloads: null): Promise<number[]> {
    for (const key of keys) this.ctx.storage.sql.exec("INSERT OR IGNORE INTO counted (key) VALUES (?)", key);
    const n = this.ctx.storage.sql.exec<{ n: number }>("SELECT n FROM counted_n").one().n;
    return keys.map(() => n);
  }
  /** Whether this shard's view answers. */
  async viewed(keys: readonly string[], _payloads: null): Promise<boolean[]> {
    const ok = this.ctx.storage.sql.exec<{ n: number }>("SELECT count(*) AS n FROM sqlite_master WHERE type = 'view' AND name = 'counted_view'").one().n === 1;
    return keys.map(() => ok);
  }

  /** Ends this instance (an eviction): what it kept in memory is gone. Called on the object, not routed. */
  async evict(): Promise<void> {
    this.ctx.abort("test: evicted");
  }

  /** The stub options this shard would make its stubs with now (see `stubOptions`). */
  async seenStubOptions(keys: readonly string[], _payloads: null): Promise<unknown[]> {
    return keys.map(() => this.stubOptions());
  }

  /** As failNextStatement, called on the object (not routed: no call checks its alarm on the way). */
  async failStatementOnce(text: string): Promise<void> {
    failStatementOnce(this.ctx.storage, text);
  }

  /** The next SQL statement here containing `payloads[0]` throws, once, as a storage error would. */
  async failNextStatement(keys: readonly string[], payloads: readonly string[]): Promise<null[]> {
    failStatementOnce(this.ctx.storage, payloads[0]!);
    return keys.map(() => null);
  }

  /** Restarts this shard (the call fails); once restarted, its first read of the atomic holds throws, as a storage error would. */
  async restartFailingHoldsLoad(_keys: readonly string[], _payloads: null): Promise<never> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_fail_holds_load (x INTEGER)");
    this.ctx.storage.sql.exec("INSERT INTO test_fail_holds_load (x) VALUES (1)");
    await this.ctx.storage.sync(); // durable before the restart, which drops what is not
    this.ctx.abort("test: restart");
    throw new Error("unreachable");
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    usePolicyOverride(ctx, () => this.splitPolicy, (p) => (this.splitPolicy = p));
    const failHoldsLoad = ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sqlite_master WHERE name = 'test_fail_holds_load'").one().n > 0;
    if (failHoldsLoad) {
      ctx.storage.sql.exec("DROP TABLE test_fail_holds_load");
      failStatementOnce(ctx.storage, "FROM sharded_atomic_holds");
    }
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
    // Counted: how many stubs this shard makes for others of its object.
    this.countedNodes ??= new Proxy(this.env.COUNTERS, {
      get: (target, prop, receiver) => {
        if (prop === "get") return (...args: unknown[]) => {
          this.stubsMade++;
          return (target.get as (...a: unknown[]) => unknown).apply(target, args);
        };
        const value = Reflect.get(target, prop, receiver);
        return typeof value === "function" ? value.bind(target) : value;
      },
    });
    return this.countedNodes as never;
  }
  private countedNodes: DurableObjectNamespace | null = null;
  private stubsMade = 0;
  /** Stubs this instance made for other shards of its object (namespace.get calls). */
  async stubCount(): Promise<number> {
    return this.stubsMade;
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
      // (Made in the constructor too; again here, for an instance whose storage was wiped.)
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS history (key TEXT PRIMARY KEY)");
      this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS listing (key TEXT PRIMARY KEY)");
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
  // Short enough for a test to see outcomes nobody forgot go.
  protected override atomicOutcomeTtlMs = 2_000;
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.PRESPLIT_COUNTERS as never;
  }
}

const keptClients = new Map<string, ReturnType<typeof sharded<CounterBase>>>();

function namespace(env: Env, cls: string): DurableObjectNamespace<CounterBase> {
  return (cls === "users" ? env.USERS : cls === "userIndexes" ? env.USER_INDEXES : cls === "evolvingUsers" ? env.EVOLVING_USERS : cls === "auto" ? env.AUTO_COUNTERS : cls === "presplit" ? env.PRESPLIT_COUNTERS : cls === "routed" ? env.ROUTED_COUNTERS : cls === "routedTable" ? env.ROUTED_DOCS : cls === "table" ? env.DOCS : cls === "tableDefaultHold" ? env.DOCS_DEFAULT_HOLD : cls === "presplitTable" ? env.PRESPLIT_DOCS : cls === "evolving" ? env.EVOLVING : cls === "flaky" ? env.FLAKY : cls === "oldAlarm" ? env.OLD_ALARM : env.COUNTERS) as never;
}

/** POST /indexed {name, op, args}: one call of the indexed table client, or a test hook. */
/** What a failed call tells its caller: its message, and any outcomes it carries. */
function outcomeOf(error: unknown): unknown {
  const e = error as { name?: string; settled?: unknown; unknown?: unknown; results?: unknown } | null;
  return { threw: String(error), name: e?.name ?? null, settled: e?.settled ?? null, unknown: e?.unknown ?? null, results: e?.results ?? null };
}

async function indexedOp(env: Env, body: { name: string; op: string; args: unknown[]; cls?: "evolving"; version?: number }): Promise<unknown> {
  // "try:<op>": the op, and what its error says if it throws.
  if (body.op.startsWith("try:")) return indexedOp(env, { ...body, op: body.op.slice(4) }).then((ok) => ({ ok }), outcomeOf);
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
    case "count": // rows in the table: each row shard's own count, summed
      return { ok: ((await untyped(rows).scan("testCount")) as number[]).reduce((a, b) => a + b, 0) };
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
    case "failStatementOnce": { // [side, bits, text]
      const ns = (body.args[0] === "rows" ? rowNs : indexNs) as unknown as DurableObjectNamespace<Users>;
      return ns.get(ns.idFromName(shardObjectName(body.name, body.args[1] as string))).failStatementOnce(body.args[2] as string);
    }
    case "failSetAlarmOnce": // [bits]: of an index shard
      return env.USER_INDEXES.get(env.USER_INDEXES.idFromName(shardObjectName(body.name, body.args[0] as string))).failSetAlarmOnce();
    case "loseAlarm": // [bits]: of a row shard
      return env.USERS.get(env.USERS.idFromName(shardObjectName(body.name, body.args[0] as string))).loseAlarm();
    case "failApply": // [fail, bits]
      return (indexNs as typeof env.USER_INDEXES).get(indexNs.idFromName(shardObjectName(body.name, (body.args[1] as string) ?? ""))).failApply(body.args[0] as boolean);
  }
  try {
    return { ok: await (t as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>)[body.op]!.call(t, ...body.args) };
  } catch (error) {
    const e = error as Error & { key?: string; version?: number; index?: string; results?: unknown[] };
    return { error: e.name, message: e.message, ...(e.version === undefined ? {} : { version: e.version }), ...(e.index === undefined ? {} : { index: e.index }), ...(e.results === undefined ? {} : { results: e.results }) };
  }
}

/** POST /table {cls, name, op, args}: one call of the table client. */
async function tableOp(env: Env, body: { cls: string; name: string; op: string; args: unknown[] }): Promise<unknown> {
  // "try:<op>": the op, and what its error says if it throws.
  if (body.op.startsWith("try:")) return tableOp(env, { ...body, op: body.op.slice(4) }).then((ok) => ({ ok }), outcomeOf);
  const route = (body as { route?: number }).route === 2 ? refRouteV2 : refRoute;
  if (body.op === "putOdd") { // [kind, name]: a row whose meta is not JSON, built here (HTTP can't carry it)
    const meta = { nan: { a: Number.NaN }, undefined: { a: undefined }, date: { d: new Date(0) }, big: { n: 1 } }[body.args[0] as "nan" | "undefined" | "date" | "big"];
    const t = table(namespace(env, body.cls) as never, body.name, docs as never) as unknown as { put(row: unknown): Promise<unknown> };
    const createdAt = body.args[0] === "big" ? 2 ** 53 + 2 : 1;
    return t.put({ name: body.args[1] as string, id: "odd", createdAt, lastEditAt: null, deleted: false, meta }).then((ok) => ({ ok }), outcomeOf);
  }
  if (body.op === "deployRoute") { // [version, bits]: of a routedTable shard
    const ns = env.ROUTED_DOCS as unknown as DurableObjectNamespace<RoutedDocs>;
    await ns.get(ns.idFromName(shardObjectName(body.name, (body.args[1] as string) ?? ""))).deployRoute(body.args[0] as number).catch(() => {});
    return null;
  }
  // With `stats`: what each call of the client took (onCall), answered along with the result.
  const stats: CallStats[] = [];
  const onCall = (body as { stats?: boolean }).stats ? { onCall: (s: CallStats) => void stats.push(s) } : {};
  const t = table(namespace(env, body.cls) as never, body.name, (body.cls === "evolving" || body.cls === "flaky" ? docsV2 : docs) as never, { ...(body.cls === "routedTable" ? { routeOf: route } : {}), commands: docCommands as never, ...((body as { options?: object }).options ?? {}), ...onCall }) as unknown as Record<string, (...a: unknown[]) => unknown>;
  if (body.op === "all") {
    const rows: unknown[] = [];
    for await (const row of (t.all as (q: ListQuery) => AsyncGenerator<unknown>).call(t, body.args[0] as ListQuery)) rows.push(row);
    return (body as { stats?: boolean }).stats ? { ok: rows, stats } : rows;
  }
  if (body.op === "upgrade") {
    const ns = namespace(env, body.cls) as unknown as DurableObjectNamespace<EvolvingDocs>;
    await ns.get(ns.idFromName(shardObjectName(body.name, (body.args[1] as string) ?? ""))).upgrade((body.args[0] as number) ?? 2).catch(() => {});
    return null;
  }
  if (body.op === "failActivations") {
    const ns = env.FLAKY;
    await ns.get(ns.idFromName(shardObjectName(body.name, body.args[1] as string))).failActivations(body.args[0] as boolean);
    return null;
  }
  if (body.op === "wakeIn" || body.op === "failStatementOnce" || body.op === "alarmNow" || body.op === "alarmRan" || body.op === "dropAlarm" || body.op === "strandAlarm" || body.op === "crashAlarmOnce" || body.op === "lateSchema") {
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
  if (body.op === "rowsRead") { // [bits]: of an evolving or flaky shard
    const ns = body.cls === "flaky" ? env.FLAKY : env.EVOLVING;
    return ns.get(ns.idFromName(shardObjectName(body.name, (body.args[0] as string) ?? ""))).rowsRead();
  }
  if (body.op === "scan") return (sharded(namespace(env, body.cls) as never, body.name, body.cls === "routedTable" ? { routeOf: refRoute } : {}).scan as (m: string, ...a: unknown[]) => Promise<unknown>)(...(body.args as [string, ...unknown[]]));
  // Rows in the table: each shard's own count, summed (the client has none, RFD0012).
  if (body.op === "count") return ((await (sharded(namespace(env, body.cls) as never, body.name, body.cls === "routedTable" ? { routeOf: route } : {}).scan as (m: string) => Promise<number[]>)("testCount")) as number[]).reduce((a, b) => a + b, 0);
  if ((body as { stats?: boolean }).stats) {
    const result = await Promise.resolve(t[body.op]!.call(t, ...body.args)).then((ok) => ({ ok }), (error: unknown) => ({ threw: String(error) }));
    return { ...result, stats };
  }
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
        case "/callSettled": {
          // A call, and what its error says if it throws: which shards' answers it still carries.
          const hint = (body as { presplitHint?: number }).presplitHint;
          try {
            const clientOptions = (body as { clientOptions?: object }).clientOptions ?? {};
            return Response.json({ ok: await sharded(ns, body.name, { ...(hint === undefined ? {} : { presplitHint: hint }), ...clientOptions }).call(body.keys, body.method as "get", (body.payloads ?? null) as null) });
          } catch (error) {
            return Response.json(outcomeOf(error));
          }
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
        case "/atomicallyLosingAnswer":
          // As /atomically, but the primary's answer to the call it coordinates is lost on its way
          // back: the call has run, and the caller sees an error.
          return Response.json(
            await sharded(losingCoordinateAnswers(ns), body.name)
              .atomically(body.keys, body.method, body.payloads ?? null)
              .catch((error: unknown) => ({ threw: String(error) })),
          );
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
        case "/encodes": {
          // This isolate's, then the object's (the same isolate, or another): both reset.
          const here = encodes;
          encodes = 0;
          const docs = env.EVOLVING;
          const there = await docs.get(docs.idFromName(shardObjectName(body.name, ""))).encodes(true);
          return Response.json(here + there);
        }
        case "/wipeAll":
          return Response.json(await sharded(ns, body.name).wipe());
        case "/object": {
          // A test method of one shard object, called directly (not routed): {cls, name, bits, method, args}.
          const { method, args } = body as unknown as { method: string; args?: unknown[] };
          const stub = ns.get(ns.idFromName(shardObjectName(body.name, body.bits ?? ""))) as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
          return Response.json((await stub[method]!(...(args ?? []))) ?? null);
        }
        case "/evict":
          await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).evict().catch(() => {});
          return Response.json(null);
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
