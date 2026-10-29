// Test Worker: counters keyed by name, on sharded objects. Routes:
//   POST /call {cls, name, method, keys, payloads}  POST /scan {cls, name, method}
//   POST /split {cls, name, bits}                    POST /describe {cls, name, bits}
//   POST /splits {cls, name}  (the splits this isolate's client knows)
import { DEFAULT_SPLIT_POLICY, shardObjectName, sharded, ShardedObject, type SplitPolicy, type UpdateResult } from "../../../src/sharded/index.ts";
import { defineTable, ShardedTable, table, type ListQuery, type TableSchema } from "../../../src/table/index.ts";

interface Env {
  readonly COUNTERS: DurableObjectNamespace<Counters>;
  readonly AUTO_COUNTERS: DurableObjectNamespace<AutoCounters>;
  readonly PRESPLIT_COUNTERS: DurableObjectNamespace<PresplitCounters>;
  readonly REPOS: DurableObjectNamespace<Repos>;
  readonly EVOLVING: DurableObjectNamespace<EvolvingRepos>;
}

// ---- A sharded table: repositories, split by hand ----

export type Repo = { name: string; id: string; createdAt: number; lastPushAt: number | null; deleted: boolean; meta: unknown };
export const repos = defineTable<Repo>({
  primaryKey: "name",
  columns: { name: "text", id: "text", createdAt: "integer", lastPushAt: "integer", deleted: "boolean", meta: "json" },
  indexes: { byCreated: ["createdAt"], byPush: ["lastPushAt"], byDeletedCreated: ["deleted", "createdAt"] },
});

export class Repos extends ShardedTable<Env, Repo> {
  protected table = repos;
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
    return this.env.REPOS as never;
  }
}

/** The repos table, then (after `upgrade`) with a `stars` column and a `byStars` index instead of `byPush`. */
type RepoV2 = Repo & { stars: number | null };
const reposV2 = defineTable<RepoV2>({
  primaryKey: "name",
  columns: { ...repos.columns, stars: "integer" },
  indexes: { byCreated: ["createdAt"], byStars: ["stars"], byDeletedCreated: ["deleted", "createdAt"] },
});

export class EvolvingRepos extends ShardedTable<Env, RepoV2> {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 6 };
  protected get table(): TableSchema<RepoV2> {
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS test_schema (version INTEGER NOT NULL)");
    const version = this.ctx.storage.sql.exec<{ version: number }>("SELECT version FROM test_schema").toArray()[0]?.version ?? 1;
    return (version === 2 ? reposV2 : repos) as TableSchema<RepoV2>;
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

class CounterBase extends ShardedObject<Env, number> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // A subclass table: which keys were added to here (a stand-in for an outbox).
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS history (key TEXT PRIMARY KEY)");
    // A subclass table that both shards clean after a split (a stand-in for a listing).
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS listing (key TEXT PRIMARY KEY)");
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

  /** Sets each key to its payload (any JSON, for large rows). */
  async put(keys: readonly string[], payloads: readonly unknown[]): Promise<number> {
    return this.update(keys, () => ({ writes: new Map(keys.map((k, i) => [k, payloads[i] as number])), result: keys.length }));
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
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.AUTO_COUNTERS as never;
  }
}

/** Splits to depth 2 while empty, never by load. */
export class PresplitCounters extends CounterBase {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 4, initialDepth: 2 };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, number>> {
    return this.env.PRESPLIT_COUNTERS as never;
  }
}

function namespace(env: Env, cls: string): DurableObjectNamespace<CounterBase> {
  return (cls === "auto" ? env.AUTO_COUNTERS : cls === "presplit" ? env.PRESPLIT_COUNTERS : cls === "table" ? env.REPOS : cls === "evolving" ? env.EVOLVING : env.COUNTERS) as never;
}

/** POST /table {cls, name, op, args}: one call of the table client. */
async function tableOp(env: Env, body: { cls: string; name: string; op: string; args: unknown[] }): Promise<unknown> {
  const t = table(namespace(env, body.cls) as never, body.name, (body.cls === "evolving" ? reposV2 : repos) as never) as unknown as Record<string, (...a: unknown[]) => unknown>;
  if (body.op === "all") {
    const rows: unknown[] = [];
    for await (const row of (t.all as (q: ListQuery) => AsyncGenerator<unknown>).call(t, body.args[0] as ListQuery)) rows.push(row);
    return rows;
  }
  if (body.op === "upgrade") {
    const ns = env.EVOLVING;
    await ns.get(ns.idFromName(shardObjectName(body.name, ""))).upgrade().catch(() => {});
    return null;
  }
  if (body.op === "indexNames") {
    const ns = env.EVOLVING;
    return ns.get(ns.idFromName(shardObjectName(body.name, ""))).indexNames();
  }
  if (body.op === "scan") return (sharded(namespace(env, body.cls) as never, body.name).scan as (m: string, ...a: unknown[]) => Promise<unknown>)(...(body.args as [string, ...unknown[]]));
  return t[body.op]!.call(t, ...body.args);
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const body = (await request.json().catch(() => ({}))) as { cls: string; name: string; method: string; keys: string[]; payloads: unknown[] | null; bits: string; args?: unknown[] };
    const ns = namespace(env, body.cls);
    try {
      switch (url.pathname) {
        case "/call": {
          const results = await sharded(ns, body.name).call(body.keys, body.method as "get", (body.payloads ?? null) as null);
          // Back in the caller's order.
          const out: unknown[] = new Array(body.keys.length);
          for (const shard of results) shard.positions.forEach((p, j) => (out[p] = (shard.result as unknown[])[j]));
          return Response.json(out);
        }
        case "/scan":
          return Response.json(await (sharded(ns, body.name).scan as (m: string, ...a: unknown[]) => Promise<unknown>)(body.method, ...((body.args as unknown[]) ?? [])));
        case "/split":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedSplit(body.name, body.bits));
        case "/describe":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedDescribe());
        case "/table":
          return Response.json((await tableOp(env, body as never)) ?? null);
        case "/splits":
          return Response.json(sharded(ns, body.name).knownSplits());
      }
      return new Response("not found", { status: 404 });
    } catch (error) {
      return new Response(String(error instanceof Error ? (error.stack ?? error.message) : error), { status: 500 });
    }
  },
};
