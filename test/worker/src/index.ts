// Test Worker: counters keyed by name, on sharded objects. Routes:
//   POST /call {cls, name, method, keys, payloads}  POST /scan {cls, name, method}
//   POST /split {cls, name, bits}                    POST /describe {cls, name, bits}
//   POST /splits {cls, name}  (the splits this isolate's client knows)
import { MAX_KNOWN_OBJECTS } from "../../../src/sharded/client.ts";
import { checkSplitPolicy, DEFAULT_SPLIT_POLICY, keyHash, shardObjectName, sharded, ShardedObject, type SplitPolicy, type StubOptions, type UpdateResult } from "../../../src/sharded/index.ts";
import { defineTable, ShardedTable, table, type ListQuery, type TableSchema } from "../../../src/table/index.ts";

interface Env {
  readonly COUNTERS: DurableObjectNamespace<Counters>;
  readonly AUTO_COUNTERS: DurableObjectNamespace<AutoCounters>;
  readonly PRESPLIT_COUNTERS: DurableObjectNamespace<PresplitCounters>;
  readonly DOCS: DurableObjectNamespace<Docs>;
  readonly EVOLVING: DurableObjectNamespace<EvolvingDocs>;
  readonly FLAKY: DurableObjectNamespace<FlakyDocs>;
  readonly OLD_ALARM: DurableObjectNamespace<OldAlarm>;
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
  protected override async onAlarm(): Promise<void> {
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
  return (cls === "auto" ? env.AUTO_COUNTERS : cls === "presplit" ? env.PRESPLIT_COUNTERS : cls === "table" ? env.DOCS : cls === "evolving" ? env.EVOLVING : cls === "flaky" ? env.FLAKY : cls === "oldAlarm" ? env.OLD_ALARM : env.COUNTERS) as never;
}

/** POST /table {cls, name, op, args}: one call of the table client. */
async function tableOp(env: Env, body: { cls: string; name: string; op: string; args: unknown[] }): Promise<unknown> {
  const t = table(namespace(env, body.cls) as never, body.name, (body.cls === "evolving" || body.cls === "flaky" ? docsV2 : docs) as never) as unknown as Record<string, (...a: unknown[]) => unknown>;
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
  if (body.op === "wakeIn" || body.op === "alarmNow" || body.op === "alarmRan" || body.op === "dropAlarm") {
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
  if (body.op === "scan") return (sharded(namespace(env, body.cls) as never, body.name).scan as (m: string, ...a: unknown[]) => Promise<unknown>)(...(body.args as [string, ...unknown[]]));
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
          const options = { ...(wait === undefined ? {} : { notReadyWaitMs: wait }), ...(body.stubOptions ? { stubOptions: body.stubOptions } : {}) };
          const results = await sharded(ns, body.name, options).call(body.keys, body.method as "get", (body.payloads ?? null) as null);
          // Back in the caller's order.
          const out: unknown[] = new Array(body.keys.length);
          for (const shard of results) shard.positions.forEach((p, j) => (out[p] = (shard.result as unknown[])[j]));
          return Response.json(out);
        }
        case "/callOne":
          return Response.json(await sharded(ns, body.name).callOneShard(body.keys, body.method as "get", (body.payloads ?? null) as null));
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
        case "/describe":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedDescribe());
        case "/table":
          return Response.json((await tableOp(env, body as never)) ?? null);
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
      }
      return new Response("not found", { status: 404 });
    } catch (error) {
      return new Response(String(error instanceof Error ? (error.stack ?? error.message) : error), { status: 500 });
    }
  },
};
