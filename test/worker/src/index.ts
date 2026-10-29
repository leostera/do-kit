// Test Worker: counters keyed by name, on sharded objects. Routes:
//   POST /call {cls, name, method, keys, payloads}  POST /scan {cls, name, method}
//   POST /split {cls, name, bits}                    POST /describe {cls, name, bits}
//   POST /splits {cls, name}  (the splits this isolate's client knows)
import { shardObjectName, sharded, ShardedObject, type SplitPolicy, type UpdateResult } from "../../../src/sharded/index.ts";

interface Env {
  readonly COUNTERS: DurableObjectNamespace<Counters>;
  readonly AUTO_COUNTERS: DurableObjectNamespace<AutoCounters>;
  readonly PRESPLIT_COUNTERS: DurableObjectNamespace<PresplitCounters>;
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

export class Counters extends CounterBase {}

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
  return (cls === "auto" ? env.AUTO_COUNTERS : cls === "presplit" ? env.PRESPLIT_COUNTERS : env.COUNTERS) as never;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const body = (await request.json().catch(() => ({}))) as { cls: string; name: string; method: string; keys: string[]; payloads: unknown[] | null; bits: string };
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
          return Response.json(await sharded(ns, body.name).scan(body.method as "entries"));
        case "/split":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedSplit(body.name, body.bits));
        case "/describe":
          return Response.json(await ns.get(ns.idFromName(shardObjectName(body.name, body.bits))).shardedDescribe());
        case "/splits":
          return Response.json(sharded(ns, body.name).knownSplits());
      }
      return new Response("not found", { status: 404 });
    } catch (error) {
      return new Response(String(error instanceof Error ? (error.stack ?? error.message) : error), { status: 500 });
    }
  },
};
