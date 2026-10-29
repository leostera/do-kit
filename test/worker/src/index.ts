// Test Worker: counters keyed by name, on splittable objects. Routes:
//   POST /call {cls, name, method, keys, payloads}  POST /scan {cls, name, method}
//   POST /split {cls, name, bits}                    POST /describe {cls, name, bits}
//   POST /splits {cls, name}  (the splits this isolate's client knows)
import { nodeName, splittable, SplittableObject, type SplitPolicy, type UpdateResult } from "../../../src/index.ts";

interface Env {
  readonly COUNTERS: DurableObjectNamespace<Counters>;
  readonly AUTO_COUNTERS: DurableObjectNamespace<AutoCounters>;
  readonly PRESPLIT_COUNTERS: DurableObjectNamespace<PresplitCounters>;
}

class CounterBase extends SplittableObject<Env, number> {
  protected nodes(): DurableObjectNamespace<SplittableObject<Env, number>> {
    return this.env.COUNTERS as never;
  }

  async get(keys: readonly string[], _payloads: null): Promise<(number | null)[]> {
    const rows = await this.read(keys);
    return keys.map((k) => rows.get(k) ?? null);
  }

  /** Adds each payload to its key's counter; returns the new values. */
  async add(keys: readonly string[], payloads: readonly number[]): Promise<number[]> {
    return this.update(keys, (current): UpdateResult<number, number[]> => {
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

  async entries(): Promise<[string, number][]> {
    return [...this.allRows()];
  }
}

export class Counters extends CounterBase {}

export class AutoCounters extends CounterBase {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 20, windowSeconds: 2, maxDepth: 3 };
  protected override nodes(): DurableObjectNamespace<SplittableObject<Env, number>> {
    return this.env.AUTO_COUNTERS as never;
  }
}

/** Splits to depth 2 while empty, never by load. */
export class PresplitCounters extends CounterBase {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e9, windowSeconds: 2, maxDepth: 4, initialDepth: 2 };
  protected override nodes(): DurableObjectNamespace<SplittableObject<Env, number>> {
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
          const results = await splittable(ns, body.name).call(body.keys, body.method as "get", (body.payloads ?? null) as null);
          // Back in the caller's order.
          const out: unknown[] = new Array(body.keys.length);
          for (const leaf of results) leaf.positions.forEach((p, j) => (out[p] = (leaf.result as unknown[])[j]));
          return Response.json(out);
        }
        case "/scan":
          return Response.json(await splittable(ns, body.name).scan(body.method as "entries"));
        case "/split":
          return Response.json(await ns.get(ns.idFromName(nodeName(body.name, body.bits))).splittableSplit(body.name, body.bits));
        case "/describe":
          return Response.json(await ns.get(ns.idFromName(nodeName(body.name, body.bits))).splittableDescribe());
        case "/splits":
          return Response.json(splittable(ns, body.name).knownSplits());
      }
      return new Response("not found", { status: 404 });
    } catch (error) {
      return new Response(String(error instanceof Error ? (error.stack ?? error.message) : error), { status: 500 });
    }
  },
};
