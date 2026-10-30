// Feature flags for every service of a company: a read-heavy control plane in one logical object.
// Each flag has a version, and a change applies only to the version it was made from, so two
// admins editing the same flag cannot overwrite each other.
//
//   GET  /api/flags?name=new-checkout&name=dark-mode&user=alice -> {"new-checkout": true, "dark-mode": false}
//   PUT  /api/flags/new-checkout {"enabled": true, "rollout": 25, "ifVersion": 0} -> {"ok": true, "version": 1}
//   GET  /api/flags/new-checkout -> {"enabled": true, "rollout": 25, "version": 1}
//   GET  /api/shards -> the object's shards, for the page
import { sharded, ShardedObject, type SplitPolicy, type UpdateResult } from "@leostera/do-kit";
import { Hono } from "hono";
import { describeShards } from "../../../_shared/shards.ts";

interface Env {
  readonly FLAGS: DurableObjectNamespace<Flags>;
}

export interface Flag {
  readonly enabled: boolean;
  /** Percentage of users who get it, 0 to 100. */
  readonly rollout: number;
  readonly version: number;
}

/** A change to a flag, made from the version the admin saw (0: the flag must not exist yet). */
export interface FlagChange {
  readonly enabled: boolean;
  readonly rollout: number;
  readonly ifVersion: number;
}

export class Flags extends ShardedObject<Env, Flag> {
  // Every service reads flags on every request, so reads, not writes, are the load. Start with 4
  // shards; load splits them further, up to 16.
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 4, initialDepth: 2 };

  protected nodes() {
    return this.env.FLAGS;
  }

  async get(keys: readonly string[], _payloads: null): Promise<(Flag | null)[]> {
    const flags = await this.read(keys);
    return keys.map((key) => flags.get(key) ?? null);
  }

  /** Compare-and-set: each change applies only if the flag is still at `ifVersion`. */
  async set(keys: readonly string[], changes: readonly FlagChange[]): Promise<{ ok: boolean; version: number }[]> {
    return this.update(keys, (current): UpdateResult<Flag, { ok: boolean; version: number }[]> => {
      const writes = new Map<string, Flag>();
      const result = keys.map((key, i) => {
        const version = (writes.get(key) ?? current.get(key))?.version ?? 0;
        const change = changes[i]!;
        if (change.ifVersion !== version) return { ok: false, version };
        writes.set(key, { enabled: change.enabled, rollout: change.rollout, version: version + 1 });
        return { ok: true, version: version + 1 };
      });
      return { writes, result };
    });
  }
}

/** Whether `user` is in a flag's rollout: the same answer for the same user every time. */
async function inRollout(flag: string, user: string, rollout: number): Promise<boolean> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(`${flag}:${user}`)));
  return ((digest[0]! << 8) | digest[1]!) % 100 < rollout;
}

const app = new Hono<{ Bindings: Env }>();

app.get("/api/flags", async (c) => {
  const names = c.req.queries("name") ?? [];
  const user = c.req.query("user") ?? "";
  const answers: Record<string, boolean> = {};
  for (const shard of await sharded(c.env.FLAGS, "production").call(names, "get", null)) {
    for (const [i, name] of shard.keys.entries()) {
      const flag = shard.result[i];
      answers[name] = !!flag?.enabled && (await inRollout(name, user, flag.rollout));
    }
  }
  return c.json(answers);
});

app.get("/api/flags/:name", async (c) => {
  const [flag] = await sharded(c.env.FLAGS, "production").callOneShard([c.req.param("name")], "get", null);
  return flag ? c.json(flag) : c.text("no such flag\n", 404);
});

app.put("/api/flags/:name", async (c) => {
  const change = await c.req.json<FlagChange>();
  const [result] = await sharded(c.env.FLAGS, "production").callOneShard([c.req.param("name")], "set", [change]);
  return c.json(result!, result!.ok ? 200 : 409);
});

app.get("/api/shards", async (c) => c.json(await describeShards(c.env.FLAGS, "production")));

export default app;
