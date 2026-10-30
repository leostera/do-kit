// Login sessions that expire, in a table of your own on a sharded object. Shows the three things
// a subclass must do with its own tables:
//   - write them only inside `transact`, which waits while a split copies those keys;
//   - delete, after a split, the rows of keys a shard gave away (`cleanupForeign`);
//   - run scheduled work through `onAlarm` and `scheduleAlarm`, and check which due work is its own.
//
//   POST   /api/sessions {"user": "alice", "ttlSeconds": 3600} -> {"id": "...", "expiresAt": ...}
//   GET    /api/sessions/:id -> {"user": "alice", "expiresAt": ...}, or 404 once expired
//   DELETE /api/sessions/:id -> 204
//   GET    /api/shards -> the object's shards, for the page
import { keyHash, sharded, ShardedObject } from "@leostera/do-kit";
import { Hono } from "hono";
import { describeShards } from "../../../_shared/shards.ts";

interface Env {
  readonly SESSIONS: DurableObjectNamespace<Sessions>;
}

interface Session {
  readonly user: string;
  readonly expiresAt: number;
}

export class Sessions extends ShardedObject<Env, never> {
  protected nodes() {
    return this.env.SESSIONS;
  }

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    // The routing hash of each key is stored, so a shard can find the rows it no longer owns.
    this.ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, hash TEXT NOT NULL, user TEXT NOT NULL, expires_at INTEGER NOT NULL)");
    this.ctx.storage.sql.exec("CREATE INDEX IF NOT EXISTS sessions_expiry ON sessions (expires_at)");
  }

  async start(keys: readonly string[], payloads: readonly Session[]): Promise<void> {
    await this.transact(keys, () => {
      keys.forEach((id, i) => this.ctx.storage.sql.exec("INSERT OR REPLACE INTO sessions (id, hash, user, expires_at) VALUES (?, ?, ?, ?)", id, keyHash(id), payloads[i]!.user, payloads[i]!.expiresAt));
    });
    // Keeps the earliest time asked for, so this never postpones an earlier expiry.
    await this.scheduleAlarm(Math.min(...payloads.map((s) => s.expiresAt)));
  }

  async get(keys: readonly string[], _payloads: null): Promise<(Session | null)[]> {
    return keys.map((id) => {
      const row = this.ctx.storage.sql.exec<{ user: string; expires_at: number }>("SELECT user, expires_at FROM sessions WHERE id = ?", id).toArray()[0];
      return row && row.expires_at > Date.now() ? { user: row.user, expiresAt: row.expires_at } : null;
    });
  }

  async end(keys: readonly string[], _payloads: null): Promise<void> {
    await this.transact(keys, () => {
      for (const id of keys) this.ctx.storage.sql.exec("DELETE FROM sessions WHERE id = ?", id);
    });
  }

  /** Sessions this shard owns, for the page's shard panel. */
  async sessionCount(): Promise<number> {
    const [lo, hi] = this.ownedHashRange();
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM sessions WHERE hash >= ? AND hash <= ?", lo, hi).one().n;
  }

  /** Deletes expired sessions, then asks to run again at the next expiry. */
  protected override async onAlarm(): Promise<void> {
    const now = Date.now();
    // After a split, both shards hold every row until cleanup: expire only the sessions this one owns.
    const expired = this.ctx.storage.sql
      .exec<{ id: string }>("SELECT id FROM sessions WHERE expires_at <= ? ORDER BY expires_at LIMIT 500", now)
      .toArray()
      .map((r) => r.id)
      .filter((id) => this.owns(id));
    if (expired.length > 0) await this.end(expired, null);
    const [lo, hi] = this.ownedHashRange();
    const next = this.ctx.storage.sql.exec<{ at: number | null }>("SELECT MIN(expires_at) AS at FROM sessions WHERE hash >= ? AND hash <= ?", lo, hi).one().at;
    if (next !== null) await this.scheduleAlarm(next);
  }

  /** After a split: deletes up to `limit` sessions of keys this shard gave away. */
  protected override cleanupForeign(_owns: (key: string) => boolean, limit: number): number {
    const [lo, hi] = this.ownedHashRange();
    return this.ctx.storage.sql.exec("DELETE FROM sessions WHERE id IN (SELECT id FROM sessions WHERE hash < ? OR hash > ? LIMIT ?)", lo, hi, limit).rowsWritten;
  }
}

const app = new Hono<{ Bindings: Env }>();

app.post("/api/sessions", async (c) => {
  const { user, ttlSeconds } = await c.req.json<{ user: string; ttlSeconds: number }>();
  const session = { user, expiresAt: Date.now() + ttlSeconds * 1000 };
  const id = crypto.randomUUID();
  await sharded(c.env.SESSIONS, "sessions").callOneShard([id], "start", [session]);
  return c.json({ id, expiresAt: session.expiresAt }, 201);
});

app.get("/api/sessions/:id", async (c) => {
  const [session] = await sharded(c.env.SESSIONS, "sessions").callOneShard([c.req.param("id")], "get", null);
  return session ? c.json(session) : c.text("no such session\n", 404);
});

app.delete("/api/sessions/:id", async (c) => {
  await sharded(c.env.SESSIONS, "sessions").callOneShard([c.req.param("id")], "end", null);
  return c.body(null, 204);
});

app.get("/api/shards", async (c) => c.json(await describeShards(c.env.SESSIONS, "sessions", (shard) => shard.sessionCount())));

export default app;
