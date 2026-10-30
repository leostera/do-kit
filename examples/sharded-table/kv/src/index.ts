// A key-value HTTP API on a sharded table with no indexes. Each value has a version, served as an
// ETag, so clients can write "only if new" (If-None-Match: *) or "only if unchanged" (If-Match).
//
//   PUT    /api/kv/:key  (If-None-Match: * | If-Match: "3")  body: any JSON  -> 201/200, ETag; 412 on mismatch
//   GET    /api/kv/:key                                                      -> 200 with ETag, or 404
//   DELETE /api/kv/:key  (If-Match: "3")                                     -> 204; 412 on mismatch
//   GET    /api/kv?prefix=user:&cursor=...                                   -> {"keys": [...], "cursor": ...}
//   GET    /api/shards                                                       -> the table's shards, for the page
import { defineTable, ShardedTable, table, type SplitPolicy } from "@leostera/do-kit";
import { Hono } from "hono";
import { describeShards, tableRows } from "../../../_shared/shards.ts";

interface Env {
  readonly KV: DurableObjectNamespace<Store>;
}

type Entry = { key: string; value: unknown; updatedAt: number };

const entries = defineTable<Entry>({
  primaryKey: "key",
  columns: { key: "text", value: "json", updatedAt: "integer" },
});

export class Store extends ShardedTable<Env, Entry> {
  protected table = entries;
  // 16 shards from the first call, and never more: keys are spread by hash, so each takes 1/16 of
  // the load, and a listing asks 16 shards, not more.
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 4, initialDepth: 4 };

  protected nodes() {
    return this.env.KV;
  }
}

/** The version an If-Match or If-None-Match header asks for, if any. */
function condition(headers: { ifNoneMatch: string | undefined; ifMatch: string | undefined }): { ifVersion?: number } {
  if (headers.ifNoneMatch === "*") return { ifVersion: 0 };
  const match = headers.ifMatch?.match(/^"(\d+)"$/);
  return match ? { ifVersion: Number(match[1]) } : {};
}

const app = new Hono<{ Bindings: Env }>();
const kv = (env: Env) => table(env.KV, "kv", entries);
const conditionOf = (header: (name: string) => string | undefined) => condition({ ifNoneMatch: header("if-none-match"), ifMatch: header("if-match") });

app.get("/api/kv", async (c) => {
  const prefix = c.req.query("prefix");
  // In key order across all 16 shards; the cursor carries on even if the shards split.
  const page = await kv(c.env).list({ where: prefix ? [{ column: "key", op: "prefix", value: prefix }] : [], limit: 100, cursor: c.req.query("cursor") ?? null });
  return c.json({ keys: page.rows.map((r) => r.row.key), cursor: page.cursor });
});

app.get("/api/kv/:key", async (c) => {
  const found = await kv(c.env).get(c.req.param("key"));
  if (!found) return c.text("not found\n", 404);
  return c.json(found.row.value as object, 200, { etag: `"${found.version}"` });
});

app.put("/api/kv/:key", async (c) => {
  const result = await kv(c.env).put({ key: c.req.param("key"), value: await c.req.json(), updatedAt: Date.now() }, conditionOf((n) => c.req.header(n)));
  if (!result.ok) return c.text("version mismatch\n", 412, { etag: `"${result.version}"` });
  return c.body(null, result.version === 1 ? 201 : 200, { etag: `"${result.version}"` });
});

app.delete("/api/kv/:key", async (c) => {
  const result = await kv(c.env).delete(c.req.param("key"), conditionOf((n) => c.req.header(n)));
  return result.ok ? c.body(null, 204) : c.text("version mismatch\n", 412);
});

app.get("/api/shards", async (c) => c.json(await describeShards(c.env.KV, "kv", tableRows)));

export default app;
