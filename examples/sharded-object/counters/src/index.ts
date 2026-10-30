// Page-view counters for a whole site, in one logical object that splits itself as traffic grows.
//
//   POST /api/hits  {"pages": ["/", "/pricing", "/"]}  -> {"/": 2, "/pricing": 1}
//   GET  /api/counts?page=/&page=/pricing              -> {"/": 2, "/pricing": 1}
//   POST /api/load                                     -> about a second of hits on 64 pages, to watch splits
//   GET  /api/shards                                   -> the object's shards, for the page
import { sharded, ShardedObject, type UpdateResult } from "@leostera/do-kit";
import { Hono } from "hono";
import { describeShards } from "../../../_shared/shards.ts";

interface Env {
  readonly COUNTERS: DurableObjectNamespace<Counters>;
}

export class Counters extends ShardedObject<Env, number> {
  // The default split policy: a shard splits once it serves more than 400 calls/s for 5 s.

  protected nodes() {
    return this.env.COUNTERS;
  }

  /** Adds `amounts[i]` to `keys[i]`, in one transaction on this shard. Returns the new totals. */
  async add(keys: readonly string[], amounts: readonly number[]): Promise<number[]> {
    return this.update(keys, (current): UpdateResult<number, number[]> => {
      const writes = new Map<string, number>();
      keys.forEach((key, i) => writes.set(key, (writes.get(key) ?? current.get(key) ?? 0) + amounts[i]!));
      return { writes, result: keys.map((key) => writes.get(key)!) };
    });
  }

  async get(keys: readonly string[], _payloads: null): Promise<number[]> {
    const current = await this.read(keys);
    return keys.map((key) => current.get(key) ?? 0);
  }
}

/** The site's pages, as one sharded object: every page is a key. */
const SITE = "site";
export const LOAD_PAGES = Array.from({ length: 64 }, (_, i) => `/page/${i}`);

const app = new Hono<{ Bindings: Env }>();

app.post("/api/hits", async (c) => {
  const { pages } = await c.req.json<{ pages: string[] }>();
  // One call per shard that owns some of the pages, each with its share.
  const totals: Record<string, number> = {};
  for (const shard of await sharded(c.env.COUNTERS, SITE).call(pages, "add", pages.map(() => 1))) shard.keys.forEach((page, i) => (totals[page] = shard.result[i]!));
  return c.json(totals);
});

app.get("/api/counts", async (c) => {
  const pages = c.req.queries("page") ?? [];
  const counts: Record<string, number> = {};
  for (const shard of await sharded(c.env.COUNTERS, SITE).call(pages, "get", null)) shard.keys.forEach((page, i) => (counts[page] = shard.result[i]!));
  return c.json(counts);
});

// About a second of one-page hits, 32 at a time (at most 900 calls, within a request's subrequests).
app.post("/api/load", async (c) => {
  const counters = sharded(c.env.COUNTERS, SITE);
  const until = Date.now() + 1000;
  let calls = 0;
  await Promise.all(
    Array.from({ length: 32 }, async () => {
      while (Date.now() < until && calls < 900) {
        calls++;
        await counters.call([LOAD_PAGES[Math.floor(Math.random() * LOAD_PAGES.length)]!], "add", [1]);
      }
    }),
  );
  return c.json({ calls });
});

app.get("/api/shards", async (c) => c.json(await describeShards(c.env.COUNTERS, SITE)));

export default app;
