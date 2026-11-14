// Listings while the table changes under them. A walk that runs while writers insert and delete
// and the table splits, with cleanup running, returns every row that existed throughout, once,
// in order; rows written or deleted during it may or may not show. And a shard-order walk goes on
// past pages over empty shards.
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { encodeShardCursor } from "../src/table/schema.ts";
import { post, shards, useWorker } from "./harness.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
type Page = { rows: { row: Doc }[]; cursor: string | null };
const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "table", name, op, args });
const doc = (name: string, i: number): Doc => ({ name, id: name, createdAt: i % 13, lastEditAt: null, deleted: false, meta: null });
const firstHex = (key: string) => Number.parseInt(createHash("sha256").update(key).digest("hex")[0]!, 16);

for (const query of [{ label: "by key", q: {} }, { label: "by an index, descending", q: { index: "byCreated", direction: "desc" } }, { label: "in shard order", q: { order: "shard" } }] as const) {
  test(`a listing ${query.label} during writes and splits returns every row that existed throughout, once`, async () => {
    const name = `live-${unique()}`;
    const stable = Array.from({ length: 120 }, (_, i) => doc(`s-${String(i).padStart(3, "0")}`, i));
    const doomed = Array.from({ length: 40 }, (_, i) => doc(`d-${String(i).padStart(3, "0")}`, i));
    await table(name, "putMany", [...stable, ...doomed].map((row) => ({ row })));

    // Writers: new rows in, doomed rows out; splits of the root and then both halves.
    let walking = true;
    const writers = (async () => {
      for (let i = 0; walking && i < 200; i++) {
        await table(name, "put", doc(`n-${String(i).padStart(3, "0")}`, i));
        if (i < doomed.length) await table(name, "delete", doomed[i]!.name);
      }
    })();
    const splits = (async () => {
      for (const bits of ["", "0", "1"]) {
        await Bun.sleep(150);
        await post("/split", { cls: "table", name, bits });
      }
    })();

    const listed: Doc[] = [];
    let cursor: string | null = null;
    for (let page = 0; page < 400; page++) {
      const result: Page = await table<Page>(name, "list", { ...query.q, limit: 6, cursor });
      listed.push(...result.rows.map((r) => r.row));
      cursor = result.cursor;
      if (cursor === null) break;
      await Bun.sleep(10);
    }
    walking = false;
    await Promise.all([writers, splits]);
    expect(cursor).toBeNull();
    expect((await shards("table", name)).length).toBeGreaterThan(1);

    const names = listed.map((d) => d.name);
    // Once each: no row twice, whatever it is.
    expect(names.filter((n, i) => names.indexOf(n) !== i)).toEqual([]);
    // Every row that existed throughout.
    expect(stable.map((d) => d.name).filter((n) => !names.includes(n))).toEqual([]);
    // In order (a shard-order walk is ordered within each shard only).
    if (!("order" in query.q)) {
      const key = (d: Doc) => ("index" in query.q ? [d.createdAt, d.name] : [0, d.name]) as [number, string];
      const sorted = [...listed].sort((a, b) => {
        const [x, y] = [key(a), key(b)];
        const d = x[0] - y[0] || (x[1] < y[1] ? -1 : x[1] > y[1] ? 1 : 0);
        return "direction" in query.q ? -d : d;
      });
      expect(names).toEqual(sorted.map((d) => d.name));
    }
  }, 180_000);
}

test("a shard-order walk goes on past pages over only empty shards, and finds the rows after them", async () => {
  const name = `empty-shards-${unique()}`;
  await table(name, "put", doc("warm", 0));
  await table(name, "delete", "warm");
  for (const bits of ["", "0", "1", "00", "01", "10", "11"]) expect(await post<boolean>("/split", { cls: "table", name, bits })).toBe(true);
  expect((await shards("table", name)).length).toBe(8);
  // Rows only in shard "100" (hashes from 0x8), and a walk that starts at "001": the three shards
  // before the rows are empty, as many as a page asks, so its first page has no rows.
  const keys = Array.from({ length: 400 }, (_, i) => `k${i}`).filter((k) => firstHex(k) >> 1 === 4).slice(0, 8);
  await table(name, "putMany", keys.map((k, i) => ({ row: doc(k, i) })));
  const query = { order: "shard", limit: 5, budget: 5 };
  const any = await table<Page>(name, "list", query);
  const { f } = JSON.parse(Buffer.from(any.cursor!, "base64url").toString()) as { f: string };
  let cursor: string | null = encodeShardCursor(f, { leaf: "001", after: null, floors: [], start: "001", wrapped: false });
  const first = await table<Page>(name, "list", { ...query, cursor });
  expect(first.rows).toEqual([]);
  expect(first.cursor).not.toBeNull();
  const names: string[] = [];
  for (let i = 0; cursor !== null && i < 50; i++) {
    const page: Page = await table<Page>(name, "list", { ...query, cursor });
    names.push(...page.rows.map((r) => r.row.name));
    cursor = page.cursor;
  }
  expect(names.sort()).toEqual([...keys].sort());
}, 120_000);

test("a shard-order walk through a shard that splits twice under it lists each row once", async () => {
  const name = `walk-twice-${unique()}`;
  const rows = Array.from({ length: 90 }, (_, i) => doc(`r-${String(i).padStart(3, "0")}`, i));
  await table(name, "putMany", rows.map((row) => ({ row })));
  const names: string[] = [];
  const leaves: string[] = [];
  let cursor: string | null = null;
  for (let page = 0; page < 100; page++) {
    const result: Page = await table<Page>(name, "list", { order: "shard", limit: 5, cursor });
    names.push(...result.rows.map((r) => r.row.name));
    cursor = result.cursor;
    if (cursor === null) break;
    const at = (JSON.parse(Buffer.from(cursor, "base64url").toString()) as { s: { leaf: string } }).s.leaf;
    leaves.push(at);
    // The root splits after the walk's second page in it; shard 0 after its second page in 0.
    if (page === 1) expect(await post<boolean>("/split", { cls: "table", name, bits: "" })).toBe(true);
    if (leaves.filter((l) => l === "0").length === 2 && !leaves.includes("00")) expect(await post<boolean>("/split", { cls: "table", name, bits: "0" })).toBe(true);
  }
  expect(leaves).toContain("00");
  expect(names.filter((n, i) => names.indexOf(n) !== i)).toEqual([]);
  expect([...names].sort()).toEqual(rows.map((r) => r.name));
}, 120_000);
