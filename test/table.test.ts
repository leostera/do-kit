// ShardedTable under cf dev: conditional writes and versions, listings every way checked against a
// reference sort while the table splits and cleans up, sparse filters with a small budget, counts,
// and schema changes on a live object.
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { post, shards, useWorker } from "./harness.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
type Versioned = { row: Doc; version: number };
type Page = { rows: Versioned[]; cursor: string | null };
type Query = { index?: string; direction?: "asc" | "desc"; where?: { column: string; op: string; value?: unknown }[]; limit?: number; budget?: number; cursor?: string | null };

const op = <T>(name: string, operation: string, ...args: unknown[]) => post<T>("/table", { cls: "table", name, op: operation, args });
const table = <T>(cls: string, name: string, operation: string, ...args: unknown[]) => post<T>("/table", { cls, name, op: operation, args });
const split = (name: string, bits: string) => post<boolean>("/split", { cls: "table", name, bits });

/** Rows with repeated and missing sort values, and names whose UTF-16 and code-point orders differ. */
function makeDocs(n: number): Doc[] {
  const special = ["x\uFFFD", "x😀", "é-doc", "Zeta", "alpha"];
  return Array.from({ length: n }, (_, i) => ({
    name: i < special.length ? special[i]! : `doc-${String(i).padStart(4, "0")}`,
    id: `id-${i}`,
    createdAt: 1_000 + ((i * 7919) % 97), // many ties
    lastEditAt: i % 5 === 0 ? null : 5_000 + ((i * 104729) % 211),
    deleted: i % 7 === 0,
    meta: { i },
  }));
}

/** SQLite's order: null, numbers, text by code point; then the key. */
function sqliteCompare(a: unknown, b: unknown): number {
  const rank = (v: unknown) => (v === null ? 0 : typeof v === "number" || typeof v === "boolean" ? 1 : 2);
  if (rank(a) !== rank(b)) return rank(a) - rank(b);
  if (a === null) return 0;
  if (rank(a) === 1) return Number(a) - Number(b);
  const ca = [...(a as string)].map((c) => c.codePointAt(0)!);
  const cb = [...(b as string)].map((c) => c.codePointAt(0)!);
  for (let i = 0; i < Math.min(ca.length, cb.length); i++) if (ca[i] !== cb[i]) return ca[i]! - cb[i]!;
  return ca.length - cb.length;
}

const INDEXES: Record<string, (keyof Doc)[]> = { primary: [], byCreated: ["createdAt"], byEdit: ["lastEditAt"], byDeletedCreated: ["deleted", "createdAt"] };

function expected(rows: Doc[], query: Query): string[] {
  const cols = [...INDEXES[query.index ?? "primary"]!, "name"] as (keyof Doc)[];
  const sign = query.direction === "desc" ? -1 : 1;
  const matches = (r: Doc) =>
    (query.where ?? []).every((c) => {
      const v = r[c.column as keyof Doc];
      if (c.op === "=") return v === c.value;
      if (c.op === "contains") return typeof v === "string" && v.toLowerCase().includes(String(c.value).toLowerCase());
      if (c.op === "notNull") return v !== null;
      throw new Error(`no reference for ${c.op}`);
    });
  return rows
    .filter(matches)
    .sort((a, b) => {
      for (const c of cols) {
        const d = sqliteCompare(a[c], b[c]);
        if (d !== 0) return sign * d;
      }
      return 0;
    })
    .map((r) => r.name);
}

/** Pages through a query; `between(page)` runs after each page (to split mid-listing). */
async function walk(name: string, query: Query, between: (page: number) => Promise<void> = async () => {}): Promise<string[]> {
  const names: string[] = [];
  let cursor: string | null = null;
  for (let page = 0; ; page++) {
    const result: Page = await op<Page>(name, "list", { ...query, cursor });
    names.push(...result.rows.map((r) => r.row.name));
    cursor = result.cursor;
    if (cursor === null) return names;
    if (page > 500) throw new Error("listing did not end");
    await between(page);
  }
}

const QUERIES: Query[] = [
  { limit: 7 },
  { index: "byCreated", direction: "desc", limit: 7 },
  { index: "byEdit", limit: 9 },
  { index: "byEdit", direction: "desc", limit: 9 },
  { index: "byDeletedCreated", direction: "desc", where: [{ column: "deleted", op: "=", value: false }], limit: 11 },
  { index: "byCreated", where: [{ column: "name", op: "contains", value: "DOC-00" }], limit: 4 },
];

test("puts, patches and deletes are conditional on the row's version", async () => {
  const name = `versions-${Date.now()}`;
  const doc: Doc = { name: "foo", id: "r1", createdAt: 1, lastEditAt: null, deleted: false, meta: { tags: ["a"] } };
  expect(await op(name, "put", doc, { ifVersion: 0 })).toEqual({ ok: true, version: 1 });
  expect(await op(name, "put", doc, { ifVersion: 0 })).toEqual({ ok: false, version: 1 });
  expect(await op(name, "get", "foo")).toEqual({ row: doc, version: 1 });
  expect(await op(name, "patch", "foo", { lastEditAt: 9 }, { ifVersion: 1 })).toEqual({ ok: true, version: 2 });
  expect(await op(name, "patch", "nope", { lastEditAt: 9 })).toEqual({ ok: false, version: 0 });
  expect(await op(name, "delete", "foo", { ifVersion: 1 })).toEqual({ ok: false, version: 2 });
  expect(((await op<Versioned>(name, "get", "foo")).row as Doc).lastEditAt).toBe(9);
  expect(await op(name, "delete", "foo", { ifVersion: 2 })).toEqual({ ok: true, version: 0 });
  expect(await op(name, "get", "foo")).toBeNull();
  expect(await op(name, "count")).toBe(0);
  // Unconditional: always writes. A row written again after a delete gets a version it never had.
  expect(await op(name, "put", doc)).toEqual({ ok: true, version: 3 });
  expect(await op(name, "put", doc)).toEqual({ ok: true, version: 4 });
}, 60_000);

test("a write conditional on a version from before a delete fails on the row written since", async () => {
  const name = `aba-${Date.now()}`;
  const doc: Doc = { name: "foo", id: "r1", createdAt: 1, lastEditAt: null, deleted: false, meta: null };
  expect(await op(name, "put", doc, { ifVersion: 0 })).toEqual({ ok: true, version: 1 });
  // A client reads version 1. Meanwhile the row is deleted and written again by someone else.
  expect(await op(name, "delete", "foo")).toEqual({ ok: true, version: 0 });
  const again = await op<{ ok: boolean; version: number }>(name, "put", { ...doc, id: "r2" }, { ifVersion: 0 });
  expect(again.ok).toBe(true);
  expect(again.version).toBeGreaterThan(1);
  // The client's write, conditional on what it read, must not land on the new row.
  expect((await op<{ ok: boolean }>(name, "patch", "foo", { lastEditAt: 5 }, { ifVersion: 1 })).ok).toBe(false);
  expect(((await op<Versioned>(name, "get", "foo")).row as Doc).id).toBe("r2");
  // A split's shards keep that guarantee for the keys they own.
  expect(await op(name, "delete", "foo")).toEqual({ ok: true, version: 0 });
  expect(await split(name, "")).toBe(true);
  expect((await op<{ version: number }>(name, "put", doc)).version).toBeGreaterThan(again.version);
}, 60_000);

test("listings match a reference sort, before, across and after splits", async () => {
  const name = `listing-${Date.now()}`;
  const rows = makeDocs(160);
  for (let i = 0; i < rows.length; i += 40) await op(name, "putMany", rows.slice(i, i + 40).map((row) => ({ row })));
  expect(await op(name, "count")).toBe(rows.length);

  for (const query of QUERIES) expect(await walk(name, query)).toEqual(expected(rows, query));
  await op(name, "scan", "pauseCleanup", true);

  // Split the root after the first page of every listing: the cursor carries on across it.
  let first = true;
  const splitOnce = async () => {
    if (!first) return;
    first = false;
    expect(await split(name, "")).toBe(true);
  };
  expect(await walk(name, QUERIES[1]!, splitOnce)).toEqual(expected(rows, QUERIES[1]!));
  // Cleanup is held back: both shards store every row, and each must be listed once.
  expect(await op<number[]>(name, "scan", "storedRows")).toEqual([rows.length, rows.length]);
  for (const query of QUERIES) expect(await walk(name, query)).toEqual(expected(rows, query));
  expect(await op(name, "count")).toBe(rows.length);

  // Two more levels, mid-listing again.
  let step = 0;
  const splitMore = async () => {
    const at = step++;
    if (at === 1) expect(await split(name, "0")).toBe(true);
    if (at === 2) expect(await split(name, "1")).toBe(true);
  };
  expect(await walk(name, QUERIES[3]!, splitMore)).toEqual(expected(rows, QUERIES[3]!));
  expect((await shards("table", name)).length).toBe(4);
  for (const query of QUERIES) expect(await walk(name, query)).toEqual(expected(rows, query));
  expect(await op(name, "count")).toBe(rows.length);

  // Cleanup runs: each shard keeps only its own rows, and nothing changes for listings.
  await op(name, "scan", "pauseCleanup", false);
  for (let i = 0; ; i++) {
    const stored = await op<number[]>(name, "scan", "storedRows");
    if (stored.reduce((a, b) => a + b, 0) === rows.length) break;
    if (i > 200) throw new Error(`cleanup did not finish: ${stored}`);
    await Bun.sleep(50);
  }
  for (const query of QUERIES) expect(await walk(name, query)).toEqual(expected(rows, query));
  expect(await op(name, "count")).toBe(rows.length);
}, 120_000);

test("a sparse filter with a small budget returns short pages but every match", async () => {
  const name = `sparse-${Date.now()}`;
  const rows = makeDocs(120);
  await op(name, "putMany", rows.map((row) => ({ row })));
  expect(await split(name, "")).toBe(true);
  const query: Query = { index: "byCreated", where: [{ column: "name", op: "contains", value: "doc-011" }], limit: 3, budget: 5 };
  const result = await walk(name, query);
  expect(result).toEqual(expected(rows, query));
  expect(result.length).toBe(10);
}, 60_000);

test("count follows inserts, deletes and splits", async () => {
  const name = `count-${Date.now()}`;
  const rows = makeDocs(100);
  await op(name, "putMany", rows.map((row) => ({ row })));
  expect(await split(name, "")).toBe(true);
  expect(await op(name, "count")).toBe(100);
  for (const row of rows.slice(0, 30)) await op(name, "delete", row.name);
  expect(await op(name, "count")).toBe(70);
  expect(await split(name, "1")).toBe(true);
  expect(await op(name, "count")).toBe(70);
}, 60_000);

type Counted = { value: unknown; count: number }[];
const byValue = (counts: Counted) => Object.fromEntries(counts.map((c) => [String(c.value), c.count]));
const reference = (rows: Map<string, Doc>) => {
  const out: Record<string, number> = {};
  for (const r of rows.values()) out[String(r.deleted)] = (out[String(r.deleted)] ?? 0) + 1;
  return out;
};

test("counts per value follow puts, patches, deletes and splits, and come with a listing", async () => {
  const name = `count-by-${Date.now()}`;
  const rows = new Map(makeDocs(120).map((r) => [r.name, r]));
  await op(name, "putMany", [...rows.values()].map((row) => ({ row })));
  const check = async () => {
    expect(byValue(await op<Counted>(name, "countBy", "byDeleted"))).toEqual(reference(rows));
    const page = await op<Page & { total: number; counts: Counted }>(name, "list", { index: "byCreated", where: [{ column: "deleted", op: "=", value: false }], limit: 5, withCount: "byDeleted" });
    expect(page.rows).toHaveLength(5);
    expect(page.total).toBe(rows.size);
    expect(byValue(page.counts)).toEqual(reference(rows));
    expect((await op<Page & { total: number; counts?: Counted }>(name, "list", { limit: 3, withCount: true })).total).toBe(rows.size);
  };
  await check();

  const names = [...rows.keys()];
  // Patch some live rows to deleted, put whole rows back as live, delete a few of each.
  for (const key of names.slice(1, 9)) {
    await op(name, "patch", key, { deleted: true });
    rows.set(key, { ...rows.get(key)!, deleted: true });
  }
  for (const key of names.filter((k) => rows.get(k)!.deleted).slice(0, 4)) {
    await op(name, "put", { ...rows.get(key)!, deleted: false });
    rows.set(key, { ...rows.get(key)!, deleted: false });
  }
  await op(name, "patch", names[20]!, { lastEditAt: 1 }); // not a counted column
  rows.set(names[20]!, { ...rows.get(names[20]!)!, lastEditAt: 1 });
  for (const key of names.slice(30, 45)) {
    await op(name, "delete", key);
    rows.delete(key);
  }
  await check();

  // Splits with cleanup held back: both shards store rows they gave away, counted once.
  await op(name, "scan", "pauseCleanup", true);
  expect(await split(name, "")).toBe(true);
  await check();
  expect(await split(name, "1")).toBe(true);
  await check();
  await op(name, "scan", "pauseCleanup", false);
  for (let i = 0; ; i++) {
    const stored = await op<number[]>(name, "scan", "storedRows");
    if (stored.reduce((a, b) => a + b, 0) === rows.size) break;
    if (i > 200) throw new Error(`cleanup did not finish: ${stored}`);
    await Bun.sleep(50);
  }
  await check();
  await expect(op(name, "countBy", "nope")).rejects.toThrow("no count group nope");
}, 120_000);

test("a new count group counts the rows already stored", async () => {
  const name = `count-group-evolve-${Date.now()}`;
  const evolve = <T>(operation: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op: operation, args });
  const rows = makeDocs(40);
  await evolve("putMany", rows.map((row) => ({ row })));
  await evolve("upgrade");
  const deleted = rows.filter((r) => r.deleted).length;
  expect(byValue(await evolve<Counted>("countBy", "deletedAgain"))).toEqual({ false: rows.length - deleted, true: deleted });
  expect(byValue(await evolve<Counted>("countBy", "byStars"))).toEqual({ null: rows.length });
  await evolve("patch", rows[1]!.name, { stars: 3 });
  expect(byValue(await evolve<Counted>("countBy", "byStars"))).toEqual({ null: rows.length - 1, 3: 1 });
  await expect(evolve("countBy", "byDeleted")).rejects.toThrow("no count group byDeleted");
}, 60_000);

test("a cursor only continues the query it came from", async () => {
  const name = `cursor-${Date.now()}`;
  await op(name, "putMany", makeDocs(20).map((row) => ({ row })));
  const page = await op<Page>(name, "list", { index: "byCreated", limit: 5 });
  expect(page.cursor).not.toBeNull();
  await expect(op(name, "list", { index: "byEdit", limit: 5, cursor: page.cursor })).rejects.toThrow("another query");
}, 60_000);

test("a new schema adds columns and swaps indexes on a live object", async () => {
  const name = `evolve-${Date.now()}`;
  const evolve = <T>(operation: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op: operation, args });
  const rows = makeDocs(12);
  await evolve("putMany", rows.map((row) => ({ row })));
  const before = await evolve<string[]>("indexNames");
  expect(before.some((n) => n.includes("_byEdit_"))).toBe(true);
  await evolve("upgrade");
  // Old rows read with the new column empty; new rows use it; the new index lists them.
  expect(((await evolve<{ row: Record<string, unknown> }>("get", rows[0]!.name)).row).stars).toBeNull();
  await evolve("put", { ...rows[1]!, stars: 5 });
  await evolve("put", { ...rows[2]!, stars: 1 });
  const after = await evolve<string[]>("indexNames");
  expect(after.some((n) => n.includes("_byEdit_"))).toBe(false);
  expect(after.some((n) => n.includes("_byStars_"))).toBe(true);
  const byStars = await evolve<Page>("list", { index: "byStars", direction: "desc", where: [{ column: "stars", op: "notNull" }] });
  expect(byStars.rows.map((r) => r.row.name)).toEqual([rows[1]!.name, rows[2]!.name]);
}, 60_000);

test("a split succeeds after an abandoned attempt left a copy with an older schema", async () => {
  const name = `flaky-${Date.now()}`;
  for (let i = 0; i < 40; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
  // The first attempt copies the object, then cannot claim the copy, and is abandoned.
  await table("flaky", name, "failClones", true);
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(false);
  // A deploy adds a column; the object migrates on its next write.
  await table("flaky", name, "upgrade");
  await table("flaky", name, "failClones", false);
  await table("flaky", name, "put", { name: "r0", id: "id0", createdAt: 0, lastEditAt: null, deleted: false, meta: null, stars: 5 });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  const rows = await table<{ row: { name: string; stars: number | null } }[]>("flaky", name, "all", {});
  expect(rows.length).toBe(40);
  expect(rows.find((r) => r.row.name === "r0")!.row.stars).toBe(5);
}, 60_000);

test("a new shard is activated once it can be, however long activation kept failing", async () => {
  const name = `activate-${Date.now()}`;
  for (let i = 0; i < 40; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
  // The copy inherits the flag: the new shard refuses to start serving.
  await table("flaky", name, "failActivations", true, "");
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  // Long enough for the runtime to give up retrying a failing alarm.
  await Bun.sleep(Number(process.env.ACTIVATE_FAIL_MS ?? 20_000));
  expect((await post<{ state: string }>("/describe", { cls: "flaky", name, bits: "1" })).state).toBe("copy");
  await table("flaky", name, "failActivations", false, "1");
  for (let i = 0; i < 50; i++) {
    if ((await post<{ state: string }>("/describe", { cls: "flaky", name, bits: "1" })).state === "shard") break;
    await Bun.sleep(100);
  }
  expect((await post<{ state: string }>("/describe", { cls: "flaky", name, bits: "1" })).state).toBe("shard");
  expect((await table<unknown[]>("flaky", name, "all", {})).length).toBe(40);
}, 120_000);

test("a split's copy that is not claimed never runs the subclass's alarm, even when an alarm fires in it", async () => {
  const name = `copy-alarm-${Date.now()}`;
  for (let i = 0; i < 20; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
  await table("flaky", name, "wakeIn", 60_000, ""); // the original has work scheduled, copied with it
  await table("flaky", name, "failClones", true);
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(false);
  // The abandoned copy holds the original's storage. An alarm fires there (a snapshot carries it).
  await table("flaky", name, "alarmNow", null, "1");
  await Bun.sleep(1000);
  const copy = await table<{ self: string; ran: string[] }>("flaky", name, "alarmRan", null, "1");
  expect(copy.ran).not.toContain(copy.self);
  // The original's onAlarm runs when its time comes.
  await table("flaky", name, "wakeIn", 0, "");
  await Bun.sleep(1000);
  const original = await table<{ self: string; ran: string[] }>("flaky", name, "alarmRan", null, "");
  expect(original.ran).toContain(original.self);
}, 60_000);

test("a new index on a table with many rows lists every row", async () => {
  const name = `evolve-many-${Date.now()}`;
  const evolve = <T>(operation: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op: operation, args });
  const rows = makeDocs(1234);
  for (let i = 0; i < rows.length; i += 200) await evolve("putMany", rows.slice(i, i + 200).map((row) => ({ row })));
  await evolve("upgrade"); // adds byStars: every existing row needs its key in it
  const listed = await evolve<Versioned[]>("all", { index: "byStars", limit: 1000 });
  expect(listed.length).toBe(rows.length);
  expect(new Set(listed.map((r) => r.row.name)).size).toBe(rows.length);
}, 120_000);

test("a shard whose alarm was lost while a new shard waits to be activated sets it again", async () => {
  const name = `lost-alarm-${Date.now()}`;
  for (let i = 0; i < 20; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
  await table("flaky", name, "failActivations", true, "");
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  // The original's alarm, which would retry the activation, is lost; then activation would work.
  await table("flaky", name, "dropAlarm", null, "");
  await table("flaky", name, "failActivations", false, "1");
  await Bun.sleep(1500);
  expect((await post<{ state: string }>("/describe", { cls: "flaky", name, bits: "1" })).state).toBe("copy");
  // Calls keep reaching the original: it notices nothing will activate the new shard.
  const stays = Array.from({ length: 20 }, (_, i) => `r${i}`).find((k) => (Number.parseInt(createHash("sha256").update(k).digest("hex")[0]!, 16) >> 3) === 0)!;
  for (let i = 0; i < 80; i++) {
    await table("flaky", name, "get", stays);
    if ((await post<{ state: string }>("/describe", { cls: "flaky", name, bits: "1" })).state === "shard") break;
    await Bun.sleep(100);
  }
  expect((await post<{ state: string }>("/describe", { cls: "flaky", name, bits: "1" })).state).toBe("shard");
  expect((await table<unknown[]>("flaky", name, "all", {})).length).toBe(20);
}, 60_000);
