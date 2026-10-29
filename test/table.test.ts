// ShardedTable under cf dev: conditional writes and versions, listings every way checked against a
// reference sort while the table splits and cleans up, sparse filters with a small budget, counts,
// and schema changes on a live object.
import { expect, test } from "bun:test";
import { post, shards, useWorker } from "./harness.ts";

useWorker();

type Repo = { name: string; id: string; createdAt: number; lastPushAt: number | null; deleted: boolean; meta: unknown };
type Versioned = { row: Repo; version: number };
type Page = { rows: Versioned[]; cursor: string | null };
type Query = { index?: string; direction?: "asc" | "desc"; where?: { column: string; op: string; value?: unknown }[]; limit?: number; budget?: number; cursor?: string | null };

const op = <T>(name: string, operation: string, ...args: unknown[]) => post<T>("/table", { cls: "table", name, op: operation, args });
const table = <T>(cls: string, name: string, operation: string, ...args: unknown[]) => post<T>("/table", { cls, name, op: operation, args });
const split = (name: string, bits: string) => post<boolean>("/split", { cls: "table", name, bits });

/** Rows with repeated and missing sort values, and names whose UTF-16 and code-point orders differ. */
function makeRepos(n: number): Repo[] {
  const special = ["x\uFFFD", "x😀", "é-repo", "Zeta", "alpha"];
  return Array.from({ length: n }, (_, i) => ({
    name: i < special.length ? special[i]! : `repo-${String(i).padStart(4, "0")}`,
    id: `id-${i}`,
    createdAt: 1_000 + ((i * 7919) % 97), // many ties
    lastPushAt: i % 5 === 0 ? null : 5_000 + ((i * 104729) % 211),
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

const INDEXES: Record<string, (keyof Repo)[]> = { primary: [], byCreated: ["createdAt"], byPush: ["lastPushAt"], byDeletedCreated: ["deleted", "createdAt"] };

function expected(rows: Repo[], query: Query): string[] {
  const cols = [...INDEXES[query.index ?? "primary"]!, "name"] as (keyof Repo)[];
  const sign = query.direction === "desc" ? -1 : 1;
  const matches = (r: Repo) =>
    (query.where ?? []).every((c) => {
      const v = r[c.column as keyof Repo];
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
  { index: "byPush", limit: 9 },
  { index: "byPush", direction: "desc", limit: 9 },
  { index: "byDeletedCreated", direction: "desc", where: [{ column: "deleted", op: "=", value: false }], limit: 11 },
  { index: "byCreated", where: [{ column: "name", op: "contains", value: "REPO-00" }], limit: 4 },
];

test("puts, patches and deletes are conditional on the row's version", async () => {
  const name = `versions-${Date.now()}`;
  const repo: Repo = { name: "foo", id: "r1", createdAt: 1, lastPushAt: null, deleted: false, meta: { tags: ["a"] } };
  expect(await op(name, "put", repo, { ifVersion: 0 })).toEqual({ ok: true, version: 1 });
  expect(await op(name, "put", repo, { ifVersion: 0 })).toEqual({ ok: false, version: 1 });
  expect(await op(name, "get", "foo")).toEqual({ row: repo, version: 1 });
  expect(await op(name, "patch", "foo", { lastPushAt: 9 }, { ifVersion: 1 })).toEqual({ ok: true, version: 2 });
  expect(await op(name, "patch", "nope", { lastPushAt: 9 })).toEqual({ ok: false, version: 0 });
  expect(await op(name, "delete", "foo", { ifVersion: 1 })).toEqual({ ok: false, version: 2 });
  expect(((await op<Versioned>(name, "get", "foo")).row as Repo).lastPushAt).toBe(9);
  expect(await op(name, "delete", "foo", { ifVersion: 2 })).toEqual({ ok: true, version: 0 });
  expect(await op(name, "get", "foo")).toBeNull();
  expect(await op(name, "count")).toBe(0);
  // Unconditional: always writes.
  expect(await op(name, "put", repo)).toEqual({ ok: true, version: 1 });
  expect(await op(name, "put", repo)).toEqual({ ok: true, version: 2 });
}, 60_000);

test("listings match a reference sort, before, across and after splits", async () => {
  const name = `listing-${Date.now()}`;
  const rows = makeRepos(160);
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
  const rows = makeRepos(120);
  await op(name, "putMany", rows.map((row) => ({ row })));
  expect(await split(name, "")).toBe(true);
  const query: Query = { index: "byCreated", where: [{ column: "name", op: "contains", value: "repo-011" }], limit: 3, budget: 5 };
  const result = await walk(name, query);
  expect(result).toEqual(expected(rows, query));
  expect(result.length).toBe(10);
}, 60_000);

test("count follows inserts, deletes and splits", async () => {
  const name = `count-${Date.now()}`;
  const rows = makeRepos(100);
  await op(name, "putMany", rows.map((row) => ({ row })));
  expect(await split(name, "")).toBe(true);
  expect(await op(name, "count")).toBe(100);
  for (const row of rows.slice(0, 30)) await op(name, "delete", row.name);
  expect(await op(name, "count")).toBe(70);
  expect(await split(name, "1")).toBe(true);
  expect(await op(name, "count")).toBe(70);
}, 60_000);

test("a cursor only continues the query it came from", async () => {
  const name = `cursor-${Date.now()}`;
  await op(name, "putMany", makeRepos(20).map((row) => ({ row })));
  const page = await op<Page>(name, "list", { index: "byCreated", limit: 5 });
  expect(page.cursor).not.toBeNull();
  await expect(op(name, "list", { index: "byPush", limit: 5, cursor: page.cursor })).rejects.toThrow("another query");
}, 60_000);

test("a new schema adds columns and swaps indexes on a live object", async () => {
  const name = `evolve-${Date.now()}`;
  const evolve = <T>(operation: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op: operation, args });
  const rows = makeRepos(12);
  await evolve("putMany", rows.map((row) => ({ row })));
  const before = await evolve<string[]>("indexNames");
  expect(before.some((n) => n.includes("_byPush_"))).toBe(true);
  await evolve("upgrade");
  // Old rows read with the new column empty; new rows use it; the new index lists them.
  expect(((await evolve<{ row: Record<string, unknown> }>("get", rows[0]!.name)).row).stars).toBeNull();
  await evolve("put", { ...rows[1]!, stars: 5 });
  await evolve("put", { ...rows[2]!, stars: 1 });
  const after = await evolve<string[]>("indexNames");
  expect(after.some((n) => n.includes("_byPush_"))).toBe(false);
  expect(after.some((n) => n.includes("_byStars_"))).toBe(true);
  const byStars = await evolve<Page>("list", { index: "byStars", direction: "desc", where: [{ column: "stars", op: "notNull" }] });
  expect(byStars.rows.map((r) => r.row.name)).toEqual([rows[1]!.name, rows[2]!.name]);
}, 60_000);

test("a split succeeds after an abandoned attempt left a copy with an older schema", async () => {
  const name = `flaky-${Date.now()}`;
  for (let i = 0; i < 40; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastPushAt: null, deleted: false, meta: null });
  // The first attempt copies the object, then cannot claim the copy, and is abandoned.
  await table("flaky", name, "failClones", true);
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(false);
  // A deploy adds a column; the object migrates on its next write.
  await table("flaky", name, "upgrade");
  await table("flaky", name, "failClones", false);
  await table("flaky", name, "put", { name: "r0", id: "id0", createdAt: 0, lastPushAt: null, deleted: false, meta: null, stars: 5 });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  const rows = await table<{ row: { name: string; stars: number | null } }[]>("flaky", name, "all", {});
  expect(rows.length).toBe(40);
  expect(rows.find((r) => r.row.name === "r0")!.row.stars).toBe(5);
}, 60_000);

test("a new shard is activated once it can be, however long activation kept failing", async () => {
  const name = `activate-${Date.now()}`;
  for (let i = 0; i < 40; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastPushAt: null, deleted: false, meta: null });
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
  for (let i = 0; i < 20; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastPushAt: null, deleted: false, meta: null });
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
