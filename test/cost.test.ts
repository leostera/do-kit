// What calls cost, and the limits they must stay inside. Rows read and written are what a
// Durable Object bills; a value, a page or a batch past SQLite's or RPC's limits must be refused
// up front, by name, not fail deep in a call (or every retry of it); and a value must come back
// as it went in, or be refused.
import { expect, test } from "bun:test";
import { defineTable } from "../src/table/schema.ts";
import { call, post, useWorker } from "./harness.ts";
import type { StatementCost } from "./worker/src/index.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const object = <T = unknown>(cls: string, name: string, bits: string, method: string, ...args: unknown[]) => post<T>("/object", { cls, name, bits, method, args });
const table = <T = unknown>(cls: string, name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls, name, op, args });
const doc = (name: string, i = 0, meta: unknown = null) => ({ name, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta });
type Settled = { ok?: unknown; threw?: string; name?: string | null };
/** What one call cost the shard: the statements it ran, less the fixture's own. */
async function costOf(cls: string, name: string, run: () => Promise<unknown>): Promise<{ read: number; written: number; statements: StatementCost[] }> {
  await object(cls, name, "", "statements", "", true);
  await run();
  const statements = (await object<StatementCost[]>(cls, name, "", "statements", "", true)).filter((s) => !s.query.includes("test_"));
  return { read: statements.reduce((n, s) => n + s.rowsRead, 0), written: statements.reduce((n, s) => n + s.rowsWritten, 0), statements };
}
async function seed(name: string, n: number): Promise<void> {
  for (let i = 0; i < n; i += 250) await table("evolving", name, "putMany", Array.from({ length: Math.min(250, n - i) }, (_, j) => ({ row: doc(`d${i + j}`, i + j) })));
}

// ---- Rows written and read ----

test("a put that changes no indexed column writes one row", async () => {
  const name = `cost-put-${unique()}`;
  await table("evolving", name, "put", doc("a"));
  const { written } = await costOf("evolving", name, () => table("evolving", name, "put", doc("a", 0, { changed: true })));
  expect(written).toBe(1);
}, 30_000);

test("a page reads about the rows it answers, by an index and by the key: not a statement's worth more", async () => {
  // A shard of a wide table is asked for a few rows a page (50 over 64 shards: 4): a page that
  // read in statements of a fixed size read, and copied, up to 16 times the rows it answered.
  const name = `cost-page-${unique()}`;
  await seed(name, 500);
  for (const query of [{ index: "byCreated", direction: "desc", limit: 4 }, { limit: 4 }]) {
    const { read, statements } = await costOf("evolving", name, () => table("evolving", name, "list", query));
    const pages = statements.filter((s) => /^SELECT (t\.\*|\*), /.test(s.query));
    expect(pages.length, JSON.stringify(query)).toBe(1);
    // The rows answered and their lookups (a key table's entry, then its row), and the seek.
    expect(read, JSON.stringify(query)).toBeLessThanOrEqual(3 * 4 + 2);
  }
}, 60_000);

test("a global index's page reads about the entries it answers", async () => {
  const name = `cost-index-page-${unique()}`;
  for (let i = 0; i < 200; i += 50) await post("/indexed", { name, op: "putMany", args: [Array.from({ length: 50 }, (_, j) => ({ row: { id: `u${i + j}`, email: `u${i + j}@example.com`, name: `u${i + j}`, team: "core", createdAt: i + j, active: true } }))] });
  await object("userIndexes", name, "", "statements", "", true);
  await post("/indexed", { name, op: "list", args: [{ index: "byTeam", partition: ["core"], direction: "desc", limit: 4 }] });
  const statements = (await object<StatementCost[]>("userIndexes", name, "", "statements", "", true)).filter((s) => !s.query.includes("test_"));
  expect(statements.reduce((n, s) => n + s.rowsRead, 0)).toBeLessThanOrEqual(2 * 4 + 2);
}, 60_000);

test("a put reads of the row it replaces only its version and the columns its indexes and counts use", async () => {
  const name = `cost-put-reads-${unique()}`;
  await table("evolving", name, "put", doc("a", 0, { big: "x".repeat(10_000) }));
  const { statements } = await costOf("evolving", name, () => table("evolving", name, "put", doc("a", 1, { big: "y".repeat(10_000) })));
  const read = statements.find((s) => s.query.startsWith("SELECT ") && s.query.includes("FROM sharded_table WHERE"));
  expect(read?.query).toBeDefined();
  // Not the row's values: a large one is not copied out of SQLite only to be replaced.
  expect(read!.query).not.toContain("*");
  expect(read!.query).not.toContain('"meta"');
  expect(read!.query).toContain("_version");
}, 30_000);

test("a put copies no text to UTF-8 to check a row's size, in the client or the shard", async () => {
  const name = `cost-put-encodes-${unique()}`;
  await table("evolving", name, "put", doc("a"));
  await post("/encodes", { name });
  await table("evolving", name, "put", doc("a", 1, { body: "é".repeat(1_000) }));
  expect(await post<number>("/encodes", { name })).toBe(0);
}, 30_000);

test("a count reads at most a row after the first, and stays exact through writes and a command that throws", async () => {
  const name = `cost-count-${unique()}`;
  await seed(name, 5_000);
  expect(await table<number>("evolving", name, "count")).toBe(5_000);
  const second = await costOf("evolving", name, () => table("evolving", name, "count"));
  expect(second.read).toBeLessThanOrEqual(1);
  await table("evolving", name, "put", doc("new", 9_999));
  await table("evolving", name, "try:apply", "d1", "boom", null);
  expect(await table<number>("evolving", name, "count")).toBe(5_001);
}, 120_000);

test("a warm get runs no statement for the alarm check when nothing is due", async () => {
  const name = `cost-get-${unique()}`;
  await table("evolving", name, "put", doc("a"));
  await table("evolving", name, "get", "a");
  const { statements } = await costOf("evolving", name, () => table("evolving", name, "get", "a"));
  expect(statements.filter((s) => s.query.includes("key = 'wake'")).map((s) => s.query)).toEqual([]);
}, 30_000);

// ---- Bytes ----

test("a page of large rows is cut by bytes, and the walk lists every row once", async () => {
  const name = `cost-page-bytes-${unique()}`;
  const big = "x".repeat(512 * 1024);
  for (let i = 0; i < 24; i++) await table("table", name, "put", doc(`d${String(i).padStart(2, "0")}`, i, { big }));
  const first = await table<{ rows: unknown[]; cursor: string | null }>("table", name, "list", { index: "byCreated", limit: 24 });
  expect(first.rows.length).toBeLessThan(24);
  expect(first.cursor).not.toBeNull();
  expect((await table<unknown[]>("table", name, "all", { index: "byCreated", limit: 24 })).length).toBe(24);
}, 120_000);

test("a batch of many rows to one shard goes in chunks of at most 1,000", async () => {
  const name = `cost-batch-${unique()}`;
  await table("evolving", name, "put", doc("warm"));
  const before = (await post<{ counters: { calls: number } }>("/describe", { cls: "evolving", name, bits: "" })).counters.calls;
  await table("evolving", name, "putMany", Array.from({ length: 2_500 }, (_, i) => ({ row: doc(`b${i}`, i) })));
  const calls = (await post<{ counters: { calls: number } }>("/describe", { cls: "evolving", name, bits: "" })).counters.calls - before;
  expect(calls).toBeGreaterThanOrEqual(3);
}, 120_000);

test("allRows refuses a shard past its row cap, and names ownedRowsPage", async () => {
  const name = `cost-all-rows-${unique()}`;
  const keys = Array.from({ length: 10_001 }, (_, i) => `k${i}`);
  for (let i = 0; i < keys.length; i += 500) await call("counters", name, "add", keys.slice(i, i + 500), keys.slice(i, i + 500).map(() => 1));
  const answer = await object<number>("counters", name, "", "allRowsCount").then(String, (e: Error) => e.message);
  expect(answer).toContain("ownedRowsPage");
}, 120_000);

// ---- Limits, checked where they are declared ----

const columns = (n: number) => Object.fromEntries([["id", "text"], ...Array.from({ length: n }, (_, i) => [`c${i}`, "integer"])]) as Record<string, "text" | "integer">;

test("defineTable refuses a schema past SQLite's 100 columns, counting the hidden ones", () => {
  // id and 97 more, with _hash and _version: 100, as many as SQLite takes (measured in workerd).
  expect(() => defineTable({ primaryKey: "id", columns: columns(97) } as never)).not.toThrow();
  expect(() => defineTable({ primaryKey: "id", columns: columns(98) } as never)).toThrow(/100/);
  // Indexes keep their keys in tables of their own, and take no column.
  expect(() => defineTable({ primaryKey: "id", columns: columns(97), indexes: { a: ["c0"], b: ["c1"], c: ["c2"] } } as never)).not.toThrow();
});

test("a listing refuses more than 32 conditions", async () => {
  const name = `cost-conditions-${unique()}`;
  await table("table", name, "put", doc("a"));
  const where = Array.from({ length: 33 }, () => ({ column: "deleted", op: "=", value: false }));
  const answer = await post<Settled>("/table", { cls: "table", name, op: "try:list", args: [{ index: "byCreated", where, limit: 5 }] });
  expect(answer.threw ?? "").toContain("32");
}, 30_000);

test("put refuses a key that is not well-formed UTF-16, and writes nothing", async () => {
  const name = `cost-surrogate-${unique()}`;
  const answer = await post<Settled>("/table", { cls: "table", name, op: "try:put", args: [doc("x\uD800")] });
  expect(answer.threw).toBeDefined();
  expect(await table<number>("table", name, "count")).toBe(0);
}, 30_000);

for (const kind of ["nan", "undefined", "date", "big"] as const) {
  test(`put refuses a row that would not come back as it went in (${kind})`, async () => {
    const name = `cost-odd-${kind}-${unique()}`;
    const answer = await post<Settled>("/table", { cls: "table", name, op: "putOdd", args: [kind, "odd"] });
    expect(answer.threw ?? "").toMatch(kind === "big" ? /createdAt/ : /meta/);
    expect(await table<number>("table", name, "count")).toBe(0);
  }, 30_000);
}

for (const kind of ["nan", "undefined"] as const) {
  test(`a sharded object refuses a value that is not JSON (${kind})`, async () => {
    const name = `cost-odd-value-${kind}-${unique()}`;
    const answer = await post<Settled>("/callSettled", { cls: "counters", name, method: "putOdd", keys: ["a"], payloads: [kind] });
    expect(answer.threw ?? "").toMatch(/not JSON/);
  }, 30_000);
}

test("put refuses a row past maxRowBytes with RowTooLarge, and writes nothing", async () => {
  const name = `cost-row-size-${unique()}`;
  const answer = await post<Settled>("/table", { cls: "table", name, op: "try:put", args: [doc("huge", 0, { big: "x".repeat(3 * 1024 * 1024) })] });
  expect(answer.name).toBe("RowTooLarge");
  expect(await table<number>("table", name, "count")).toBe(0);
}, 60_000);

// ---- Background deletes ----

test("dropping a unique global index clears its fences in slices", async () => {
  const name = `cost-fences-${unique()}`;
  const ev = (op: string, args: unknown[], version?: number) => post("/indexed", { cls: "evolving", name, op, args, version });
  await ev("upgrade", [""], 2);
  for (let i = 0; i < 1_000; i += 100) await ev("putMany", [Array.from({ length: 100 }, (_, j) => ({ row: { id: `u${i + j}`, email: `u${i + j}@example.com`, name: `u${i + j}`, team: "core", createdAt: 1, active: true } }))], 2);
  await object("evolvingUsers", name, "", "statements", "", true);
  await ev("upgrade", [""], 3);
  await ev("get", ["u1"], 3);
  await Bun.sleep(3_000);
  const deletes = await object<StatementCost[]>("evolvingUsers", name, "", "statements", "DELETE FROM indexed_fences", true);
  expect(deletes.length).toBeGreaterThan(0);
  expect(Math.max(...deletes.map((s) => s.rowsWritten))).toBeLessThanOrEqual(260);
}, 120_000);

// ---- Load ----

test("a shard flooded with full-budget listings refuses some of them", async () => {
  const name = `cost-flood-${unique()}`;
  await seed(name, 10_000);
  await Promise.all(Array.from({ length: 50 }, () => table("evolving", name, "list", { index: "byCreated", where: [{ column: "name", op: "contains", value: "zzz" }], budget: 10_000, limit: 10 }).catch(() => undefined)));
  expect((await post<{ counters: { notReady: number } }>("/describe", { cls: "evolving", name, bits: "" })).counters.notReady).toBeGreaterThan(0);
}, 180_000);
