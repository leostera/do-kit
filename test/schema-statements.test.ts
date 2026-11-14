// What a schema change costs, statement by statement. One statement over every row of a shard
// blocks it for as long as it takes, and on a large shard takes longer than a call may: a build
// must be steps that each touch a page of rows, seek to where they start, and run from the
// shard's alarm, so the shard keeps serving and the build ends without traffic.
import { expect, test } from "bun:test";
import { call, post, useWorker } from "./harness.ts";
import type { StatementCost } from "./worker/src/index.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op, args });
const object = <T = unknown>(cls: string, name: string, method: string, ...args: unknown[]) => post<T>("/object", { cls, name, bits: "", method, args });
const statements = (cls: string, name: string, filter = "", reset = true) => object<StatementCost[]>(cls, name, "statements", filter, reset);
const docs = (n: number) => Array.from({ length: n }, (_, i) => ({ name: `doc-${String(i).padStart(5, "0")}`, id: `id-${i}`, createdAt: i, lastEditAt: null, deleted: i % 2 === 0, meta: null }));
/** EvolvingDocs builds 300 rows a step: a statement may touch two steps' worth, and a few more. */
const STEP_BOUND = 2 * 300 + 50;
const cost = (s: StatementCost) => s.rowsRead + s.rowsWritten;
const worst = (all: StatementCost[]) => all.reduce((w, s) => (cost(s) > cost(w) ? s : w), { query: "", rowsRead: 0, rowsWritten: 0, plan: [] });

async function seed(name: string, n: number): Promise<ReturnType<typeof docs>> {
  const rows = docs(n);
  for (let i = 0; i < rows.length; i += 150) await table(name, "putMany", rows.slice(i, i + 150).map((row) => ({ row })));
  return rows;
}
/** Deploys `version` and runs its whole migration (the client retries not ready until it is done). */
async function deploy(name: string, version: number): Promise<StatementCost[]> {
  await table(name, "upgrade", version, "");
  await table(name, "get", "doc-00000");
  return statements("evolving", name);
}

test("no statement of a new index's build touches more than a step of rows", async () => {
  const name = `build-statements-${unique()}`;
  await seed(name, 3_000);
  const w = worst(await deploy(name, 2));
  expect({ query: w.query.slice(0, 80), cost: cost(w) }).toEqual({ query: w.query.slice(0, 80), cost: Math.min(cost(w), STEP_BOUND) });
}, 120_000);

test("an index build's pages seek to where they start, not scan from the first row", async () => {
  const name = `build-seek-${unique()}`;
  await seed(name, 3_000);
  const pages = (await deploy(name, 2)).filter((s) => /^SELECT .* FROM sharded_table WHERE/.test(s.query) && s.query.includes("ORDER BY"));
  expect(pages.length).toBeGreaterThan(5);
  expect(pages.filter((p) => p.plan.some((d) => d.startsWith("SCAN"))).map((p) => p.query.slice(0, 60))).toEqual([]);
  expect(Math.max(...pages.map((p) => p.rowsRead))).toBeLessThanOrEqual(STEP_BOUND);
}, 120_000);

test("a new index's key table is made empty, and its keys go in a page at a time", async () => {
  const name = `build-keys-${unique()}`;
  await seed(name, 1_000);
  const all = await deploy(name, 2);
  const create = all.findIndex((s) => s.query.startsWith("CREATE TABLE") && s.query.includes("_keys_byStars"));
  const fills = all.filter((s) => s.query.startsWith("INSERT OR REPLACE INTO") && s.query.includes("_keys_byStars"));
  const fill = all.findIndex((s) => fills.includes(s));
  expect(create).toBeGreaterThanOrEqual(0);
  expect(fill).toBeGreaterThan(create);
  expect(Math.max(...fills.map(cost))).toBeLessThanOrEqual(STEP_BOUND);
}, 120_000);

test("a rollback runs no statement that rewrites every row", async () => {
  const name = `rollback-statements-${unique()}`;
  await seed(name, 3_000);
  await deploy(name, 2);
  const all = await deploy(name, 1);
  expect(all.filter((s) => s.query.includes("DROP COLUMN")).map((s) => s.query)).toEqual([]);
  expect(cost(worst(all))).toBeLessThanOrEqual(STEP_BOUND);
}, 120_000);

test("an index declared again before its dropped keys are deleted starts empty, and every statement stays within a step", async () => {
  const name = `redeclare-${unique()}`;
  // Steps by calls only, and none between the deploys: the dropped index's keys are all still there.
  await object("evolving", name, "pauseAlarmBuilds", true);
  await seed(name, 3_000);
  await deploy(name, 2);
  const redeployed = await deploy(name, 1);
  // The build still ends, a step a call, and the index lists every row once.
  let steps = 0;
  while (!(await object<boolean>("evolving", name, "migrateOnce")) && steps < 100) steps++;
  const all = [...redeployed, ...(await statements("evolving", name))];
  expect(cost(worst(all))).toBeLessThanOrEqual(STEP_BOUND);
  const rows: unknown[] = [];
  let cursor: string | undefined;
  do {
    const page = await table<{ rows: { row: { name: string } }[]; cursor: string | null }>(name, "list", { index: "byEdit", limit: 1_000, ...(cursor ? { cursor } : {}) });
    rows.push(...page.rows.map((r) => r.row.name));
    cursor = page.cursor ?? undefined;
  } while (cursor);
  expect(new Set(rows).size).toBe(3_000);
  expect(rows.length).toBe(3_000);
}, 120_000);

test("a shard in the earlier layout (index keys in a hidden column) moves its keys to key tables in steps, and lists the same", async () => {
  const name = `legacy-${unique()}`;
  const rows = await seed(name, 1_000);
  await object("evolving", name, "legacyLayout").catch(() => undefined);
  await statements("evolving", name);
  const listed: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await table<{ rows: { row: { name: string } }[]; cursor: string | null }>(name, "list", { index: "byEdit", limit: 300, ...(cursor ? { cursor } : {}) });
    listed.push(...page.rows.map((r) => r.row.name));
    cursor = page.cursor ?? undefined;
  } while (cursor);
  // lastEditAt is null for every row: byEdit is in key order.
  expect(listed).toEqual(rows.map((r) => r.name));
  for (let i = 0; i < 50 && (await object<{ keyed: number }>("evolving", name, "legacyLeft")).keyed > 0; i++) await Bun.sleep(100);
  expect(await object("evolving", name, "legacyLeft")).toEqual({ keyed: 0, indexes: 0 });
  // Not counting legacyLeft's own count of keyed rows.
  const w = worst((await statements("evolving", name)).filter((s) => !s.query.startsWith("SELECT COUNT(*) AS n FROM sharded_table WHERE")));
  expect({ query: w.query.slice(0, 80), cost: cost(w) }).toEqual({ query: w.query.slice(0, 80), cost: Math.min(cost(w), STEP_BOUND) });
}, 120_000);

test("a new index is built by the shard's alarm, with no calls after the first", async () => {
  const name = `build-alarm-${unique()}`;
  await seed(name, 3_000);
  await table(name, "upgrade", 2, "");
  expect(await object<boolean>("evolving", name, "migrateOnce")).toBe(false); // a build of 10 steps started
  for (let i = 0; i < 50 && (await object<number>("evolving", name, "buildsLeft")) > 0; i++) await Bun.sleep(200);
  expect(await object<number>("evolving", name, "buildsLeft")).toBe(0);
}, 60_000);

test("a split during an index build cleans up both halves", async () => {
  const name = `build-split-${unique()}`;
  await seed(name, 3_000);
  await table(name, "upgrade", 2, "");
  expect(await object<boolean>("evolving", name, "migrateOnce")).toBe(false);
  expect(await post<boolean>("/split", { cls: "evolving", name, bits: "" })).toBe(true);
  const settled = async () => {
    const [left, right] = await Promise.all(["", "1"].map((bits) => post<{ rows: number; storedRows: number }>("/describe", { cls: "evolving", name, bits })));
    return left!.storedRows === left!.rows && right!.storedRows === right!.rows;
  };
  for (let i = 0; i < 50 && !(await settled()); i++) await Bun.sleep(200);
  expect(await settled()).toBe(true);
}, 60_000);

test("a page of owned rows reads about as many rows as it returns, wherever it starts", async () => {
  const name = `owned-page-${unique()}`;
  const rows = await seed(name, 5_000);
  for (const after of [null, rows[4_500]!.name]) {
    await statements("evolving", name);
    expect((await object<string[]>("evolving", name, "ownedPage", after, 100)).length).toBe(100);
    const page = await statements("evolving", name, "_hash");
    expect(page.filter((p) => p.plan.some((d) => d.includes("TEMP B-TREE")))).toEqual([]);
    expect(Math.max(...page.map((p) => p.rowsRead))).toBeLessThanOrEqual(2 * 100 + 1);
  }
}, 120_000);

test("a page of a sharded object's own rows reads about as many rows as it returns, wherever it starts", async () => {
  const name = `owned-rows-page-${unique()}`;
  const keys = Array.from({ length: 5_000 }, (_, i) => `k${String(i).padStart(5, "0")}`);
  for (let i = 0; i < keys.length; i += 250) await call("counters", name, "add", keys.slice(i, i + 250), keys.slice(i, i + 250).map(() => 1));
  for (const after of [null, keys[4_500]!]) {
    await statements("counters", name);
    expect((await object<string[]>("counters", name, "ownedPage", after, 100)).length).toBe(100);
    const page = await statements("counters", name, "sharded_rows");
    expect(page.filter((p) => p.plan.some((d) => d.includes("TEMP B-TREE")))).toEqual([]);
    expect(Math.max(...page.map((p) => p.rowsRead))).toBeLessThanOrEqual(2 * 100 + 1);
  }
}, 120_000);

test("a shard whose schema is current wakes with at most two statements of migration", async () => {
  const name = `wake-cost-${unique()}`;
  await seed(name, 10);
  // The table layer's statements: not the base class's (sharded_meta, holds) or the fixture's (test_schema).
  const tableLayer = (all: StatementCost[]) => all.filter((s) => /sharded_table|PRAGMA|sqlite_master WHERE type = 'index'/.test(s.query) && !s.query.includes("sharded_atomic")).length;
  await table(name, "get", "doc-00001");
  await statements("evolving", name);
  await table(name, "get", "doc-00001");
  const warm = tableLayer(await statements("evolving", name));
  await object("evolving", name, "restart").catch(() => undefined);
  await table(name, "get", "doc-00001");
  const cold = tableLayer(await statements("evolving", name));
  expect(cold - warm).toBeLessThanOrEqual(2);
}, 60_000);

test("the global index's sweep of stale reservations reads only pending entries", async () => {
  const name = `sweep-plan-${unique()}`;
  const user = (i: number) => ({ id: `u${i}`, email: `u${i}@example.com`, name: `u${i}`, team: "core", createdAt: i, active: true });
  for (let i = 0; i < 300; i += 50) await post("/indexed", { name, op: "putMany", args: [Array.from({ length: 50 }, (_, j) => ({ row: user(i + j) }))] });
  await statements("userIndexes", name);
  await object("userIndexes", name, "alarmNow");
  const sweep = await statements("userIndexes", name, "pending IS NOT NULL AND at <=");
  expect(sweep.length).toBeGreaterThan(0);
  expect(sweep.map((s) => s.plan.join("; "))).toEqual(sweep.map(() => expect.stringContaining("_pending")));
  expect(Math.max(...sweep.map((s) => s.rowsRead))).toBeLessThanOrEqual(10);
}, 60_000);

// SQLite refuses DROP and RENAME (SQLITE_LOCKED) while any statement on the connection is unfinished,
// and a cursor left part-read stays open until it is collected: a page that stopped early would block
// every key-table drop, compaction and schema change of its shard.
const dropWorks = (cls: string, name: string) =>
  post<unknown>("/object", { cls, name, bits: "", method: "sqlRun", args: [[{ query: "CREATE TABLE zz_probe (x)" }, { query: "DROP TABLE zz_probe" }]] }).then(
    () => "dropped",
    (error: unknown) => String(error),
  );

test("a listing page that stops early leaves no statement open: its shard can still drop and rename tables", async () => {
  const name = `open-statement-${unique()}`;
  await post("/table", { cls: "evolving", name, op: "putMany", args: [docs(50).map((row) => ({ row }))] });
  // Full after 2 rows, by the primary key and by an index: the page stops with rows left to read.
  await post("/table", { cls: "evolving", name, op: "list", args: [{ limit: 2 }] });
  expect(await dropWorks("evolving", name)).toBe("dropped");
  await post("/table", { cls: "evolving", name, op: "list", args: [{ index: "byCreated", limit: 2 }] });
  expect(await dropWorks("evolving", name)).toBe("dropped");
}, 60_000);

test("a global index page that stops early leaves no statement open on the index shard", async () => {
  const name = `open-statement-index-${unique()}`;
  for (let i = 0; i < 20; i++) await post("/indexed", { name, op: "put", args: [{ id: `u${i}`, email: `u${i}@example.com`, name: `u${i}`, team: "core", createdAt: i, active: true }] });
  await post("/indexed", { name, op: "list", args: [{ index: "byTeam", partition: ["core"], limit: 2 }] });
  expect(await dropWorks("userIndexes", `${name}`)).toBe("dropped");
}, 60_000);
