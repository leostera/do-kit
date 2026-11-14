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

test("no statement of a new index's build touches more than a step of rows, but the one that makes its SQL index, which reads each row once and writes nothing", async () => {
  const name = `build-statements-${unique()}`;
  await seed(name, 3_000);
  const all = await deploy(name, 2);
  // SQLite reads every row to make an index over a table with rows. Made while its column is
  // empty, it holds nothing yet: no sort, no writes, and the build's pages fill it.
  const creates = all.filter((s) => s.query.startsWith("CREATE INDEX") && s.query.includes("_ix_byStars"));
  expect(creates.map((s) => ({ read: s.rowsRead <= 3_000 + 10, written: s.rowsWritten <= 1 }))).toEqual([{ read: true, written: true }]);
  const w = worst(all.filter((s) => !creates.includes(s)));
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

test("an index's SQL index is made before its keys are filled, so no statement sorts every row", async () => {
  const name = `build-partial-${unique()}`;
  await seed(name, 1_000);
  const all = await deploy(name, 2);
  const create = all.findIndex((s) => s.query.startsWith("CREATE INDEX") && s.query.includes("_ix_byStars"));
  const fill = all.findIndex((s) => s.query.startsWith("UPDATE sharded_table SET \"_ix_byStars"));
  expect(create).toBeGreaterThanOrEqual(0);
  expect(fill).toBeGreaterThanOrEqual(0);
  expect(create).toBeLessThan(fill);
}, 120_000);

test("a rollback runs no statement that rewrites every row", async () => {
  const name = `rollback-statements-${unique()}`;
  await seed(name, 3_000);
  await deploy(name, 2);
  const all = await deploy(name, 1);
  expect(all.filter((s) => s.query.includes("DROP COLUMN")).map((s) => s.query)).toEqual([]);
  // The index the rollback declares again is made over its column once cleared: one read of each
  // row, and nothing written (the stale keys the drop left would have to be sorted).
  const remade = all.filter((s) => s.query.startsWith("CREATE INDEX") && s.rowsRead > STEP_BOUND);
  expect(remade.map((s) => s.rowsWritten <= 1)).toEqual(remade.map(() => true));
  expect(cost(worst(all.filter((s) => !remade.includes(s))))).toBeLessThanOrEqual(STEP_BOUND);
}, 120_000);

test("an index declared again before its old keys are cleared is made empty, and its keys cleared and filled in steps", async () => {
  const name = `redeclare-${unique()}`;
  // Steps by calls only, and none between the deploys: the dropped index's keys are all still there.
  await object("evolving", name, "pauseAlarmBuilds", true);
  await seed(name, 3_000);
  await deploy(name, 2);
  const all = await deploy(name, 1);
  const remade = all.filter((s) => s.query.startsWith("CREATE INDEX") && s.query.includes("_ix_byEdit"));
  expect(remade.map((s) => s.rowsWritten <= 1)).toEqual(remade.map(() => true));
  // The build still ends, a step a call, and the index lists every row.
  let steps = 0;
  while (!(await object<boolean>("evolving", name, "migrateOnce")) && steps < 60) steps++;
  const after = await statements("evolving", name);
  expect(cost(worst(after.filter((s) => !s.query.startsWith("CREATE INDEX"))))).toBeLessThanOrEqual(STEP_BOUND);
  expect(after.filter((s) => s.query.startsWith("CREATE INDEX") && s.rowsWritten > 1).map((s) => s.query)).toEqual([]);
  const listed = await table<{ rows: unknown[] }>(name, "list", { index: "byEdit", limit: 1_000 });
  expect(listed.rows.length).toBe(1_000);
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
