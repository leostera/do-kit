// Query plans and cost per operation, pinned. Every statement one of each table operation runs
// uses an index (no full SCAN of a table, no temporary B-tree for an ORDER BY), with and without
// ANALYZE; a get and a page read as many rows at 20,000 rows as at 2,000; and indexed writes don't
// set the alarm per write.
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";
import type { StatementCost } from "./worker/src/index.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op, args });
const object = <T = unknown>(cls: string, name: string, method: string, ...args: unknown[]) => post<T>("/object", { cls, name, bits: "", method, args });
const doc = (i: number) => ({ name: `d${String(i).padStart(6, "0")}`, id: `${i}`, createdAt: i % 50, lastEditAt: i % 3 ? i : null, deleted: i % 7 === 0, meta: null });

async function seed(name: string, n: number): Promise<void> {
  for (let i = 0; i < n; i += 250) await table(name, "putMany", Array.from({ length: Math.min(250, n - i) }, (_, j) => ({ row: doc(i + j) })));
}

/** The operations, one of each. */
const OPERATIONS: [string, (name: string) => Promise<unknown>][] = [
  ["get", (n) => table(n, "get", "d000010")],
  ["put existing", (n) => table(n, "put", { ...doc(11), createdAt: 9 })],
  ["put new", (n) => table(n, "put", doc(999_999))],
  ["patch", (n) => table(n, "patch", "d000012", { createdAt: 3 })],
  ["delete", (n) => table(n, "delete", "d000013")],
  ["list by key", (n) => table(n, "list", { limit: 10 })],
  ["list by an index, descending", (n) => table(n, "list", { index: "byCreated", direction: "desc", limit: 10 })],
  ["list with a condition", (n) => table(n, "list", { index: "byDeletedCreated", where: [{ column: "deleted", op: "=", value: false }], limit: 10 })],
  ["count", (n) => table(n, "count")],
  ["countBy", (n) => table(n, "countBy", "byDeleted")],
];

/** The statements an operation ran, but the fixtures' and the schema checks'. */
async function measured(name: string, run: () => Promise<unknown>): Promise<StatementCost[]> {
  await object("evolving", name, "statements", "", true);
  await run();
  return (await object<StatementCost[]>("evolving", name, "statements", "", true)).filter((s) => !s.query.includes("test_") && !s.query.includes("sqlite_master"));
}

for (const analyzed of [false, true]) {
  test(`every statement of every table operation uses an index${analyzed ? ", after ANALYZE" : ""}`, async () => {
    const name = `plans-${unique()}`;
    await seed(name, 2_000);
    if (analyzed) await object("evolving", name, "sqlRun", [{ query: "ANALYZE" }]);
    const bad: string[] = [];
    for (const [label, run] of OPERATIONS) {
      for (const s of await measured(name, () => run(name))) {
        for (const line of s.plan) {
          if (/^SCAN \S+$/.test(line.trim()) || line.includes("USE TEMP B-TREE")) bad.push(`${label}: ${s.query.replace(/\s+/g, " ").slice(0, 90)}: ${line.trim()}`);
        }
      }
    }
    expect(bad).toEqual([]);
  }, 120_000);
}

test("a get and a page read as many rows at 20,000 rows as at 2,000", async () => {
  const rowsRead = async (n: number) => {
    const name = `flat-${n}-${unique()}`;
    await seed(name, n);
    const read = async (run: () => Promise<unknown>) => (await measured(name, run)).reduce((sum, s) => sum + s.rowsRead, 0);
    return {
      get: await read(() => table(name, "get", "d000010")),
      page: await read(() => table(name, "list", { index: "byCreated", limit: 10 })),
      conditionPage: await read(() => table(name, "list", { index: "byDeletedCreated", where: [{ column: "deleted", op: "=", value: false }], limit: 10 })),
    };
  };
  expect(await rowsRead(20_000)).toEqual(await rowsRead(2_000));
}, 300_000);

const user = (i: number) => ({ id: `u${i}`, email: `u${i}@example.com`, name: `u${i}`, team: `t${i % 4}`, createdAt: i, active: true });

test("a steady stream of indexed writes sets the alarm at most once", async () => {
  const name = `alarm-sets-${unique()}`;
  await post("/indexed", { name, op: "put", args: [user(0)] });
  await object("users", name, "alarmSets", true);
  for (let i = 1; i <= 200; i++) {
    await post("/indexed", { name, op: "put", args: [user(i)] });
    await Bun.sleep(25);
  }
  expect(await object<number>("users", name, "alarmSets", true)).toBeLessThanOrEqual(1);
}, 120_000);
