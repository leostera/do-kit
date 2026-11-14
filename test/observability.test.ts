// What a shard tells an operator: its size, its rows (a table's too), why and how each split ran,
// pending background work by age, an overdue alarm, what calls cost and waited, and errors that
// say what kind they are, with no row keys in them. Plus the bench tooling that keeps published
// numbers honest, and the alarm rules written down.
import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { loadClient } from "./fake-tree.ts";
import { call, describe, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const doc = (name: string, i: number) => ({ name, id: name, createdAt: i, lastEditAt: null, deleted: false, meta: null });
type Loose = Record<string, unknown> & { counters: Record<string, unknown>; splits: Record<string, unknown>[] };
const described = (cls: string, name: string, bits: string) => describe(cls, name, bits) as unknown as Promise<Loose>;

// ---- Size and rows ----

test("describe reports the shard's database size, and it grows with rows", async () => {
  const name = `size-${unique()}`;
  await call("counters", name, "add", ["a"], [1]);
  const before = (await described("counters", name, "")).databaseSize;
  const keys = Array.from({ length: 1_000 }, (_, i) => `k${i}`);
  await call("counters", name, "add", keys, keys.map(() => 1));
  const after = (await described("counters", name, "")).databaseSize;
  expect(typeof before).toBe("number");
  expect(after as number).toBeGreaterThan(before as number);
}, 30_000);

test("a table's describe counts its rows, and a table split records the rows it moved", async () => {
  const name = `table-rows-${unique()}`;
  await post("/table", { cls: "table", name, op: "putMany", args: [Array.from({ length: 100 }, (_, i) => ({ row: doc(`d${i}`, i) }))] });
  expect((await described("table", name, "")).rows).toBe(100);
  expect(await post<boolean>("/split", { cls: "table", name, bits: "" })).toBe(true);
  const split = (await described("table", name, "")).splits.at(-1)!;
  expect(split.rows).toBe(100);
  expect(split.rightRows as number).toBeGreaterThan(0);
  expect(split.rightRows as number).toBeLessThan(100);
}, 60_000);

// ---- Splits ----

test("a split records why it ran: operator when forced, load when calls drove it", async () => {
  const name = `trigger-${unique()}`;
  await call("counters", name, "add", ["a"], [1]);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  expect((await described("counters", name, "")).splits.at(-1)!.trigger).toBe("operator");
  const auto = `trigger-load-${unique()}`;
  for (let i = 0; i < 400; i++) await call("auto", auto, "add", [`k${i % 50}`], [1]);
  const splits = (await described("auto", auto, "")).splits;
  expect(splits.length).toBeGreaterThan(0);
  expect(splits.at(-1)!.trigger).toBe("load");
}, 120_000);

test("a split interrupted by a restart is recorded as interrupted", async () => {
  const name = `interrupted-${unique()}`;
  await call("counters", name, "add", ["a", "b", "c"], [1, 1, 1]);
  await post("/object", { cls: "counters", name, bits: "", method: "slowHandOut", args: [2_000] });
  const splitting = post("/split", { cls: "counters", name, bits: "" }).catch(() => false);
  await Bun.sleep(300);
  // Mid-split: the restart interrupts it.
  expect((await described("counters", name, "")).state).toBe("splitting");
  await post("/evict", { cls: "counters", name, bits: "" });
  await splitting;
  await call("counters", name, "get", ["a"]).catch(() => undefined);
  expect((await described("counters", name, "")).splits.at(-1)?.outcome).toBe("interrupted");
}, 60_000);

test("describe reports activation pending by age, and nothing once the copy serves", async () => {
  const name = `pending-${unique()}`;
  await post("/table", { cls: "flaky", name, op: "put", args: [doc("a", 0)] });
  await post("/table", { cls: "flaky", name, op: "failActivations", args: [true, ""] });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  await Bun.sleep(1_200);
  expect((await described("flaky", name, "")).activatePendingMs as number).toBeGreaterThan(1_000);
  // The copy carries the flag: it is the one that refuses.
  await post("/table", { cls: "flaky", name, op: "failActivations", args: [false, "1"] });
  for (let i = 0; i < 50 && (await described("flaky", name, "")).activatePendingMs !== null; i++) await Bun.sleep(200);
  expect((await described("flaky", name, "")).activatePendingMs).toBeNull();
}, 60_000);

test("cleanup progress is visible, and ends with the rows given away", async () => {
  const name = `cleanup-progress-${unique()}`;
  const keys = Array.from({ length: 600 }, (_, i) => `k${i}`);
  await call("counters", name, "add", keys, keys.map(() => 1));
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  let cleanup: { pendingRanges: number; rows: number } | undefined;
  for (let i = 0; i < 25; i++) {
    cleanup = (await described("counters", name, "")).cleanup as typeof cleanup;
    if (cleanup?.pendingRanges === 0) break;
    await Bun.sleep(200);
  }
  const kept = (await described("counters", name, "")).rows as number;
  expect(cleanup?.pendingRanges).toBe(0);
  expect(cleanup?.rows).toBe(600 - kept);
}, 60_000);

// ---- Alarms ----

test("describe reports a stranded alarm as overdue, and a routed call clears it", async () => {
  const name = `overdue-${unique()}`;
  await post("/table", { cls: "flaky", name, op: "put", args: [doc("a", 0)] });
  await post("/table", { cls: "flaky", name, op: "wakeIn", args: [60_000, ""] });
  await post("/table", { cls: "flaky", name, op: "strandAlarm", args: [undefined, ""] });
  const alarm = (await described("flaky", name, "")).alarm as { overdueMs: number } | undefined;
  expect(alarm?.overdueMs).toBeGreaterThanOrEqual(600_000);
  await Bun.sleep(5_100);
  await post("/table", { cls: "flaky", name, op: "get", args: ["a"] });
  expect(((await described("flaky", name, "")).alarm as { overdueMs: number }).overdueMs).toBeLessThanOrEqual(1_000);
}, 60_000);

// ---- Calls ----

test("describe counts rows read and written per method", async () => {
  const name = `call-cost-${unique()}`;
  await post("/table", { cls: "table", name, op: "put", args: [doc("a", 0)] });
  await post("/table", { cls: "table", name, op: "get", args: ["a"] });
  const counters = (await described("table", name, "")).counters as { methods?: Record<string, { rowsRead: number; rowsWritten: number; statements: number }> };
  expect(counters.methods?.tablePut?.rowsWritten).toBeGreaterThan(0);
  expect(counters.methods?.tableGet?.rowsRead).toBeGreaterThan(0);
}, 30_000);

test("a shard reports how long calls waited to start, and null before any call", async () => {
  const name = `waited-${unique()}`;
  const fresh = (await described("counters", name, "")).counters;
  expect("waitedMs" in fresh).toBe(true);
  expect(fresh.waitedMs).toBeNull();
  for (let i = 0; i < 20; i++) await call("counters", name, "add", ["a"], [1]);
  expect((await described("counters", name, "")).counters.waitedMs as number).toBeGreaterThanOrEqual(0);
}, 30_000);

test("a sparse listing's budget stops are counted in the shard", async () => {
  const name = `budget-stops-${unique()}`;
  await post("/table", { cls: "table", name, op: "putMany", args: [Array.from({ length: 200 }, (_, i) => ({ row: doc(`d${String(i).padStart(3, "0")}`, i) }))] });
  await post("/table", { cls: "table", name, op: "list", args: [{ index: "byCreated", where: [{ column: "name", op: "contains", value: "d19" }], limit: 5, budget: 20 }] });
  const table = (await described("table", name, "")).counters.table as { pages: number; scanned: number; budgetStops: number } | undefined;
  expect(table?.budgetStops).toBeGreaterThan(0);
}, 30_000);

// ---- Errors ----

const RUNTIME_ERRORS: [string, string][] = [
  ["Durable Object is overloaded. Requests queued for too long.", "overloaded"],
  ["jsg.Error: Durable Object is overloaded. Too many requests queued.", "overloaded"],
  ["Error: too many concurrent storage operations", "overloaded"],
  ["Network connection lost.", "unknown"],
  ["jsg.Error: Network connection lost.", "unknown"],
  ["internal error; reference = abc123", "unknown"],
  ["Error: internal error; reference = abc123", "unknown"],
  ["no such row", "app"],
];

test("classifyShardError labels the runtime's errors, with and without a prefix, and an app error as app", async () => {
  const mod = (await loadClient()) as unknown as Record<string, unknown>;
  const classify = mod.classifyShardError as ((e: unknown) => string) | undefined;
  expect(typeof classify).toBe("function");
  expect(RUNTIME_ERRORS.map(([message]) => classify!(new Error(message)))).toEqual(RUNTIME_ERRORS.map(([, kind]) => kind));
});

test("redact takes row keys out of error text and bounds its length", async () => {
  const mod = (await import("../src/table/schema.ts")) as Record<string, unknown>;
  const redact = mod.redact as ((e: unknown) => string) | undefined;
  expect(typeof redact).toBe("function");
  const text = redact!(new Error(`row key "alice@example.com" is not owned here ${"x".repeat(400)}`));
  expect(text).not.toContain("alice@example.com");
  expect(text.length).toBeLessThanOrEqual(200);
});

// ---- Bench tooling and docs ----

test("bench/compare.ts passes a result file against itself", () => {
  expect(existsSync("bench/compare.ts")).toBe(true);
  const file = `bench/results/${readdirSync("bench/results").filter((f) => f.endsWith(".json")).sort().at(-1)}`;
  expect(spawnSync("bun", ["bench/compare.ts", file, file]).status).toBe(0);
});

test("bench/compare.ts fails when a measured number moved more than 2%, or a point went missing", () => {
  const dir = mkdtempSync(join(tmpdir(), "compare-"));
  const point = { target: "table", operation: "put", concurrency: 64, throughput: 1_000, p99Ms: 50 };
  const write = (name: string, points: object[]) => {
    const file = join(dir, name);
    writeFileSync(file, JSON.stringify({ points }));
    return file;
  };
  const before = write("before.json", [point, { ...point, concurrency: 128 }]);
  const run = (after: string) => spawnSync("bun", ["bench/compare.ts", before, after]).status;
  expect(run(write("same.json", [point, { ...point, concurrency: 128 }]))).toBe(0);
  expect(run(write("within.json", [{ ...point, throughput: 1_015 }, { ...point, concurrency: 128 }]))).toBe(0);
  expect(run(write("slower.json", [{ ...point, throughput: 970 }, { ...point, concurrency: 128 }]))).toBe(1);
  expect(run(write("tail.json", [{ ...point, p99Ms: 52 }, { ...point, concurrency: 128 }]))).toBe(1);
  expect(run(write("missing.json", [point]))).toBe(1);
});

test("the docs' benchmark tables are generated from results and current", () => {
  expect(existsSync("bench/tables.ts")).toBe(true);
  expect(spawnSync("bun", ["bench/tables.ts", "--check"]).status).toBe(0);
});

test("RFD0001 says which work the alarm arms, the backstop, the stranded rule and the backoff", () => {
  const rfd = readFileSync("docs/rfds/RFD0001-sharded-object.md", "utf8");
  const section = rfd.split(/^#+ /m).find((s) => s.startsWith("Alarms"));
  expect(section).toBeDefined();
  for (const claim of [/backstop/i, /5 min/i, /backoff/i, /nudge/i]) expect(section ?? "").toMatch(claim);
});
