// Calls and background work that can't run forever: a write that its caller gave up on must not
// land later, a write whose answer was lost can be retried as itself, deliveries and alarms work
// in bounded steps and back off from what keeps failing, and app code in onAlarm can't stop the
// library's own work.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, post, useWorker } from "./harness.ts";
import type { StatementCost } from "./worker/src/index.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const object = <T = unknown>(cls: string, name: string, bits: string, method: string, ...args: unknown[]) => post<T>("/object", { cls, name, bits, method, args });
const keyIn = (bit: string, from = 0) => {
  for (let i = from; ; i++) if (hashBit(keyHash(`k${i}`), 0) === bit) return `k${i}`;
};
type Settled = { ok?: unknown; threw?: string; name?: string | null };
const doc = (name: string, i = 0) => ({ name, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
const table = <T = unknown>(cls: string, name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls, name, op, args });
const user = (id: string) => ({ id, email: `${id}@example.com`, name: id, team: "core", createdAt: 1, active: true });

// ---- Deadlines ----

test("a write held by a split past its caller's deadline writes nothing", async () => {
  const name = `deadline-${unique()}`;
  const right = keyIn("1");
  await call("counters", name, "add", [right], [1]);
  await post("/slowHandOut", { cls: "counters", name, bits: "", ms: 2_000 });
  const split = post<boolean>("/split", { cls: "counters", name, bits: "" });
  await Bun.sleep(200);
  const late = await post<Settled>("/callSettled", { cls: "counters", name, method: "add", keys: [right], payloads: [10], clientOptions: { callTimeoutMs: 500 } });
  expect(late.threw).toBeDefined();
  expect(await split).toBe(true);
  await Bun.sleep(500);
  expect(await call<number[]>("counters", name, "get", [right])).toEqual([1]);
}, 30_000);

test("a call that reaches its write past its deadline is refused as expired, and writes nothing", async () => {
  const name = `deadline-late-${unique()}`;
  await call("counters", name, "add", ["b"], [10]);
  // As a client whose callTimeoutMs ran out while the call was on its way, or waiting on a hold.
  const answer = await object("counters", name, "", "shardedCall", name, "", "add", ["b"], [1], [], null, 0, null, Date.now() - 1);
  expect(answer).toEqual({ expired: "" });
  expect(await call<number[]>("counters", name, "get", ["b"])).toEqual([10]);
}, 30_000);

test("a prepare that arrives after its deadline holds nothing", async () => {
  const name = `late-prepare-${unique()}`;
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  const answer = await post<{ ok?: unknown }>("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx: `tx-${unique()}`, method: "transfer", primaryKey: "a", deadline: Date.now() - 1 }, keys: ["b"], payloads: [5] });
  expect(answer.ok).not.toEqual({ prepared: true });
  expect((await post<{ holds: unknown[] }>("/atomicTables", { cls: "counters", name, bits: "" })).holds).toEqual([]);
}, 30_000);

// ---- Operation ids ----

test("a put retried with its opId answers the first attempt's result and writes once", async () => {
  const name = `op-retry-${unique()}`;
  const first = await table<{ ok: boolean; version: number }>("table", name, "put", doc("a"), { ifVersion: 0, opId: "op-1" });
  expect(first).toMatchObject({ ok: true, version: 1 });
  // The answer was lost; the caller sends the same write again.
  expect(await table("table", name, "put", doc("a"), { ifVersion: 0, opId: "op-1" })).toMatchObject({ ok: true, version: 1 });
  expect(await table("table", name, "get", "a")).toMatchObject({ version: 1 });
}, 30_000);

test("an opId sent again with another write is refused with OperationMismatch", async () => {
  const name = `op-mismatch-${unique()}`;
  await table("table", name, "put", doc("a"), { opId: "op-1" });
  const again = await post<Settled>("/table", { cls: "table", name, op: "try:put", args: [{ ...doc("a"), createdAt: 99 }, { opId: "op-1" }] });
  expect(again.name).toBe("OperationMismatch");
  expect(await table("table", name, "get", "a")).toMatchObject({ version: 1, row: { createdAt: 0 } });
}, 30_000);

test("an opId recorded before a split is honoured by the shard that owns its key after it", async () => {
  const name = `op-split-${unique()}`;
  const key = Array.from({ length: 50 }, (_, i) => `d${i}`).find((k) => hashBit(keyHash(k), 0) === "1")!;
  await table("table", name, "put", doc(key), { ifVersion: 0, opId: "op-1" });
  expect(await post<boolean>("/split", { cls: "table", name, bits: "" })).toBe(true);
  expect(await table("table", name, "put", doc(key), { ifVersion: 0, opId: "op-1" })).toMatchObject({ ok: true, version: 1 });
}, 30_000);

test("an atomic call whose answer is lost answers not committed when its primary recorded an abort", async () => {
  const name = `atomic-lost-abort-${unique()}`;
  const [a, b] = [keyIn("0"), keyIn("1")].sort() as [string, string];
  await call("counters", name, "add", [a, b], [100, 5]);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  await Bun.sleep(1_000);
  // Refused (below zero): the primary records an abort, and its answer is lost.
  const lost = await post<{ committed?: boolean; threw?: string }>("/atomicallyLosingAnswer", { cls: "counters", name, method: "transfer", keys: [a, b], payloads: [-1000, 1000] });
  expect(lost).toMatchObject({ committed: false });
}, 30_000);

// ---- Index deliveries ----

test("deliveries that never answer free their slots, and later writes are indexed", async () => {
  const name = `hung-deliveries-${unique()}`;
  await post("/indexed", { name, op: "put", args: [user("warm")] });
  await object("userIndexes", name, "", "hangApplies", 4);
  await Promise.all(["h1", "h2", "h3", "h4"].map((id) => post("/indexed", { name, op: "put", args: [user(id)] })));
  await Bun.sleep(12_000); // past any send timeout
  expect(await post("/indexed", { name, op: "put", args: [user("after")] })).toMatchObject({ ok: { ok: true } });
  expect(await post("/indexed", { name, op: "lookup", args: ["byEmail", ["after@example.com"]] })).toMatchObject({ ok: { row: { id: "after" } } });
}, 60_000);

test("a large outbox is delivered over several alarms, a bounded batch each", async () => {
  const name = `outbox-backlog-${unique()}`;
  await post("/indexed", { name, op: "put", args: [user("warm")] });
  await object("users", name, "", "countDeliveriesPerAlarm");
  await post("/indexed", { name, op: "failApply", args: [true] });
  for (let i = 0; i < 2_500; i += 250) await post("/indexed", { name, op: "putMany", args: [Array.from({ length: 250 }, (_, j) => ({ row: user(`u${i + j}`) }))] });
  await post("/indexed", { name, op: "failApply", args: [false] });
  for (let i = 0; i < 100 && (await object<number>("users", name, "", "outboxSize")) > 0; i++) await Bun.sleep(200);
  expect(await object<number>("users", name, "", "outboxSize")).toBe(0);
  const delivered = (await object<number[]>("users", name, "", "deliveredPerAlarm")).filter((n) => n > 0);
  expect(Math.max(...delivered)).toBeLessThanOrEqual(2_000);
}, 120_000);

test("a split of a row shard with a backlog of deliveries claims without walking it", async () => {
  const name = `outbox-claim-${unique()}`;
  await post("/indexed", { name, op: "put", args: [user("warm")] });
  await post("/indexed", { name, op: "failApply", args: [true] });
  for (let i = 0; i < 2_000; i += 250) await post("/indexed", { name, op: "putMany", args: [Array.from({ length: 250 }, (_, j) => ({ row: user(`u${i + j}`) }))] });
  expect(await post<boolean>("/split", { cls: "users", name, bits: "" })).toBe(true);
  const walks = await object<StatementCost[]>("users", name, "1", "statements", "FROM indexed_outbox", false);
  expect(Math.max(0, ...walks.map((s) => s.rowsRead))).toBeLessThanOrEqual(1_000);
  await post("/indexed", { name, op: "failApply", args: [false] });
}, 120_000);

test("an app onAlarm that throws before super does not stop index deliveries", async () => {
  const name = `app-alarm-throws-${unique()}`;
  await post("/indexed", { name, op: "put", args: [user("warm")] });
  await object("users", name, "", "throwOnAlarm", true);
  await post("/indexed", { name, op: "failApply", args: [true] });
  await post("/indexed", { name, op: "put", args: [{ ...user("late"), team: "late-team" }] });
  // Only the row shard's alarm may deliver now: nothing of the write is left in memory.
  await object("users", name, "", "restart").catch(() => undefined);
  await post("/indexed", { name, op: "failApply", args: [false] });
  // Listed by team (not looked up by its unique email, which would ask the row shard): only a delivery puts it there.
  const found = async () => (await post<{ ok?: { rows: { row: { id: string } }[] } }>("/indexed", { name, op: "list", args: [{ index: "byTeam", partition: ["late-team"], limit: 10 }] })).ok?.rows.map((r) => r.row.id) ?? [];
  for (let i = 0; i < 50 && !(await found()).includes("late"); i++) await Bun.sleep(100);
  expect(await found()).toEqual(["late"]);
}, 60_000);

// ---- Backoff ----

test("an onAlarm that throws every time keeps its alarm, and is retried less and less often", async () => {
  const name = `alarm-backoff-${unique()}`;
  await table("flaky", name, "put", doc("a"));
  await object("flaky", name, "", "failOnAlarm", true);
  await table("flaky", name, "wakeIn", 0, "");
  await Bun.sleep(30_000);
  const runs = await object<number>("flaky", name, "", "onAlarmRuns");
  // 1, 2, 4, 8, 16 s: about 5 runs in 30 s, where a flat second gives about 30.
  expect(runs).toBeGreaterThanOrEqual(4);
  expect(runs).toBeLessThanOrEqual(10);
  expect((await object<{ alarm: number | null }>("flaky", name, "", "storage")).alarm).not.toBeNull();
}, 60_000);

test("a sibling that keeps refusing activation is asked less and less often", async () => {
  const name = `activate-backoff-${unique()}`;
  for (let i = 0; i < 10; i++) await table("flaky", name, "put", doc(`r${i}`, i));
  await table("flaky", name, "failActivations", true, "");
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  await Bun.sleep(20_000);
  expect(await object<number>("flaky", name, "1", "activationAttempts")).toBeLessThan(10);
}, 60_000);

// ---- Atomic housekeeping ----

test("a failing outcome purge does not hold up the recovery of holds past their deadline", async () => {
  const name = `purge-recover-${unique()}`;
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  const tx = `tx-${unique()}`;
  const deadline = Date.now() + 300;
  await post("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx, method: "transfer", primaryKey: "a", deadline }, keys: ["b"], payloads: [5] });
  await object("counters", name, "", "seedOutcomes", 1, 0);
  await object("counters", name, "", "failStatementOnce", "DELETE FROM sharded_atomic_outcomes");
  // The alarm at the deadline fails to purge; the hold must be recovered by that same alarm,
  // not by its retry a second later.
  await Bun.sleep(deadline + 400 - Date.now());
  expect((await post<{ holds: unknown[] }>("/atomicTables", { cls: "counters", name, bits: "" })).holds).toEqual([]);
}, 30_000);

test("old atomic outcomes are purged in bounded batches, through an index on their time", async () => {
  const name = `purge-bounded-${unique()}`;
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  // A prepare makes the shard's atomic tables, as a real call would.
  await post("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx: `tx-${unique()}`, method: "transfer", primaryKey: "a", deadline: Date.now() + 60_000 }, keys: ["b"], payloads: [1] });
  await object("counters", name, "", "seedOutcomes", 2_500, 0);
  await object("counters", name, "", "statements", "", true);
  await object("counters", name, "", "alarmNow");
  const purge = await object<StatementCost[]>("counters", name, "", "statements", "sharded_atomic_outcomes", true);
  expect(Math.max(0, ...purge.map((s) => s.rowsRead + s.rowsWritten))).toBeLessThanOrEqual(2 * 1_001);
  expect(await object<number>("counters", name, "", "outcomeCount")).toBeGreaterThanOrEqual(1_500);
}, 60_000);

test("a refused call over two shards leaves no outcome on its primary", async () => {
  const name = `refused-forgotten-${unique()}`;
  const [a, b] = [keyIn("0"), keyIn("1")].sort() as [string, string];
  await call("counters", name, "add", [a, b], [1, 1]);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  await Bun.sleep(1_000);
  expect(await post("/atomically", { cls: "counters", name, method: "transfer", keys: [a, b], payloads: [-100, 100] })).toMatchObject({ committed: false });
  const primary = hashBit(keyHash(a), 0);
  for (let i = 0; i < 20 && (await object<number>("counters", name, primary, "outcomeCount")) > 0; i++) await Bun.sleep(100);
  expect(await object<number>("counters", name, primary, "outcomeCount")).toBe(0);
}, 30_000);

test("a prepared hold has its alarm though its object is evicted right after storing it", async () => {
  const name = `arm-first-${unique()}`;
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  await object("counters", name, "", "abortOnNextGetAlarm");
  await post("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx: `tx-${unique()}`, method: "transfer", primaryKey: "a", deadline: Date.now() + 60_000 }, keys: ["b"], payloads: [5] }).catch(() => undefined);
  const report = await object<{ tables: { name: string; rows: number }[]; alarm: number | null }>("counters", name, "", "storage");
  const holds = report.tables.find((t) => t.name === "sharded_atomic_holds")?.rows ?? 0;
  expect({ holds, alarm: holds > 0 ? report.alarm !== null : true }).toEqual({ holds, alarm: true });
}, 30_000);

// ---- Count groups ----

test("a count group past maxValues is reported truncated, not returned whole", async () => {
  const name = `count-values-${unique()}`;
  await table("evolving", name, "upgrade", 2, "");
  for (let i = 0; i < 1_500; i += 150) await table("evolving", name, "putMany", Array.from({ length: 150 }, (_, j) => ({ row: { ...doc(`d${i + j}`, i + j), stars: i + j } })));
  const counts = await table<{ counts?: unknown[]; truncated?: boolean } | unknown[]>("evolving", name, "countBy", "byStars");
  expect(Array.isArray(counts) ? { truncated: false, n: counts.length } : { truncated: counts.truncated, n: counts.counts?.length }).toEqual({ truncated: true, n: 1_000 });
}, 120_000);

test("cleanup of a count group with many values deletes a bounded batch at a time", async () => {
  const name = `count-cleanup-${unique()}`;
  await table("evolving", name, "upgrade", 2, "");
  for (let i = 0; i < 2_000; i += 200) await table("evolving", name, "putMany", Array.from({ length: 200 }, (_, j) => ({ row: { ...doc(`d${i + j}`, i + j), stars: i + j } })));
  await object("evolving", name, "", "statements", "", true);
  expect(await post<boolean>("/split", { cls: "evolving", name, bits: "" })).toBe(true);
  await Bun.sleep(3_000);
  const deletes = await object<StatementCost[]>("evolving", name, "", "statements", "DELETE FROM sharded_table_group_counts", true);
  expect(deletes.length).toBeGreaterThan(0);
  expect(Math.max(...deletes.map((s) => s.rowsWritten))).toBeLessThanOrEqual(260);
}, 120_000);

// ---- Work graphs ----

test("a work graph run refuses a lease shorter than three chunks' planned time", async () => {
  const run = `short-lease-${unique()}`;
  const started = await post<unknown>("/workgraph", { op: "start", run, shape: { trees: 4, size: 3, chainy: 0.2, joins: 0, seed: 3 }, runners: 1, chunkMs: 1_000, leaseMs: 1_500 }).then(
    () => "started",
    (e: Error) => e.message,
  );
  expect(started).toContain("leaseMs");
}, 30_000);

// ---- Timers ----

test("writes held by a split share their timers", async () => {
  const name = `held-timers-${unique()}`;
  const keys = Array.from({ length: 400 }, (_, i) => `k${i}`).filter((k) => hashBit(keyHash(k), 0) === "1").slice(0, 150);
  await call("counters", name, "add", keys, keys.map(() => 1));
  await object("counters", name, "", "setPolicy", { holdWaitMs: 2_000 });
  await post("/slowHandOut", { cls: "counters", name, bits: "", ms: 2_500 });
  const before = await object<number>("counters", name, "", "liveTimers");
  const split = post<boolean>("/split", { cls: "counters", name, bits: "" });
  await Bun.sleep(300);
  const writes = keys.map((k) => call("counters", name, "add", [k], [1]).catch(() => undefined));
  await Bun.sleep(700);
  const during = await object<number>("counters", name, "", "liveTimers");
  await Promise.all(writes);
  await split;
  expect(during - before).toBeLessThanOrEqual(20);
}, 60_000);
