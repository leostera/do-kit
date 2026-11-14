// A tree after its splits settles: every shard serves, keeps only its own rows and has no alarm
// left armed; and a split's cleanup runs in bounded alarms, not one long one.
import { expect, test } from "bun:test";
import { call, post, settle, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const doc = (name: string, i: number) => ({ name, id: name, createdAt: i, lastEditAt: null, deleted: false, meta: null });

test("a table settles after splits and their cleanup: every shard serves its own rows, no alarm left armed", async () => {
  const name = `settle-${unique()}`;
  await post("/table", { cls: "table", name, op: "putMany", args: [Array.from({ length: 200 }, (_, i) => ({ row: doc(`d${i}`, i) }))] });
  for (const bits of ["", "0"]) expect(await post<boolean>("/split", { cls: "table", name, bits })).toBe(true);
  const nodes = await settle("table", name);
  expect(nodes.map((n) => n.bits)).toEqual(["00", "01", "1"]);
  // Each shard keeps only its own rows (the table's own count: describe reports none for a table).
  expect((await post<number[]>("/table", { cls: "table", name, op: "scan", args: ["storedRows"] })).reduce((a, b) => a + b, 0)).toBe(200);
}, 60_000);

test("a split's cleanup of 2,000 rows runs in alarms of at most 250 rows each, and finishes", async () => {
  const name = `cleanup-batches-${unique()}`;
  const keys = Array.from({ length: 2_000 }, (_, i) => `k${i}`);
  for (let i = 0; i < keys.length; i += 500) await call("counters", name, "add", keys.slice(i, i + 500), keys.slice(i, i + 500).map(() => 1));
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  const nodes = await settle("counters", name);
  expect(nodes.reduce((n, s) => n + s.rows, 0)).toBe(2_000);
  const perAlarm = (await Promise.all(nodes.map((n) => post<number[]>("/object", { cls: "counters", name, bits: n.bits, method: "cleanupPerAlarm", args: [] })))).flat();
  expect(Math.max(...perAlarm)).toBeLessThanOrEqual(250);
  expect(perAlarm.length).toBeGreaterThanOrEqual(8);
}, 120_000);
