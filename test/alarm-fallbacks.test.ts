// Work one object's alarm drives must not depend on that alarm alone: an alarm can be lost, and
// the object may get no calls to notice. A split's new shard nudges its parent when calls reach
// it; a work-graph coordinator is checked by the runners that call it.
import { createHash } from "node:crypto";
import { expect, test } from "bun:test";
import type { RunStatus } from "../src/workgraph/coordinator.ts";
import { post, useWorker } from "./harness.ts";
import type { Shape } from "./workgraph-fixture.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(cls: string, name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls, name, op, args });
const state = async (name: string, bits: string) => (await post<{ state: string }>("/describe", { cls: "flaky", name, bits })).state;
const firstBit = (key: string) => Number.parseInt(createHash("sha256").update(key).digest("hex")[0]!, 16) >> 3;

for (const loss of ["dropAlarm", "strandAlarm"] as const) {
  test(`a new shard is activated by calls to it alone, when its parent's alarm is ${loss === "dropAlarm" ? "lost" : "stranded"} and the parent gets no calls`, async () => {
    const name = `activate-from-child-${loss}-${unique()}`;
    const rows = Array.from({ length: 20 }, (_, i) => ({ name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null, stars: null }));
    for (const row of rows) await table("flaky", name, "put", row);
    await table("flaky", name, "failActivations", true, "");
    expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
    const moved = rows.filter((r) => firstBit(r.name) === 1).map((r) => r.name);
    await table("flaky", name, "try:get", moved[0]); // the isolate learns the split
    await table("flaky", name, loss, null, "");
    await table("flaky", name, "failActivations", false, "1");
    // Every call that reaches the parent: served, or answered moved, not ready or stale.
    const parentCalls = async () => {
      const c = (await post<{ counters: { calls: number; moved: number; notReady: number; stale: number } }>("/describe", { cls: "flaky", name, bits: "" })).counters;
      return c.calls + c.moved + c.notReady + c.stale;
    };
    const before = await parentCalls();
    // Only keys of the new shard are called, routed straight to it: none of those calls reaches the parent.
    for (let i = 0; i < 100 && (await state(name, "1")) !== "shard"; i++) {
      await table("flaky", name, "try:get", moved[i % moved.length]);
      await Bun.sleep(100);
    }
    expect(await state(name, "1")).toBe("shard");
    expect(await parentCalls()).toBe(before);
    expect(await table<{ row: { name: string } }>("flaky", name, "get", moved[0])).toMatchObject({ row: { name: moved[0] } });
  }, 60_000);
}

test("a work graph run whose coordinator lost its alarm still finishes when a runner is lost holding a lease", async () => {
  // "lose": runner 0 abandons its first chunk; its lease must run out for the chunk to go on, and
  // only the coordinator's alarm (or a backstop) lets leases lapse.
  const run = `lose-coordinator-alarm-${unique()}`;
  const shape: Shape = { trees: 12, size: 5, chainy: 0.3, joins: 0, seed: 21 };
  await post("/workgraph", { op: "start", run, shape, runners: 2, chunkMs: 30, leaseMs: 2_000, tickMs: 500, wakeAfterMs: 1_000 });
  await post("/workgraph", { op: "dropCoordinatorAlarm", run });
  const until = Date.now() + 20_000;
  let status: RunStatus;
  for (;;) {
    status = await post<RunStatus>("/workgraph", { op: "status", run });
    if (status.state !== "running" || Date.now() > until) break;
    await Bun.sleep(250);
  }
  expect(status.state).toBe("done");
}, 60_000);
