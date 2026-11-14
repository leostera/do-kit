// A stranded alarm: the platform can leave getAlarm() answering a time in the past for an alarm
// that will never fire. Every check that asks "is an alarm already set?" must treat an alarm well
// past due as missing, or the work it serves stops for good while calls keep arriving.
import { createHash } from "node:crypto";
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import type { RunStatus } from "../src/workgraph/coordinator.ts";
import { call, post, useWorker } from "./harness.ts";
import type { Shape } from "./workgraph-fixture.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(cls: string, name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls, name, op, args });
const state = async (cls: string, name: string, bits: string) => (await post<{ state: string }>("/describe", { cls, name, bits })).state;
type Tables = { holds: { tx: string }[]; outcomes: { tx: string; outcome: string }[] };

test("a split's new shard is activated though the original's alarm is stranded", async () => {
  const name = `stranded-activate-${unique()}`;
  for (let i = 0; i < 20; i++) await table("flaky", name, "put", { name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
  await table("flaky", name, "failActivations", true, "");
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  await table("flaky", name, "strandAlarm", null, "");
  await table("flaky", name, "failActivations", false, "1");
  const stays = Array.from({ length: 20 }, (_, i) => `r${i}`).find((k) => Number.parseInt(createHash("sha256").update(k).digest("hex")[0]!, 16) >> 3 === 0)!;
  // Calls keep reaching the original, past the backstop's interval.
  for (let i = 0; i < 100 && (await state("flaky", name, "1")) !== "shard"; i++) {
    await table("flaky", name, "get", stays);
    await Bun.sleep(100);
  }
  expect(await state("flaky", name, "1")).toBe("shard");
}, 60_000);

test("onAlarm asked for while the alarm is stranded still runs", async () => {
  const name = `stranded-wake-${unique()}`;
  await table("flaky", name, "put", { name: "r0", id: "id0", createdAt: 0, lastEditAt: null, deleted: false, meta: null });
  await table("flaky", name, "strandAlarm", null, "");
  await table("flaky", name, "wakeIn", 200, "");
  let ran: string[] = [];
  for (let i = 0; i < 30 && ran.length === 0; i++) {
    await Bun.sleep(200);
    ran = (await table<{ ran: string[] }>("flaky", name, "alarmRan", null, "")).ran;
  }
  expect(ran.length).toBe(1);
}, 30_000);

test("an atomic hold whose alarm is stranded still ends, once anything reaches its shard", async () => {
  const name = `stranded-hold-${unique()}`;
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  const tx = `tx-${unique()}`;
  expect((await post<{ ok?: unknown }>("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx, method: "transfer", primaryKey: "a", deadline: Date.now() + 300 }, keys: ["b"], payloads: [5] })).ok).toEqual({ prepared: true });
  await call("counters", name, "strandAlarm", ["a"]);
  await Bun.sleep(5_500);
  expect(await call("counters", name, "get", ["a"])).toEqual([10]);
  for (let i = 0; i < 40 && (await post<Tables>("/atomicTables", { cls: "counters", name, bits: "" })).holds.length > 0; i++) await Bun.sleep(100);
  expect((await post<Tables>("/atomicTables", { cls: "counters", name, bits: "" })).holds).toEqual([]);
}, 30_000);

test("an atomic outcome whose alarm is stranded still goes, once anything reaches its shard", async () => {
  const name = `stranded-outcome-${unique()}`;
  await call("presplit", name, "add", ["a"], [10]);
  const bits = hashBit(keyHash("a"), 0) + hashBit(keyHash("a"), 1);
  const tx = `tx-${unique()}`;
  expect((await post<{ ok?: unknown }>("/atomicStep", { cls: "presplit", name, bits, step: { step: "commit", tx, method: "transfer" }, keys: ["a"], payloads: [-5] })).ok).toEqual({ applied: [5] });
  await call("presplit", name, "strandAlarm", ["a"]);
  await Bun.sleep(5_500);
  expect(await call("presplit", name, "get", ["a"])).toEqual([5]);
  for (let i = 0; i < 40 && (await post<Tables>("/atomicTables", { cls: "presplit", name, bits })).outcomes.length > 0; i++) await Bun.sleep(100);
  expect((await post<Tables>("/atomicTables", { cls: "presplit", name, bits })).outcomes).toEqual([]);
}, 30_000);

test("a work-graph runner whose alarm is stranded is woken by the coordinator and finishes the run", async () => {
  // "each": one chunk an alarm invocation, so the runner needs its alarm again after the first.
  const run = `strand-each-${unique()}`;
  const shape: Shape = { trees: 6, size: 4, chainy: 0.3, joins: 0, seed: 11 };
  const started = await post<{ chunks: number }>("/workgraph", { op: "start", run, shape, runners: 1, chunkMs: 8, replayMs: 0, tickMs: 300, wakeAfterMs: 500, deadlineMs: 60_000 });
  expect(started.chunks).toBeGreaterThan(1);
  const until = Date.now() + 25_000;
  let status: RunStatus;
  for (;;) {
    status = await post<RunStatus>("/workgraph", { op: "status", run });
    if (status.state !== "running" || Date.now() > until) break;
    await Bun.sleep(250);
  }
  expect(status.state).toBe("done");
}, 60_000);
