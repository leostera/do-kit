// A runner's report is about the lease it was given. Once that lease ran out and the chunk went to
// another runner, a late report from the first must not move the chunk: its new runner holds it.
import { expect, test } from "bun:test";
import type { RunStatus } from "../src/workgraph/coordinator.ts";
import { post, useWorker } from "./harness.ts";
import type { Shape } from "./workgraph-fixture.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
type Next = { chunk?: number; attempt?: number; wait?: number; done?: true; failed?: string };
const shape: Shape = { trees: 8, size: 5, chainy: 0.3, joins: 0, seed: 5 };

/** A run whose runners never start ("idle"): the test is runners 0 and 1. Its leases run out at once. */
async function idleRun(maxAttempts: number): Promise<string> {
  const run = `idle-late-failure-${unique()}`;
  await post("/workgraph", { op: "start", run, shape, runners: 2, leaseMs: 200, tickMs: 100, wakeAfterMs: 60_000, maxAttempts });
  return run;
}
const next = (run: string, runner: number, finished: unknown = null) => post<Next>("/workgraph", { op: "next", run, runner, finished });
const status = (run: string) => post<RunStatus>("/workgraph", { op: "status", run });

/** Runner 0 takes a chunk; its lease runs out; runner 1 gets the same chunk. */
async function leaseMovedOn(run: string): Promise<{ chunk: number; attempt: number }> {
  const first = await next(run, 0);
  expect(first.chunk).toBeDefined();
  for (let i = 0; i < 50 && (await status(run)).expired === 0; i++) await Bun.sleep(100);
  const second = await next(run, 1);
  expect(second.chunk).toBe(first.chunk!);
  expect(second.attempt).toBe(2);
  return { chunk: first.chunk!, attempt: first.attempt! };
}

test("a failure reported on a lease that ran out leaves the next runner's lease alone", async () => {
  const run = await idleRun(3);
  const { chunk, attempt } = await leaseMovedOn(run);
  // Runner 0's late report; and what it is given next is some other chunk.
  expect((await next(run, 0, { chunk, attempt, error: "late" })).chunk).not.toBe(chunk);
  expect((await status(run)).failures).toBe(0);
  // Runner 1 still holds the chunk: no third runner is given it.
  for (let i = 0; i < 20; i++) {
    const other = await next(run, 100 + i);
    if (other.chunk === undefined) break;
    expect(other.chunk).not.toBe(chunk);
  }
}, 60_000);

test("failures reported on leases that ran out do not count towards the run's attempts", async () => {
  const run = await idleRun(1);
  const { chunk, attempt } = await leaseMovedOn(run);
  await next(run, 0, { chunk, attempt, error: "late" });
  expect((await status(run)).state).toBe("running");
}, 60_000);
