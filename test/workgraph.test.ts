// Work graph runs on objects (RFD0005; specs/work-graph-run.ts), under cf dev or deployed: a
// coordinator, runners and a results store run random forests, and every stored result is
// compared with evaluating the graph directly. Runners are evicted, lost, and fail.
import { expect, test } from "bun:test";
import { planWorkGraph } from "../src/workgraph/plan.ts";
import type { RunStatus } from "../src/workgraph/coordinator.ts";
import { post, useWorker } from "./harness.ts";
import { forest, type Shape } from "./workgraph-fixture.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

interface Started {
  chunks: number;
  units: number;
  work: number;
  span: number;
  status: RunStatus;
}

async function start(run: string, shape: Shape, options: { results?: string; runners?: number; chunkMs?: number; replayMs?: number; leaseMs?: number; tickMs?: number; wakeAfterMs?: number } = {}): Promise<Started> {
  return post<Started>("/workgraph", { op: "start", run, shape, results: options.results ?? run, ...options });
}

async function finished(run: string, timeoutMs = 60_000): Promise<RunStatus> {
  const until = Date.now() + timeoutMs;
  for (;;) {
    const status = await post<RunStatus>("/workgraph", { op: "status", run });
    if (status.state === "done" || status.state === "failed") return status;
    if (Date.now() > until) throw new Error(`run ${run} did not finish: ${JSON.stringify(status)}`);
    await Bun.sleep(200);
  }
}

async function expectResults(run: string, shape: Shape, results: string, options: { chunkMs?: number; replayMs?: number } = {}) {
  const stored = await post<Record<string, unknown>>("/workgraph", { op: "results", run, shape, results, ...options });
  expect(stored).toEqual(Object.fromEntries(forest(shape).expected));
}

test("a forest of small trees: every unit gets its result", async () => {
  const run = `small-${unique()}`;
  const shape: Shape = { trees: 80, size: 10, chainy: 0.3, joins: 0, seed: 1 };
  const started = await start(run, shape, { runners: 4, chunkMs: 40 });
  expect(started.chunks).toBeGreaterThan(4);
  const status = await finished(run);
  expect(status).toMatchObject({ state: "done", chunks: { done: started.chunks, total: started.chunks }, units: { computed: started.units, reused: 0 }, failures: 0 });
  await expectResults(run, shape, run, { chunkMs: 40 });
}, 90_000);

test("big trees cut into chunks, with replayed and stored cuts, and joins across trees", async () => {
  const run = `big-${unique()}`;
  const shape: Shape = { trees: 3, size: 300, chainy: 0.6, joins: 20, seed: 2 };
  const options = { chunkMs: 60, replayMs: 15 };
  const plan = planWorkGraph(forest(shape).g, options);
  expect(plan.chunks.some((c) => c.units.some((u) => u.replay))).toBe(true);
  expect(plan.chunks.some((c) => c.units.some((u) => u.load !== null))).toBe(true);
  expect(plan.rounds).toBe(2);
  await start(run, shape, { runners: 4, ...options });
  expect((await finished(run)).state).toBe("done");
  await expectResults(run, shape, run, options);
}, 90_000);

test("a long chain runs one chunk after another", async () => {
  const run = `chain-${unique()}`;
  const shape: Shape = { trees: 1, size: 400, chainy: 1, joins: 0, seed: 3, exact: true };
  const options = { chunkMs: 100, replayMs: 20 };
  // Each chunk but the top one loads the value of the one above it.
  const plan = planWorkGraph(forest(shape).g, options);
  expect(plan.chunks.length).toBeGreaterThanOrEqual(10);
  expect(plan.chunks.filter((c) => c.needs.length === 1).length).toBe(plan.chunks.length - 1);
  const started = await start(run, shape, { runners: 3, ...options });
  expect(started.span).toBe(started.work);
  expect((await finished(run)).state).toBe("done");
  await expectResults(run, shape, run, options);
}, 90_000);

test("a runner evicted after computing, before writing, computes its chunk again", async () => {
  const run = `evict-${unique()}`;
  const shape: Shape = { trees: 30, size: 8, chainy: 0.3, joins: 5, seed: 4 };
  const started = await start(run, shape, { runners: 3, chunkMs: 40 });
  const status = await finished(run, 120_000);
  expect(status).toMatchObject({ state: "done", units: { computed: started.units }, failures: 0 });
  await expectResults(run, shape, run, { chunkMs: 40 });
}, 150_000);

test("a runner lost holding a lease: the lease runs out, and another runner takes the chunk", async () => {
  const run = `lose-${unique()}`;
  const shape: Shape = { trees: 20, size: 6, chainy: 0.3, joins: 0, seed: 5 };
  await start(run, shape, { runners: 2, chunkMs: 30, leaseMs: 1_500, tickMs: 300 });
  const status = await finished(run);
  expect(status.state).toBe("done");
  // Runner 0 abandons one chunk and stops: one lease runs out.
  expect(status.expired).toBe(1);
  await expectResults(run, shape, run, { chunkMs: 30 });
}, 90_000);

test("a later run over a changed graph computes only the changed units", async () => {
  const results = `shared-${unique()}`;
  const before: Shape = { trees: 25, size: 15, chainy: 0.5, joins: 10, seed: 6 };
  // Changes t3.1 (and t3.10 to t3.14): every unit below them changes too.
  const after: Shape = { ...before, changed: "t3.1" };
  await start(`first-${results}`, before, { results, runners: 4, chunkMs: 40 });
  expect((await finished(`first-${results}`)).state).toBe("done");
  const oldHashes = planWorkGraph(forest(before).g, { chunkMs: 40 }).hashes;
  const newHashes = planWorkGraph(forest(after).g, { chunkMs: 40 }).hashes;
  const changed = [...newHashes].filter(([key, hash]) => oldHashes.get(key) !== hash).length;
  expect(changed).toBeGreaterThan([...newHashes.keys()].filter((k) => k.startsWith("t3.1")).length);
  const started = await start(`second-${results}`, after, { results, runners: 4, chunkMs: 40 });
  const status = await finished(`second-${results}`);
  expect(status).toMatchObject({ state: "done", units: { computed: changed, reused: started.units - changed } });
  await expectResults(`second-${results}`, after, results, { chunkMs: 40 });
}, 120_000);

test("a unit that always fails fails the run, after three attempts", async () => {
  const run = `boom-${unique()}`;
  await start(run, { trees: 2, size: 3, chainy: 0, joins: 0, seed: 7, boom: true }, { runners: 2 });
  const status = await finished(run);
  expect(status.state).toBe("failed");
  expect(status.error).toContain("unit boom: boom");
  expect(status.failures).toBe(3);
}, 90_000);
