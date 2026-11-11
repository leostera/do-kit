// Work graph runs on objects (RFD0005; specs/work-graph-run.ts), under cf dev or deployed: a
// coordinator, runners and a results store run random forests, and every stored result is
// compared with evaluating the graph directly. Runners are evicted, lost, and fail.
import { expect, test } from "bun:test";
import { planWorkGraph } from "../src/workgraph/plan.ts";
import type { RunStatus } from "../src/workgraph/coordinator.ts";
import { post, useWorker } from "./harness.ts";
import { forest, slices as fixtureSlices, type Shape } from "./workgraph-fixture.ts";

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
  expect(status.busyMs).toBeGreaterThanOrEqual(0);
  await expectResults(run, shape, run, { chunkMs: 40 });
  // Submitting and starting again, as a retried request would, changes nothing.
  const again = await start(run, shape, { runners: 4, chunkMs: 40 });
  expect(again.status).toMatchObject({ state: "done", chunks: { done: started.chunks, total: started.chunks }, units: { computed: started.units } });
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

test("binary inputs, in chunk bodies of several parts, reach their units as bytes", async () => {
  const run = `bytes-${unique()}`;
  // Units of 10 in chunks of 45: four slices of 700 KiB a chunk, a body of about 2.8 MB, stored in
  // parts of 1 MiB. (Rows hold at most 8 MB on Cloudflare, and cf dev does not enforce it: the
  // bench's bodies suite runs bodies of 10 MiB on the edge.) The
  // pieces load their root's stored value (its path costs 10, over replayMs).
  const slices = { count: 8, bytes: 700 * 1024, pieces: 3, seed: 1 };
  const options = { chunkMs: 45, replayMs: 5, identity: "key" as const };
  const started = await post<Started & { bodyBytes: number[] }>("/workgraph", { op: "start", run, results: run, slices, runners: 3, ...options });
  expect(Math.max(...started.bodyBytes)).toBeGreaterThan(2 * 1024 * 1024);
  // Bytes as bytes: under 1% over the inputs themselves.
  expect(started.bodyBytes.reduce((a, b) => a + b, 0)).toBeLessThan(1.01 * 12 * 700 * 1024);
  const status = await finished(run);
  expect(status).toMatchObject({ state: "done", units: { computed: 12 }, failures: 0 });
  const stored = await post<Record<string, unknown>>("/workgraph", { op: "results", run, results: run, slices, ...options });
  expect(stored).toEqual(Object.fromEntries(fixtureSlices(slices).expected));
}, 120_000);

test("identity by key: a later run with the same keys reuses them, whatever their inputs; by inputs, it computes what changed", async () => {
  const results = `identity-${unique()}`;
  const before = { count: 6, bytes: 1024, pieces: 2, seed: 2 };
  const after = { ...before, changed: "s3" };
  const runOf = async (slices: typeof before, identity: "key" | "inputs") => {
    const run = `${results}-${identity}-${unique()}`;
    await post("/workgraph", { op: "start", run, results: `${results}-${identity}`, slices, runners: 2, chunkMs: 25, identity });
    return finished(run);
  };
  for (const identity of ["key", "inputs"] as const) expect((await runOf(before, identity)).units.computed).toBe(9);
  expect((await runOf(after, "key")).units).toMatchObject({ computed: 0, reused: 9 });
  expect((await runOf(after, "inputs")).units).toMatchObject({ computed: 1, reused: 8 });
}, 120_000);

// ---- Nothing left behind: every object a run touched ends with no storage and no alarm ----

interface Report {
  tables: { name: string; rows: number }[];
  keys: number;
  alarm: number | null;
}
interface Left {
  coordinator: Report;
  runners: Report[];
  results: Report[];
}

const left = (run: string, runners: number, results?: string) => post<Left>("/workgraph", { op: "storage", run, runners, ...(results ? { results } : {}) });
const empty = { tables: [], keys: 0, alarm: null };

/** What the run's objects hold, each from a new instance: one whose constructor makes nothing. */
async function leftAfterRestart(run: string, runners: number, results?: string): Promise<Left> {
  await post("/workgraph", { op: "restart", run, runners, ...(results ? { results } : {}) });
  // A status call to the gone run, then the report: neither may make anything.
  expect(await post("/workgraph", { op: "status", run })).toBeNull();
  return left(run, runners, results);
}

function expectNothingLeft(l: Left) {
  expect(l.coordinator).toEqual(empty);
  for (const r of l.runners) expect(r).toEqual(empty);
  for (const r of l.results) expect(r).toEqual(empty);
}

/** Polls until the run's status is gone. */
async function gone(run: string, timeoutMs = 60_000) {
  const until = Date.now() + timeoutMs;
  while ((await post<RunStatus | null>("/workgraph", { op: "status", run })) !== null) {
    if (Date.now() > until) throw new Error(`run ${run} was not wiped`);
    await Bun.sleep(200);
  }
}

test("a done run keeps only its status, then nothing: not a lost runner, nor its own results store", async () => {
  // Runner 0 abandons its first chunk and keeps its state, as a lost object would; its lease runs out.
  const run = `lose-tidy-${unique()}`;
  const shape: Shape = { trees: 20, size: 6, chainy: 0.3, joins: 0, seed: 5 };
  await post("/workgraph", { op: "start", run, shape, runners: 2, chunkMs: 30, leaseMs: 1_500, tickMs: 300, keepStatusMs: 3_000 });
  const status = await finished(run);
  expect(status).toMatchObject({ state: "done", expired: 1, ownResults: true, results: `${run}/results` });
  // Kept: the status, and the run's results, until keepStatusMs is up.
  for (let i = 0; (await post<RunStatus>("/workgraph", { op: "status", run })).cleanup !== "kept"; i++) {
    if (i > 100) throw new Error("the runners were not stopped");
    await Bun.sleep(100);
  }
  const stored = await post<Record<string, unknown>>("/workgraph", { op: "results", run, shape, chunkMs: 30 });
  expect(stored).toEqual(Object.fromEntries(forest(shape).expected));
  const kept = await left(run, 2);
  expect(kept.coordinator.tables).toEqual([{ name: "graph_meta", rows: 1 }]);
  expect(kept.coordinator.alarm).not.toBeNull();
  for (const r of kept.runners) expect(r).toEqual(empty);
  await gone(run);
  expectNothingLeft(await left(run, 2));
  expectNothingLeft(await leftAfterRestart(run, 2));
}, 90_000);

test("a failed run leaves nothing once its status time is up", async () => {
  const run = `boom-tidy-${unique()}`;
  const shape: Shape = { trees: 4, size: 3, chainy: 0, joins: 0, seed: 1, boom: true };
  await post("/workgraph", { op: "start", run, shape, runners: 2, chunkMs: 1_000, tickMs: 300, keepStatusMs: 500 });
  expect((await finished(run)).state).toBe("failed");
  await gone(run);
  expectNothingLeft(await left(run, 2));
}, 60_000);

test("delete cancels a run partway, runners evicted mid-chunk included, and leaves nothing", async () => {
  // Each chunk throws once after computing, as an eviction would: runners wait on their alarm's retry.
  const run = `evict-delete-${unique()}`;
  const shape: Shape = { trees: 40, size: 6, chainy: 0.3, joins: 0, seed: 9 };
  await post("/workgraph", { op: "start", run, shape, runners: 3, chunkMs: 20, tickMs: 300 });
  await Bun.sleep(1_500);
  const before = await post<RunStatus>("/workgraph", { op: "status", run });
  expect(before.state).toBe("running");
  expect(before.chunks.done).toBeLessThan(before.chunks.total);
  expect(await post("/workgraph", { op: "delete", run })).toBeNull();
  expectNothingLeft(await left(run, 3));
  // Idempotent: deleting what is gone is nothing, and makes nothing.
  expect(await post("/workgraph", { op: "delete", run })).toBeNull();
  expectNothingLeft(await left(run, 3));
  expectNothingLeft(await leftAfterRestart(run, 3));
}, 60_000);

test("a run past its deadline fails, and leaves nothing, though its only runner is lost", async () => {
  // One runner, which abandons its first chunk: nothing else would end the run.
  const run = `lose-deadline-${unique()}`;
  const shape: Shape = { trees: 10, size: 4, chainy: 0.3, joins: 0, seed: 3 };
  await post("/workgraph", { op: "start", run, shape, runners: 1, chunkMs: 30, leaseMs: 60_000, tickMs: 300, deadlineMs: 1_500, keepStatusMs: 1_000 });
  const status = await finished(run);
  expect(status.state).toBe("failed");
  expect(status.error).toContain("deadline");
  await gone(run);
  expectNothingLeft(await left(run, 1));
  expectNothingLeft(await leftAfterRestart(run, 1));
}, 60_000);

test("a shared results store outlives the run; the run itself leaves nothing", async () => {
  const run = `shared-tidy-${unique()}`;
  const results = `shared-${unique()}`;
  const shape: Shape = { trees: 10, size: 5, chainy: 0.3, joins: 0, seed: 4 };
  await post("/workgraph", { op: "start", run, shape, results, runners: 2, chunkMs: 30, tickMs: 300, keepStatusMs: 1_000 });
  expect((await finished(run)).ownResults).toBe(false);
  await gone(run);
  const l = await left(run, 2, results);
  expect(l.coordinator).toEqual(empty);
  for (const r of l.runners) expect(r).toEqual(empty);
  expect(l.results.some((r) => r.tables.some((t) => t.name === "sharded_rows" && t.rows > 0))).toBe(true);
  await expectResults(run, shape, results, { chunkMs: 30 });
}, 60_000);
