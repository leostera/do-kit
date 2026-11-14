// The benchmarks measure the target, not themselves, and check their answers: a point that lost
// writes, lost rows, or walked a table incompletely fails instead of publishing a good number, and
// one whose drivers could not keep up says so. The pure parts directly, then the bench Worker in
// local development.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { join } from "node:path";
import { createProblem, walkProblems, WriteLog } from "../bench/worker/src/answers.ts";
import { missingBaselines } from "../bench/baselines.ts";
import { callsPerOperation, driversFor, LATE_MS, openLoop, scheduleOf } from "../bench/worker/src/drivers.ts";

test("a key reads back as its last acknowledged write, or a later write whose outcome is unknown", () => {
  const log = new WriteLog();
  log.acked("a", "a1");
  log.acked("a", "a2");
  log.unknown("a", "a3");
  log.unknown("b", "b1");
  const before = () => "seed";
  expect(log.lost(new Map([["a", "a2"], ["b", "seed"]]), before)).toEqual([]);
  expect(log.lost(new Map([["a", "a3"], ["b", "b1"]]), before)).toEqual([]);
  // An older acknowledged value, the value before an acknowledged write, or nothing: lost.
  expect(log.lost(new Map([["a", "a1"], ["b", "seed"]]), before)).toEqual(["a"]);
  expect(log.lost(new Map([["a", "seed"], ["b", "seed"]]), before)).toEqual(["a"]);
  expect(log.lost(new Map([["b", "seed"]]), before)).toEqual(["a"]);
  // An unknown write before the last acknowledged one is no longer allowed.
  log.acked("b", "b2");
  expect(log.lost(new Map([["a", "a2"], ["b", "b1"]]), before)).toEqual(["b"]);
});

test("a walk that repeats or skips rows, and a create count off its outcomes, are wrong", () => {
  expect(walkProblems(["a", "b", "c"], 3)).toEqual({ duplicates: 0, missing: 0 });
  expect(walkProblems(["a", "b", "b"], 3)).toEqual({ duplicates: 1, missing: 1 });
  expect(walkProblems(["a", "b"], 3)).toEqual({ duplicates: 0, missing: 1 });
  expect(createProblem(10, 15, 5, 0)).toBeNull();
  expect(createProblem(10, 17, 5, 2)).toBeNull();
  expect(createProblem(10, 14, 5, 2)).toContain("lost");
  expect(createProblem(10, 18, 5, 2)).toContain("invented");
});

test("drivers scale with the calls an operation makes, not the operations", () => {
  expect(callsPerOperation("read", "table-16")).toBe(1);
  expect(callsPerOperation("rmw", "table-16")).toBe(2);
  expect(callsPerOperation("atomic-4", "split")).toBe(5);
  expect(callsPerOperation("multi-4", "split")).toBe(4);
  expect(callsPerOperation("list", "table-16")).toBe(48);
  expect(callsPerOperation("create", "indexed-16")).toBe(2);
  expect(driversFor(512, callsPerOperation("rmw", "table-16"), 8192)).toBe(2 * driversFor(512, callsPerOperation("write", "table-16"), 8192));
  expect(driversFor(64, 48, 8192)).toBe(64);
});

/** A clock of the test's own: time moves only when every call and sleep waits on it. */
function fakeClock() {
  let now = 0;
  const timers: { at: number; resolve: () => void }[] = [];
  const clock = {
    now: () => now,
    sleep: (ms: number) => new Promise<void>((resolve) => timers.push({ at: now + ms, resolve })),
  };
  /** Runs `work` to its end, moving time to each next timer once nothing else can run. */
  const run = async <T>(work: Promise<T>): Promise<T> => {
    let done = false;
    void work.finally(() => (done = true));
    while (!done) {
      await new Promise((resolve) => setImmediate(resolve));
      if (done) break;
      timers.sort((a, b) => a.at - b.at);
      const next = timers.shift();
      if (!next) throw new Error("nothing to wait on");
      now = Math.max(now, next.at);
      next.resolve();
    }
    return work;
  };
  return { clock, run, oversleep: (ms: number) => (clock.sleep = (d: number) => new Promise<void>((resolve) => timers.push({ at: now + d + ms, resolve }))) };
}

test("the open loop sends on schedule whatever the answers, and counts what it could not send", async () => {
  // Calls of 10 ms at 100 a second on 4 lanes: every send made, on time.
  let f = fakeClock();
  const call = (ms: number, clock = f.clock) => () => clock.sleep(ms);
  let s = await f.run(openLoop(Array.from({ length: 4 }, () => call(10)), 100, 0, 1_000, f.clock));
  expect(s).toEqual({ due: 100, sent: 100, late: 0, skipped: 0 });
  expect(scheduleOf([s]).driverLimited).toBe(false);
  // One lane and calls of 50 ms: the lane is busy for 4 sends of every 5, which are skipped.
  f = fakeClock();
  s = await f.run(openLoop([call(50, f.clock)], 100, 0, 1_000, f.clock));
  expect(s.due).toBe(100);
  expect(s.sent).toBe(20);
  expect(s.skipped).toBe(80);
  expect(scheduleOf([s]).driverLimited).toBe(true);
  // A driver that wakes late (its isolate busy): the sends it makes late are counted.
  f = fakeClock();
  f.oversleep(LATE_MS + 10);
  const lanes = Array.from({ length: 200 }, () => () => Promise.resolve());
  s = await f.run(openLoop(lanes, 100, 0, 1_000, f.clock));
  expect(s.late).toBeGreaterThan(0);
  expect(scheduleOf([s, { due: 1000, sent: 1000, late: 0, skipped: 0 }]).driverLimited).toBe(s.late / 1100 > 0.01);
});

test("a table series is published only with a plain baseline at each of its loads", () => {
  const at = (target: string, concurrency: number, operation = "write") => ({ target, operation, distribution: "uniform", concurrency });
  expect(missingBaselines([at("table-16", 64), at("plain-16", 64), at("table-1", 64), at("table-16", 1), at("plain-1", 1)])).toEqual([]);
  expect(missingBaselines([at("table-16", 64), at("plain-16", 16)])).toEqual(["table-16 write uniform 64"]);
  expect(missingBaselines([at("table-16", 64, "list"), at("plain-16", 64, "write")])).toEqual(["table-16 list uniform 64"]);
  expect(missingBaselines([{ ...at("table-16", 64), arrivalRate: 500 }, { ...at("plain-16", 64), arrivalRate: 1000 }])).toEqual(["table-16 write uniform 500/s"]);
});

// ---- The bench Worker, in local development ----

let base = "";
let stop = async () => {};
beforeAll(async () => {
  const probe = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = probe.port!;
  probe.stop(true);
  const dir = join(import.meta.dir, "../bench/worker");
  // No BENCH_TOKEN: local development, where only localhost is let in.
  const env: Record<string, string | undefined> = { ...process.env, NO_COLOR: "1" };
  delete env.BENCH_TOKEN;
  const child = Bun.spawn([join(import.meta.dir, "../node_modules/.bin/vite"), "--port", String(port), "--strictPort", "--host", "127.0.0.1"], { cwd: dir, stdout: "ignore", stderr: "ignore", env });
  // Asked by the name "localhost" (the address the server listens on is 127.0.0.1).
  base = `http://localhost:${port}`;
  for (let i = 0; ; i++) {
    try {
      if ((await fetch(`${base}/`)).status > 0) break;
    } catch {
      // not up yet
    }
    if (i > 300) throw new Error("the bench Worker did not start");
    await Bun.sleep(100);
  }
  stop = async () => {
    child.kill();
    await child.exited;
  };
}, 60_000);
afterAll(() => stop());

/** The result line of a streamed job. */
async function job(path: string, body: unknown): Promise<Record<string, unknown>> {
  const text = await (await fetch(`${base}${path}`, { method: "POST", body: JSON.stringify(body) })).text();
  const lines = text.trim().split("\n").map((line) => JSON.parse(line) as { result?: Record<string, unknown>; error?: string });
  const last = lines.at(-1)!;
  if (last.error) throw new Error(last.error);
  return last.result!;
}

test("without a token the bench Worker lets in only localhost", async () => {
  const port = new URL(base).port;
  expect((await fetch(`http://127.0.0.1:${port}/split-job?objectName=x`)).status).toBe(401);
  expect((await fetch(`http://127.0.0.1:${port}/split-job?objectName=x`, { headers: { authorization: "Bearer undefined" } })).status).toBe(401);
  expect((await fetch(`${base}/split-job?objectName=x`)).status).not.toBe(401);
}, 30_000);

test("a split point reads every written key back and counts the rows the shards own", async () => {
  const result = await job("/split-bench", { target: "split", objectName: `answers-${Date.now()}`, rows: 2_000, payloadBytes: 256, writers: 4 });
  expect(result.copy).toBe("logical");
  expect(result.checked as number).toBeGreaterThan(0);
  expect({ lostWrites: result.lostWrites, rowDelta: result.rowDelta, ok: result.ok }).toEqual({ lostWrites: 0, rowDelta: 0, ok: true });
}, 120_000);

test("table points check what they read: walks and listings complete, writes read back, creates counted", async () => {
  const point = async (operation: string) => {
    const response = await fetch(`${base}/measure`, { method: "POST", body: JSON.stringify({ target: "table-4", operation, distribution: "uniform", concurrency: 4, objectName: `answers-${operation}-${Date.now()}`, durationMs: 5_000, keyCount: 300, payloadBytes: 64 }) });
    return (await response.json()) as { successes: number; wrongAnswers: number; wrongSamples: string[]; ok: boolean };
  };
  for (const operation of ["walk", "list", "write", "create"]) {
    const result = await point(operation);
    expect(result.successes).toBeGreaterThan(0);
    expect({ operation, wrongAnswers: result.wrongAnswers, wrongSamples: result.wrongSamples, ok: result.ok }).toEqual({ operation, wrongAnswers: 0, wrongSamples: [], ok: true });
  }
}, 180_000);

test("the plain baselines, one object and sixteen sharded by hand, answer the table points right", async () => {
  for (const target of ["plain-1", "plain-16"])
    for (const operation of ["write", "read", "rmw", "list", "walk", "create"]) {
      const response = await fetch(`${base}/measure`, { method: "POST", body: JSON.stringify({ target, operation, distribution: "uniform", concurrency: 4, objectName: `plain-${operation}-${Date.now()}`, durationMs: 5_000, keyCount: 300, payloadBytes: 64 }) });
      const result = (await response.json()) as { successes: number; errors: number; wrongAnswers: number; wrongSamples: string[]; topology: { leaves: number; rows: number }; errorSample: string | null };
      expect({ target, operation, errors: result.errors, errorSample: result.errorSample, wrongAnswers: result.wrongAnswers, wrongSamples: result.wrongSamples }).toEqual({ target, operation, errors: 0, errorSample: null, wrongAnswers: 0, wrongSamples: [] });
      expect(result.successes).toBeGreaterThan(0);
      expect(result.topology.leaves).toBe(Number(target.slice("plain-".length)));
      expect(result.topology.rows).toBeGreaterThanOrEqual(300);
    }
}, 300_000);

test("an open-loop point sends at its arrival rate, and says when its drivers could not", async () => {
  const point = async (extra: object) => {
    const response = await fetch(`${base}/measure`, { method: "POST", body: JSON.stringify({ target: "table-4", operation: "read", distribution: "uniform", objectName: `open-${Date.now()}`, durationMs: 5_000, keyCount: 300, payloadBytes: 64, ...extra }) });
    return (await response.json()) as { successes: number; operationsPerSecond: number; schedule: { due: number; sent: number; late: number; skipped: number }; driverLimited: boolean; withinLatency?: boolean; error?: string };
  };
  const steady = await point({ arrivalRate: 20, concurrency: 16, maxP99Ms: 10_000 });
  expect(steady.error).toBeUndefined();
  expect(steady.schedule.due).toBeGreaterThanOrEqual(95);
  expect(steady.schedule.sent).toBe(steady.schedule.due);
  expect(steady.driverLimited).toBe(false);
  expect(steady.withinLatency).toBe(true);
  expect(steady.operationsPerSecond).toBeGreaterThan(15);
  expect(steady.operationsPerSecond).toBeLessThan(25);
  // One driver with one lane, asked for far more than one call at a time can do.
  const starved = await point({ arrivalRate: 5_000, concurrency: 1, drivers: 1, maxP99Ms: 0.001 });
  expect(starved.schedule.skipped).toBeGreaterThan(0);
  expect(starved.driverLimited).toBe(true);
  expect(starved.withinLatency).toBe(false);
}, 180_000);
