// The benchmarks check their answers: a point that lost writes, lost rows, or walked a table
// incompletely fails instead of publishing a good number. The checks, and a split point run on
// the bench Worker in local development.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { join } from "node:path";
import { createProblem, walkProblems, WriteLog } from "../bench/worker/src/answers.ts";

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
