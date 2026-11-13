import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";

type Target = "single" | "split" | "legacy" | "presplit" | "presplit-4096" | "table-1" | "table-4" | "table-16" | "table-64" | "indexed-1" | "indexed-4" | "indexed-16" | "reader-single" | "reader-leased";
type Operation = "read" | "write" | "list" | "walk" | "create" | "atomic-2" | "atomic-4" | "multi-2" | "multi-4" | "mixed";
type Distribution = "uniform" | "hot" | "control-plane";

interface Point {
  readonly target: Target;
  readonly operation: Operation;
  readonly distribution: Distribution;
  readonly concurrency: number;
  readonly payloadBytes: number;
  readonly successes: number;
  readonly errors: number;
  readonly bytes: number;
  readonly elapsedMs: number;
  readonly operationsPerSecond: number;
  readonly mebibytesPerSecond: number;
  readonly latencyMs: { readonly p50: number; readonly p95: number; readonly p99: number; readonly max: number };
  readonly topology: { readonly leaves: number; readonly rows: number; readonly depth: number } | null;
}

const baseUrl = process.env.BENCH_URL?.replace(/\/$/, "");
const token = process.env.BENCH_TOKEN;
// An Access token lasts less than a long run: with BENCH_ACCESS_APP set, it is fetched again from
// cloudflared every 10 minutes, and when Access answers with its sign-in page.
let accessToken = process.env.BENCH_ACCESS_TOKEN;
const accessApp = process.env.BENCH_ACCESS_APP;
let accessTokenAt = Date.now();
async function refreshAccessToken(force: boolean): Promise<void> {
  if (!accessApp || (!force && Date.now() - accessTokenAt < 10 * 60_000)) return;
  let token = (await $`cloudflared access token -app=${accessApp}`.quiet().nothrow().text()).trim();
  // Expired: log in again (it prints the token; nothing of it is kept).
  if (!token.startsWith("ey")) {
    await $`cloudflared access login --no-verbose ${accessApp}`.quiet().nothrow();
    token = (await $`cloudflared access token -app=${accessApp}`.quiet().nothrow().text()).trim();
  }
  if (token.startsWith("ey")) {
    accessToken = token;
    accessTokenAt = Date.now();
    headers["cf-access-token"] = token;
  }
}
if (!baseUrl || !token) throw new Error("Set BENCH_URL and BENCH_TOKEN in the environment.");

const timestamp = new Date().toISOString().replaceAll(/[:.]/g, "-");
const suite = `bench-${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
const concurrencyLevels = (process.env.BENCH_CONCURRENCY ?? "1,16,64,256,1024").split(",").map(Number);
/** Each level first runs unmeasured, so splits that load causes settle before measuring. */
const warmup = process.env.BENCH_WARMUP !== "0";
const payloadBytes = Number(process.env.BENCH_PAYLOAD_BYTES ?? 1024);
const durationMs = Number(process.env.BENCH_DURATION_MS ?? 10_000);
const keyCount = Number(process.env.BENCH_KEY_COUNT ?? 10_000);
const suites = new Set((process.env.BENCH_SUITES ?? "load,split").split(","));
const splitSizesMiB = (process.env.BENCH_SPLIT_MIB ?? "1,16,128,1024").split(",").map(Number);
const splitRowBytes = Number(process.env.BENCH_SPLIT_ROW_BYTES ?? 1024);
// What was measured: the commit (and whether the tree had changes), and the deployed Worker version.
const provenance = {
  commit: (await $`git rev-parse HEAD`.quiet().text()).trim(),
  // The results a run writes are not the tree it measured.
  dirty: (await $`git status --porcelain -- src bench ${":!bench/results"}`.quiet().text()).trim().length > 0,
  doKitVersion: (JSON.parse(await Bun.file(join(import.meta.dir, "..", "package.json")).text()) as { version: string }).version,
  workerVersion: process.env.BENCH_WORKER_VERSION ?? null,
};
const points: Point[] = [];
const splits: unknown[] = [];
const headers: Record<string, string> = { authorization: `Bearer ${token}`, ...(accessToken ? { "cf-access-token": accessToken } : {}), "content-type": "application/json" };

/** One split of a freshly seeded object, run as a background job on the Worker and polled. */
async function splitBench(config: { target: "split" | "legacy" | "two-copy"; objectName: string; rows: number; payloadBytes: number; writers: number }): Promise<Record<string, unknown>> {
  const started = await fetch(`${baseUrl}/split-job`, { method: "POST", headers, body: JSON.stringify(config) });
  if (!started.ok) throw new Error(`Split benchmark failed to start (${started.status}): ${await started.text()}`);
  for (;;) {
    await Bun.sleep(5000);
    await refreshAccessToken(false);
    const response = await fetch(`${baseUrl}/split-job?objectName=${config.objectName}`, { headers });
    if (!response.ok) throw new Error(`Split benchmark poll failed (${response.status}): ${await response.text()}`);
    const state = (await response.json()) as { result?: Record<string, unknown>; error?: string; running: boolean };
    if (state.error) throw new Error(state.error);
    if (state.result) return state.result;
    if (!state.running) throw new Error("split benchmark stopped without a result");
  }
}

async function measure(config: {
  target: Target;
  operation: Operation;
  distribution: Distribution;
  concurrency: number;
  objectName: string;
}): Promise<Point> {
  // A failed point is retried once; what failed is logged, as the platform answered it.
  for (let attempt = 1; ; attempt++) {
    await refreshAccessToken(false);
    // A measurement takes durationMs plus setup; an answer that never comes is a failed attempt, not a hang.
    // The Worker bounds each stage of a point (180 s to prepare, the run plus 60 s, 20 s to describe
    // and 20 s to count) and reports the ones that stalled: this is past all of them.
    const signal = AbortSignal.timeout(durationMs + 330_000);
    let response: Response;
    let text: string;
    try {
      response = await fetch(`${baseUrl}/measure`, { method: "POST", headers, body: JSON.stringify({ ...config, durationMs, keyCount, payloadBytes }), timeout: false, signal } as RequestInit);
      text = await response.text();
    } catch (error) {
      console.error(JSON.stringify({ failed: config, error: String(error) }));
      if (attempt >= 2) throw error;
      continue;
    }
    if (response.ok) {
      try {
        const point = JSON.parse(text) as Point | null;
        // Anything but a measured point (the Worker answered null, say) is a failed attempt.
        if (point !== null && typeof point === "object" && typeof point.operationsPerSecond === "number") return point;
        // Every driver failed (no time measured): say why, not the start of the answer.
        const p = (point ?? {}) as { stalls?: unknown; errorSamples?: unknown; failedDrivers?: unknown };
        console.error(JSON.stringify({ failed: config, noMeasurement: { stalls: p.stalls, errorSamples: p.errorSamples, failedDrivers: p.failedDrivers } }));
        if (attempt >= 2) throw new Error("Benchmark answered no measurement");
        continue;
      } catch {
        // not JSON: fall through
      }
    }
    const title = /<title>([^<]*)<\/title>/.exec(text)?.[1] ?? text.slice(0, 200);
    if (/Cloudflare Access/.test(title)) await refreshAccessToken(true);
    console.error(JSON.stringify({ failed: config, status: response.status, cfRay: response.headers.get("cf-ray"), answer: title.trim() }));
    if (attempt >= 2) throw new Error(`Benchmark request failed (${response.status}): ${title}`);
  }
}

function save(): void {
const results = {
  schemaVersion: 3,
  suite,
  generatedAt: new Date().toISOString(),
  provenance,
  runtime: { compatibilityDate: "2026-10-29", payloadBytes, keyCount, durationMs, concurrencyLevels, splitSizesMiB, splitRowBytes },
  notes: [
    "One request is one one-key read or write RPC. Keyspace is uniform or a single hot key.",
    "Each target/workload uses a fresh object name; concurrency levels increase in sequence, and each level runs once unmeasured first (unless BENCH_WARMUP=0) so the splits its load causes settle before it is measured.",
    "Latency p50/p95/p99 are histogram estimates with 1 ms buckets. Max is the largest observed client-to-object method duration.",
    "Driver Durable Objects run in multiple hinted locations; locationHint is advisory, not a placement guarantee.",
  ],
  points,
  splits,
};
const resultsDir = join(import.meta.dir, "results");
mkdirSync(resultsDir, { recursive: true });
const json = JSON.stringify(results, null, 2) + "\n";
writeFileSync(join(resultsDir, `${timestamp}.json`), json);
writeFileSync(join(resultsDir, process.env.BENCH_RESULTS_NAME ? `${process.env.BENCH_RESULTS_NAME}.json` : "latest.json"), json);

const csvHeader = "target,operation,distribution,concurrency,payloadBytes,successes,errors,opsPerSecond,MiBPerSecond,p50Ms,p95Ms,p99Ms,maxMs,leaves,rows,depth";
const csvRows = points.map((p) => [p.target, p.operation, p.distribution, p.concurrency, p.payloadBytes, p.successes, p.errors, p.operationsPerSecond.toFixed(2), p.mebibytesPerSecond.toFixed(3), p.latencyMs.p50, p.latencyMs.p95, p.latencyMs.p99, p.latencyMs.max, p.topology?.leaves ?? 1, p.topology?.rows ?? "", p.topology?.depth ?? 0].join(","));
if (!process.env.BENCH_RESULTS_NAME) writeFileSync(join(resultsDir, "latest.csv"), `${csvHeader}\n${csvRows.join("\n")}\n`);
console.log(`Saved ${points.length} points in ${resultsDir}`);
}

if (suites.has("load")) for (const operation of (process.env.BENCH_OPERATIONS ?? "write,read").split(",") as Operation[]) {
  for (const distribution of (process.env.BENCH_DISTRIBUTIONS ?? "uniform,hot").split(",") as Distribution[]) {
    for (const target of (process.env.BENCH_TARGETS ?? "single,split,presplit,legacy").split(",") as Target[]) {
      const objectName = `${suite}-${target}-${operation}-${distribution}`;
      for (const concurrency of concurrencyLevels) {
        let point: Point;
        try {
          if (warmup) await measure({ target, operation, distribution, concurrency, objectName });
          point = await measure({ target, operation, distribution, concurrency, objectName });
        } catch (error) {
          // A point that failed twice is logged and left out: the rest of the suite still runs.
          console.error(JSON.stringify({ skipped: { target, operation, distribution, concurrency }, error: String(error) }));
          continue;
        }
        points.push(point);
        save();
        console.log(JSON.stringify({ target, operation, distribution, concurrency: point.concurrency, opsPerSecond: Math.round(point.operationsPerSecond), mebibytesPerSecond: Number(point.mebibytesPerSecond.toFixed(3)), latencyMs: point.latencyMs, errors: point.errors, leaves: point.topology?.leaves ?? 1, ...((point as { stalls?: string[] }).stalls?.length ? { stalls: (point as { stalls?: string[] }).stalls } : {}), ...((point as { leased?: unknown }).leased ? { leased: (point as { leased?: unknown }).leased } : {}) }));
      }
    }
  }
}

if (suites.has("split"))
  for (const mib of splitSizesMiB) {
    for (const target of (process.env.BENCH_SPLIT_TARGETS ?? "split,two-copy,legacy").split(",") as ("split" | "legacy" | "two-copy")[]) {
      // The row copy of v0.1 did not finish a 1 GiB split in two hours: past this size it is skipped.
      if (target === "legacy" && mib > Number(process.env.BENCH_SPLIT_LEGACY_MAX_MIB ?? 128)) continue;
      const rows = Math.round((mib * 1024 * 1024) / splitRowBytes);
      const result = await splitBench({ target, objectName: `${suite}-splitbench-${target}-${mib}`, rows, payloadBytes: splitRowBytes, writers: 8 });
      splits.push(result);
      save();
      console.log(JSON.stringify({ target, mib, copy: result.copy, servingMs: result.servingMs, doneMs: result.doneMs, writeLatencyMs: result.writeLatencyMs, errors: result.errors }));
    }
  }


save();
