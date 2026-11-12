// Runs the work graph benchmarks (RFD0005) against the deployed bench Worker, and saves every point
// to bench/results/workgraph.json as it finishes.
//
//   BENCH_URL=https://<its domain> BENCH_TOKEN=… BENCH_ACCESS_TOKEN=… \
//   BENCH_SUITES=scaling,faults,ceiling bun bench/workgraph/run.ts
//
// scaling: many small trees, one huge tree, and one chain, each on 1 to 64 runners.
// faults: a share of chunks evicted once, and runners lost for good, against the same run without.
// ceiling: chunks that cost nothing, on many runners: how many chunks a second one coordinator hands out.
import { execSync } from "node:child_process";
import { mkdir, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";

const URL_ = process.env.BENCH_URL ?? "";
if (!URL_) throw new Error("BENCH_URL is the bench Worker's custom domain, e.g. https://do-kit-workgraph.<your-zone>");
const TOKEN = process.env.BENCH_TOKEN ?? "";
let ACCESS = process.env.BENCH_ACCESS_TOKEN ?? "";
// An Access token lasts less than a long run: fetched again from cloudflared every 10 minutes, and
// when Access answers with its sign-in page (logging in again if it expired; nothing of it is kept).
let accessAt = Date.now();
function refreshAccess(force: boolean): void {
  if (!force && Date.now() - accessAt < 10 * 60_000) return;
  const token = (app: string) => execSync(`cloudflared access token -app=${app}`, { stdio: ["ignore", "pipe", "ignore"] }).toString().trim();
  let t = "";
  try {
    t = token(URL_);
  } catch {}
  if (!t.startsWith("ey")) {
    try {
      execSync(`cloudflared access login --no-verbose ${URL_}`, { stdio: "ignore" });
      t = token(URL_);
    } catch {}
  }
  if (t.startsWith("ey")) {
    ACCESS = t;
    accessAt = Date.now();
  }
}
const SUITES = (process.env.BENCH_SUITES ?? "scaling,faults,ceiling").split(",");
/** The coordinator's wakeAfterMs for every run (its default when unset). */
const WAKE_AFTER_MS = process.env.BENCH_WAKE_AFTER_MS ? Number(process.env.BENCH_WAKE_AFTER_MS) : null;
const RUNNERS = (process.env.BENCH_RUNNERS ?? "1,2,4,8,16,32,64").split(",").map(Number);
const FILE = process.env.BENCH_RESULTS_FILE ?? join(import.meta.dir, "..", "results", "workgraph.json");

/** CPU per unit, and the chunk size, for the scaling and fault suites. */
const UNIT_MS = 5;
const CHUNK_MS = 1_000;
const SHAPES = [
  { shape: "forest", units: 40_000, what: "40,000 units in trees of 5" },
  { shape: "tree", units: 40_000, what: "one random tree of 40,000 units" },
  { shape: "chain", units: 2_000, what: "one chain of 2,000 units" },
] as const;

interface Status {
  state: string;
  chunks: { total: number; done: number };
  units: { computed: number; reused: number; replayed: number };
  expired: number;
  woken: number;
  failures: number;
  error: string | null;
  elapsedMs: number | null;
  busyMs?: number;
}

interface Point {
  suite: string;
  shape: string;
  runners: number;
  units: number;
  chunks: number;
  unitMs: number;
  chunkMs: number;
  /** Planned costs (ms): T₁ and T∞. */
  work: number;
  span: number;
  replayed: number;
  planMs: number;
  /** From the coordinator: graphStart to the last chunk done. */
  elapsedMs: number;
  computed: number;
  expired: number;
  woken: number;
  failures: number;
  faults?: { evict?: number; lose?: number; leaseMs?: number };
  wakeAfterMs?: number;
  state: string;
  run: string;
  /** The runners' time on chunks (ms): this run's measured work. */
  busyMs?: number;
  /** The plan's chunk span (ms), and its biggest chunk body (bytes). */
  chunkSpan?: number;
  bodyBytes?: number;
  /** Bytes of entries per slice, and the plan's identity. */
  bytes?: number;
  identity?: string;
  /** The rounds per ms its units burned. */
  itersPerMs: number;
  startedAt: string;
}

async function post<T>(path: string, body: unknown): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    refreshAccess(false);
    const response = await fetch(URL_ + path, { method: "POST", body: JSON.stringify(body), headers: { authorization: `Bearer ${TOKEN}`, "cf-access-token": ACCESS } });
    const text = await response.text();
    if (/Cloudflare Access/.test(text.slice(0, 2000))) {
      refreshAccess(true);
      if (attempt < 2) continue;
    }
    if (response.ok) return JSON.parse(text) as T;
    if (attempt >= 2) throw new Error(`${path}: ${response.status} ${text.slice(0, 500)}`);
    await Bun.sleep(1_000);
  }
}

const results: { schemaVersion: 1; generatedAt: string; commit: string; dirty: boolean; itersPerMs: number; calibrations: unknown[]; notes: string[]; points: Point[] } = await readFile(FILE, "utf8")
  .then((t) => JSON.parse(t))
  .catch(() => null) ?? {
  schemaVersion: 1,
  generatedAt: "",
  commit: "",
  dirty: false,
  itersPerMs: 0,
  calibrations: [],
  notes: [
    "Units burn SHA-256 rounds, calibrated on the edge: rounds per ms of a call to a runner object, timed by the Worker around the call, less an empty call.",
    "elapsedMs is the coordinator's: from graphStart (before the runners are started) to the completion of the last chunk. It includes every runner's first call, every chunk's reads and writes of the results store, and the coordinator's calls.",
    "Each run is a new graph (inputs salted by run), new coordinator, new runners and a new results store (presplit to 16 shards): nothing is reused, and every object starts cold.",
    "The bound is T1/N + T∞ with T1 measured (the 1-runner run of the same shape) and T∞ the planned span in ms.",
  ],
  points: [],
};

async function save() {
  results.generatedAt = new Date().toISOString();
  results.commit = execSync("git rev-parse HEAD").toString().trim();
  results.dirty = execSync("git status --porcelain").toString().trim().length > 0;
  await mkdir(join(import.meta.dir, "..", "results"), { recursive: true });
  await writeFile(`${FILE}.tmp`, JSON.stringify(results, null, 2));
  await rename(`${FILE}.tmp`, FILE);
}

async function calibrate(): Promise<number> {
  const samples: number[] = [];
  for (let i = 0; i < 3; i++) {
    const c = await post<{ itersPerMs: number; ms: number }>("/calibrate", { rounds: 1_000_000 });
    results.calibrations.push({ at: new Date().toISOString(), ...c });
    samples.push(c.itersPerMs);
  }
  samples.sort((a, b) => a - b);
  return samples[1]!;
}

async function measure(suite: string, p: { shape: string; units: number; unitMs: number; chunkMs: number; runners: number; maxUnitsPerChunk?: number; leaseMs?: number; faults?: { evict?: number; lose?: number }; bytes?: number; identity?: "inputs" | "key" }): Promise<Point> {
  const run = `${suite}-${p.shape}-${p.runners}-${Date.now().toString(36)}`;
  const started = await post<{ chunks: number; units: number; work: number; span: number; chunkSpan: number; bodyBytes: number; replayed: number; planMs: number }>("/start", { run, itersPerMs: results.itersPerMs, ...p, ...(WAKE_AFTER_MS ? { wakeAfterMs: WAKE_AFTER_MS } : {}) });
  // Slow machines stretch the longest runs (400 slices of 10 s on 4 runners): BENCH_RUN_TIMEOUT_MIN.
  const timeout = Date.now() + Number(process.env.BENCH_RUN_TIMEOUT_MIN ?? 30) * 60_000;
  let status: Status;
  for (;;) {
    await Bun.sleep(1_000);
    status = await post<Status>("/status", { run });
    if (status.state === "done" || status.state === "failed") break;
    if (Date.now() > timeout) throw new Error(`${run} did not finish: ${JSON.stringify(status)}`);
  }
  const point: Point = { suite, shape: p.shape, runners: p.runners, units: started.units, chunks: started.chunks, unitMs: p.unitMs, chunkMs: p.chunkMs, work: started.work, span: started.span, replayed: started.replayed, planMs: started.planMs, elapsedMs: status.elapsedMs ?? 0, computed: status.units.computed, expired: status.expired, woken: status.woken, failures: status.failures, state: status.state, run, itersPerMs: results.itersPerMs, chunkSpan: started.chunkSpan, bodyBytes: started.bodyBytes, busyMs: status.busyMs ?? 0, ...(WAKE_AFTER_MS ? { wakeAfterMs: WAKE_AFTER_MS } : {}), ...(p.bytes ? { bytes: p.bytes } : {}), ...(p.identity ? { identity: p.identity } : {}), startedAt: new Date(parseInt(run.split("-").at(-1)!, 36)).toISOString(), ...(p.faults || p.leaseMs ? { faults: { ...p.faults, ...(p.leaseMs ? { leaseMs: p.leaseMs } : {}) } } : {}) };
  results.points.push(point);
  await save();
  console.log(`${suite} ${p.shape} N=${p.runners}: ${(point.elapsedMs / 1000).toFixed(1)} s, ${point.chunks} chunks, ${point.computed}/${point.units} computed, expired ${point.expired}, failures ${point.failures}, ${point.state}`);
  if (status.state !== "done" || point.computed !== point.units) throw new Error(`${run}: ${status.state}, ${point.computed} of ${point.units} units computed: ${status.error}`);
  return point;
}

// Pinned (BENCH_ITERS_PER_MS) so that runs from separate invocations do the same work: the rounds
// per ms a calibration measures varies by machine.
const measured = await calibrate();
results.itersPerMs = Number(process.env.BENCH_ITERS_PER_MS ?? measured);
console.log(`calibrated: ${measured.toFixed(0)} rounds/ms, using ${results.itersPerMs.toFixed(0)}`);
await save();

const SHAPE_NAMES = (process.env.BENCH_SHAPES ?? SHAPES.map((s) => s.shape).join(",")).split(",");
const CEILING_RUNNERS = (process.env.BENCH_CEILING_RUNNERS ?? "16,64,128,256,512,1024").split(",").map(Number);
const FAULTS = (process.env.BENCH_FAULTS ?? "none,evict,lose").split(",");

if (SUITES.includes("scaling")) for (const s of SHAPES.filter((s) => SHAPE_NAMES.includes(s.shape))) for (const runners of RUNNERS) await measure("scaling", { shape: s.shape, units: s.units, unitMs: UNIT_MS, chunkMs: CHUNK_MS, runners });

if (SUITES.includes("faults")) {
  const base = { shape: "forest", units: 40_000, unitMs: UNIT_MS, chunkMs: CHUNK_MS, runners: 16 };
  if (FAULTS.includes("none")) await measure("faults", base);
  if (FAULTS.includes("evict")) await measure("faults", { ...base, faults: { evict: 0.2 } });
  if (FAULTS.includes("lose")) await measure("faults", { ...base, faults: { lose: 4 }, leaseMs: 10_000 });
}

// The framework's own cost per unit: units that cost nothing, 5,000 to a chunk, on one runner.
if (SUITES.includes("overhead"))
  for (const identity of ["inputs", "key"] as const) await measure("overhead", { shape: "tiny", units: 40_000, unitMs: 0, chunkMs: 1_000, maxUnitsPerChunk: 5_000, runners: 1, identity });

// Coarse units, as an app that slices its own work would give: 400 slices of about 10 s with 64 KiB
// of binary entries each, and 6 big trees cut into pieces, in chunks of 30 s.
const SLICE_RUNNERS = (process.env.BENCH_SLICE_RUNNERS ?? "4,8,16,32,64").split(",").map(Number);
if (SUITES.includes("slices")) for (const runners of SLICE_RUNNERS) await measure("slices", { shape: "slices", units: 400, unitMs: 10_000, chunkMs: 30_000, runners, bytes: 64 * 1024, identity: "key" });

// Bodies over a row's 8 MB: 4 slices of 2.5 MiB to a chunk, a body of 10 MiB in parts.
if (SUITES.includes("bodies")) await measure("bodies", { shape: "bodies", units: 8, unitMs: 1, chunkMs: 4, runners: 2, bytes: 2.5 * 1024 * 1024, identity: "key" });

if (SUITES.includes("ceiling")) for (const runners of CEILING_RUNNERS) await measure("ceiling", { shape: "tiny", units: 40_000, unitMs: 0, chunkMs: 1, maxUnitsPerChunk: 2, runners });
