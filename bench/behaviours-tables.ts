// The overhead tables of RFD0009, from the results files that back them:
//   bun bench/behaviours-tables.ts
// Reads the repeated runs behaviours-probe-edge-<n>.json and behaviours-load-edge-<n>.json, and
// behaviours-client-bun.json, in bench/results. Each cell is the median over runs of each run's
// median, with the range of the runs' medians: the edge's hosts vary from run to run, so one run's
// spread over its rounds says less than the spread between runs.
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const dir = join(import.meta.dir, "results");
const load = <T>(file: string): T => JSON.parse(readFileSync(join(dir, file), "utf8")) as T;
const runsOf = <T>(prefix: string): T[] =>
  readdirSync(dir)
    .filter((f) => new RegExp(`^${prefix}-\\d+\\.json$`).test(f))
    .sort()
    .map((f) => load<T>(f));
const median = (xs: readonly number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
/** "median (min–max)" of the runs' values, formatted by `f`. */
const across = (values: readonly number[], f: (v: number) => string) => (values.length > 1 ? `${f(median(values))} (${f(Math.min(...values))}–${f(Math.max(...values))})` : f(values[0]!));
const micros = (v: number) => `${v.toFixed(1)} µs`;
const millis = (v: number) => `${(v / 1000).toFixed(1)} ms`;

const EVENT_ROWS: [string, string][] = [
  ["plain-put", "put, plain transaction"],
  ["events-put", "put, state machine event"],
  ["receive-put", "put, envelope with a deadline"],
  ["receive-put-keyed", "put, envelope with an idempotency key (kept)"],
  ["plain-inc", "counter in SQL, plain"],
  ["events-inc", "counter in the machine's context"],
  ["seat-plain", "seat grabbed and released, by hand"],
  ["seat-machine", "seat grabbed and released, state machine"],
];
const RPC_ROWS: [string, string][] = [
  ["single", "plain object, raw stub"],
  ["single-client", "plain object, Client"],
  ["single-client-opts", "plain object, Client with a deadline, retries, an interceptor"],
  ["state-machine-plain", "state machine's object, raw stub, plain write"],
  ["state-machine-receive", "state machine's object, raw stub, envelope"],
  ["state-machine", "DurableStateMachine client"],
  ["state-machine-keyed", "DurableStateMachine client with retries (idempotency key)"],
];

type Probe = { generatedAt: string; provenance: { workerVersion: string | null }; events?: { microsPerEvent: Record<string, number[]> }; rpc?: { microsPerCall: Record<string, number[]> } };
const probes = runsOf<Probe>("behaviours-probe-edge");
if (probes.length > 0) {
  console.log(`From ${probes.length} runs of behaviours-probe-edge-<n>.json (${probes[0]!.generatedAt.slice(0, 10)}, Worker ${probes[0]!.provenance.workerVersion?.slice(0, 8)}):\n`);
  console.log("| Inside one object, per event | median of runs (range) |\n|---|---|");
  for (const [kind, label] of EVENT_ROWS) {
    const values = probes.flatMap((p) => (p.events?.microsPerEvent[kind] ? [median(p.events.microsPerEvent[kind]!)] : []));
    if (values.length) console.log(`| ${label} | ${across(values, micros)} |`);
  }
  console.log("\n| One put end to end, one at a time | median of runs (range) |\n|---|---|");
  for (const [target, label] of RPC_ROWS) {
    const values = probes.flatMap((p) => (p.rpc?.microsPerCall[target] ? [median(p.rpc.microsPerCall[target]!)] : []));
    if (values.length) console.log(`| ${label} | ${across(values, millis)} |`);
  }
}

interface LoadPoint {
  target: string;
  operation: string;
  concurrency: number;
  operationsPerSecond: number;
  errors: number;
  latencyMs: { p50: number; p99: number };
}
const loads = runsOf<{ generatedAt: string; points: LoadPoint[] }>("behaviours-load-edge");
if (loads.length > 0) {
  const all = loads.flatMap((run) => run.points);
  const targets = [...new Set(all.map((p) => p.target))];
  const levels = [...new Set(all.map((p) => p.concurrency))].sort((a, b) => a - b);
  for (const operation of ["write", "read"]) {
    console.log(`\nFrom ${loads.length} runs of behaviours-load-edge-<n>.json, one-key ${operation}s of 1 KiB on one object; calls/s, median of runs (range), and the median p50 / p99:\n`);
    console.log(`| Target | ${levels.map((c) => `${c} in flight`).join(" | ")} |\n|---|${levels.map(() => "---").join("|")}|`);
    for (const target of targets) {
      const cells = levels.map((c) => {
        const points = loads.flatMap((run) => run.points.filter((p) => p.target === target && p.operation === operation && p.concurrency === c));
        if (points.length === 0) return "–";
        const failed = points.reduce((n, p) => n + p.errors, 0);
        const rate = across(points.map((p) => p.operationsPerSecond), (v) => Math.round(v).toLocaleString("en-US"));
        return `${rate}/s, ${median(points.map((p) => p.latencyMs.p50))} / ${median(points.map((p) => p.latencyMs.p99))} ms${failed ? `, ${failed.toLocaleString("en-US")} failed` : ""}`;
      });
      console.log(`| ${target} | ${cells.join(" | ")} |`);
    }
  }
}

if (existsSync(join(dir, "behaviours-client-bun.json"))) {
  const client = load<{ nanosPerCall: Record<string, number[]> }>("behaviours-client-bun.json");
  console.log("\nFrom behaviours-client-bun.json:\n");
  console.log("| In the caller, per call, no network | median |\n|---|---|");
  for (const [name, xs] of Object.entries(client.nanosPerCall)) console.log(`| ${name} | ${Math.round(median(xs))} ns |`);
}
