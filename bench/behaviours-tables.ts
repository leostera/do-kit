// The overhead tables of RFD0009, from the results files that back them:
//   bun bench/behaviours-tables.ts
// Reads behaviours-probe-edge.json, behaviours-load-edge.json and behaviours-client-bun.json in
// bench/results; a missing file's tables are skipped.
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const dir = join(import.meta.dir, "results");
const load = <T>(file: string): T | null => (existsSync(join(dir, file)) ? (JSON.parse(readFileSync(join(dir, file), "utf8")) as T) : null);
const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
const micros = (v: number) => `${v.toFixed(1)} µs`;
const millis = (v: number) => `${(v / 1000).toFixed(2)} ms`;

const EVENT_ROWS: [string, string][] = [
  ["plain-put", "put, plain transaction"],
  ["events-put", "put, behaviour event"],
  ["receive-put", "put, envelope with a deadline"],
  ["receive-put-keyed", "put, envelope with an idempotency key (kept)"],
  ["plain-inc", "counter in SQL, plain"],
  ["events-inc", "counter in the behaviour's JSON state"],
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

const probe = load<{ generatedAt: string; events?: { microsPerEvent: Record<string, number[]> }; rpc?: { microsPerCall: Record<string, number[]> } }>("behaviours-probe-edge.json");
if (probe?.events) {
  console.log(`From behaviours-probe-edge.json (${probe.generatedAt.slice(0, 10)}):\n`);
  console.log("| Inside one object, per event | best | median |\n|---|---|---|");
  for (const [kind, label] of EVENT_ROWS) {
    const xs = probe.events.microsPerEvent[kind];
    if (xs) console.log(`| ${label} | ${micros(Math.min(...xs))} | ${micros(median(xs))} |`);
  }
}
if (probe?.rpc) {
  console.log("\n| One put end to end, one at a time | best | median |\n|---|---|---|");
  for (const [target, label] of RPC_ROWS) {
    const xs = probe.rpc.microsPerCall[target];
    if (xs) console.log(`| ${label} | ${millis(Math.min(...xs))} | ${millis(median(xs))} |`);
  }
}

interface LoadPoint {
  target: string;
  operation: string;
  concurrency: number;
  operationsPerSecond: number;
  errors: number;
  successes: number;
  latencyMs: { p50: number; p99: number };
}
for (const file of ["behaviours-load-edge.json"]) {
  const loadRun = load<{ generatedAt: string; points: LoadPoint[] }>(file);
  if (!loadRun) continue;
  const targets = [...new Set(loadRun.points.map((p) => p.target))];
  const levels = [...new Set(loadRun.points.map((p) => p.concurrency))].sort((a, b) => a - b);
  for (const operation of ["write", "read"]) {
    const points = loadRun.points.filter((p) => p.operation === operation);
    if (points.length === 0) continue;
    console.log(`\nFrom ${file} (${loadRun.generatedAt.slice(0, 10)}), one-key ${operation}s of 1 KiB on one object; calls/s, p50 / p99 ms, failed calls:\n`);
    console.log(`| Target | ${levels.map((c) => `${c} in flight`).join(" | ")} |\n|---|${levels.map(() => "---").join("|")}|`);
    for (const target of targets) {
      const cells = levels.map((c) => {
        const p = points.find((x) => x.target === target && x.concurrency === c);
        return p ? `${Math.round(p.operationsPerSecond).toLocaleString("en-US")}/s, ${p.latencyMs.p50} / ${p.latencyMs.p99} ms${p.errors ? `, ${p.errors.toLocaleString("en-US")} failed` : ""}` : "–";
      });
      console.log(`| ${target} | ${cells.join(" | ")} |`);
    }
  }
}

const client = load<{ nanosPerCall: Record<string, number[]> }>("behaviours-client-bun.json");
if (client) {
  console.log("\nFrom behaviours-client-bun.json:\n");
  console.log("| In the caller, per call, no network | median |\n|---|---|");
  for (const [name, xs] of Object.entries(client.nanosPerCall)) console.log(`| ${name} | ${Math.round(median(xs))} ns |`);
}
