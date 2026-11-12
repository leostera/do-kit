// What behaviours and Client cost (RFD0009), from the bench Worker's two probes, saved to
// bench/results/$BENCH_RESULTS_NAME.json:
//   events: N events inside one object, through the behaviour and done plainly (no RPC)
//   rpc:    one-key puts end to end, one at a time, through each client against a raw stub
//
//   BENCH_URL=… BENCH_TOKEN=… [BENCH_ACCESS_TOKEN=…] BENCH_RESULTS_NAME=behaviours-probe-<what> bun bench/probe-behaviours.ts
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";

const baseUrl = process.env.BENCH_URL?.replace(/\/$/, "");
const token = process.env.BENCH_TOKEN;
if (!baseUrl || !token) throw new Error("Set BENCH_URL and BENCH_TOKEN in the environment.");
const headers: Record<string, string> = { authorization: `Bearer ${token}`, "content-type": "application/json", ...(process.env.BENCH_ACCESS_TOKEN ? { "cf-access-token": process.env.BENCH_ACCESS_TOKEN } : {}) };
const suites = new Set((process.env.BENCH_SUITES ?? "events,rpc").split(","));
const config = {
  events: { n: Number(process.env.BENCH_EVENTS ?? 20_000), rounds: Number(process.env.BENCH_ROUNDS ?? 9) },
  rpc: { calls: Number(process.env.BENCH_CALLS ?? 300), rounds: Number(process.env.BENCH_ROUNDS ?? 9), ...(process.env.BENCH_RPC_TARGETS ? { targets: process.env.BENCH_RPC_TARGETS.split(",") } : {}) },
};
const run = `probe-${crypto.randomUUID().slice(0, 8)}`;

async function post<T>(path: string, body: unknown): Promise<T> {
  const response = await fetch(baseUrl + path, { method: "POST", headers, body: JSON.stringify(body), signal: AbortSignal.timeout(900_000) });
  const text = await response.text();
  if (!response.ok) throw new Error(`${path}: ${response.status} ${text.slice(0, 300)}`);
  return JSON.parse(text) as T;
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
const report = (title: string, unit: string, rows: Record<string, number[]>) => {
  console.log(title);
  for (const [name, xs] of Object.entries(rows)) console.log(`  ${name.padEnd(22)} ${median(xs).toFixed(2).padStart(10)} ${unit}  (min ${Math.min(...xs).toFixed(2)}, max ${Math.max(...xs).toFixed(2)})`);
};

const results: Record<string, unknown> = {
  schemaVersion: 1,
  kind: "behaviours-probe",
  generatedAt: new Date().toISOString(),
  provenance: {
    commit: (await $`git rev-parse HEAD`.quiet().text()).trim(),
    dirty: (await $`git status --porcelain -- src bench ${":!bench/results"}`.quiet().text()).trim().length > 0,
    target: process.env.BENCH_WHERE ?? (baseUrl.includes("localhost") ? "local workerd (cf dev)" : "deployed"),
    workerVersion: process.env.BENCH_WORKER_VERSION ?? null,
  },
  config,
  notes: [
    "events: one call to the object runs N events; the caller times it (the object's clock does not move while it computes), less an empty call. Each timed call follows an untimed empty one, so it doesn't pay to flush the writes of the call before.",
    "events kinds: plain-put/plain-inc do the writes in a transaction each, as a plain object would; events-* run them as behaviour events (this.call); receive-put runs them as envelopes with a deadline; receive-put-keyed adds an idempotency key, kept per call.",
    "rpc: sequential one-key puts of 1 KiB values from the Worker; targets alternate within a round, in a rotating order. Medians are over rounds.",
  ],
};

if (suites.has("events")) {
  const events = await post<{ microsPerEvent: Record<string, number[]>; emptyCallMs: number[] }>("/probe-behaviours", { objectName: `${run}-events`, ...config.events });
  results.events = events;
  report(`Inside one object, ${config.events.n} events per call:`, "µs/event", events.microsPerEvent);
}
if (suites.has("rpc")) {
  const rpc = await post<{ microsPerCall: Record<string, number[]> }>("/probe-rpc", { objectName: `${run}-rpc`, ...config.rpc });
  results.rpc = rpc;
  report(`End to end, ${config.rpc.calls} sequential puts per round:`, "µs/call", rpc.microsPerCall);
}

if (process.env.BENCH_RESULTS_NAME) {
  const dir = join(import.meta.dir, "results");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `${process.env.BENCH_RESULTS_NAME}.json`), JSON.stringify(results, null, 2) + "\n");
  console.log(`Saved bench/results/${process.env.BENCH_RESULTS_NAME}.json`);
}
