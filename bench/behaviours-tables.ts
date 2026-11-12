// The overhead tables of RFD0009, from the results files that back them:
//   bun bench/behaviours-tables.ts [events results] [client results]
import { readFileSync } from "node:fs";
import { join } from "node:path";

const dir = join(import.meta.dir, "results");
const eventsFile = process.argv[2] ?? "behaviours-probe-events-local.json";
const clientFile = process.argv[3] ?? "behaviours-client-bun.json";
const events = JSON.parse(readFileSync(join(dir, eventsFile), "utf8")) as { events: { microsPerEvent: Record<string, number[]> } };
const client = JSON.parse(readFileSync(join(dir, clientFile), "utf8")) as { nanosPerCall: Record<string, number[]> };

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
const micros = (v: number) => `${v.toFixed(2)} µs`;

const EVENT_ROWS: [string, string][] = [
  ["plain-put", "put, plain transaction"],
  ["events-put", "put, behaviour event"],
  ["receive-put", "put, envelope with a deadline"],
  ["receive-put-keyed", "put, envelope with an idempotency key (kept)"],
  ["plain-inc", "counter in SQL, plain"],
  ["events-inc", "counter in the behaviour's JSON state"],
];
console.log(`From ${eventsFile}:\n`);
console.log("| Inside one object, per event | best | median |\n|---|---|---|");
for (const [kind, label] of EVENT_ROWS) {
  const xs = events.events.microsPerEvent[kind]!;
  console.log(`| ${label} | ${micros(Math.min(...xs))} | ${micros(median(xs))} |`);
}

console.log(`\nFrom ${clientFile}:\n`);
console.log("| In the caller, per call, no network | median |\n|---|---|");
for (const [name, xs] of Object.entries(client.nanosPerCall)) console.log(`| ${name} | ${Math.round(median(xs))} ns |`);
