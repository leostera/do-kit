// Builds the results files bench/plot.ts reads from the named runs that back the published numbers.
// Each output names the runs it was collected from (their suite, date and provenance). An output
// whose runs are not all there is left as it is, and said so.
//
//   bun bench/collect.ts
import { existsSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

const dir = join(import.meta.dir, "results");

interface Run {
  suite: string;
  generatedAt: string;
  provenance?: unknown;
  runtime?: unknown;
  notes?: string[];
  points?: unknown[];
  splits?: unknown[];
}

/** What each output is made of: runs, and for the core results, the control plane's key count. */
const OUTPUTS: Record<string, { runs: string[]; extra?: Record<string, unknown> }> = {
  "latest.json": { runs: ["v050-load", "v050-control-plane", "v050-splits", "v050-split-1g"], extra: { controlPlaneKeys: 10 } },
  "table.json": { runs: ["v050-table", "v050-table-list", "v050-table-64-read"] },
  "indexed.json": { runs: ["v050-indexed"] },
  "atomic.json": { runs: ["v050-atomic"] },
  "leased.json": { runs: ["v050-leased"] },
};

for (const [output, { runs, extra }] of Object.entries(OUTPUTS)) {
  const missing = runs.filter((name) => !existsSync(join(dir, `${name}.json`)));
  if (missing.length) {
    console.log(`${output}: left as it is, missing ${missing.join(", ")}`);
    continue;
  }
  const loaded = await Promise.all(runs.map(async (name) => ({ name, run: JSON.parse(await readFile(join(dir, `${name}.json`), "utf8")) as Run })));
  // A point measured again by a later run replaces the earlier one (same target, operation, distribution, concurrency).
  const points = new Map<string, unknown>();
  for (const { run } of loaded)
    for (const p of run.points ?? []) {
      const q = p as { target: string; operation: string; distribution: string; concurrency: number };
      points.set(`${q.target} ${q.operation} ${q.distribution} ${q.concurrency}`, p);
    }
  const collected = {
    schemaVersion: 3,
    suite: "0.5.0",
    generatedAt: loaded.map(({ run }) => run.generatedAt).sort().at(-1),
    runs: loaded.map(({ name, run }) => ({ name, suite: run.suite, generatedAt: run.generatedAt, provenance: run.provenance ?? null, runtime: run.runtime ?? null })),
    notes: loaded[0]!.run.notes ?? [],
    ...extra,
    points: [...points.values()],
    splits: loaded.flatMap(({ run }) => run.splits ?? []),
  };
  await writeFile(join(dir, output), JSON.stringify(collected, null, 2) + "\n");
  console.log(`${output}: ${collected.points.length} points, ${collected.splits.length} splits, from ${runs.join(", ")}`);
}

// Work graphs have a runner and a format of their own (bench/workgraph/run.ts): the run is the output.
const workgraph = join(dir, "v050-workgraph.json");
if (existsSync(workgraph)) {
  await writeFile(join(dir, "workgraph.json"), await readFile(workgraph, "utf8"));
  console.log("workgraph.json: from v050-workgraph");
} else console.log("workgraph.json: left as it is, missing v050-workgraph");
