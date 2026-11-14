// Renders the docs' benchmark tables from bench/results, between markers:
//
//   <!-- bench:table <id> -->
//   ...generated...
//   <!-- /bench:table -->
//
// `bun bench/tables.ts` rewrites them; `bun bench/tables.ts --check` exits 1 if any differs from
// what the results say (CI runs it), so a number typed by hand never reaches the docs.
import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

interface Point {
  readonly target: string;
  readonly operation: string;
  readonly concurrency: number;
  readonly operationsPerSecond: number;
  readonly errors: number;
  readonly latencyMs: { readonly p50: number };
}

const root = join(import.meta.dir, "..");
const number = (n: number) => Math.round(n).toLocaleString("en-US");

async function points(file: string): Promise<Point[]> {
  const parsed = JSON.parse(await readFile(join(root, "bench/results", file), "utf8")) as { points: Point[] };
  return parsed.points;
}

/** Throughput by concurrency (rows) and shard count (columns), from one operation's points. */
function shardTable(heading: string, all: readonly Point[], operation: string, levels: readonly number[]): string {
  const shards = [1, 4, 16, 64];
  // A file may hold a point more than once (a later run measured it again): the last one counts.
  const at = new Map<string, Point>();
  for (const p of all) if (p.operation === operation) at.set(`${p.target}/${p.concurrency}`, p);
  for (const c of levels) for (const s of shards) if (!at.has(`table-${s}/${c}`)) throw new Error(`no ${operation} point for table-${s} at ${c} in flight`);
  const shown = levels;
  const lines = [`| ${heading} | ${shards.map((s) => `${s} shard${s === 1 ? "" : "s"}`).join(" | ")} |`, `|${shards.map(() => "---|").join("")}---|`];
  shown.forEach((c, i) => {
    const cells = shards.map((s) => {
      const p = at.get(`table-${s}/${c}`)!;
      return `${number(p.operationsPerSecond)}/s, p50 ${number(p.latencyMs.p50)} ms${p.errors > 0 ? `, ${number(p.errors)} failed` : ""}`;
    });
    lines.push(`| ${number(c)}${i === 0 ? " in flight" : ""} | ${cells.join(" | ")} |`);
  });
  return lines.join("\n");
}

/** Every table by id: what renders it. */
const TABLES: Record<string, () => Promise<string>> = {
  "table-writes": async () => shardTable("Writes", await points("table.json"), "write", [64, 256, 1_024]),
  "table-reads": async () => shardTable("Reads", await points("table.json"), "read", [64, 256, 1_024]),
  "table-list": async () => shardTable("Query order (`list`)", await points("v050-table-list.json"), "list", [1, 64, 256, 1_024, 2_048]),
  "table-walk": async () => shardTable('Shard order (`order: "shard"`)', await points("v050-table-list.json"), "walk", [1, 64, 256, 1_024, 2_048]),
};

const DOCS = ["README.md", "docs/performance.md"];
const MARKED = /<!-- bench:table ([\w-]+) -->\n[\s\S]*?<!-- \/bench:table -->/g;

const check = process.argv.includes("--check");
let stale = 0;
for (const doc of DOCS) {
  const path = join(root, doc);
  const text = await readFile(path, "utf8");
  const ids = [...text.matchAll(MARKED)].map((m) => m[1]!);
  const rendered = new Map<string, string>();
  for (const id of ids) {
    const render = TABLES[id];
    if (!render) throw new Error(`${doc}: no table ${id} (known: ${Object.keys(TABLES).join(", ")})`);
    rendered.set(id, await render());
  }
  const next = text.replace(MARKED, (_, id: string) => `<!-- bench:table ${id} -->\n${rendered.get(id)}\n<!-- /bench:table -->`);
  if (next === text) continue;
  if (check) {
    stale++;
    console.error(`${doc}: its benchmark tables differ from bench/results (run bun bench/tables.ts)`);
  } else {
    await writeFile(path, next);
    console.log(`${doc}: tables rewritten`);
  }
}
if (stale > 0) process.exit(1);
