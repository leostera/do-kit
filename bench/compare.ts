// Compares two benchmark result files: `bun bench/compare.ts <before.json> <after.json> [--tolerance 0.02]`.
//
// Each point (an object in a `points` array) is matched by what names it, its text and boolean
// fields (target, operation, distribution...) and its concurrency; then every number they both
// measure is compared. It prints the ones that moved by more than the tolerance (2% by default)
// and exits 1 if any did, or if a point is in one file only.
import { readFile } from "node:fs/promises";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

const args = process.argv.slice(2);
const files = args.filter((a) => !a.startsWith("--") && !/^\d*\.?\d+$/.test(a));
const at = args.indexOf("--tolerance");
const tolerance = at >= 0 ? Number(args[at + 1]) : 0.02;
if (files.length !== 2 || !Number.isFinite(tolerance)) {
  console.error("usage: bun bench/compare.ts <before.json> <after.json> [--tolerance 0.02]");
  process.exit(2);
}

/** What names a point: its text and boolean fields, and its concurrency. */
function identity(point: { [key: string]: Json }): string {
  const named = Object.entries(point).filter(([key, value]) => typeof value === "string" || typeof value === "boolean" || key === "concurrency");
  // Names that differ run to run (an object's name has the run's id in it) don't name the point.
  return JSON.stringify(named.filter(([key]) => !/name$|^run|At$|Sample/i.test(key)).sort(([a], [b]) => a.localeCompare(b)));
}

/** Every number in a point, by its path. */
function numbers(value: Json, path: string, out: Map<string, number>): Map<string, number> {
  if (typeof value === "number") out.set(path, value);
  else if (Array.isArray(value)) value.forEach((v, i) => numbers(v, `${path}[${i}]`, out));
  else if (value && typeof value === "object") for (const [k, v] of Object.entries(value)) numbers(v, path ? `${path}.${k}` : k, out);
  return out;
}

/** The points of a result file, by identity (the last one wins where a file measured one twice). */
function pointsOf(results: Json): Map<string, { [key: string]: Json }> {
  const out = new Map<string, { [key: string]: Json }>();
  const visit = (value: Json) => {
    if (Array.isArray(value)) value.forEach(visit);
    else if (value && typeof value === "object") {
      for (const [key, v] of Object.entries(value)) {
        if (key === "points" && Array.isArray(v)) for (const p of v) if (p && typeof p === "object" && !Array.isArray(p)) out.set(identity(p), p);
        else visit(v);
      }
    }
  };
  visit(results);
  return out;
}

const [before, after] = await Promise.all(files.map(async (f) => pointsOf(JSON.parse(await readFile(f, "utf8")) as Json)));
const problems: string[] = [];
for (const [id, a] of before!) {
  const b = after!.get(id);
  if (!b) {
    problems.push(`only in ${files[0]}: ${id}`);
    continue;
  }
  const [na, nb] = [numbers(a, "", new Map()), numbers(b, "", new Map())];
  for (const [path, x] of na) {
    const y = nb.get(path);
    if (y === undefined || x === y) continue;
    const moved = Math.abs(y - x) / Math.max(Math.abs(x), Math.abs(y));
    if (moved > tolerance) problems.push(`${id} ${path}: ${x} -> ${y} (${(moved * 100).toFixed(1)}%)`);
  }
}
for (const id of after!.keys()) if (!before!.has(id)) problems.push(`only in ${files[1]}: ${id}`);
for (const p of problems) console.log(p);
console.log(`${before!.size} points compared, ${problems.length} moved past ${(tolerance * 100).toFixed(0)}% or unmatched`);
process.exit(problems.length > 0 ? 1 : 0);
