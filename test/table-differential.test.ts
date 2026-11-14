// Oracles that don't reuse the code under test: SQLite itself, in workerd.
//
// 1. Index keys: random tuples ordered by encodeIndexKey must come out as a workerd SQLite table
//    orders them (`ORDER BY c1, c2, c3`), NUL and U+FFFF included.
// 2. A seeded differential test: the same operations on a sharded table (splitting as it goes)
//    and on a plain SQLite table, on adversarial keys; every read, listing and count agrees.
//    FC_SEED picks the run; a failure prints the seed and the operations.
import { expect, test } from "bun:test";
import fc from "fast-check";
import { compareText, encodeIndexKey, type SqlValue } from "../src/table/schema.ts";
import { post, useWorker } from "./harness.ts";

useWorker();

const seed = Number(process.env.FC_SEED ?? 42);
const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const sql = (name: string, statements: { query: string; bindings?: unknown[] }[]) => post<unknown[][]>("/object", { cls: "counters", name: `reference-${name}`, bits: "", method: "sqlRun", args: [statements] });

// ---- 1. Index keys against SQLite's order ----

const value: fc.Arbitrary<SqlValue> = fc.oneof(
  fc.constant(null),
  fc.double({ noNaN: true, noDefaultInfinity: true }),
  fc.integer(),
  fc.constantFrom(0, 1, -1, Number.MAX_VALUE, -Number.MAX_VALUE, Number.MIN_VALUE),
  fc.string({ unit: "binary", maxLength: 6 }),
  fc.constantFrom("", "\u0000", "a\u0000", "a", "ab", "\uFFFF", "😀", "é", "\uD7FF", "\uE000"),
);

test("index keys order tuples as workerd's SQLite orders them", async () => {
  const tuples = fc.sample(fc.tuple(value, value, value), { numRuns: 2_000, seed });
  const name = unique();
  await sql(name, [{ query: "CREATE TABLE t (i INTEGER PRIMARY KEY, c1, c2, c3)" }]);
  for (let i = 0; i < tuples.length; i += 200) await sql(name, tuples.slice(i, i + 200).map((t, j) => ({ query: "INSERT INTO t VALUES (?, ?, ?, ?)", bindings: [i + j, ...t] })));
  const [sqlite] = (await sql(name, [{ query: "SELECT i FROM t ORDER BY c1, c2, c3, i" }])) as { i: number }[][];
  const ours = tuples.map((t, i) => ({ i, key: encodeIndexKey(t) })).sort((a, b) => compareText(a.key, b.key) || a.i - b.i);
  // Where the two orders first part, both tuples: the reproduction.
  const theirs = sqlite!.map((r) => r.i);
  const at = ours.findIndex((o, k) => o.i !== theirs[k]);
  expect(at === -1 ? null : { seed, ours: tuples[ours[at]!.i], sqlite: tuples[theirs[at]!] }).toBeNull();
}, 60_000);

// ---- 2. A sharded table against a plain SQLite table ----

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
const KEYS = ["", "\u0000", "a\u0000", "\uFFFF", "😀", "é", "É", "A", "a", "ab", "a/b", "x\uFFFD", "x😀"];
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "table", name, op, args });
const tryTable = async (name: string, op: string, ...args: unknown[]) => post<{ ok?: unknown; threw?: string }>("/table", { cls: "table", name, op: `try:${op}`, args });

function random(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
  };
}

const LISTINGS: { index?: string; order: string; where?: { column: string; op: string; value: unknown }[]; filter?: string; direction?: "desc" }[] = [
  { order: "name" },
  { order: "name DESC", direction: "desc" },
  { index: "byCreated", order: "createdAt, name" },
  { index: "byEdit", order: "lastEditAt, name" },
  { index: "byDeletedCreated", order: "deleted DESC, createdAt DESC, name DESC", direction: "desc", where: [{ column: "deleted", op: "=", value: false }], filter: "deleted = 0" },
];

test("seeded operations on a sharded table and a plain SQLite table agree on every read, listing and count", async () => {
  const rand = random(seed);
  const pick = <T>(xs: readonly T[]) => xs[Math.floor(rand() * xs.length)]!;
  const name = `differential-${unique()}`;
  await sql(name, [{ query: "CREATE TABLE ref (name TEXT PRIMARY KEY, createdAt INTEGER, lastEditAt INTEGER, deleted INTEGER)" }]);
  const log: string[] = [];
  const differences: string[] = [];
  const keyOf = (k: string) => JSON.stringify(k);
  const splits = ["", "0", "1", "00"];
  for (let step = 0; step < 120; step++) {
    const r = rand();
    const key = pick(KEYS);
    if (r < 0.35) {
      const row: Doc = { name: key, id: "x", createdAt: Math.floor(rand() * 4), lastEditAt: rand() < 0.3 ? null : Math.floor(rand() * 5), deleted: rand() < 0.3, meta: null };
      log.push(`put ${keyOf(key)} ${row.createdAt} ${row.lastEditAt} ${row.deleted}`);
      const ours = await tryTable(name, "put", row);
      const theirs = await sql(name, [{ query: "INSERT INTO ref VALUES (?, ?, ?, ?) ON CONFLICT (name) DO UPDATE SET createdAt = excluded.createdAt, lastEditAt = excluded.lastEditAt, deleted = excluded.deleted", bindings: [row.name, row.createdAt, row.lastEditAt, row.deleted ? 1 : 0] }]).then(() => "ok", (e: Error) => e.message);
      if ((ours.threw === undefined) !== (theirs === "ok")) differences.push(`step ${step}, put ${keyOf(key)}: table ${ours.threw ?? "ok"}, sqlite ${theirs}`);
    } else if (r < 0.45) {
      log.push(`delete ${keyOf(key)}`);
      await tryTable(name, "delete", key);
      await sql(name, [{ query: "DELETE FROM ref WHERE name = ?", bindings: [key] }]);
    } else if (r < 0.5 && splits.length) {
      const bits = splits.shift()!;
      log.push(`split ${keyOf(bits)}`);
      await post("/split", { cls: "table", name, bits });
    } else if (r < 0.75) {
      log.push(`get ${keyOf(key)}`);
      const ours = await tryTable(name, "get", key);
      const [theirs] = (await sql(name, [{ query: "SELECT name FROM ref WHERE name = ?", bindings: [key] }])) as { name: string }[][];
      const got = (ours.ok as { row: Doc } | null | undefined)?.row.name ?? null;
      if (got !== (theirs![0]?.name ?? null)) differences.push(`step ${step}, get ${keyOf(key)}: table ${keyOf(String(got))}${ours.threw ? ` (${ours.threw})` : ""}, sqlite ${keyOf(String(theirs![0]?.name ?? null))}`);
    } else {
      const listing = pick(LISTINGS);
      log.push(`list ${listing.index ?? "primary"}`);
      const ours = (await table<{ row: Doc }[]>(name, "all", { index: listing.index, direction: listing.direction, where: listing.where, limit: 3 })).map((r) => r.row.name);
      const [theirs] = (await sql(name, [{ query: `SELECT name FROM ref ${listing.filter ? `WHERE ${listing.filter}` : ""} ORDER BY ${listing.order}` }])) as { name: string }[][];
      if (JSON.stringify(ours) !== JSON.stringify(theirs!.map((t) => t.name))) differences.push(`step ${step}, list ${listing.index ?? "primary"}: table ${JSON.stringify(ours)}, sqlite ${JSON.stringify(theirs!.map((t) => t.name))}`);
      const count = await table<number>(name, "count");
      const [n] = (await sql(name, [{ query: "SELECT COUNT(*) AS n FROM ref" }])) as { n: number }[][];
      if (count !== n![0]!.n) differences.push(`step ${step}, count: table ${count}, sqlite ${n![0]!.n}`);
    }
    if (differences.length) break;
  }
  if (differences.length) console.log(`seed ${seed}; operations:\n  ${log.join("\n  ")}`);
  expect(differences).toEqual([]);
}, 300_000);
