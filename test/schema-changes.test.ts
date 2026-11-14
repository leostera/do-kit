// Schema changes on a live table: a deploy that adds an index or a count group to a shard with
// rows. The change must survive a storage error at any statement (each is retried by the next
// call, which must not count or build twice, or skip), and must be done in bounded steps while
// the shard keeps serving: a shard that refuses every call until a build is done is down for the
// whole build, and work proportional to the rows in one call never finishes on a large shard.
import { expect, test } from "bun:test";
import type { StatementCost } from "./worker/src/index.ts";
import { post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown; stars?: number | null };
type Page = { rows: { row: Doc }[]; cursor: string | null };
type Counted = { value: unknown; count: number }[];

const table = <T = unknown>(cls: string, name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls, name, op, args });
/** Rows of schema 1 (no stars), half of them deleted. */
const docs = (n: number): Doc[] => Array.from({ length: n }, (_, i) => ({ name: `doc-${String(i).padStart(5, "0")}`, id: `id-${i}`, createdAt: i, lastEditAt: null, deleted: i % 2 === 0, meta: null }));
const byValue = (counts: Counted) => Object.fromEntries(counts.map((c) => [String(c.value), c.count]));

/** Every row of a listing by byStars, `limit` a page. */
async function walk(cls: string, name: string, direction: "asc" | "desc", limit = 7): Promise<string[]> {
  const out: string[] = [];
  let cursor: string | null = null;
  do {
    const page: Page = await table<Page>(cls, name, "list", { index: "byStars", direction, limit, cursor });
    out.push(...page.rows.map((r) => r.row.name));
    cursor = page.cursor;
  } while (cursor !== null);
  return out;
}
/** byStars order: stars (null first), then the key. */
const expectedByStars = (rows: Doc[], direction: "asc" | "desc") => {
  const sorted = [...rows].sort((a, b) => (a.stars ?? -1) - (b.stars ?? -1) || (a.name < b.name ? -1 : 1)).map((r) => r.name);
  return direction === "asc" ? sorted : sorted.reverse();
};

/** Seeds `rows` under schema 1 (no stars, no byStars, no deletedAgain or byStars groups), then deploys schema 2. */
/** The rows a call read, since the last time asked. */
async function callReads(cls: string, name: string): Promise<number> {
  const all = await post<StatementCost[]>("/object", { cls, name, bits: "", method: "statements", args: ["", true] });
  return all.reduce((n, s) => n + s.rowsRead, 0);
}

async function seedAndUpgrade(cls: string, name: string, rows: Doc[]): Promise<void> {
  for (let i = 0; i < rows.length; i += 150) await table(cls, name, "putMany", rows.slice(i, i + 150).map((row) => ({ row })));
  await table(cls, name, "upgrade", 2, "");
}

// Every statement the migration runs to add byStars and the count groups deletedAgain and byStars.
const MIGRATION_STATEMENTS = [
  "CREATE TABLE \"sharded_table_keys_",
  "INSERT OR REPLACE INTO sharded_table_builds",
  "INSERT OR REPLACE INTO \"sharded_table_keys_",
  "DELETE FROM sharded_table_builds",
  "ALTER TABLE \"sharded_table_keys_",
  "DELETE FROM \"sharded_table_dropped_",
  "DROP TABLE IF EXISTS \"sharded_table_dropped_",
  "INSERT INTO sharded_table_group_counts",
  "INSERT INTO sharded_table_count_groups",
  "DELETE FROM sharded_table_group_counts",
  "DELETE FROM sharded_table_count_groups",
];

for (const statement of MIGRATION_STATEMENTS) {
  test(`a schema change that fails once at \`${statement}\` lists and counts every row exactly once`, async () => {
    const name = `migrate-fault-${unique()}`;
    const rows = docs(40);
    for (let i = 0; i < rows.length; i += 20) await table("flaky", name, "putMany", rows.slice(i, i + 20).map((row) => ({ row })));
    await table("flaky", name, "upgrade", 2, "");
    await table("flaky", name, "failStatementOnce", statement, "");
    // The first call may fail; the ones after it must see the migration done right.
    await table("flaky", name, "get", rows[0]!.name).catch(() => undefined);
    await table("flaky", name, "get", rows[0]!.name).catch(() => undefined);
    expect(await table<number>("flaky", name, "count")).toBe(rows.length);
    const deleted = rows.filter((r) => r.deleted).length;
    expect(byValue(await table<Counted>("flaky", name, "countBy", "deletedAgain"))).toEqual({ false: rows.length - deleted, true: deleted });
    // Rows from before the deploy have no stars.
    expect(byValue(await table<Counted>("flaky", name, "countBy", "byStars"))).toEqual({ null: rows.length });
    expect(await walk("flaky", name, "asc")).toEqual(expectedByStars(rows, "asc"));
    expect(await walk("flaky", name, "desc")).toEqual(expectedByStars(rows, "desc"));
  }, 60_000);
}

test("a new count group is counted in bounded steps: no call reads more than a few build pages", async () => {
  const name = `count-group-steps-${unique()}`;
  const rows = docs(3_000);
  // Calls only: the alarm's steps would land between two calls' and be counted with them.
  await post("/object", { cls: "evolving", name, bits: "", method: "pauseAlarmBuilds", args: [true] });
  await seedAndUpgrade("evolving", name, rows);
  await callReads("evolving", name);
  // The first call after the deploy, and each after it: each does at most a step of the work.
  // EvolvingDocs builds 300 rows a call; allow two steps' worth, and the call's own reads.
  for (let i = 0; i < 30; i++) {
    await table("evolving", name, "get", rows[i]!.name).catch(() => undefined);
    expect(await callReads("evolving", name)).toBeLessThanOrEqual(2 * 300 + 50);
  }
  const deleted = rows.filter((r) => r.deleted).length;
  expect(byValue(await table<Counted>("evolving", name, "countBy", "deletedAgain"))).toEqual({ false: rows.length - deleted, true: deleted });
}, 120_000);

test("a new local index is built in bounded steps: no call reads more than a few build pages", async () => {
  const name = `index-steps-${unique()}`;
  // One step at a time, as calls that each get "not ready" would take them: a client call retries
  // until the build is done, so it would measure the whole build.
  const rows = docs(3_000);
  await post("/object", { cls: "evolving", name, bits: "", method: "pauseAlarmBuilds", args: [true] });
  await seedAndUpgrade("evolving", name, rows);
  const step = () => post<boolean>("/object", { cls: "evolving", name, bits: "", method: "migrateOnce", args: [] });
  await step(); // the count groups, measured above
  let steps = 0;
  for (let done = false; !done && steps < 40; steps++) {
    await table("evolving", name, "rowsRead", "");
    done = await step();
    expect(await table<number>("evolving", name, "rowsRead", "")).toBeLessThanOrEqual(2 * 300 + 50);
  }
  expect(steps).toBeGreaterThan(5);
}, 120_000);

test("a shard building a new index keeps serving gets and puts", async () => {
  const name = `index-serves-${unique()}`;
  const rows = docs(1_500);
  await seedAndUpgrade("evolving", name, rows);
  // Reads and writes of single rows don't need the new index: none is refused while it is built.
  for (let i = 0; i < 10; i++) {
    await table("evolving", name, "get", rows[i]!.name);
    await table("evolving", name, "put", { ...rows[i]!, stars: 9 });
  }
  const shard = await post<{ counters: { notReady: number } }>("/describe", { cls: "evolving", name, bits: "" });
  expect(shard.counters.notReady).toBe(0);
}, 120_000);

test("a new index serves listings once built, with every row, after a build over several calls", async () => {
  const name = `index-built-${unique()}`;
  const rows = docs(1_500);
  await seedAndUpgrade("evolving", name, rows);
  // Listings by the new index may wait for its build; once it is done they list every row.
  let listed: string[] = [];
  for (let i = 0; i < 60; i++) {
    listed = await walk("evolving", name, "asc", 500).catch(() => []);
    if (listed.length === rows.length) break;
    await Bun.sleep(250);
  }
  expect(listed).toEqual(expectedByStars(rows, "asc"));
}, 120_000);
