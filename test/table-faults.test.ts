// A statement-level fault sweep. For each table operation, record every SQL statement a clean run
// issues; then, on a fresh table each time, fail each one of them once (as a storage error would),
// retry a call that threw once (as a caller would), and check that the table still agrees with
// itself and with the clean run: its count, every index's walk and every row's
// version. A case is named `<scenario>/<statement>#n`: the name is the reproduction.
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";
import type { StatementCost } from "./worker/src/index.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown; stars?: number | null };
type Versioned = { row: Doc; version: number };
type Settled = { ok?: unknown; threw?: string };
type Step = readonly [op: string, ...args: unknown[]];

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "flaky", name, op, args });
const statements = (name: string) => post<StatementCost[]>("/object", { cls: "flaky", name, bits: "", method: "statements", args: ["", true] });
const doc = (i: number): Doc => ({ name: `d${String(i).padStart(2, "0")}`, id: `${i}`, createdAt: i % 4, lastEditAt: i % 3 === 0 ? null : i, deleted: i % 5 === 0, meta: { i } });

interface Scenario {
  readonly name: string;
  readonly steps: readonly Step[];
}
const SCENARIOS: Scenario[] = [
  { name: "put new", steps: [["put", doc(99)]] },
  { name: "put existing", steps: [["put", { ...doc(1), createdAt: 7, deleted: true }]] },
  { name: "patch an indexed column", steps: [["patch", "d02", { createdAt: 9 }]] },
  { name: "patch a column of a two-column index", steps: [["patch", "d02", { deleted: true }]] },
  { name: "delete", steps: [["delete", "d03"]] },
  { name: "apply", steps: [["apply", "d04", "bump", { by: 2 }]] },
  // The fault is armed once the deploy restarted the object (a fault lives in an instance): in the migration.
  { name: "upgrade", steps: [["upgrade", 2, ""], ["arm"], ["get", "d01"], ["count"]] },
];
const SEED = 24;

async function fresh(): Promise<string> {
  const name = `faults-${unique()}`;
  await table(name, "putMany", Array.from({ length: SEED }, (_, i) => ({ row: doc(i) })));
  return name;
}

/** Runs the steps, each retried once if it threw, arming `fault` at "arm" (or first): whether every step got through. */
async function run(name: string, steps: readonly Step[], fault?: string): Promise<boolean> {
  let sure = true;
  const arm = async () => void (fault === undefined ? undefined : await table(name, "failStatementOnce", fault, ""));
  if (!steps.some(([op]) => op === "arm")) await arm();
  for (const [op, ...args] of steps) {
    if (op === "arm") {
      await arm();
      continue;
    }
    if (op === "upgrade") {
      await table(name, "upgrade", ...args);
      continue;
    }
    const once = async () => (await post<Settled>("/table", { cls: "flaky", name, op: `try:${op}`, args })).threw === undefined;
    if (!(await once()) && !(await once())) sure = false;
  }
  return sure;
}

type Snapshot = { rows: Record<string, Doc>; count: number; problems: string[] };

/** What the table says, every way: and where it disagrees with itself. */
async function snapshot(name: string): Promise<Snapshot> {
  const problems: string[] = [];
  const primary = await table<Versioned[]>(name, "all", { limit: 50 });
  const rows = Object.fromEntries(primary.map((r) => [r.row.name, r.row]));
  const names = primary.map((r) => r.row.name).sort();
  for (const index of ["byCreated", "byDeletedCreated"]) {
    const walked = (await table<Versioned[]>(name, "all", { index, limit: 7 })).map((r) => r.row.name).sort();
    if (JSON.stringify(walked) !== JSON.stringify(names)) problems.push(`${index} walks ${walked.length} rows, primary ${names.length}`);
  }
  const count = await table<number>(name, "count");
  if (count !== names.length) problems.push(`count ${count}, rows ${names.length}`);
  for (const r of primary) {
    const got = await table<Versioned | null>(name, "get", r.row.name);
    if (got?.version !== r.version) problems.push(`${r.row.name}: get version ${got?.version}, listed ${r.version}`);
  }
  return { rows, count, problems };
}

for (const scenario of SCENARIOS) {
  test(`every statement of "${scenario.name}" can fail once and the table still agrees with its clean run`, async () => {
    // The clean run: its statements, and where it ends.
    const clean = await fresh();
    await statements(clean);
    expect(await run(clean, scenario.steps)).toBe(true);
    const issued = [...new Set((await statements(clean)).map((s) => s.query).filter((q) => !q.includes("test_")))];
    const reference = await snapshot(clean);
    expect(reference.problems).toEqual([]);
    expect(issued.length).toBeGreaterThan(0);

    const failures: string[] = [];
    const seen = new Map<string, number>();
    const labelled = issued.map((statement) => {
      const short = statement.replace(/\s+/g, " ").slice(0, 60);
      const n = (seen.get(short) ?? 0) + 1;
      seen.set(short, n);
      return { statement, label: `${scenario.name}/${short}#${n}` };
    });
    // Each fault on a table of its own, a few at a time.
    const one = async ({ statement, label }: { statement: string; label: string }) => {
      const name = await fresh();
      const sure = await run(name, scenario.steps, statement);
      const got = await snapshot(name);
      for (const p of got.problems) failures.push(`${label}: ${p}`);
      if (sure && JSON.stringify(got.rows) !== JSON.stringify(reference.rows)) failures.push(`${label}: rows differ from the clean run`);
    };
    for (let i = 0; i < labelled.length; i += 8) await Promise.all(labelled.slice(i, i + 8).map(one));
    failures.sort();
    expect(failures).toEqual([]);
  }, 600_000);
}
