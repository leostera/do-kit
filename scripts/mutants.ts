// Mutation checks: each mutant breaks one guard in the source, and the named tests must fail
// with it. A mutant that survives means a guard no test holds. The source is always restored.
//
//   bun scripts/mutants.ts            every mutant
//   bun scripts/mutants.ts nul-escape the mutants whose name contains "nul-escape"
import { spawnSync } from "node:child_process";
import { readFileSync, rmSync, writeFileSync } from "node:fs";

interface Edit {
  readonly file: string;
  readonly find: string;
  readonly replace: string;
}
interface Mutant {
  readonly name: string;
  /** What the mutant breaks. */
  readonly breaks: string;
  readonly edits: readonly Edit[];
  /** The test file and name pattern that must fail. */
  readonly test: string;
  readonly pattern?: string;
}

const MUTANTS: Mutant[] = [
  {
    name: "write-batch-transaction",
    breaks: "a write batch runs in one transaction",
    edits: [{ file: "src/table/index.ts", find: "    return this.transact(keys, write);\n  }", replace: "    return this.waitForHold(keys).then(() => (this.assertOwner(keys), write()));\n  }" }],
    test: "test/table-faults.test.ts",
    pattern: "put new",
  },
  {
    name: "stale-get",
    breaks: "a shard that split stops serving the keys it gave away",
    edits: [
      { file: "src/sharded/index.ts", find: "    if (answer) return this.answered(answer);\n    this.counters.calls++;\n    for (const key of keys) if (!ownsHash(bits, this.routeHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);\n    this.countCall(keys.length);\n    const fn = this.routedMethod(method);", replace: "    if (answer && !(method === \"tableGet\" && \"moved\" in answer)) return this.answered(answer);\n    this.counters.calls++;\n    for (const key of keys) if (!ownsHash(bits, this.routeHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);\n    this.countCall(keys.length);\n    const fn = this.routedMethod(method);" },
      { file: "src/table/index.ts", find: "    this.migrate();\n    this.assertOwns(keys);\n    return keys.map((key) => this.getRow(key));", replace: "    this.migrate();\n    return keys.map((key) => this.getRow(key));" },
    ],
    test: "test/table-history.test.ts",
    pattern: "no read is stale",
  },
  {
    name: "nul-escape",
    breaks: "index keys escape NUL, so a\\0 sorts after a",
    edits: [{ file: "src/table/schema.ts", find: '        if (b === 0x00) hex += "ff";\n', replace: "" }],
    test: "test/table-differential.test.ts",
    pattern: "index keys",
  },
  {
    name: "utf16-index-keys",
    breaks: "index keys encode text as UTF-8, in code point order",
    edits: [{ file: "src/table/schema.ts", find: "      const bytes = ENCODER.encode(v);", replace: "      const bytes = Uint8Array.from([...Array(v.length).keys()].flatMap((i) => [v.charCodeAt(i) >> 8, v.charCodeAt(i) & 0xff])); void ENCODER;" }],
    test: "test/table-differential.test.ts",
    pattern: "index keys",
  },
  {
    name: "utf16-compare-text",
    breaks: "keys compare by code point, as SQLite orders them",
    edits: [{ file: "src/table/schema.ts", find: "export function compareText(a: string, b: string): number {\n  if (a === b) return 0;", replace: "export function compareText(a: string, b: string): number {\n  return a < b ? -1 : a > b ? 1 : 0;" }],
    test: "test/table-differential.test.ts",
    pattern: "seeded operations",
  },
  {
    name: "empty-page-ends-walk",
    breaks: "a shard-order page over empty shards keeps its cursor",
    edits: [{ file: "src/table/client.ts", find: "cursor: at === null ? null : encodeShardCursor(fingerprint, at) };", replace: "cursor: at === null || rows.length === 0 ? null : encodeShardCursor(fingerprint, at) };" }],
    test: "test/table-live.test.ts",
    pattern: "empty shards",
  },
  {
    name: "owned-once",
    breaks: "a listing returns a row a split gave away once (the page's owner check and the merge's dedup: each covers the other)",
    edits: [
      { file: "src/table/index.ts", find: "WHERE ${after}_hash >= ? AND _hash <= ? ", replace: "WHERE ${after}(_hash >= ? OR 1) AND (_hash <= ? OR 1) " },
      { file: "src/table/schema.ts", find: "      if (seen.has(r.key)) return false;\n", replace: "" },
    ],
    test: "test/table.test.ts",
    pattern: "listings match a reference sort",
  },
  {
    name: "default-hold-wait",
    breaks: "a write held by a split waits for it by default",
    edits: [{ file: "src/sharded/index.ts", find: "    if (!this.holding(keys, exceptTx)) return;\n    const start = Date.now();", replace: "    if (!this.holding(keys, exceptTx) || this.policy.holdWaitMs === undefined) return;\n    const start = Date.now();" }],
    test: "test/table-variants.test.ts",
    pattern: "during a split",
  },
  {
    name: "cleanup-batch",
    breaks: "a split's cleanup deletes 250 rows an alarm",
    edits: [{ file: "src/sharded/index.ts", find: "const DELETE_BATCH_ROWS = 250;", replace: "const DELETE_BATCH_ROWS = 10_000;" }],
    test: "test/settle.test.ts",
    pattern: "2,000 rows",
  },
  {
    name: "cleanup-never-done",
    breaks: "a table's cleanup finishes and the shard settles",
    edits: [{ file: "src/table/index.ts", find: "    return deleted + super.cleanupForeign(owns, limit);", replace: "    return Math.max(1, deleted + super.cleanupForeign(owns, limit));" }],
    test: "test/settle.test.ts",
    pattern: "table settles",
  },
  {
    name: "page-indexed-by",
    breaks: "a page walks its index, not the table",
    edits: [{ file: "src/table/index.ts", find: 'FROM ${TABLE} INDEXED BY "${sqlIndex}" WHERE', replace: "FROM ${TABLE} WHERE" }],
    test: "test/query-plans.test.ts",
    pattern: "uses an index",
  },
  {
    name: "alarm-per-write",
    breaks: "writes ask for the alarm only when it is not set for earlier",
    edits: [
      { file: "src/sharded/index.ts", find: "    if (current === null || time < current) await this.ctx.storage.setAlarm(time);", replace: "    void current;\n    await this.ctx.storage.setAlarm(time);" },
      { file: "src/indexed/table.ts", find: "    if (this.wakeAt !== null && this.wakeAt <= time && this.wakeAt > Date.now()) return;\n", replace: "" },
    ],
    test: "test/query-plans.test.ts",
    pattern: "alarm at most once",
  },
];

const only = process.argv[2];
const survivors: string[] = [];
for (const mutant of MUTANTS.filter((m) => !only || m.name.includes(only))) {
  const originals = new Map<string, string>();
  try {
    for (const edit of mutant.edits) {
      const source = originals.get(edit.file) ?? readFileSync(edit.file, "utf8");
      originals.set(edit.file, source);
      const current = readFileSync(edit.file, "utf8");
      const at = current.indexOf(edit.find);
      if (at < 0 || current.indexOf(edit.find, at + 1) >= 0) throw new Error(`${mutant.name}: the text to mutate is not in ${edit.file} exactly once; update the mutant`);
      writeFileSync(edit.file, current.replace(edit.find, edit.replace));
    }
    rmSync("test/worker/.cloudflare", { recursive: true, force: true });
    const run = spawnSync("bun", ["test", mutant.test, ...(mutant.pattern ? ["-t", mutant.pattern] : [])], { encoding: "utf8", timeout: 900_000 });
    const killed = run.status !== 0 && /\(fail\)/.test(`${run.stdout}${run.stderr}`);
    console.log(`${killed ? "killed  " : "SURVIVED"} ${mutant.name}: ${mutant.breaks}`);
    if (!killed) survivors.push(mutant.name);
  } finally {
    for (const [file, source] of originals) writeFileSync(file, source);
    rmSync("test/worker/bun.lock", { force: true });
  }
}
if (survivors.length) {
  console.log(`\n${survivors.length} mutant(s) survived: ${survivors.join(", ")}`);
  process.exit(1);
}
