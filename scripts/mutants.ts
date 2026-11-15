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
      { file: "src/sharded/index.ts", find: "    if (answer) return this.answered(answer);\n    this.counters.calls++;\n    for (const key of keys) if (!ownsHash(bits, this.routeHash(key))) throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);\n    this.sampleRoutes(keys);\n    this.countCall(keys.length);\n    const fn = this.routedMethod(method);", replace: "    if (answer && !(method === \"tableGet\" && \"moved\" in answer)) return this.answered(answer);\n    this.counters.calls++;\n    this.sampleRoutes(keys);\n    this.countCall(keys.length);\n    const fn = this.routedMethod(method);" },
      { file: "src/table/index.ts", find: "    this.ready();\n    this.assertOwns(keys);\n    return keys.map((key) => this.getRow(key));", replace: "    this.ready();\n    return keys.map((key) => this.getRow(key));" },
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
    edits: [{ file: "src/sharded/values.ts", find: "export function compareText(a: string, b: string): number {\n  if (a === b) return 0;", replace: "export function compareText(a: string, b: string): number {\n  return a < b ? -1 : a > b ? 1 : 0;" }],
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
    breaks: "a listing returns a row a split gave away once: a shard pages only the rows it owns",
    edits: [
      { file: "src/table/index.ts", find: "AND k._hash >= ? AND k._hash <= ? ORDER BY k._key", replace: "AND (k._hash >= ? OR 1) AND (k._hash <= ? OR 1) ORDER BY k._key" },
      { file: "src/table/index.ts", find: "? AND `}_hash >= ? AND _hash <= ? ORDER BY ${this.q(pk)}", replace: "? AND `}(_hash >= ? OR 1) AND (_hash <= ? OR 1) ORDER BY ${this.q(pk)}" },
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
    name: "page-in-key-order",
    breaks: "a page reads its index's key table in key order, with no sort",
    edits: [{ file: "src/table/index.ts", find: "AND k._hash <= ? ORDER BY k._key ${order} LIMIT ?", replace: "AND k._hash <= ? ORDER BY k._key || '' ${order} LIMIT ?" }],
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
  {
    name: "atomic-client-per-call",
    breaks: "a primary keeps its client and stubs across the atomic calls it coordinates",
    edits: [{ file: "src/sharded/index.ts", find: "    if (this.atomicClientFor?.options === options) return this.atomicClientFor.client;\n", replace: "" }],
    test: "test/atomic.test.ts",
    pattern: "keeps its client",
  },
  {
    name: "compare-never-fails",
    breaks: "bench/compare.ts exits 1 when a number moved past the tolerance",
    edits: [{ file: "bench/compare.ts", find: "process.exit(problems.length > 0 ? 1 : 0);", replace: "process.exit(0);" }],
    test: "test/observability.test.ts",
    pattern: "fails when a measured",
  },
  {
    name: "copy-drops-rows",
    breaks: "a split point finds rows a copy lost (lostWrites, rowDelta)",
    edits: [{ file: "src/sharded/index.ts", find: "        await target.shardedReceive(meta.name, right, epoch, first, table, order.rowid ? names.slice(1) : names, rows);\n        first = false;", replace: "        const last = rows.length === 0 || (rows.length < COPY_PAGE_ROWS && !cut);\n        await target.shardedReceive(meta.name, right, epoch, first, table, order.rowid ? names.slice(1) : names, last && table === \"sharded_rows\" ? rows.slice(0, rows.length >> 1) : rows);\n        first = false;" }],
    test: "test/bench-worker.test.ts",
    pattern: "split point",
  },
  {
    name: "bench-token-unset",
    breaks: "an unset BENCH_TOKEN lets in no one but localhost",
    edits: [{ file: "bench/worker/src/index.ts", find: "    if (!local && (!env.BENCH_TOKEN || request.headers.get(\"authorization\") !== `Bearer ${env.BENCH_TOKEN}`))", replace: "    if (!local && request.headers.get(\"authorization\") !== `Bearer ${env.BENCH_TOKEN}`)" }],
    test: "test/bench-worker.test.ts",
    pattern: "only localhost",
  },
  {
    name: "walk-skips-rows",
    breaks: "a table point fails a walk that skipped rows",
    edits: [{ file: "src/table/client.ts", find: "    return { rows: rows.map(({ row, version }) => ({ row: row as Row, version })), cursor: at === null ? null : encodeShardCursor(fingerprint, at) };", replace: "    return { rows: rows.slice(0, at === null ? rows.length : -1).map(({ row, version }) => ({ row: row as Row, version })), cursor: at === null ? null : encodeShardCursor(fingerprint, at) };" }],
    test: "test/bench-worker.test.ts",
    pattern: "table points",
  },
  {
    name: "open-loop-waits",
    breaks: "the open loop sends whatever the answers",
    edits: [{ file: "bench/worker/src/drivers.ts", find: "      running.add(done);", replace: "      running.add(done);\n      await done;" }],
    test: "test/bench-worker.test.ts",
    pattern: "open loop sends",
  },
  {
    name: "driver-limited-never",
    breaks: "a point whose drivers fell behind says driverLimited",
    edits: [{ file: "bench/worker/src/drivers.ts", find: "driverLimited: due > 0 && (late + skipped) / due > DRIVER_LIMITED_SHARE", replace: "driverLimited: false" }],
    test: "test/bench-worker.test.ts",
    pattern: "open",
  },
  {
    name: "slowest-shard-first",
    breaks: "onCall names the slowest shard call, not the first",
    edits: [{ file: "src/sharded/client.ts", find: "    if (!this.slowest || ms > this.slowest.ms) this.slowest = { bits, ms };", replace: "    if (!this.slowest) this.slowest = { bits, ms };" }],
    test: "test/observability.test.ts",
    pattern: "waited on",
  },
  {
    name: "moved-uncounted",
    breaks: "onCall counts moved answers",
    edits: [{ file: "src/sharded/client.ts", find: "          else if (\"moved\" in answer) {\n            if (tally) tally.retries.moved++;", replace: "          else if (\"moved\" in answer) {" }],
    test: "test/observability.test.ts",
    pattern: "sent a call again",
  },
  {
    name: "slow-calls-without-rounds",
    breaks: "a bench point's slowest operations carry their client calls",
    edits: [{ file: "bench/worker/src/index.ts", find: "          keepIfSlow(elapsed, calls);", replace: "          keepIfSlow(elapsed, []);" }],
    test: "test/bench-worker.test.ts",
    pattern: "slowest operations",
  },
  {
    name: "stranded-alarm-trusted",
    breaks: "an alarm long past due counts as missing, so work arms a new one",
    edits: [{ file: "src/sharded/alarms.ts", find: "  return at !== null && at < Date.now() - STRANDED_ALARM_MS ? null : at;", replace: "  return at;" }],
    test: "test/observability.test.ts",
    pattern: "stranded alarm",
  },
  {
    name: "wipe-window",
    breaks: "a wiped shard refuses calls for its refusal window",
    edits: [{ file: "src/sharded/index.ts", find: "    if (Date.now() - this.wipedAt < WIPED_REFUSE_MS) throw new Error(\"this shard was wiped\");\n", replace: "" }],
    test: "test/wipe.test.ts",
    pattern: "within its refusal window",
  },
  {
    name: "deadline-ignored",
    breaks: "a call past its caller's deadline is refused, and writes nothing",
    edits: [{ file: "src/sharded/index.ts", find: "    if (deadline !== null && deadline !== undefined && Date.now() > deadline) throw new Expired(", replace: "    if (false && deadline !== null && deadline !== undefined && Date.now() > deadline) throw new Expired(" }],
    test: "test/background.test.ts",
    pattern: "past its deadline is refused",
  },
  {
    name: "size-cap-off",
    breaks: "a shard at refuseAtBytes refuses writes that grow it",
    edits: [{ file: "src/sharded/index.ts", find: "(after >= cap || this.fullAt !== null)", replace: "(false)" }],
    test: "test/size.test.ts",
    pattern: "refuses writes that grow it",
  },
  {
    name: "refusal-never-splits",
    breaks: "a write refused at the cap runs the split decision too",
    edits: [{ file: "src/sharded/index.ts", find: "      if (error instanceof Full) this.checkSize();\n", replace: "" }],
    test: "test/size.test.ts",
    pattern: "the refusal splits it",
  },
  {
    name: "split-backoff-off",
    breaks: "splits given up one after another wait longer and longer",
    edits: [{ file: "src/sharded/index.ts", find: "    if (failed === 0) return 0;\n    return splits.at(-1)!.at", replace: "    if (failed >= 0) return 0;\n    return splits.at(-1)!.at" }],
    test: "test/size.test.ts",
    pattern: "longer and longer",
  },
  {
    name: "compaction-mirror",
    breaks: "a write during key-table compaction reaches the compacted copy",
    edits: [{ file: "src/table/index.ts", find: "    if (c && c.after !== null && compareText(key, c.after) <= 0) this.db.exec(`INSERT OR REPLACE INTO \"${c.into}\" (_key, _pk, _hash) VALUES (?, ?, ?)`, key, pk, hash);\n", replace: "" }],
    test: "test/table-compaction.test.ts",
  },
  {
    name: "legacy-rows-unread",
    breaks: "a shard of the first layout reads its rows where they are",
    edits: [{ file: "src/sharded/index.ts", find: "    if (row || !this.legacyRows) return row?.value;", replace: "    if (row || this.legacyRows || !this.legacyRows) return row?.value;" }],
    test: "test/size.test.ts",
    pattern: "first layout",
  },
  {
    name: "route-mismatch-unchecked",
    breaks: "a client routing differently from the object is refused, and nothing is written",
    edits: [{ file: "src/sharded/index.ts", find: "    // client would look for these keys elsewhere once the tree splits.\n    if (hashes) {", replace: "    // client would look for these keys elsewhere once the tree splits.\n    if (hashes && false) {" }],
    test: "test/colocation.test.ts",
    pattern: "route differently are refused",
  },
  {
    name: "routes-unsampled",
    breaks: "a shard keeps a sample of keys and their routes, so a deploy that changes routeOf is refused",
    edits: [{ file: "src/sharded/index.ts", find: "    if (kept.length >= ROUTE_SAMPLE || keys.length === 0) return;", replace: "    return;" }],
    test: "test/route-changes.test.ts",
  },
  {
    name: "alarm-backoff-forgotten",
    breaks: "an onAlarm that keeps throwing is retried less and less often",
    edits: [{ file: "src/sharded/index.ts", find: "      const failures = threw ? this.loadWakeFailures() + 1 : 0;", replace: "      const failures = threw ? 1 : 0;" }],
    test: "test/background.test.ts",
    pattern: "less and less often",
  },
  {
    name: "layer-alarm-with-app",
    breaks: "the library's alarm work runs though the app's onAlarm throws",
    edits: [{ file: "src/sharded/index.ts", find: "      try {\n        await this.layerAlarm();\n      } catch (error) {\n        threw = true;\n        console.error(JSON.stringify({ event: \"layer-alarm-failed\", node: this.nodeLabel(), error: String(error) }));\n      }\n      try {\n        await this.onAlarm();", replace: "      try {\n        await this.onAlarm();\n        await this.layerAlarm();" }],
    test: "test/background.test.ts",
    pattern: "throws before super",
  },
  {
    name: "flood-unshed",
    breaks: "a shard flooded with listings refuses some, so its writes still run",
    edits: [{ file: "src/table/index.ts", find: "    if (this.scannedThisSecond >= this.scanRowsPerSecond) this.notReady", replace: "    if (this.scannedThisSecond >= Infinity) this.notReady" }],
    test: "test/cost.test.ts",
    pattern: "flooded",
  },
  {
    name: "abandon-after-commit",
    breaks: "a split that committed is never given up, which would discard the shard that owns its right half",
    edits: [{ file: "src/sharded/index.ts", find: "      // which would discard the copy that now owns it. What is left runs after the finally.\n    } catch (error) {", replace: "      // which would discard the copy that now owns it. What is left runs after the finally.\n      await this.armAlarm(Date.now());\n    } catch (error) {" }],
    test: "test/sharded.test.ts",
    pattern: "after a split commits",
  },
  {
    name: "meta-set-before-stored",
    breaks: "a meta write that fails leaves the meta in memory as stored, so a failed split commit is not read back as committed",
    edits: [{ file: "src/sharded/index.ts", find: "    this.sql.exec(\"INSERT INTO sharded_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value\", JSON.stringify(meta));\n    this.meta = meta;", replace: "    this.meta = meta;\n    this.sql.exec(\"INSERT INTO sharded_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value\", JSON.stringify(meta));" }],
    test: "test/sharded.test.ts",
    pattern: "commit fails to store",
  },
  {
    name: "page-cursor-left-open",
    breaks: "a listing page reads each statement to its end, so a page that stops early blocks no DROP or ALTER TABLE",
    edits: [{ file: "src/table/schema.ts", find: "    const n = Math.min(size, limit - read);\n    const rows = chunk(n).toArray();\n    read += rows.length;\n    yield* rows;\n    if (rows.length < n) return;", replace: "    for (const row of chunk(limit)) yield row;\n    return;" }],
    test: "test/schema-statements.test.ts",
    pattern: "stops early",
  },
  {
    name: "copy-cursor-left-open",
    breaks: "a row copy reads a cut batch's statement to its end",
    edits: [{ file: "src/sharded/index.ts", find: "            cut = true;\n            continue;", replace: "            cut = true;\n            break;" }],
    test: "test/size.test.ts",
    pattern: "batch was cut",
  },
  {
    name: "restarted-run-unticked",
    breaks: "a start made again for a run stored as running arms its next tick",
    edits: [{ file: "src/workgraph/coordinator.ts", find: "    if (meta.state === \"running\") {\n      await this.armTick(Date.now());", replace: "    if (meta.state === \"running\") {" }],
    test: "test/workgraph.test.ts",
    pattern: "all fail",
  },
  {
    name: "start-meta-after-rollback",
    breaks: "a start rolled back leaves the run ready in memory, as stored",
    edits: [{ file: "src/workgraph/coordinator.ts", find: "      this.meta = meta; // rolled back: as stored, ready to start\n", replace: "" }],
    test: "test/workgraph.test.ts",
    pattern: "same instance",
  },
  {
    name: "start-not-atomic",
    breaks: "a start stores the run and its runners all or nothing",
    edits: [{ file: "src/workgraph/coordinator.ts", find: "      this.ctx.storage.transactionSync(() => {\n        this.save(started);", replace: "      ((f: () => void) => f())(() => {\n        this.save(started);" }],
    test: "test/workgraph.test.ts",
    pattern: "after a restart",
  },
  {
    name: "graph-meta-set-before-stored",
    breaks: "a coordinator's meta write that fails leaves the meta in memory as stored",
    edits: [{ file: "src/workgraph/coordinator.ts", find: "    this.sql.exec(\"INSERT INTO graph_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value\", JSON.stringify(meta));\n    this.meta = meta;", replace: "    this.meta = meta;\n    this.sql.exec(\"INSERT INTO graph_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value\", JSON.stringify(meta));" }],
    test: "test/workgraph.test.ts",
    pattern: "seal whose run",
  },
  {
    name: "graph-alarm-swallows",
    breaks: "a coordinator alarm that fails to re-arm throws, so the platform retries it",
    edits: [{ file: "src/workgraph/coordinator.ts", find: "      await this.ctx.storage.setAlarm(Date.now() + (this.meta?.tickMs || 5_000));\n    }\n  }", replace: "      await this.ctx.storage.setAlarm(Date.now() + (this.meta?.tickMs || 5_000)).catch(() => undefined);\n    }\n  }" }],
    test: "test/workgraph.test.ts",
    pattern: "fails to re-arm",
  },
  {
    name: "graph-status-no-backstop",
    breaks: "a status poll re-arms a running run's lost alarm",
    edits: [{ file: "src/workgraph/coordinator.ts", find: "    if (this.meta.state === \"running\") await this.backstopAlarm(Date.now()).catch(() => undefined);\n", replace: "" }],
    test: "test/workgraph.test.ts",
    pattern: "brought back by status polls",
  },
  {
    name: "row-bound-too-low",
    breaks: "the bound that skips counting a row's UTF-8 is never below its size",
    edits: [{ file: "src/table/schema.ts", find: "bytes += typeof v === \"string\" ? 3 * v.length", replace: "bytes += typeof v === \"string\" ? 2 * v.length" }],
    test: "test/schema.test.ts",
    pattern: "UTF-8, and maxRowBytes",
  },
  {
    name: "row-always-encoded",
    breaks: "a row well under maxRowBytes is checked without encoding its text",
    edits: [{ file: "src/table/schema.ts", find: "  if (rowBytesAtMost(schema, row) <= max) return;\n", replace: "" }],
    test: "test/schema.test.ts",
    pattern: "without encoding",
  },
  {
    name: "lanes-rounded-up",
    breaks: "a point runs at, and reports, the load it was asked for",
    edits: [{ file: "bench/worker/src/index.ts", find: "      concurrency: input.concurrency,", replace: "      concurrency: Math.max(...lanesFor(input.concurrency, driverCount)) * driverCount," }],
    test: "test/bench-worker.test.ts",
    pattern: "load asked for",
  },
  {
    name: "page-reads-whole-chunks",
    breaks: "a page reads about the rows it answers, not a statement's worth more",
    edits: [{ file: "src/table/index.ts", find: "lo, hi, n), request.limit)) {", replace: "lo, hi, n))) {" }],
    test: "test/cost.test.ts",
    pattern: "a page reads about",
  },
  {
    name: "index-page-reads-whole-chunks",
    breaks: "a global index's page reads about the entries it answers",
    edits: [{ file: "src/indexed/global-index.ts", find: "...(last === null ? [] : [last]), n), request.limit)) {", replace: "...(last === null ? [] : [last]), n))) {" }],
    test: "test/cost.test.ts",
    pattern: "global index's page reads",
  },
  {
    name: "read-chunks-not-doubled",
    breaks: "a read starts with the rows asked for, then doubles them",
    edits: [{ file: "src/table/schema.ts", find: "size = Math.min(READ_CHUNK, size * 2)", replace: "size = Math.min(READ_CHUNK, size)" }],
    test: "test/schema.test.ts",
    pattern: "then doubles them",
  },
  {
    name: "put-reads-whole-row",
    breaks: "a put reads of the row it replaces only its version and the columns its indexes and counts use",
    edits: [{ file: "src/table/index.ts", find: "    if (this.watchesRows()) return \"*\";\n", replace: "    return \"*\";\n" }],
    test: "test/cost.test.ts",
    pattern: "a put reads of the row",
  },
  {
    name: "shard-fits-encodes",
    breaks: "a put copies no text to UTF-8 to check a row's size, in the shard",
    edits: [{ file: "src/table/index.ts", find: "    if (bound <= max) return;\n", replace: "" }],
    test: "test/cost.test.ts",
    pattern: "copies no text",
  },
  {
    name: "client-fits-encodes-on-put",
    breaks: "a put copies no text to UTF-8 to check a row's size, in the client",
    edits: [{ file: "src/table/schema.ts", find: "  if (rowBytesAtMost(schema, row) <= max) return;\n", replace: "" }],
    test: "test/cost.test.ts",
    pattern: "copies no text",
  },
  {
    name: "owned-row-count-all-buckets",
    breaks: "a shard's row count is of the rows it owns, not every bucket it stores until cleanup",
    edits: [{ file: "src/table/index.ts", find: "SELECT SUM(n) AS n FROM ${COUNTS} WHERE bucket >= ? AND bucket <= ?`, lo, hi).one().n ?? 0;\n  }", replace: "SELECT SUM(n) AS n FROM ${COUNTS} WHERE bucket >= ? OR bucket <= ?`, lo, hi).one().n ?? 0;\n  }" }],
    test: "test/colocation.test.ts",
    pattern: "a routed table lists and counts",
  },
  {
    name: "overload-not-answered",
    breaks: "an Overloaded a method throws is answered as data, so the caller gets its class",
    edits: [{ file: "src/sharded/index.ts", find: "      if (error instanceof Expired) return { expired: bits };\n      if (error instanceof Overloaded) return this.answered({ overloaded: error.toAnswer() });\n", replace: "      if (error instanceof Expired) return { expired: bits };\n" }],
    test: "test/overload.test.ts",
    pattern: "an app's kind thrown in a shard",
  },
  {
    name: "overload-registry-ignored",
    breaks: "an overload answer becomes the class of its kind",
    edits: [{ file: "src/sharded/client.ts", find: "return (this.overloads.get(parsed.kind) ?? Overloaded).fromAnswer(parsed, \"object\");", replace: "return Overloaded.fromAnswer(parsed, \"object\");" }],
    test: "test/overload-client.test.ts",
    pattern: "class of its kind",
  },
  {
    name: "runtime-overload-unmapped",
    breaks: "the runtime's shedding is a ShardOverloaded",
    edits: [{ file: "src/sharded/client.ts", find: "      throw runtimeOverload(bits, error) ?? error;", replace: "      throw error;" }],
    test: "test/overload-client.test.ts",
    pattern: "the runtime's shedding",
  },
  {
    name: "runtime-overload-trusts-methods",
    breaks: "a method's error that reads like the runtime's overload is not a refusal",
    edits: [{ file: "src/sharded/overload.ts", find: "  if (e?.remote === true) return null;\n", replace: "" }],
    test: "test/overload-client.test.ts",
    pattern: "the runtime's shedding",
  },
  {
    name: "refused-listed-unknown",
    breaks: "a refused shard's keys are listed as refused, not unknown",
    edits: [{ file: "src/sharded/client.ts", find: "refused: failed.filter((f) => f.error instanceof Overloaded).map(listed), unknown: failed.filter((f) => !(f.error instanceof Overloaded)).map(listed)", replace: "refused: [], unknown: failed.map(listed)" }],
    test: "test/overload-client.test.ts",
    pattern: "lists that shard's keys as refused",
  },
  {
    name: "scan-overload-not-answered",
    breaks: "an Overloaded a scan method throws is answered as data too",
    edits: [{ file: "src/sharded/index.ts", find: "      if (error instanceof Full) return { full: bits, bytes: error.bytes, cap: error.cap };\n      if (error instanceof Overloaded) return this.answered({ overloaded: error.toAnswer() });\n      throw error;\n    }\n  }\n\n  /** Runs a routed method", replace: "      if (error instanceof Full) return { full: bits, bytes: error.bytes, cap: error.cap };\n      throw error;\n    }\n  }\n\n  /** Runs a routed method" }],
    test: "test/overload.test.ts",
    pattern: "a scan method's overload",
  },
];

/** A mutant whose text to change is gone from the source. */
class StaleMutant extends Error {}

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
      if (at < 0 || current.indexOf(edit.find, at + 1) >= 0) throw new StaleMutant(`${mutant.name}: the text to mutate is not in ${edit.file} exactly once; update the mutant`);
      writeFileSync(edit.file, current.replace(edit.find, edit.replace));
    }
    rmSync("test/worker/.cloudflare", { recursive: true, force: true });
    const run = spawnSync("bun", ["test", mutant.test, ...(mutant.pattern ? ["-t", mutant.pattern] : [])], { encoding: "utf8", timeout: 900_000 });
    const killed = run.status !== 0 && /\(fail\)/.test(`${run.stdout}${run.stderr}`);
    console.log(`${killed ? "killed  " : "SURVIVED"} ${mutant.name}: ${mutant.breaks}`);
    if (!killed) survivors.push(mutant.name);
  } catch (error) {
    if (!(error instanceof StaleMutant)) throw error;
    // The source moved on: the guard may still be held, but this mutant no longer says so.
    console.log(`STALE    ${error.message}`);
    survivors.push(mutant.name);
  } finally {
    for (const [file, source] of originals) writeFileSync(file, source);
    rmSync("test/worker/bun.lock", { force: true });
  }
}
if (survivors.length) {
  console.log(`\n${survivors.length} mutant(s) survived or are stale: ${survivors.join(", ")}`);
  process.exit(1);
}
