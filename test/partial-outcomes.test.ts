// A call over several shards is several calls. When one shard fails, the others may have
// committed: the caller must learn which, or its retry can't tell its own writes from someone
// else's (a retried create with ifVersion 0 conflicts with itself). Every shard's answer that
// arrived is reported with the failure, and the rows of the shard that failed are unknown, not
// failed: its call may have committed too.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
type ShardResult = { positions: number[]; result: unknown[] };
type Outcome = { ok?: unknown; threw?: string; settled: ShardResult[] | null; unknown: { positions: number[] }[] | null; results: unknown[] | null };

/** The depth-2 shard of a key (PresplitCounters start at depth 2). */
const shardOf = (key: string) => hashBit(keyHash(key), 0) + hashBit(keyHash(key), 1);

test("a call over several shards, one of which fails, reports the answers of the shards that committed", async () => {
  const name = `partial-call-${unique()}`;
  const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);
  await call("presplit", name, "get", keys);
  const failing = shardOf(keys[0]!);
  await call("presplit", name, "failNextStatement", [keys[0]!], ["INSERT OR IGNORE INTO history"]);
  const outcome = await post<Outcome>("/callSettled", { cls: "presplit", name, method: "add", keys, payloads: keys.map(() => 1) });
  expect(outcome.threw).toBeDefined();
  const healthy = keys.map((k, i) => [k, i] as const).filter(([k]) => shardOf(k) !== failing).map(([, i]) => i);
  const settled = (outcome.settled ?? []).flatMap((s) => s.positions).sort((a, b) => a - b);
  expect(settled).toEqual(healthy);
  for (const s of outcome.settled ?? []) expect(s.result).toEqual(s.positions.map(() => 1));
}, 30_000);

test("a call over several shards, one of which fails, reports that shard's keys as unknown", async () => {
  const name = `partial-unknown-${unique()}`;
  const keys = Array.from({ length: 16 }, (_, i) => `k${i}`);
  await call("presplit", name, "get", keys);
  const failing = shardOf(keys[0]!);
  await call("presplit", name, "failNextStatement", [keys[0]!], ["INSERT OR IGNORE INTO history"]);
  const outcome = await post<Outcome>("/callSettled", { cls: "presplit", name, method: "add", keys, payloads: keys.map(() => 1) });
  const lost = keys.map((k, i) => [k, i] as const).filter(([k]) => shardOf(k) === failing).map(([, i]) => i);
  expect((outcome.unknown ?? []).flatMap((u) => u.positions).sort((a, b) => a - b)).toEqual(lost);
}, 30_000);

test("a table write of many rows over two shards, one of which fails, reports each row's outcome in order", async () => {
  const name = `partial-put-${unique()}`;
  const rows = Array.from({ length: 12 }, (_, i) => ({ name: `r${i}`, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null, stars: null }));
  await post("/table", { cls: "flaky", name, op: "put", args: [rows[0]] });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  const right = (key: string) => hashBit(keyHash(key), 0) === "1";
  await post("/table", { cls: "flaky", name, op: "failStatementOnce", args: ["INSERT INTO sharded_table ", "1"] });
  const fresh = rows.slice(1);
  const outcome = await post<Outcome>("/table", { cls: "flaky", name, op: "try:putMany", args: [fresh.map((row) => ({ row, ifVersion: 0 }))] });
  expect(outcome.threw).toBeDefined();
  expect(outcome.results).not.toBeNull();
  expect(outcome.results!.length).toBe(fresh.length);
  fresh.forEach((row, i) => {
    if (!right(row.name)) expect(outcome.results![i]).toMatchObject({ ok: true, version: 1 });
    else expect(outcome.results![i]).not.toMatchObject({ ok: true });
  });
}, 30_000);

test("an indexed write of many rows whose index deliveries are late returns every row's version", async () => {
  const name = `partial-indexed-${unique()}`;
  const user = (id: string) => ({ id, email: `${id}@example.com`, name: id, team: "core", createdAt: 1, active: true });
  await post("/indexed", { name, op: "put", args: [user("warm")] }); // the index shard exists
  await post("/indexed", { name, op: "failApply", args: [true] });
  type Answer = { ok: { version: number }[] } | { error: string; results?: { version: number }[] };
  const answer = await post<Answer>("/indexed", { name, op: "putMany", args: [["a", "b", "c"].map((id) => ({ row: user(id) }))] });
  // Either answered with every row, or thrown with every row's outcome: never one row's error alone.
  const rows = "ok" in answer ? answer.ok : (answer.results ?? []);
  expect(rows.map((r) => r.version)).toEqual([1, 1, 1]);
  await post("/indexed", { name, op: "failApply", args: [false] });
}, 30_000);
