// What a batch across shards does not isolate, pinned and documented; and counts that can be
// recounted from the rows when they drift.
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const doc = (name: string, i: number) => ({ name, id: name, createdAt: i, lastEditAt: null, deleted: false, meta: null });
const half = (key: string) => (Number.parseInt(createHash("sha256").update(key).digest("hex")[0]!, 16) >> 3 === 0 ? "0" : "1");

test("a putMany whose second shard fails leaves the first shard's rows written", async () => {
  const name = `isolation-${unique()}`;
  await post("/table", { cls: "flaky", name, op: "put", args: [doc("warm", 0)] });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
  const rows = Array.from({ length: 20 }, (_, i) => doc(`r${i}`, i));
  expect(new Set(rows.map((r) => half(r.name))).size).toBe(2);
  await post("/table", { cls: "flaky", name, op: "failStatementOnce", args: ["INSERT INTO sharded_table", "1"] });
  const answer = await post<{ threw?: string }>("/table", { cls: "flaky", name, op: "try:putMany", args: [rows.map((row) => ({ row }))] });
  expect(answer.threw).toBeDefined();
  const got = await post<({ row: { name: string } } | null)[]>("/table", { cls: "flaky", name, op: "getMany", args: [rows.map((r) => r.name)] });
  const written = rows.filter((_, i) => got[i] !== null).map((r) => half(r.name));
  // One transaction per shard: shard 0's part is written, shard 1's is not.
  expect([...new Set(written)]).toEqual(["0"]);
  expect(written.length).toBe(rows.filter((r) => half(r.name) === "0").length);
}, 60_000);

test("the README says what is atomic, and what is not", () => {
  const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const section = readme.split(/^#+ /m).find((s) => s.startsWith("What is atomic"));
  expect(section).toBeDefined();
  // One row; one shard's part of a batch; a batch across shards; reads across shards; counts.
  for (const claim of [/one transaction/i, /partial/i, /snapshot/i, /count/i]) expect(section ?? "").toMatch(claim);
});

test("verifyCounts recounts a table's counts from its rows, and repairs those that drifted", async () => {
  const name = `verify-counts-${unique()}`;
  await post("/table", { cls: "table", name, op: "putMany", args: [Array.from({ length: 50 }, (_, i) => ({ row: doc(`v${i}`, i) }))] });
  // A bucket's count drifts (as a subclass writing the table directly, or a bad migration, would make it).
  await post("/object", { cls: "table", name, bits: "", method: "sqlRun", args: [[{ query: "UPDATE sharded_table_counts SET n = n + 5 WHERE bucket = (SELECT MIN(bucket) FROM sharded_table_counts)" }]] });
  expect(await post<number>("/table", { cls: "table", name, op: "count", args: [] })).toBe(55);
  const report = await post<{ ok?: unknown; threw?: string }>("/table", { cls: "table", name, op: "try:scan", args: ["tableVerifyCounts", null, 70_000] });
  expect(report.threw).toBeUndefined();
  expect(await post<number>("/table", { cls: "table", name, op: "count", args: [] })).toBe(50);
}, 60_000);

test("verifyCounts walks every shard a slice at a time, and repairs each drifted bucket", async () => {
  const name = `verify-walk-${unique()}`;
  // 15 routes of 20 rows: a route's rows share its hash, so a bucket holds 20 rows, more than a slice reads.
  const rows = Array.from({ length: 300 }, (_, i) => ({ row: doc(`refs/r${i % 15}/x/${i}`, i) }));
  await post("/table", { cls: "routedTable", name, op: "putMany", args: [rows] });
  expect(await post<boolean>("/split", { cls: "routedTable", name, bits: "" })).toBe(true);
  // A bucket too high on one shard, one missing on the other (its row count deleted).
  await post("/object", { cls: "routedTable", name, bits: "0", method: "sqlRun", args: [[{ query: "UPDATE sharded_table_counts SET n = n + 3 WHERE bucket = (SELECT MAX(bucket) FROM sharded_table_counts WHERE bucket < '8000')" }]] });
  await post("/object", { cls: "routedTable", name, bits: "1", method: "sqlRun", args: [[{ query: "DELETE FROM sharded_table_counts WHERE bucket = (SELECT MIN(bucket) FROM sharded_table_counts WHERE bucket >= '8000')" }]] });
  expect(await post<number>("/table", { cls: "routedTable", name, op: "count", args: [] })).not.toBe(300);
  // 7 rows a call: a slice ends inside a bucket, and a bucket is read in several.
  const report = await post<{ shards: number; repaired: number }>("/table", { cls: "routedTable", name, op: "verifyCounts", args: [7] });
  expect(report).toEqual({ shards: 2, repaired: 2 });
  expect(await post<number>("/table", { cls: "routedTable", name, op: "count", args: [] })).toBe(300);
  expect(await post<{ shards: number; repaired: number }>("/table", { cls: "routedTable", name, op: "verifyCounts", args: [30] })).toEqual({ shards: 2, repaired: 0 });
}, 60_000);
