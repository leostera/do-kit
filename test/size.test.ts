// Size: a shard must split before its storage gets big, whatever its load, and refuse to grow past
// a cap below the platform's, while it still drains (deletes, atomic outcomes). What a row costs on
// disk decides how much a shard holds; how rows lie on disk decides how much a split frees. And a
// split that is given up must not leave a copy of the shard behind.
import { readFileSync } from "node:fs";
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, post, useWorker } from "./harness.ts";
import type { StatementCost } from "./worker/src/index.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const object = <T = unknown>(cls: string, name: string, bits: string, method: string, ...args: unknown[]) => post<T>("/object", { cls, name, bits, method, args });
const text = (n: number, seed: number) => `${seed}:`.padEnd(n, "x");
const leaves = async (cls: string, name: string) => (await post<{ bits: string; state: string }[]>("/tree", { cls, name })).filter((n) => n.state === "shard");
const KIB = 1024;

async function putTexts(name: string, count: number, bytes: number, from = 0): Promise<string[]> {
  const keys = Array.from({ length: count }, (_, i) => `t${from + i}`);
  for (let i = 0; i < keys.length; i += 20) {
    const batch = keys.slice(i, i + 20);
    await call("counters", name, "putText", batch, batch.map((_, j) => text(bytes, from + i + j)));
  }
  return keys;
}

test("a quiet shard splits once its database passes splitAtBytes", async () => {
  const name = `size-split-${unique()}`;
  await call("counters", name, "get", ["a"]);
  await object("counters", name, "", "setPolicy", { splitAtBytes: 256 * KIB });
  const keys = await putTexts(name, 600, 1_000);
  for (let i = 0; i < 50 && (await leaves("counters", name)).length < 2; i++) await Bun.sleep(100);
  expect((await leaves("counters", name)).length).toBeGreaterThanOrEqual(2);
  const values = await call<(string | null)[]>("counters", name, "get", keys);
  expect(values.every((v, i) => v === text(1_000, i))).toBe(true);
}, 120_000);

test("lowering splitAtBytes on a live shard splits it on its next write", async () => {
  const name = `size-lowered-${unique()}`;
  await putTexts(name, 200, 1_000);
  await object("counters", name, "", "setPolicy", { splitAtBytes: 64 * KIB });
  await putTexts(name, 1, 100, 1_000);
  for (let i = 0; i < 50 && (await leaves("counters", name)).length < 2; i++) await Bun.sleep(100);
  expect((await leaves("counters", name)).length).toBeGreaterThanOrEqual(2);
}, 120_000);

test("a shard past refuseAtBytes refuses writes that grow it, and still deletes", async () => {
  const name = `size-full-${unique()}`;
  // It cannot split (maxDepth 0): only the cap protects it.
  await call("counters", name, "get", ["a"]);
  await object("counters", name, "", "setPolicy", { maxDepth: 0, refuseAtBytes: 128 * KIB });
  // 200 KB of values: the fill runs into the cap partway.
  const filled = await putTexts(name, 200, 1_000).then(() => "all of it", (e: Error) => e.message);
  expect(filled).toContain("ShardFull");
  const grow = await post<{ threw?: string; name?: string | null }>("/callSettled", { cls: "counters", name, method: "putText", keys: ["more"], payloads: [text(1_000, 9)] });
  expect(grow.name).toBe("ShardFull");
  const shrink = await post<{ ok?: unknown; threw?: string }>("/callSettled", { cls: "counters", name, method: "remove", keys: ["t0", "t1"], payloads: null });
  expect(shrink.threw).toBeUndefined();
}, 120_000);

test("a shard whose rows all share one route stays one shard, however big it gets", async () => {
  const name = `size-floor-${unique()}`;
  const doc = (i: number) => ({ name: `refs/heads/a/${i}`, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: text(500, i) });
  await post("/table", { cls: "routedTable", name, op: "put", args: [doc(0)] });
  await object("routedTable", name, "", "setPolicy", { splitAtBytes: 64 * KIB });
  for (let i = 1; i < 300; i++) await post("/table", { cls: "routedTable", name, op: "put", args: [doc(i)] });
  await Bun.sleep(2_000);
  expect((await leaves("routedTable", name)).length).toBe(1);
  // Nothing to gain, so nothing tried: no split attempt that would move nothing or everything.
  const described = await post<{ splits: { rows: number; rightRows: number }[] }>("/describe", { cls: "routedTable", name, bits: "" });
  expect(described.splits.filter((s) => s.rightRows === 0 || s.rightRows === s.rows)).toEqual([]);
}, 120_000);

test("a split policy with timers past 2^31-1 ms is refused: setTimeout would fire at once", async () => {
  const ok = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12 };
  // The answer is the error, or null for a policy accepted.
  expect(String(await post<string | null>("/checkPolicy", { policy: { ...ok, splitTimeoutMs: 2 ** 31 } }))).toContain("splitPolicy.splitTimeoutMs");
  expect(String(await post<string | null>("/checkPolicy", { policy: { ...ok, holdWaitMs: 2 ** 31 } }))).toContain("splitPolicy.holdWaitMs");
});

test("splits given up one after another wait longer and longer before the next", async () => {
  const name = `size-backoff-${unique()}`;
  const put = (i: number) => post("/table", { cls: "flaky", name, op: "put", args: [{ name: `r${i}`, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null }] });
  await put(0);
  await object("flaky", name, "", "setPolicy", { callsPerSecond: 20, windowSeconds: 1, splitTimeoutMs: 1_000 });
  await post("/table", { cls: "flaky", name, op: "failClones", args: [true] });
  // Load, for long enough to see four attempts at gaps that double.
  const until = Date.now() + 20_000;
  for (let i = 1; Date.now() < until; i++) await Promise.all(Array.from({ length: 5 }, (_, j) => put(i * 5 + j).catch(() => undefined)));
  const splits = (await post<{ splits: { at: number; outcome: string }[] }>("/describe", { cls: "flaky", name, bits: "" })).splits.filter((s) => s.outcome === "abandoned");
  expect(splits.length).toBeGreaterThanOrEqual(3);
  const gaps = splits.slice(1).map((s, i) => s.at - splits[i]!.at);
  for (let i = 1; i < gaps.length; i++) expect(gaps[i]!).toBeGreaterThanOrEqual(1.8 * gaps[i - 1]!);
}, 60_000);

test("a value of 1 KiB costs at most 1.6 times its size on disk", async () => {
  const name = `size-row-bytes-${unique()}`;
  await putTexts(name, 1, 1_000);
  const before = await object<number>("counters", name, "", "databaseSize");
  await putTexts(name, 2_000, 1_000, 1);
  const perRow = ((await object<number>("counters", name, "", "databaseSize")) - before) / 2_000;
  // The value, its key, and its hash.
  expect(perRow).toBeLessThanOrEqual(1.6 * (1_000 + 6 + 16));
}, 120_000);

test("both halves of a table's split end near half the parent's size", async () => {
  const name = `size-halves-${unique()}`;
  for (let i = 0; i < 5_000; i += 250) {
    const rows = Array.from({ length: 250 }, (_, j) => ({ row: { name: `d${i + j}`, id: `${i + j}`, createdAt: i + j, lastEditAt: null, deleted: false, meta: text(60, i + j) } }));
    await post("/table", { cls: "table", name, op: "putMany", args: [rows] });
  }
  const parent = await object<number>("table", name, "", "databaseSize");
  expect(await post<boolean>("/split", { cls: "table", name, bits: "" })).toBe(true);
  const settled = async () => (await Promise.all(["", "1"].map((bits) => post<{ rows: number; storedRows: number }>("/describe", { cls: "table", name, bits })))).every((d) => d.storedRows === d.rows);
  for (let i = 0; i < 100 && !(await settled()); i++) await Bun.sleep(200);
  expect(await settled()).toBe(true);
  for (const bits of ["", "1"]) expect((await object<number>("table", name, bits, "databaseSize")) / parent).toBeLessThanOrEqual(0.6);
}, 120_000);

test("a split that is given up leaves nothing in the object it copied to", async () => {
  const name = `size-discard-${unique()}`;
  for (let i = 0; i < 20; i++) await post("/table", { cls: "flaky", name, op: "put", args: [{ name: `r${i}`, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null }] });
  await post("/table", { cls: "flaky", name, op: "failClones", args: [true] });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(false);
  await Bun.sleep(3_000);
  expect((await object<{ tables: unknown[] }>("flaky", name, "1", "storage")).tables).toEqual([]);
  // And the next split, once clones are claimed again, succeeds.
  await post("/table", { cls: "flaky", name, op: "failClones", args: [false] });
  expect(await post<boolean>("/split", { cls: "flaky", name, bits: "" })).toBe(true);
}, 60_000);

test("a shard splits while an atomic call holds a key of the half that stays", async () => {
  const name = `size-split-held-${unique()}`;
  const left = Array.from({ length: 50 }, (_, i) => `k${i}`).find((k) => hashBit(keyHash(k), 0) === "0")!;
  await call("counters", name, "add", [left], [10]);
  const tx = `tx-${unique()}`;
  expect((await post<{ ok?: unknown }>("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx, method: "transfer", primaryKey: left, deadline: Date.now() + 60_000 }, keys: [left], payloads: [-1] })).ok).toEqual({ prepared: true });
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
}, 60_000);

test("a row copy sends batches of at most 4 MiB on the heap, whatever the text", async () => {
  const name = `size-batches-${unique()}`;
  // Two bytes a character on the heap: each value is about 1.2 MB there, and 0.6 M by length.
  const cjk = (i: number) => `${i}`.padEnd(600_000, "界");
  const keys = Array.from({ length: 50 }, (_, i) => `c${i}`).filter((k) => hashBit(keyHash(k), 0) === "1").slice(0, 8);
  for (const [i, key] of keys.entries()) await call("counters", name, "putText", [key], [cjk(i)]);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  const batches = await object<{ heapBytes: number }[]>("counters", name, "1", "receivedBatches");
  expect(batches.length).toBeGreaterThan(0);
  expect(Math.max(...batches.map((b) => b.heapBytes))).toBeLessThanOrEqual(4 * 1024 * 1024);
}, 120_000);

test("a row copy of a table sends only the rows of the half that moves", async () => {
  const name = `size-copy-half-${unique()}`;
  const names = Array.from({ length: 400 }, (_, i) => `d${i}`);
  await post("/table", { cls: "table", name, op: "putMany", args: [names.map((n, i) => ({ row: { name: n, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null } }))] });
  expect(await post<boolean>("/split", { cls: "table", name, bits: "" })).toBe(true);
  const sent = (await object<{ table: string; rows: number }[]>("table", name, "1", "receivedBatches")).filter((b) => b.table === "sharded_table").reduce((n, b) => n + b.rows, 0);
  expect(sent).toBe(names.filter((n) => hashBit(keyHash(n), 0) === "1").length);
}, 60_000);

test("a row copy refuses a first batch sent again after later ones: it would wipe them", async () => {
  const name = `size-replay-${unique()}`;
  const receive = (first: boolean, value: number) => object("counters", name, "1", "shardedReceive", name, "1", 1, first, "t", ["a"], [[value]]);
  await object("counters", name, "1", "shardedReceiveSchema", name, "1", 1, [{ name: "t", sql: "CREATE TABLE t (a INTEGER)" }]);
  await receive(true, 1);
  await receive(false, 2);
  await expect(receive(true, 1)).rejects.toThrow();
}, 30_000);

test("starting a split counts no more than a few rows on the way", async () => {
  const name = `size-split-count-${unique()}`;
  const keys = Array.from({ length: 5_000 }, (_, i) => `k${i}`);
  for (let i = 0; i < keys.length; i += 250) await call("counters", name, "add", keys.slice(i, i + 250), keys.slice(i, i + 250).map(() => 1));
  await object("counters", name, "", "statements", "", true);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  const counts = await object<StatementCost[]>("counters", name, "", "statements", "COUNT(", true);
  expect(Math.max(0, ...counts.map((c) => c.rowsRead))).toBeLessThanOrEqual(100);
}, 120_000);

test("the README says what a shard holds as do-kit counts it, and that a shard is never restored on its own", () => {
  const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  expect({
    noTenGbPerShard: !/10 GB per shard|160 GB/.test(readme),
    splitAtBytes: readme.includes("splitAtBytes"),
    restore: /point-in-time/i.test(readme),
  }).toEqual({ noTenGbPerShard: true, splitAtBytes: true, restore: true });
});
