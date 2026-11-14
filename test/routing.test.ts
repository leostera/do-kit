// Routing through a busy tree: a client keeps what it learned while shards are busy (a split
// holding writes), asks no more shards per page than it can afford, and a scan of a presplit tree
// makes no shard that was never used.
import { readFileSync } from "node:fs";
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const keyIn = (bits: string, from = 0) => {
  for (let i = from; ; i++) {
    const key = `k${i}`;
    if ([...bits].every((b, d) => hashBit(keyHash(key), d) === b)) return key;
  }
};
type Counters = { calls: number; moved: number; notReady: number; stale: number };
const reached = async (cls: string, name: string, bits: string) => {
  const c = (await post<{ counters: Counters }>("/describe", { cls, name, bits })).counters;
  return c.calls + c.moved + c.notReady + c.stale;
};

test("a write held by a split for longer than a second never reaches the root", async () => {
  const name = `held-root-${unique()}`;
  const [left, right] = [keyIn("0"), keyIn("11")];
  await call("counters", name, "add", [left, right], [1, 1]);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  await call("counters", name, "get", [left, right]); // the client learns the split
  await post("/slowHandOut", { cls: "counters", name, bits: "1", ms: 3_000 });
  const split = post<boolean>("/split", { cls: "counters", name, bits: "1" });
  await Bun.sleep(300);
  const before = await reached("counters", name, "");
  expect(await call<number[]>("counters", name, "add", [right], [1])).toEqual([2]);
  expect(await split).toBe(true);
  expect(await reached("counters", name, "")).toBe(before);
}, 60_000);

test("a listing in query order refuses a tree with more shards than it may ask per page", async () => {
  const name = `too-many-shards-${unique()}`;
  for (let i = 0; i < 40; i++) await post("/table", { cls: "table", name, op: "put", args: [{ name: `d${i}`, id: `${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null }] });
  for (const bits of ["", "0", "1"]) expect(await post<boolean>("/split", { cls: "table", name, bits })).toBe(true);
  const list = (query: object) => post<{ ok?: { rows: unknown[] }; name?: string | null }>("/table", { cls: "table", name, op: "try:list", args: [{ index: "byCreated", limit: 10, ...query }], options: { maxShardsPerPage: 2 } });
  expect((await list({})).name).toBe("TooManyShards");
  expect((await list({ order: "shard" })).ok?.rows.length).toBeGreaterThan(0);
}, 60_000);

test("a scan of a presplit tree makes none of the shards never used", async () => {
  const name = `scan-presplit-${unique()}`;
  await post("/scan", { cls: "presplit", name, method: "entries" });
  const tree = await post<{ bits: string; state: string }[]>("/tree", { cls: "presplit", name, presplitHint: 2 });
  // The root's object hosts shard 00; the other three were never called.
  expect(tree.filter((n) => n.state === "new").length).toBe(3);
}, 30_000);

test("an object answers a call from a client of the previous release, which sends fewer arguments", async () => {
  const name = `old-client-${unique()}`;
  await call("counters", name, "add", ["a"], [3]);
  // shardedCall(name, bits, method, keys, payloads, args): no stub options, presplit depth or hashes.
  expect(await post("/object", { cls: "counters", name, bits: "", method: "shardedCall", args: [name, "", "get", ["a"], null, []] })).toEqual({ ok: [3] });
}, 30_000);

test("the README says how to upgrade: objects before clients, and which code update strategy", () => {
  const readme = readFileSync(new URL("../README.md", import.meta.url), "utf8");
  const section = readme.split(/^## /m).find((s) => s.startsWith("Upgrading")) ?? "";
  expect({ section: section.length > 0, order: /before (any )?clients?/i.test(section), strategy: section.includes("code_update_strategy") }).toEqual({ section: true, order: true, strategy: true });
});
