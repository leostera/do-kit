// Co-location under cf dev (RFD0006): keys with one route live in one shard, through any number
// of splits, so a transaction over them never spans shards; git's file/folder ref conflict,
// refused in one transaction; the default route stores the same hashes as before; and a client
// and an object that route differently are refused on the first call.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { describe, post, requestHeaders, useWorker, workerUrl } from "./harness.ts";

useWorker();

const refRoute = (key: string) => key.split("/").slice(0, 3).join("/");
const unique = () => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

/** A call through a client declared with the refs routeOf (or with none). */
const routed = <T>(cls: string, name: string, method: string, keys: string[], payloads: unknown[] | null = null, routeOf: "refs" | null = "refs") => post<T>("/call", { cls, name, method, keys, payloads, ...(routeOf ? { routeOf } : {}) });

/** Waits until each of these shards serves, holding only its own rows. */
async function settle(cls: string, name: string, bits: string[]) {
  for (let i = 0; i < 150; i++) {
    const described = await Promise.all(bits.map((b) => describe(cls, name, b)));
    if (described.every((d, j) => d.bits === bits[j] && d.state === "shard" && d.storedRows === d.rows)) return;
    await Bun.sleep(100);
  }
  throw new Error(`the split did not settle: ${bits.join(", ")}`);
}

/** Splits to six shards: 00, 010, 011, 10, 110, 111. */
async function splitToSix(cls: string, name: string) {
  expect(await post<boolean>("/split", { cls, name, bits: "" })).toBe(true);
  await settle(cls, name, ["0", "1"]);
  for (const bits of ["0", "1"]) expect(await post<boolean>("/split", { cls, name, bits })).toBe(true);
  await settle(cls, name, ["00", "01", "10", "11"]);
  for (const bits of ["01", "11"]) expect(await post<boolean>("/split", { cls, name, bits })).toBe(true);
  await settle(cls, name, ["00", "010", "011", "10", "110", "111"]);
}

/** 40 branches, each a ref and four refs below it: 40 routes of 5 keys. */
const branches = Array.from({ length: 40 }, (_, i) => `refs/heads/b${i}`);
const groups = branches.map((b) => [b, ...Array.from({ length: 4 }, (_, j) => `${b}/r${j}`)]);

test("keys with one route stay in one shard through splits, and one transaction reaches them all", async () => {
  const name = `coloc-${unique()}`;
  const keys = groups.flat();
  await routed("routed", name, "add", keys, keys.map(() => 1));
  // Writes go on while the tree splits: those to a half being copied wait for it, by route.
  let splitting = true;
  let acked = 0;
  const writers = Array.from({ length: 8 }, (_, w) =>
    (async () => {
      for (let i = 0; splitting; i++) {
        // One key a call: a write is held by its own route, not by another key's in its batch.
        const batch = [keys[(w * 101 + i * 37) % keys.length]!];
        await routed("routed", name, "add", batch, batch.map(() => 1));
        acked += batch.length;
      }
    })(),
  );
  await splitToSix("routed", name);
  splitting = false;
  await Promise.all(writers);
  expect(acked).toBeGreaterThan(0);
  const values = await routed<number[]>("routed", name, "get", keys);
  expect(values.reduce((a, b) => a + b, 0)).toBe(keys.length + acked);
  // The subclass's tables follow routes too: each shard's listing ends up holding its own keys,
  // every key once (cleanupForeign), and each key's history is kept by the shard that owns it (afterClone).
  for (let i = 0; ; i++) {
    const listings = await post<{ key: string; owned: boolean }[][]>("/scan", { cls: "routed", name, method: "listing" });
    const flat = listings.flat();
    if (flat.every((e) => e.owned) && flat.length === keys.length) {
      expect(flat.map((e) => e.key).sort()).toEqual([...keys].sort());
      break;
    }
    if (i > 100) throw new Error(`listings did not settle: ${flat.filter((e) => !e.owned).length} not owned, ${flat.length} of ${keys.length}`);
    await Bun.sleep(100);
  }
  const histories = (await post<{ key: string; owned: boolean }[][]>("/scan", { cls: "routed", name, method: "history" })).flat();
  for (const key of keys) expect(histories.some((e) => e.key === key && e.owned)).toBe(true);
  // Every shard's own keys, from the shards themselves.
  const perShard = await post<string[][]>("/scan", { cls: "routed", name, method: "pagedKeys", args: [1000] });
  expect(perShard.length).toBe(6);
  expect(perShard.flat().sort()).toEqual([...keys].sort());
  // The routes spread over the shards, and no route is in two.
  expect(perShard.filter((s) => s.length > 0).length).toBeGreaterThanOrEqual(4);
  for (const group of groups) expect(perShard.filter((s) => s.some((k) => group.includes(k))).length).toBe(1);
  // Each route is one shard's: callOneShard over all its keys never spans shards.
  const value = new Map(keys.map((k, i) => [k, values[i]]));
  for (const group of groups) expect(await post<number[]>("/callOne", { cls: "routed", name, method: "get", keys: group, payloads: null, routeOf: "refs" })).toEqual(group.map((k) => value.get(k)!));
  // Without a route, the same keys and splits do span shards: the routing is what keeps them together.
  const plain = `coloc-plain-${unique()}`;
  await routed("m", plain, "add", keys, keys.map(() => 1), null);
  await splitToSix("m", plain);
  const plainShards = await post<string[][]>("/scan", { cls: "m", name: plain, method: "pagedKeys", args: [1000] });
  expect(groups.some((group) => plainShards.filter((s) => s.some((k) => group.includes(k))).length > 1)).toBe(true);
}, 120_000);

test("git's file/folder ref conflict is refused in one transaction, before and after splits, and under concurrency", async () => {
  const name = `refs-${unique()}`;
  const create = (...refs: string[]) => routed<string[]>("routed", name, "createRef", refs, refs.map(() => 1));
  expect(await create("refs/heads/a")).toEqual(["created"]);
  expect(await create("refs/heads/a/b")).toEqual(["conflict"]); // a is a file: no folder of that name
  expect(await create("refs/heads/c/d")).toEqual(["created"]);
  expect(await create("refs/heads/c")).toEqual(["conflict"]); // c is a folder: no file of that name
  expect(await create("refs/heads/c/d/e")).toEqual(["conflict"]);
  expect(await create("refs/heads/c/e", "refs/heads/cd", "refs/heads/a-b")).toEqual(["created", "created", "created"]);

  await splitToSix("routed", name);
  expect(await create("refs/heads/a/x")).toEqual(["conflict"]);
  expect(await create("refs/heads/c")).toEqual(["conflict"]);
  // Each family raced from both ends at once: exactly one of each pair is created.
  const families = Array.from({ length: 16 }, (_, i) => `refs/heads/f${i}`);
  const answers = await Promise.all(families.flatMap((f) => [create(f), create(`${f}/x`)]));
  for (let i = 0; i < families.length; i++) expect([answers[2 * i]![0], answers[2 * i + 1]![0]].sort()).toEqual(["conflict", "created"]);
}, 120_000);

test("the default route stores each key under its own hash, as before; a route stores it under the route's", async () => {
  const name = `hashes-${unique()}`;
  const keys = ["refs/heads/a/b", "refs/tags/v1", "plain"];
  await routed("m", name, "add", keys, keys.map(() => 1), null);
  await routed("routed", name, "add", keys, keys.map(() => 1));
  const [plain] = await post<{ key: string; hash: string }[][]>("/scan", { cls: "m", name, method: "rowHashes" });
  expect(plain).toEqual([...keys].sort().map((key) => ({ key, hash: keyHash(key) })));
  const [byRoute] = await post<{ key: string; hash: string }[][]>("/scan", { cls: "routed", name, method: "rowHashes" });
  expect(byRoute).toEqual([...keys].sort().map((key) => ({ key, hash: keyHash(refRoute(key)) })));
});

test("a client and an object that route differently are refused on the first call, and nothing is written", async () => {
  const mismatch = async (cls: string, routeOf: "refs" | null) => {
    const name = `mismatch-${unique()}`;
    const response = await fetch(`${workerUrl()}/call`, { method: "POST", headers: requestHeaders(), body: JSON.stringify({ cls, name, method: "add", keys: ["refs/heads/a/b"], payloads: [1], ...(routeOf ? { routeOf } : {}) }) });
    expect(response.status).toBe(409);
    expect(await response.json()).toEqual({ error: "RouteMismatch", key: "refs/heads/a/b" });
    // Refused before the root did anything: it is still a new object.
    expect((await describe(cls, name, "")).state).toBe("new");
  };
  await mismatch("m", "refs"); // the client routes, the object does not
  await mismatch("routed", null); // the object routes, the client does not
  // A key whose route is itself routes the same either way.
  expect(await routed<number[]>("m", `mismatch-${unique()}`, "add", ["plain"], [1])).toEqual([1]);
});

test("a routed table lists and counts every row once through a split, before and after cleanup", async () => {
  const name = `routed-table-${unique()}`;
  const op = <T>(operation: string, ...args: unknown[]) => post<T>("/table", { cls: "routedTable", name, op: operation, args });
  const names = groups.flat();
  for (const n of names) await op("put", { name: n, id: n, createdAt: 1, lastEditAt: null, deleted: false, meta: null });
  await op("scan", "pauseCleanup", true);
  expect(await post<boolean>("/split", { cls: "routedTable", name, bits: "" })).toBe(true);
  // Both shards still store every row; each lists and counts only the routes it owns.
  const listed = async () => (await op<{ row: { name: string } }[]>("all", {})).map((r) => r.row.name);
  for (let i = 0; i < 100 && (await describe("routedTable", name, "1")).state !== "shard"; i++) await Bun.sleep(100);
  expect(await op<number[]>("scan", "storedRows")).toEqual([names.length, names.length]);
  expect(await listed()).toEqual([...names].sort());
  expect(await op("count")).toBe(names.length);
  await op("scan", "pauseCleanup", false);
  for (let i = 0; i < 100; i++) {
    const stored = await op<number[]>("scan", "storedRows");
    if (stored[0]! + stored[1]! === names.length) break;
    await Bun.sleep(100);
  }
  const stored = await op<number[]>("scan", "storedRows");
  expect(stored[0]! + stored[1]!).toBe(names.length);
  expect(await listed()).toEqual([...names].sort());
  expect(await op("count")).toBe(names.length);
  // Rows written after the split are counted by the shard that owns their route.
  const more = branches.map((b) => `${b}/late`);
  for (const n of more) await op("put", { name: n, id: n, createdAt: 2, lastEditAt: null, deleted: true, meta: null });
  expect(await op("count")).toBe(names.length + more.length);
  expect(await op("countBy", "byDeleted")).toEqual(expect.arrayContaining([{ value: true, count: more.length }, { value: false, count: names.length }]));
  // Reads by key route by the same routes, and find every row.
  for (const group of groups) expect((await op<({ row: { name: string } } | null)[]>("getMany", group)).map((r) => r?.row.name)).toEqual(group);
}, 120_000);

test("a split holds a write by its route: a key whose route moves waits, though its own hash would stay", async () => {
  const name = `hold-${unique()}`;
  const bit = (key: string) => hashBit(keyHash(key), 0);
  const find = (routeBit: string, keyBit: string) => {
    for (let i = 0; ; i++) {
      const key = `refs/heads/h${i}/x`;
      if (bit(refRoute(key)) === routeBit && bit(key) === keyBit) return key;
    }
  };
  const moving = find("1", "0");
  const staying = find("0", "1");
  await routed("routed", name, "add", [moving, staying], [1, 1]);
  // The split holds the right half for 1.5 s before it copies.
  await post("/slowHandOut", { cls: "routed", name, bits: "", ms: 1_500 });
  let splitDone = false;
  const split = post<boolean>("/split", { cls: "routed", name, bits: "" }).then((ok) => ((splitDone = true), ok));
  await Bun.sleep(300);
  // Its route stays: it goes ahead during the split.
  await routed("routed", name, "add", [staying], [1]);
  expect(splitDone).toBe(false);
  // Its route moves: it waits for the split (answered "not ready", and retried), then lands in the new shard.
  await routed("routed", name, "add", [moving], [1]);
  expect(splitDone).toBe(true);
  expect(await split).toBe(true);
  const [record] = (await describe("routed", name, "0")).splits;
  expect(record!.held).toBeGreaterThan(0);
  expect(await routed<number[]>("routed", name, "get", [moving, staying])).toEqual([2, 2]);
}, 60_000);

test("an indexed table refuses a routeOf: its index shards reach its rows with a client of their own", async () => {
  expect(await post<string>("/indexed", { name: `routed-indexed-${unique()}`, op: "withRouteOf", args: [] })).toContain("routeOf is not supported");
});
