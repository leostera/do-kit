// How the client finds its way, and waits, through a tree of shards it only partly knows. Unit
// tests over a faked tree: each shard answers what the test says, and the test counts the calls.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { fakeColoCache, fakeTree, loadClient, type Answer, type Leaf } from "./fake-tree.ts";

const { sharded } = await loadClient();
const { table } = await import("../src/table/client.ts");
const { defineTable } = await import("../src/table/schema.ts");

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
/** A key whose hash starts with `bits`. */
const keyIn = (bits: string) => {
  for (let i = 0; ; i++) {
    const key = `k${i}`;
    const hash = keyHash(key);
    if ([...bits].every((b, d) => hashBit(hash, d) === b)) return key;
  }
};
const ok = (keys: readonly string[]): Answer => ({ ok: keys.map(() => 1) });
type Calls = { call(keys: string[], method: string, payloads: null): Promise<{ result: unknown[] }[]> };
/** Every leaf of a tree `depth` deep. */
const leavesAt = (depth: number) => Array.from({ length: 2 ** depth }, (_, i) => i.toString(2).padStart(depth, "0"));

test("waiting on a shard that says when it will serve keeps the client's map: the root is not asked again", async () => {
  const started = Date.now();
  const leaf: Leaf = (_kind, _method, keys) => (Date.now() - started < 2_000 ? { notReady: "1", until: Date.now() + 5_000 } : ok(keys));
  const tree = fakeTree(["0", "1"], leaf);
  const client = sharded(tree.namespace, `held-${unique()}`) as unknown as Calls & { knownLeaves(): Promise<string[]> };
  await client.call([keyIn("1")], "get", null);
  expect(tree.calls.filter((c) => c.bits === "").length).toBe(1);
  expect(await client.knownLeaves()).toEqual(["0", "1"]);
}, 10_000);

test("two isolates saving what they learned leave the union of their splits in the colo cache", async () => {
  fakeColoCache();
  const name = `colo-${unique()}`;
  const tree = fakeTree(["0", "10", "11"], (_k, _m, keys) => ok(keys));
  // Three isolates: three instances of the client's module, with nothing in memory in common.
  const isolate = async (n: string) => (await import(`../src/sharded/client.ts?isolate=${n}-${name}`)).sharded(tree.namespace, name) as Calls & { knownLeaves(): Promise<string[]> };
  const [a, b, c] = [await isolate("a"), await isolate("b"), await isolate("c")];
  await b.knownLeaves(); // b reads the cache while it is empty
  await a.call([keyIn("11")], "get", null); // a learns "" and "1" split, and saves them
  await b.call([keyIn("0")], "get", null); // b learns "" split, and saves what it knows
  const before = tree.calls.length;
  await c.call([keyIn("11")], "get", null);
  expect(tree.calls.slice(before).map((x) => x.bits)).toEqual(["11"]);
});

test("a scan over 16 shards that are not ready for 3 s waits for them", async () => {
  const started = Date.now();
  const tree = fakeTree(leavesAt(4), () => (Date.now() - started < 3_000 ? { notReady: "x" } : { ok: 1 }));
  const client = sharded(tree.namespace, `scan-wait-${unique()}`) as unknown as { scan(method: string): Promise<unknown[]> };
  expect((await client.scan("count")).length).toBe(16);
}, 20_000);

test("a scan held by a shard that says when it will serve waits until then, past the client's own limit", async () => {
  const started = Date.now();
  const tree = fakeTree([""], () => (Date.now() - started < 1_000 ? { notReady: "", until: started + 1_500 } : { ok: 1 }));
  const client = sharded(tree.namespace, `scan-until-${unique()}`, { notReadyWaitMs: 300 }) as unknown as { scan(method: string): Promise<unknown[]> };
  expect(await client.scan("count")).toEqual([1]);
}, 10_000);

test("a scan over 256 shards keeps at most 32 calls in flight", async () => {
  const tree = fakeTree(leavesAt(8), () => ({ ok: 1 }));
  const client = sharded(tree.namespace, `scan-bound-${unique()}`) as unknown as { scan(method: string): Promise<unknown[]> };
  expect((await client.scan("count")).length).toBe(256);
  expect(tree.maxInFlight).toBeLessThanOrEqual(32);
}, 20_000);

test("an answer the client does not know is an error at once, not a wait", async () => {
  const tree = fakeTree([""], () => ({ future: "x" }));
  const client = sharded(tree.namespace, `unknown-${unique()}`, { notReadyWaitMs: 2_000 }) as unknown as Calls;
  const started = Date.now();
  const error = await client.call(["a"], "get", null).then(() => null, (e: Error) => e);
  expect(error?.name).toBe("UnknownAnswer");
  expect(Date.now() - started).toBeLessThan(200);
}, 10_000);

test("an answer a scan does not know is an error at once, not a wait", async () => {
  const tree = fakeTree([""], () => ({ future: "x" }));
  const client = sharded(tree.namespace, `unknown-scan-${unique()}`, { notReadyWaitMs: 2_000 }) as unknown as { scan(method: string): Promise<unknown[]> };
  const started = Date.now();
  const error = await client.scan("count").then(() => null, (e: Error) => e);
  expect(error?.name).toBe("UnknownAnswer");
  expect(Date.now() - started).toBeLessThan(200);
}, 10_000);

type Row = { id: string; n: number };
const rows = defineTable<Row>({ primaryKey: "id", columns: { id: "text", n: "integer" } });

test("a table read is sent again after a transient error, once", async () => {
  let failures = 0;
  const tree = fakeTree([""], (_kind, method, keys) => {
    if (method === "tableGet" && failures++ === 0) return new Error("Network connection lost.");
    return { ok: keys.map(() => null) };
  });
  expect(await table(tree.namespace, `resend-read-${unique()}`, rows).get("a")).toBeNull();
  expect(tree.calls.filter((c) => c.method === "tableGet").length).toBe(2);
});

test("a table write is not sent again after a transient error: it may have applied", async () => {
  const tree = fakeTree([""], (_kind, method) => (method === "tablePut" ? new Error("Network connection lost.") : { ok: [] }));
  await expect(table(tree.namespace, `no-resend-write-${unique()}`, rows).put({ id: "a", n: 1 })).rejects.toThrow("Network connection lost");
  expect(tree.calls.filter((c) => c.method === "tablePut").length).toBe(1);
});

test("a table read is not sent again when its shard is overloaded", async () => {
  const tree = fakeTree([""], (_kind, method) => (method === "tableGet" ? new Error("Durable Object is overloaded. Too many requests queued.") : { ok: [] }));
  await expect(table(tree.namespace, `no-resend-overloaded-${unique()}`, rows).get("a")).rejects.toThrow("overloaded");
  expect(tree.calls.filter((c) => c.method === "tableGet").length).toBe(1);
});

test("describeTree makes its stubs with the client's stub options", async () => {
  const tree = fakeTree([""], () => ({ ok: 1 }));
  await (sharded(tree.namespace, `describe-options-${unique()}`, { stubOptions: { locationHint: "weur" } }) as unknown as { describeTree(): Promise<unknown[]> }).describeTree();
  expect(tree.stubOptions.length).toBeGreaterThan(0);
  expect(tree.stubOptions.every((o) => o !== null)).toBe(true);
});
