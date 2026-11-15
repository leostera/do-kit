// Overloaded on the client (RFD0013), over a faked tree: a shard's overload answer becomes the
// class of its kind, the runtime's shedding becomes ShardOverloaded, and neither is sent again.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { KeyOverloaded, Overloaded, ShardOverloaded } from "../src/sharded/overload.ts";
import { fakeTree, loadClient, type Answer } from "./fake-tree.ts";

const { sharded, UnknownAnswer } = await loadClient();
const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const keyIn = (bits: string) => {
  for (let i = 0; ; i++) {
    const key = `k${i}`;
    if ([...bits].every((b, d) => hashBit(keyHash(key), d) === b)) return key;
  }
};
type Calls = { call(keys: string[], method: string, payloads: null): Promise<{ result: unknown[] }[]>; scan(method: string): Promise<unknown[]> };
const answer = (kind: string, extra: object = {}): Answer => ({ overloaded: { kind, message: `${kind} is busy`, retryAfterMs: 250, scope: null, detail: {}, ...extra } });

class TenantOverQuota extends Overloaded {
  static override readonly kind: string = "tenantQuota";
}

test("a shard's overload answer is thrown as the class of its kind, with its fields, and the call is not sent again", async () => {
  const tree = fakeTree([""], () => answer("key", { scope: { key: "a" }, detail: { share: 0.9 } }));
  const error = await (sharded(tree.namespace, `kind-${unique()}`) as unknown as Calls).call(["a"], "get", null).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(KeyOverloaded);
  expect(error).toBeInstanceOf(Overloaded);
  const e = error as KeyOverloaded;
  expect({ kind: e.kind, key: e.key, source: e.source, retryAfterMs: e.retryAfterMs, detail: e.detail, name: e.name }).toEqual({ kind: "key", key: "a", source: "object", retryAfterMs: 250, detail: { share: 0.9 }, name: "KeyOverloaded" });
  expect(tree.calls.filter((c) => c.method === "get").length).toBe(1);
});

test("an app's kind is its registered class; unregistered, it is a plain Overloaded that keeps its kind", async () => {
  const tree = fakeTree([""], () => answer("tenantQuota"));
  const registered = await (sharded(tree.namespace, `app-${unique()}`, { overloads: [TenantOverQuota] }) as unknown as Calls).call(["a"], "get", null).catch((e: unknown) => e);
  expect(registered).toBeInstanceOf(TenantOverQuota);
  const plain = await (sharded(tree.namespace, `app-${unique()}`) as unknown as Calls).call(["a"], "get", null).catch((e: unknown) => e);
  expect(plain).toBeInstanceOf(Overloaded);
  expect(plain).not.toBeInstanceOf(TenantOverQuota);
  expect((plain as Overloaded).kind).toBe("tenantQuota");
});

test("a client refuses an app kind that is do-kit's, or a class with no kind", () => {
  const tree = fakeTree([""], () => ({ ok: [] }));
  class Clash extends Overloaded {
    static override readonly kind: string = "shard";
  }
  expect(() => sharded(tree.namespace, "clash", { overloads: [Clash] })).toThrow(/kind "shard" is already do-kit's/);
  expect(() => sharded(tree.namespace, "kindless", { overloads: [{ kind: "", fromAnswer: Overloaded.fromAnswer }] })).toThrow(/static kind/);
  expect(() => new TenantOverQuota("x", { retryAfterMs: -1 })).toThrow(/retryAfterMs/);
});

test("an overload answer that is not one is an UnknownAnswer, not an Overloaded", async () => {
  const tree = fakeTree([""], () => ({ overloaded: { kind: "shard", retryAfterMs: "soon" } }));
  const error = await (sharded(tree.namespace, `bad-${unique()}`) as unknown as Calls).call(["a"], "get", null).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(UnknownAnswer);
});

test("the runtime's shedding is ShardOverloaded from the runtime, naming the shard; a method's error that says the same is not", async () => {
  const shed = fakeTree(["0", "1"], () => new Error("Durable Object is overloaded. Too many requests queued."));
  const error = await (sharded(shed.namespace, `shed-${unique()}`) as unknown as Calls).call([keyIn("1")], "get", null).catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ShardOverloaded);
  expect({ source: (error as ShardOverloaded).source, bits: (error as ShardOverloaded).bits, retryAfterMs: (error as ShardOverloaded).retryAfterMs }).toEqual({ source: "runtime", bits: "1", retryAfterMs: 1_000 });
  expect(shed.calls.filter((c) => c.bits === "1").length).toBe(1);
  // From the method (marked remote by RPC): it may have run, so it is not a refusal.
  const said = fakeTree([""], () => Object.assign(new Error("Durable Object is overloaded. Too many requests queued."), { remote: true }));
  const raw = await (sharded(said.namespace, `said-${unique()}`) as unknown as Calls).call(["a"], "get", null).catch((e: unknown) => e);
  expect(raw).not.toBeInstanceOf(Overloaded);
});

test("a call over two shards, one overloaded, lists that shard's keys as refused and the other's as settled", async () => {
  const [left, right] = [keyIn("0"), keyIn("1")];
  const tree = fakeTree(["0", "1"], (_kind, _method, keys) => (keys.includes(right) ? answer("shard", { scope: { shard: "1" } }) : { ok: keys.map(() => 1) }));
  const error = (await (sharded(tree.namespace, `partial-${unique()}`) as unknown as Calls).call([left, right], "get", null).catch((e: unknown) => e)) as ShardOverloaded & { settled: { keys: string[] }[]; refused: { keys: string[] }[]; unknown: unknown[] };
  expect(error).toBeInstanceOf(ShardOverloaded);
  expect(error.settled.map((s) => s.keys)).toEqual([[left]]);
  expect(error.refused.map((s) => s.keys)).toEqual([[right]]);
  expect(error.unknown).toEqual([]);
});

test("a scan that meets an overloaded shard throws its Overloaded", async () => {
  const tree = fakeTree(["0", "1"], () => answer("shard"));
  const error = await (sharded(tree.namespace, `scan-${unique()}`) as unknown as Calls).scan("count").catch((e: unknown) => e);
  expect(error).toBeInstanceOf(ShardOverloaded);
});
