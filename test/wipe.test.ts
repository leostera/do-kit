// A wiped object: its storage is deleted, and for a while it refuses calls still in flight, so
// none of them recreates it. Then the name is free to use again, as a new, empty object.
import { expect, test } from "bun:test";
import { call, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const doc = (n: number) => ({ name: `d${n}`, id: `id${n}`, createdAt: n, lastEditAt: null, deleted: false, meta: null });
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "table", name, op, args });
/** WIPED_REFUSE_MS, and a margin. */
const REFUSAL_OVER_MS = 5_500;

test("a table wiped by the client takes writes, reads, listings and counts again once its refusal window passes", async () => {
  const name = `wiped-table-${unique()}`;
  for (let i = 0; i < 3; i++) await table(name, "put", doc(i));
  await post("/wipeAll", { cls: "table", name });
  await Bun.sleep(REFUSAL_OVER_MS);
  await table(name, "put", doc(10));
  await table(name, "put", doc(11));
  expect(await table(name, "get", "d10")).toMatchObject({ row: { name: "d10" } });
  expect(await table<number>(name, "count")).toBe(2);
  expect((await table<{ rows: unknown[] }>(name, "list", { index: "byCreated" })).rows.length).toBe(2);
}, 60_000);

test("an indexed table wiped by the client finds rows by its global index again once its refusal window passes", async () => {
  const name = `wiped-indexed-${unique()}`;
  const user = (id: string) => ({ id, email: `${id}@example.com`, name: id, team: "core", createdAt: 1, active: true });
  await post("/indexed", { name, op: "put", args: [user("a")] });
  await post("/wipeAll", { cls: "users", name });
  await post("/wipeAll", { cls: "userIndexes", name });
  await Bun.sleep(REFUSAL_OVER_MS);
  expect(await post("/indexed", { name, op: "put", args: [user("b")] })).toMatchObject({ ok: { ok: true } });
  expect(await post("/indexed", { name, op: "lookup", args: ["byEmail", ["b@example.com"]] })).toMatchObject({ ok: { row: { id: "b" } } });
}, 60_000);

test("an atomic call on a name used again after a wipe applies", async () => {
  const name = `wiped-atomic-${unique()}`;
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  await post("/atomically", { cls: "counters", name, method: "transfer", keys: ["a", "b"], payloads: [-1, 1] });
  await post("/wipeAll", { cls: "counters", name });
  await Bun.sleep(REFUSAL_OVER_MS);
  await call("counters", name, "add", ["a", "b"], [10, 10]);
  expect(await post("/atomically", { cls: "counters", name, method: "transfer", keys: ["a", "b"], payloads: [-1, 1] })).toMatchObject({ committed: true });
}, 60_000);

test("a write that reaches a wiped object within its refusal window is refused, though the object was evicted meanwhile", async () => {
  const name = `wiped-evicted-${unique()}`;
  await call("counters", name, "add", ["a"], [1]);
  await post("/wipeAll", { cls: "counters", name });
  await post("/evict", { cls: "counters", name, bits: "" });
  await expect(call("counters", name, "add", ["a"], [1])).rejects.toThrow();
  expect(await post<{ bits: string }[]>("/tree", { cls: "counters", name }).catch(() => [])).toEqual([]);
}, 30_000);
