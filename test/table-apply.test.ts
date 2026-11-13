// ShardedTable.apply under cf dev (RFD0010): commands run in the row's shard, in one transaction,
// with no version retries, through contention and splits. The commands are the docs table's
// (`docCommands` in test/worker/src/index.ts).
import { expect, test } from "bun:test";
import { post, shards, useWorker } from "./harness.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
type Outcome<R> = { ok: true; result: R; version: number } | { ok: false; conflict: "unique"; constraint: string; version: number };

const op = <T>(name: string, operation: string, ...args: unknown[]) => post<T>("/table", { cls: "table", name, op: operation, args });
const apply = <R>(name: string, key: string, command: string, args: unknown) => op<Outcome<R>>(name, "apply", key, command, args);
const split = (name: string, bits: string) => post<boolean>("/split", { cls: "table", name, bits });
const doc = (name: string): Doc => ({ name, id: `id-${name}`, createdAt: 1, lastEditAt: null, deleted: false, meta: null });
const unique = (what: string) => `${what}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;

test("apply creates, updates and deletes a row, and answers the command's result and the row's version", async () => {
  const name = unique("apply");
  expect(await apply(name, "a", "bump", { by: 1 })).toEqual({ ok: true, result: null, version: 0 });
  expect(await apply(name, "a", "ensure", { row: doc("a") })).toEqual({ ok: true, result: "created", version: 1 });
  expect(await apply(name, "a", "ensure", { row: { ...doc("a"), id: "other" } })).toEqual({ ok: true, result: "exists", version: 1 });
  expect(await apply(name, "a", "bump", { by: 5 })).toEqual({ ok: true, result: 5, version: 2 });
  expect(await op(name, "get", "a")).toEqual({ row: { ...doc("a"), lastEditAt: 5 }, version: 2 });
  expect(await apply(name, "a", "remove", null)).toEqual({ ok: true, result: true, version: 0 });
  expect(await op(name, "get", "a")).toBeNull();
  expect(await apply(name, "a", "remove", null)).toEqual({ ok: true, result: false, version: 0 });
  expect(await op(name, "count")).toBe(0);
});

test("concurrent applies to one row all commit, one after another, with no retries", async () => {
  const name = unique("apply-hot");
  await apply(name, "hot", "ensure", { row: doc("hot") });
  const outcomes = await Promise.all(Array.from({ length: 40 }, () => apply<number>(name, "hot", "bump", { by: 1 })));
  expect(outcomes.every((o) => o.ok)).toBe(true);
  // Each saw the one before it: every result and version once.
  expect(outcomes.map((o) => (o.ok ? o.result : -1)).sort((a, b) => a - b)).toEqual(Array.from({ length: 40 }, (_, i) => i + 1));
  expect(outcomes.map((o) => o.version).sort((a, b) => a - b)).toEqual(Array.from({ length: 40 }, (_, i) => i + 2));
  expect(((await op<{ row: Doc }>(name, "get", "hot")).row).lastEditAt).toBe(40);
});

test("a command that throws, answers late, or does not exist writes nothing", async () => {
  const name = unique("apply-bad");
  await apply(name, "a", "ensure", { row: doc("a") });
  expect(apply(name, "a", "boom", null)).rejects.toThrow(/the command failed/);
  expect(apply(name, "a", "later", null)).rejects.toThrow(/synchronously/);
  expect(apply(name, "a", "nope", null)).rejects.toThrow(/no command/);
  await Bun.sleep(100);
  expect(await op(name, "get", "a")).toEqual({ row: doc("a"), version: 1 });
});

test("applies made while their shard splits run exactly once each", async () => {
  const name = unique("apply-split");
  const keys = Array.from({ length: 24 }, (_, i) => `k-${i}`);
  for (const key of keys) await apply(name, key, "ensure", { row: doc(key) });
  // Three rounds of a bump per key, the first racing a split of the root and the second one of
  // its left half: calls answered "moved" or "not ready" are routed again, and must not run twice.
  const round = () => Promise.all(keys.map((key) => apply<number>(name, key, "bump", { by: 1 })));
  await Promise.all([round(), split(name, "")]);
  await Promise.all([round(), split(name, "0")]);
  await round();
  expect((await shards("table", name)).length).toBe(3);
  const rows = await op<({ row: Doc } | null)[]>(name, "getMany", keys);
  expect(rows.map((r) => r!.row.lastEditAt)).toEqual(keys.map(() => 3));
}, 60_000);

test("an apply to a row its shard is copying waits for the split, then lands once, in the shard that owns it", async () => {
  const name = unique("apply-held");
  const keys = Array.from({ length: 16 }, (_, i) => `h-${i}`);
  for (const key of keys) await apply(name, key, "ensure", { row: doc(key) });
  await post("/slowHandOut", { cls: "table", name, bits: "", ms: 1500 });
  const splitting = split(name, "");
  await Bun.sleep(200); // writes to the moving half are held now
  const outcomes = await Promise.all(keys.map((key) => apply<number>(name, key, "bump", { by: 1 })));
  expect(await splitting).toBe(true);
  expect(outcomes.every((o) => o.ok && o.result === 1)).toBe(true);
  // Once it commits, an apply sent to the shard that gave a row away is answered "moved" and
  // routed again: written there, it would be lost.
  await Promise.all(keys.map((key) => apply<number>(name, key, "bump", { by: 1 })));
  const rows = await op<({ row: Doc } | null)[]>(name, "getMany", keys);
  expect(rows.map((r) => r?.row.lastEditAt)).toEqual(keys.map(() => 2));
}, 60_000);

test("an IndexedTable refuses apply: its writes reserve their index entries first", async () => {
  expect(post("/table", { cls: "users", name: unique("apply-indexed"), op: "apply", args: ["a", "bump", { by: 1 }] })).rejects.toThrow(/IndexedTable/);
});
