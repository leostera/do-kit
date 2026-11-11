// Atomic calls across shards under cf dev (RFD0008): every shard's share applies or none does;
// writes to held keys wait and then apply; a caller that stops after any step leaves holds that
// end at their deadline with the primary's outcome; a shard holding keys does not split; and a
// call in one shard is one call, with no tables. The protocol is in specs/atomic-across-shards.ts.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, describe, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
type Outcome = { committed: true; results: { keys: string[]; result: number[] }[] } | { committed: false; refusals: { keys: string[]; result: string }[] };
type Tables = { holds: { tx: string; keys: string; deadline: number }[]; outcomes: { tx: string; outcome: string }[]; gathers: number };

const half = (key: string) => hashBit(keyHash(key), 0);
/** `n` keys in the left half ("0") and `n` in the right ("1"), once the root splits. */
function keysByHalf(n: number): { left: string[]; right: string[] } {
  const left: string[] = [];
  const right: string[] = [];
  for (let i = 0; left.length < n || right.length < n; i++) {
    const key = `k${i}`;
    if (half(key) === "0" && left.length < n) left.push(key);
    else if (half(key) === "1" && right.length < n) right.push(key);
  }
  return { left, right };
}

const atomically = (name: string, keys: string[], deltas: number[]) => post<Outcome>("/atomically", { cls: "counters", name, method: "transfer", keys, payloads: deltas });
const values = (name: string, keys: string[]) => call<(number | null)[]>("counters", name, "get", keys);
const tables = (name: string, bits: string) => post<Tables>("/atomicTables", { cls: "counters", name, bits });
const step = (name: string, bits: string, s: unknown, keys: string[], payloads: unknown[] | null = null) => post<{ ok?: unknown }>("/atomicStep", { cls: "counters", name, bits, step: s, keys, payloads });

/** A tree split in two (shards "0" and "1"), with these counters set. */
async function twoShards(name: string, start: Record<string, number>) {
  await call("counters", name, "add", Object.keys(start), Object.values(start));
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "" })).toBe(true);
  for (let i = 0; i < 150; i++) {
    const [l, r] = await Promise.all([describe("counters", name, "0"), describe("counters", name, "1")]);
    if (l.state === "shard" && r.state === "shard" && l.storedRows === l.rows && r.storedRows === r.rows) return;
    await Bun.sleep(100);
  }
  throw new Error("the split did not settle");
}

test("a call across two shards applies in both, and leaves no hold behind", async () => {
  const name = `atomic-commit-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!];
  await twoShards(name, { [a]: 100, [b]: 5 });
  const out = await atomically(name, [a, b], [-30, 30]);
  expect(out.committed).toBe(true);
  expect(await values(name, [a, b])).toEqual([70, 35]);
  const [l, r] = await Promise.all([tables(name, "0"), tables(name, "1")]);
  expect([...l.holds, ...r.holds]).toEqual([]);
  // The primary forgot the outcome once every share was resolved.
  expect([...l.outcomes, ...r.outcomes]).toEqual([]);
}, 30_000);

test("a refusal by either shard applies nothing anywhere, and says why", async () => {
  const name = `atomic-refused-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!];
  await twoShards(name, { [a]: 10, [b]: 10 });
  const primaryFirst = [a, b].sort();
  for (const [lowKey, deltas] of [
    [primaryFirst[0]!, [-11, 11]],
    [primaryFirst[1]!, [11, -11]],
  ] as const) {
    const out = await atomically(name, primaryFirst, [...deltas]);
    expect(out).toEqual({ committed: false, refusals: [{ keys: [lowKey], result: `below zero: ${lowKey}` }] });
    expect(await values(name, [a, b])).toEqual([10, 10]);
  }
  const [l, r] = await Promise.all([tables(name, "0"), tables(name, "1")]);
  expect([...l.holds, ...r.holds]).toEqual([]);
}, 30_000);

test("a call in one shard is one call: no holds, no outcomes, no tables", async () => {
  const name = `atomic-solo-${unique()}`;
  await call("counters", name, "add", ["x", "y"], [5, 0]);
  expect((await atomically(name, ["x", "y"], [-5, 5])).committed).toBe(true);
  expect(await atomically(name, ["x", "y"], [-1, 1])).toEqual({ committed: false, refusals: [{ keys: ["x", "y"], result: "below zero: x" }] });
  expect(await values(name, ["x", "y"])).toEqual([0, 5]);
  const t = await tables(name, "");
  expect(t.holds).toEqual([]);
  expect(t.outcomes).toEqual([]);
  expect(t.gathers).toBe(2);
});

/** A share prepared by hand, as a caller that then stops: its primary is `primaryKey`, its deadline `ms` from now. */
async function prepareAndStop(name: string, bits: string, key: string, delta: number, primaryKey: string, ms: number) {
  const tx = `tx-${unique()}`;
  const answer = await step(name, bits, { step: "prepare", tx, method: "transfer", primaryKey, deadline: Date.now() + ms }, [key], [delta]);
  expect(answer.ok).toEqual({ prepared: true });
  return tx;
}

test("a caller gone after the prepares: the hold ends at its deadline, aborted, and the waiting write applies", async () => {
  const name = `atomic-gone-prepared-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!].sort() as [string, string];
  await twoShards(name, { [a]: 10, [b]: 10 });
  const bBits = half(b);
  const tx = await prepareAndStop(name, bBits, b, 5, a, 400);
  expect((await tables(name, bBits)).holds.map((h) => h.tx)).toEqual([tx]);
  // A write to the held key waits for the hold, then applies on the value from before.
  const started = Date.now();
  expect(await call("counters", name, "add", [b], [1])).toEqual([11]);
  expect(Date.now() - started).toBeGreaterThanOrEqual(300);
  expect((await tables(name, bBits)).holds).toEqual([]);
  expect((await tables(name, half(a))).outcomes).toEqual([{ tx, outcome: "abort" }]);
}, 30_000);

test("a caller gone after the commit: the hold asks the primary, applies, and the waiting write lands on top", async () => {
  const name = `atomic-gone-committed-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!].sort() as [string, string];
  await twoShards(name, { [a]: 10, [b]: 10 });
  const tx = await prepareAndStop(name, half(b), b, 5, a, 400);
  // The primary's share: committed and applied there, then the caller stops before resolving.
  expect((await step(name, half(a), { step: "commit", tx, method: "transfer" }, [a], [-5])).ok).toEqual({ applied: [5] });
  expect(await values(name, [a])).toEqual([5]);
  expect(await call("counters", name, "add", [b], [1])).toEqual([16]);
  expect((await tables(name, half(b))).holds).toEqual([]);
}, 30_000);

test("a recovery that reaches the primary first aborts the call, and the late commit is refused", async () => {
  const name = `atomic-late-commit-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!].sort() as [string, string];
  await twoShards(name, { [a]: 10, [b]: 10 });
  const tx = await prepareAndStop(name, half(b), b, 5, a, 50);
  // The hold's deadline passes; its next write sets recovery off, which records the abort.
  expect(await call("counters", name, "add", [b], [1])).toEqual([11]);
  expect((await step(name, half(a), { step: "commit", tx, method: "transfer" }, [a], [-5])).ok).toEqual({ outcome: "abort" });
  expect(await values(name, [a, b])).toEqual([10, 11]);
}, 30_000);

test("a shard holding keys does not split until its holds end", async () => {
  const name = `atomic-no-split-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!].sort() as [string, string];
  await twoShards(name, { [a]: 10, [b]: 10 });
  const bBits = half(b);
  const tx = await prepareAndStop(name, bBits, b, 5, a, 60_000);
  expect(await post<boolean>("/split", { cls: "counters", name, bits: bBits })).toBe(false);
  expect((await step(name, bBits, { step: "resolve", tx, outcome: "abort" }, [b])).ok).toEqual({ outcome: "abort" });
  expect(await post<boolean>("/split", { cls: "counters", name, bits: bBits })).toBe(true);
}, 30_000);

test("a hold past its deadline ends by the alarm, with no write to set it off", async () => {
  const name = `atomic-alarm-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!].sort() as [string, string];
  await twoShards(name, { [a]: 10, [b]: 10 });
  await prepareAndStop(name, half(b), b, 5, a, 200);
  for (let i = 0; i < 50 && (await tables(name, half(b))).holds.length > 0; i++) await Bun.sleep(100);
  expect((await tables(name, half(b))).holds).toEqual([]);
  expect(await values(name, [a, b])).toEqual([10, 10]);
}, 30_000);

test("a prepare whose gather outlasts another call's hold on its keys waits that hold out before holding them", async () => {
  const name = `atomic-gather-${unique()}`;
  const { left, right } = keysByHalf(1);
  const [a, b] = [left[0]!, right[0]!].sort() as [string, string];
  await twoShards(name, { [a]: 10, [b]: 10 });
  const started = Date.now();
  // The call's share at b gathers for 300 ms; meanwhile another caller holds b until 1 s.
  const slow = post<Outcome>("/atomically", { cls: "counters", name, method: "transferSlow", keys: [a, b], payloads: [-1, 1], args: [300] });
  await Bun.sleep(100);
  const other = await prepareAndStop(name, half(b), b, 5, a, 1000);
  const out = await slow;
  expect(out.committed).toBe(true);
  // It held b only once the other hold had ended (aborted at its deadline, by its recovery).
  expect(Date.now() - started).toBeGreaterThanOrEqual(1000);
  expect((await tables(name, half(a))).outcomes).toEqual([{ tx: other, outcome: "abort" }]);
  expect(await values(name, [a, b])).toEqual([9, 11]);
}, 30_000);

test("a refusal in one of three shards drops the hold another one took, at once", async () => {
  const name = `atomic-three-${unique()}`;
  // Shards 0, 10 and 11: keys whose hashes start that way.
  const keys: Record<string, string> = {};
  for (let i = 0; Object.keys(keys).length < 3; i++) {
    const h = keyHash(`t${i}`);
    const bits = hashBit(h, 0) === "0" ? "0" : `1${hashBit(h, 1)}`;
    keys[bits] ??= `t${i}`;
  }
  const all = Object.values(keys);
  await twoShards(name, Object.fromEntries(all.map((k) => [k, 10])));
  expect(await post<boolean>("/split", { cls: "counters", name, bits: "1" })).toBe(true);
  for (let i = 0; i < 150; i++) {
    const [x, y] = await Promise.all([describe("counters", name, "10"), describe("counters", name, "11")]);
    if (x.state === "shard" && y.state === "shard" && x.storedRows === x.rows && y.storedRows === y.rows) break;
    await Bun.sleep(100);
  }
  const sorted = [...all].sort();
  // The last key (not the primary) refuses; the middle one prepares, and must drop at once.
  const out = await atomically(name, sorted, [1, 1, -11]);
  expect(out).toEqual({ committed: false, refusals: [{ keys: [sorted[2]!], result: `below zero: ${sorted[2]}` }] });
  const bitsOf = Object.fromEntries(Object.entries(keys).map(([bits, k]) => [k, bits]));
  expect((await tables(name, bitsOf[sorted[1]!]!)).holds).toEqual([]);
  expect(await values(name, sorted)).toEqual([10, 10, 10]);
}, 30_000);

test("transfers racing across shards, with plain writes beside them, never lose or make money", async () => {
  const name = `atomic-race-${unique()}`;
  const { left, right } = keysByHalf(3);
  const keys = [...left, ...right];
  await twoShards(name, Object.fromEntries(keys.map((k) => [k, 50])));
  let committed = 0;
  let refused = 0;
  let deposits = 0;
  const transfers = Array.from({ length: 36 }, async (_, i) => {
    const from = keys[i % keys.length]!;
    const to = keys[(i * 5 + 1) % keys.length]!;
    if (from === to) return;
    const out = await atomically(name, [from, to], [-35, 35]);
    if (out.committed) committed++;
    else refused++;
  });
  // Plain writes to the same keys, each waiting out any hold on its key.
  const plain = Array.from({ length: 24 }, async (_, i) => {
    await call("counters", name, "add", [keys[(i * 7) % keys.length]!], [1]);
    deposits++;
  });
  // One that no balance covers, among them.
  const tooMuch = atomically(name, [left[0]!, right[0]!], [-1000, 1000]).then((out) => (out.committed ? committed++ : refused++));
  await Promise.all([...transfers, ...plain, tooMuch]);
  expect(committed).toBeGreaterThan(0);
  expect(refused).toBeGreaterThan(0);
  const after = await values(name, keys);
  expect(after.every((v) => v !== null && v >= 0)).toBe(true);
  expect(after.reduce((s, v) => s! + v!, 0)).toBe(50 * keys.length + deposits);
  const [l, r] = await Promise.all([tables(name, "0"), tables(name, "1")]);
  expect([...l.holds, ...r.holds]).toEqual([]);
  console.log(JSON.stringify({ committed, refused, deposits }));
}, 60_000);
