// Work graph planning and chunk execution, without objects (RFD0005): graphs are planned, then their
// chunks executed one at a time in a random order that respects their needs, against an in-memory
// store, and every unit's result compared with evaluating the graph directly.
import { describe, expect, test } from "bun:test";
// The pure modules: the barrel also exports the object classes, which need cloudflare:workers.
import { createHash } from "node:crypto";
import { decodeBody, encodeBody } from "../src/workgraph/body.ts";
import { ComputeFailed, executeChunk } from "../src/workgraph/execute.ts";
import { defineTask, workGraphOf, type NodeRef, type ResultOfNode } from "../src/workgraph/graph.ts";
import { planWorkGraph, unitHash, type Chunk, type WorkGraphPlan } from "../src/workgraph/plan.ts";
import { resultKey } from "../src/workgraph/stored.ts";
import { bytesOf, calls, count, forest as fixtureForest, join, random, seed, slices, step, TASKS, type Shape } from "./workgraph-fixture.ts";

const TASKS_IN_TYPES = { seed, step, join, count };

describe("the graph's types follow its edges", () => {
  test("a unit's parent, after units and input are checked against its task", () => {
    const g = workGraphOf(TASKS_IN_TYPES);
    const a = g.add(seed, "a", { salt: "a" }, { cost: 1 });
    const b = g.add(step, "b", { salt: "b" }, { cost: 1, parent: a });
    const j = g.add(join, "j", { salt: "j" }, { cost: 1, after: { left: a, right: b } });
    const c = g.add(count, "c", null, { cost: 1, parent: j });
    // @ts-expect-error a step transforms a parent's value: it must have one
    g.add(step, "x1", { salt: "x" }, { cost: 1 });
    // @ts-expect-error a seed has no parent
    g.add(seed, "x2", { salt: "x" }, { cost: 1, parent: a });
    // @ts-expect-error a count transforms a number, and a step's value is a string
    g.add(count, "x3", null, { cost: 1, parent: b });
    // @ts-expect-error a join reads string results, and a count's result is a number
    g.add(join, "x4", { salt: "x" }, { cost: 1, after: { left: a, right: c } });
    // @ts-expect-error a join reads two named results
    g.add(join, "x5", { salt: "x" }, { cost: 1, after: { left: a } });
    // @ts-expect-error a seed's input is { salt }
    g.add(seed, "x6", { pepper: "x" }, { cost: 1 });
    const result: ResultOfNode<typeof c> = 1;
    const ref: NodeRef<string, string> = b;
    expect([result, ref.key]).toEqual([1, "b"]);
  });

  test("a task the graph does not have, or a unit added twice, is refused", () => {
    const g = workGraphOf({ seed });
    expect(() => g.add(step as never, "s", { salt: "" } as never, { cost: 1 } as never)).toThrow("not in this graph's task set");
    g.add(seed, "a", { salt: "a" }, { cost: 1 });
    expect(() => g.add(seed, "a", { salt: "a" }, { cost: 1 })).toThrow("appears twice");
  });
});

/** A forest of the fixture, with `salt` changing the inputs of keys it is given. */
function forest(shape: Omit<Shape, "seed" | "changed">, seedNumber: number, changed?: string) {
  return fixtureForest({ ...shape, seed: seedNumber, ...(changed ? { changed } : {}) });
}

/** Executes a plan's chunks one at a time, in a random order that respects their needs. */
async function execute(plan: WorkGraphPlan, store: Map<string, string>, seedNumber: number) {
  const rand = random(seedNumber);
  const done = new Set<number>();
  let computed = 0;
  let replayed = 0;
  const pending = [...plan.chunks];
  while (pending.length) {
    const ready = pending.filter((c) => c.needs.every((n) => done.has(n)));
    expect(ready.length).toBeGreaterThan(0);
    const chunk = ready[Math.floor(rand() * ready.length)]!;
    const out = await executeChunk(chunk, TASKS, store, {});
    for (const [k, v] of out.writes) store.set(k, v);
    computed += out.computed;
    replayed += out.replayed;
    done.add(chunk.id);
    pending.splice(pending.indexOf(chunk), 1);
  }
  return { computed, replayed };
}

function checkPlan(plan: WorkGraphPlan, keys: readonly string[], chunkMs: number) {
  // Every unit is computed for its result in exactly one chunk.
  const primary = new Map<string, number>();
  for (const c of plan.chunks) for (const u of c.units) if (!u.replay) primary.set(u.key, (primary.get(u.key) ?? 0) + 1);
  expect([...primary.keys()].sort()).toEqual([...keys].sort());
  expect([...primary.values()].every((n) => n === 1)).toBe(true);
  // Chunks fit, unless one unit alone does not.
  for (const c of plan.chunks) if (c.units.length > 1) expect(c.cost).toBeLessThanOrEqual(chunkMs);
  // A chunk only needs chunks before it in some order: the planner checks there is no cycle.
  for (const c of plan.chunks) expect(c.needs.includes(c.id)).toBe(false);
}

function resultsOf(plan: WorkGraphPlan, store: Map<string, string>) {
  return new Map([...plan.hashes].map(([key, hash]) => [key, JSON.parse(store.get(resultKey(hash)) ?? "null")]));
}

describe("plans compute every unit's result", () => {
  const shapes: [string, Omit<Shape, "seed">, number][] = [
    ["many small trees", { trees: 60, size: 8, chainy: 0.3, joins: 0 }, 40],
    ["a few big bushy trees", { trees: 3, size: 400, chainy: 0.05, joins: 0 }, 60],
    ["long chains", { trees: 4, size: 300, chainy: 0.95, joins: 0 }, 50],
    ["trees with joins across them", { trees: 20, size: 30, chainy: 0.4, joins: 25 }, 40],
  ];
  for (const [name, shape, chunkMs] of shapes) {
    test(name, async () => {
      for (let s = 1; s <= 5; s++) {
        const { g, expected } = forest(shape, s * 7919);
        const plan = planWorkGraph(g, { chunkMs, replayMs: 10 });
        checkPlan(plan, [...expected.keys()], chunkMs);
        const store = new Map<string, string>();
        const { computed } = await execute(plan, store, s);
        expect(computed).toBe(expected.size);
        expect(resultsOf(plan, store)).toEqual(expected);
      }
    });
  }

  test("cuts replay a short path, and load a long one's value", () => {
    // One chain of 100 units of cost 1, in chunks of 40, cut into pieces of 30 from the bottom:
    // the piece just below the top replays its 10-unit path; the ones below load their parent's value.
    const g = workGraphOf(TASKS);
    let node: NodeRef<string, string> = g.add(seed, "u0", { salt: "0" }, { cost: 1 });
    for (let i = 1; i < 100; i++) node = g.add(step, `u${i}`, { salt: String(i) }, { cost: 1, parent: node });
    const plan = planWorkGraph(g, { chunkMs: 40, replayMs: 10 });
    const replaying = plan.chunks.filter((c) => c.units.some((u) => u.replay));
    const loading = plan.chunks.filter((c) => c.units.some((u) => u.load !== null));
    expect(replaying.length).toBeGreaterThan(0);
    expect(loading.length).toBeGreaterThan(0);
    for (const c of loading) expect(c.needs.length).toBe(1);
    expect(plan.chunks.flatMap((c) => c.exports).length).toBe(loading.length);
    expect(plan.span).toBe(100);
    expect(plan.work).toBe(100);
  });

  test("a chain of 100,000 units plans without recursion", () => {
    const g = workGraphOf(TASKS);
    let node: NodeRef<string, string> = g.add(seed, "u0", { salt: "0" }, { cost: 1 });
    for (let i = 1; i < 100_000; i++) node = g.add(step, `u${i}`, { salt: String(i) }, { cost: 1, parent: node });
    const plan = planWorkGraph(g, { chunkMs: 1_000 });
    // Cut into pieces of 750 (room for a replayed path of 250), each loading its parent's value.
    expect(plan.chunks.length).toBe(Math.ceil(100_000 / 750));
    expect(plan.span).toBe(100_000);
    // The piece below the top replays its path, so it runs beside the top; the rest wait in turn.
    const top = plan.chunks.find((c) => c.units.some((u) => u.key === "u0" && !u.replay))!;
    expect(plan.chunkSpan).toBe(plan.chunks.reduce((sum, c) => sum + c.cost, 0) - top.cost);
    // With no replays, each chunk needs the one above it: no two run at once.
    const strict = planWorkGraph(g, { chunkMs: 1_000, replayMs: 0 });
    expect(strict.chunkSpan).toBe(strict.chunks.reduce((sum, c) => sum + c.cost, 0));
  });

  test("the chunk span is the most expensive chain of chunks", () => {
    // Trees that fit a chunk: no chunk needs another, so the biggest chunk is the whole chain.
    const independent = planWorkGraph(forest({ trees: 300, size: 8, chainy: 0.5, joins: 0 }, 4).g, { chunkMs: 40 });
    expect(independent.chunks.every((c) => c.needs.length === 0)).toBe(true);
    expect(independent.chunkSpan).toBe(Math.max(...independent.chunks.map((c) => c.cost)));
    // Big trees and joins: between the unit span and the cost of every chunk.
    for (const seed of [1, 2, 3]) {
      const plan = planWorkGraph(forest({ trees: 12, size: 300, chainy: 0.7, joins: 20 }, seed).g, { chunkMs: 60, replayMs: 10 });
      const total = plan.chunks.reduce((sum, c) => sum + c.cost, 0);
      expect(plan.chunkSpan).toBeGreaterThanOrEqual(Math.max(plan.span, ...plan.chunks.map((c) => c.cost)));
      expect(plan.chunkSpan).toBeLessThanOrEqual(total);
      expect(plan.chunkSpan).toBeLessThan(total);
    }
  });

  test("joins run in a later round than what they read", () => {
    const { g } = forest({ trees: 5, size: 5, chainy: 0.5, joins: 5 }, 3);
    const plan = planWorkGraph(g, { chunkMs: 1_000 });
    expect(plan.rounds).toBe(2);
    const roundOf = new Map(plan.chunks.flatMap((c) => c.units.filter((u) => !u.replay).map((u) => [u.key, c.round] as const)));
    for (const c of plan.chunks) for (const u of c.units) for (const a of u.after) expect(roundOf.get(a.key)!).toBeLessThan(c.round);
  });
});

describe("later runs reuse results", () => {
  test("only units whose inputs changed, and the units below them, are computed again", async () => {
    const shape = { trees: 30, size: 20, chainy: 0.5, joins: 10 };
    const first = forest(shape, 42);
    const firstPlan = planWorkGraph(first.g, { chunkMs: 30, replayMs: 8 });
    const store = new Map<string, string>();
    await execute(firstPlan, store, 1);
    // Change the input of every unit of tree 3 below its root, and nothing else.
    const second = forest(shape, 42, "t3.");
    const secondPlan = planWorkGraph(second.g, { chunkMs: 30, replayMs: 8 });
    const changed = [...secondPlan.hashes].filter(([key, hash]) => firstPlan.hashes.get(key) !== hash).map(([key]) => key);
    expect(changed.length).toBeGreaterThan(0);
    expect(changed.every((k) => k.startsWith("t3.") || k.startsWith("j") || k.startsWith("c"))).toBe(true);
    calls.n = 0;
    const { computed } = await execute(secondPlan, store, 2);
    expect(computed).toBe(changed.length);
    expect(resultsOf(secondPlan, store)).toEqual(second.expected);
    // Computed again: the changed units, and the units above them for their values (in the chunk).
    expect(calls.n).toBeGreaterThanOrEqual(changed.length);
  });

  test("a changed unit changes the hash of every unit below it", async () => {
    // Chains of exactly 12: changing t2.1 changes t2.2 to t2.11 too, whose own inputs did not change.
    const shape = { trees: 4, size: 12, chainy: 1, joins: 0, exact: true };
    const first = fixtureForest({ ...shape, seed: 9 });
    const firstPlan = planWorkGraph(first.g, { chunkMs: 8, replayMs: 2 });
    const store = new Map<string, string>();
    await execute(firstPlan, store, 1);
    const second = fixtureForest({ ...shape, seed: 9, changed: "t2.1" });
    const secondPlan = planWorkGraph(second.g, { chunkMs: 8, replayMs: 2 });
    const changed = [...secondPlan.hashes].filter(([key, hash]) => firstPlan.hashes.get(key) !== hash).map(([key]) => key);
    expect(changed.sort()).toEqual(Array.from({ length: 11 }, (_, i) => `t2.${i + 1}`).sort());
    expect((await execute(secondPlan, store, 2)).computed).toBe(11);
    expect(resultsOf(secondPlan, store)).toEqual(second.expected);
  });

  test("a task's version is in every hash", () => {
    const build = (version: string) => {
      const v = defineTask({ ...seed, version });
      const g = workGraphOf({ seed: v });
      g.add(v, "a", { salt: "a" }, { cost: 1 });
      return planWorkGraph(g).hashes.get("a");
    };
    expect(build("1")).not.toBe(build("2"));
  });
});

describe("chunks that cannot run", () => {
  test("a chunk run before one it needs throws, rather than computing without its input", async () => {
    const g = workGraphOf(TASKS);
    let node: NodeRef<string, string> = g.add(seed, "u0", { salt: "0" }, { cost: 1 });
    for (let i = 1; i < 40; i++) node = g.add(step, `u${i}`, { salt: String(i) }, { cost: 1, parent: node });
    const plan = planWorkGraph(g, { chunkMs: 10, replayMs: 0 });
    const loading = plan.chunks.find((c) => c.needs.length > 0) as Chunk;
    await expect(executeChunk(loading, TASKS, new Map(), {})).rejects.toThrow("is not stored");
  });

  test("a compute that throws names its unit", async () => {
    const bad = defineTask({
      name: "bad",
      version: "1",
      compute(_input: null, _parent: null, _after: {}, _ctx): { result: number; value: number } {
        throw new Error("no");
      },
    });
    const g = workGraphOf({ bad });
    g.add(bad, "b", null, { cost: 1 });
    const plan = planWorkGraph(g);
    await expect(executeChunk(plan.chunks[0]!, { bad }, new Map(), {})).rejects.toThrow("unit b: no");
  });

  // Each fails the same way on every attempt: a failure of the chunk (counted against maxAttempts),
  // not an error the alarm retries forever.
  test("a task the runner lacks, or a result too big to store, fails the chunk", async () => {
    const huge = defineTask({
      name: "huge",
      version: "1",
      compute(_input: null, _parent: null, _after: {}, _ctx) {
        return { result: "x".repeat(1024 * 1024 + 1), value: null };
      },
    });
    const g = workGraphOf({ huge });
    g.add(huge, "h", null, { cost: 1 });
    const chunk = planWorkGraph(g).chunks[0]!;
    const big = executeChunk(chunk, { huge }, new Map(), {});
    await expect(big).rejects.toBeInstanceOf(ComputeFailed);
    await expect(big).rejects.toThrow("the result of h is");
    const missing = executeChunk(chunk, {}, new Map(), {});
    await expect(missing).rejects.toBeInstanceOf(ComputeFailed);
    await expect(missing).rejects.toThrow("its task huge is not one of this runner's tasks");
  });
});

describe("binary inputs and identity", () => {
  test("a chunk's body keeps bytes as bytes, wherever they are in an input", () => {
    const big = bytesOf(300 * 1024, 1);
    const plan = planWorkGraph(slices({ count: 2, bytes: 300 * 1024, pieces: 1, seed: 1 }).g, { chunkMs: 1_000 });
    const body = { units: [...plan.chunks[0]!.units, { ...plan.chunks[0]!.units[0]!, key: "nested", input: { entries: [big, big.subarray(10, 20)], n: 2 } }], exports: ["e"] };
    const encoded = encodeBody(body);
    const decoded = decodeBody(encoded);
    expect(decoded).toEqual(body);
    expect((decoded.units.at(-1)!.input as { entries: Uint8Array[] }).entries[1]).toBeInstanceOf(Uint8Array);
    // No base64: the body is its inputs, plus a little JSON.
    const inputs = (plan.chunks[0]!.units.length + 1) * 300 * 1024 + 10;
    expect(encoded.byteLength).toBeGreaterThan(inputs);
    expect(encoded.byteLength).toBeLessThan(inputs + 4 * 1024);
  });

  test("hashes of JSON inputs are unchanged; bytes are hashed by their contents", () => {
    const before = (task: string, version: string, input: unknown, parent: string | null, after: Record<string, string>) =>
      createHash("sha256")
        .update(JSON.stringify([task, version, input, parent, Object.keys(after).sort().map((k) => [k, after[k]])]))
        .digest("hex");
    for (const input of [null, { salt: "a" }, [1, "two", { three: 3 }], "s"]) expect(unitHash("t", "1", input, "p", { b: "x", a: "y" })).toBe(before("t", "1", input, "p", { b: "x", a: "y" }));
    const a = bytesOf(1024, 1);
    expect(unitHash("t", "1", a, null, {})).toBe(unitHash("t", "1", a.slice(), null, {}));
    const b = a.slice();
    b[1000]! ^= 1;
    expect(unitHash("t", "1", b, null, {})).not.toBe(unitHash("t", "1", a, null, {}));
    // Not the same as the object JSON would make of the bytes.
    expect(unitHash("t", "1", a, null, {})).not.toBe(before("t", "1", a, null, {}));
  });

  test("identity by key ignores inputs and parents, and follows key, task and version", async () => {
    const shape = { count: 4, bytes: 64, pieces: 2, seed: 3 };
    const first = planWorkGraph(slices(shape).g, { identity: "key" });
    const changed = planWorkGraph(slices({ ...shape, changed: "root" }).g, { identity: "key" });
    expect([...changed.hashes]).toEqual([...first.hashes]);
    const byInputs = planWorkGraph(slices({ ...shape, changed: "root" }).g);
    // By inputs, the changed root changes its own hash and its pieces'.
    expect([...byInputs.hashes.keys()].filter((k) => byInputs.hashes.get(k) !== planWorkGraph(slices(shape).g).hashes.get(k)).sort()).toEqual(["p0", "p1", "root"]);
    expect(new Set(first.hashes.values()).size).toBe(first.hashes.size);
    expect(() => planWorkGraph(slices(shape).g, { identity: "bogus" as "key" })).toThrow("identity");
  });
});
