// Work graphs for the tests (RFD0005), shared by the tests and the test Worker: tasks whose results
// are hashes of their inputs, random forests of them with joins across trees, and each unit's
// expected result, evaluated directly.
import { createHash } from "node:crypto";
import { defineTask, workGraphOf, type NodeRef } from "../src/workgraph/graph.ts";

export const sha = (text: string) => createHash("sha256").update(text).digest("hex").slice(0, 16);

/** Counts compute calls, in this isolate. */
export const calls = { n: 0 };

/** A root: its value from its input alone. */
export const seed = defineTask({
  name: "seed",
  version: "1",
  compute(input: { salt: string }, _parent: null, _after: {}, _ctx) {
    calls.n++;
    const value = sha(`seed:${input.salt}`);
    return { result: value, value };
  },
});
/** A child: transforms its parent's value (a string). */
export const step = defineTask({
  name: "step",
  version: "1",
  compute(input: { salt: string }, parent: string, _after: {}, _ctx) {
    calls.n++;
    const value = sha(`${parent}:${input.salt}`);
    return { result: value, value };
  },
});
/** Reads two results across trees; its value is a number, its result a string. */
export const join = defineTask({
  name: "join",
  version: "1",
  compute(input: { salt: string }, _parent: null, after: { left: string; right: string }, _ctx) {
    calls.n++;
    const result = sha(`${after.left}+${after.right}:${input.salt}`);
    return { result, value: result.length };
  },
});
/** Transforms a number: only a join's value fits its parent. */
export const count = defineTask({
  name: "count",
  version: "1",
  compute(_input: null, parent: number, _after: {}, _ctx) {
    calls.n++;
    return { result: parent + 1, value: parent + 1 };
  },
});
/** Always throws. */
export const boom = defineTask({
  name: "boom",
  version: "1",
  compute(_input: null, _parent: null, _after: {}, _ctx): { result: number; value: number } {
    throw new Error("boom");
  },
});
/** A slice: its input is bytes (a compact description of its work); its result, their digest. */
export const slice = defineTask({
  name: "slice",
  version: "1",
  compute(input: Uint8Array, _parent: null, _after: {}, _ctx) {
    calls.n++;
    if (!(input instanceof Uint8Array)) throw new Error(`a slice's input is ${typeof input}, not bytes`);
    const digest = createHash("sha256").update(input).digest("hex").slice(0, 16);
    return { result: { bytes: input.length, digest }, value: digest };
  },
});
/** A piece of a slice cut below its root: its own bytes, and the root's value. */
export const piece = defineTask({
  name: "piece",
  version: "1",
  compute(input: Uint8Array, parent: string, _after: {}, _ctx) {
    calls.n++;
    if (!(input instanceof Uint8Array)) throw new Error(`a piece's input is ${typeof input}, not bytes`);
    const digest = sha(`${parent}:${createHash("sha256").update(input).digest("hex")}`);
    return { result: { bytes: input.length, digest }, value: digest };
  },
});

export const TASKS = { seed, step, join, count, boom, slice, piece };

/** Slices of bytes: `count` independent slices, and one slice cut into `pieces` pieces below it. */
export interface Slices {
  readonly count: number;
  readonly bytes: number;
  readonly pieces: number;
  readonly seed: number;
  /** A slice whose bytes differ (its key). */
  readonly changed?: string;
}

/** Deterministic bytes. */
export function bytesOf(n: number, seedNumber: number): Uint8Array {
  const rand = random(seedNumber);
  const out = new Uint8Array(n);
  for (let i = 0; i < n; i += 4) {
    const v = (rand() * 2 ** 32) >>> 0;
    for (let j = 0; j < 4 && i + j < n; j++) out[i + j] = (v >>> (8 * j)) & 0xff;
  }
  return out;
}

export function slices(shape: Slices) {
  const g = workGraphOf(TASKS);
  const expected = new Map<string, { bytes: number; digest: string }>();
  const digest = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
  const input = (key: string, i: number) => bytesOf(shape.bytes, shape.seed * 100_000 + i + (shape.changed === key ? 7_777 : 0));
  for (let i = 0; i < shape.count; i++) {
    const b = input(`s${i}`, i);
    g.add(slice, `s${i}`, b, { cost: 10 });
    expected.set(`s${i}`, { bytes: b.length, digest: digest(b).slice(0, 16) });
  }
  const rootBytes = input("root", shape.count);
  const root = g.add(slice, "root", rootBytes, { cost: 10 });
  const rootValue = digest(rootBytes).slice(0, 16);
  expected.set("root", { bytes: rootBytes.length, digest: rootValue });
  for (let i = 0; i < shape.pieces; i++) {
    const b = input(`p${i}`, shape.count + 1 + i);
    g.add(piece, `p${i}`, b, { cost: 10, parent: root });
    expected.set(`p${i}`, { bytes: b.length, digest: sha(`${rootValue}:${digest(b)}`) });
  }
  return { g, expected };
}

/** A deterministic random number generator (mulberry32). */
export function random(seedNumber: number) {
  let t = seedNumber >>> 0;
  return () => {
    t = (t + 0x6d2b79f5) >>> 0;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

export interface Shape {
  readonly trees: number;
  /** Units per tree, at most (exactly, with `exact`). */
  readonly size: number;
  readonly exact?: boolean;
  /** Probability that a unit extends the newest unit (long chains) rather than a random one. */
  readonly chainy: number;
  /** Join units over random pairs of tree units (each with a count below it). */
  readonly joins: number;
  readonly seed: number;
  /** Units whose input is changed (their keys start with this), for incremental runs. */
  readonly changed?: string;
  /** Adds a unit that always fails. */
  readonly boom?: boolean;
}

/** A random forest, and the direct evaluation of every unit. */
export function forest(shape: Shape) {
  const rand = random(shape.seed);
  const salt = (key: string) => (shape.changed && key.startsWith(shape.changed) ? `${key}!` : key);
  const g = workGraphOf(TASKS);
  const expected = new Map<string, unknown>();
  const treeNodes: NodeRef<string, string>[] = [];
  for (let t = 0; t < shape.trees; t++) {
    const rootKey = `t${t}`;
    const nodes: NodeRef<string, string>[] = [g.add(seed, rootKey, { salt: salt(rootKey) }, { cost: 1 + Math.floor(rand() * 5) })];
    expected.set(rootKey, sha(`seed:${salt(rootKey)}`));
    const n = shape.exact ? shape.size : 1 + Math.floor(rand() * shape.size);
    for (let i = 1; i < n; i++) {
      const parent = rand() < shape.chainy ? nodes[nodes.length - 1]! : nodes[Math.floor(rand() * nodes.length)]!;
      const key = `t${t}.${i}`;
      nodes.push(g.add(step, key, { salt: salt(key) }, { cost: 1 + Math.floor(rand() * 5), parent }));
      expected.set(key, sha(`${expected.get(parent.key)}:${salt(key)}`));
    }
    treeNodes.push(...nodes);
  }
  for (let j = 0; j < shape.joins; j++) {
    const left = treeNodes[Math.floor(rand() * treeNodes.length)]!;
    const right = treeNodes[Math.floor(rand() * treeNodes.length)]!;
    const key = `j${j}`;
    const joined = g.add(join, key, { salt: salt(key) }, { cost: 2, after: { left, right } });
    const result = sha(`${expected.get(left.key)}+${expected.get(right.key)}:${salt(key)}`);
    expected.set(key, result);
    g.add(count, `c${j}`, null, { cost: 1, parent: joined });
    expected.set(`c${j}`, result.length + 1);
  }
  if (shape.boom) g.add(boom, "boom", null, { cost: 1 });
  return { g, expected };
}
