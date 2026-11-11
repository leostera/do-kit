// The benchmark's graphs (RFD0005), without objects, so the plot script can plan them too: units
// that burn a known number of SHA-256 rounds, in four shapes.
import { createHash } from "node:crypto";
import { defineTask, workGraphOf, type NodeRef } from "../../../src/workgraph/graph.ts";

/** `rounds` SHA-256 rounds over a 32-byte digest, starting from `text`. */
export function burn(text: string, rounds: number): string {
  let h = createHash("sha256").update(text).digest();
  for (let i = 0; i < rounds; i++) h = createHash("sha256").update(h).digest();
  return h.toString("hex").slice(0, 16);
}

const root = defineTask({
  name: "root",
  version: "1",
  compute(input: { salt: string; rounds: number }, _parent: null, _after: {}, _ctx) {
    const value = burn(input.salt, input.rounds);
    return { result: value, value };
  },
});
const child = defineTask({
  name: "child",
  version: "1",
  compute(input: { salt: string; rounds: number }, parent: string, _after: {}, _ctx) {
    const value = burn(`${parent}:${input.salt}`, input.rounds);
    return { result: value, value };
  },
});
/** A slice: burns its rounds and digests its bytes, and returns a receipt. */
const slice = defineTask({
  name: "slice",
  version: "1",
  compute(input: { rounds: number; entries: Uint8Array }, _parent: null, _after: {}, _ctx) {
    const digest = createHash("sha256").update(input.entries).digest("hex").slice(0, 16);
    const value = burn(digest, input.rounds);
    return { result: { entries: input.entries.length / 20, digest }, value };
  },
});
/** A piece of a big tree, cut below its root slice: needs the root's value (its path state). */
const piece = defineTask({
  name: "piece",
  version: "1",
  compute(input: { rounds: number; entries: Uint8Array }, parent: string, _after: {}, _ctx) {
    const digest = createHash("sha256").update(parent).update(input.entries).digest("hex").slice(0, 16);
    burn(digest, input.rounds);
    return { result: { entries: input.entries.length / 20, digest }, value: digest };
  },
});
export const TASKS = { root, child, slice, piece };

export type Shape = "forest" | "tree" | "chain" | "tiny" | "slices" | "bodies";

/** A deterministic random number generator (mulberry32). */
function random(seed: number) {
  let t = seed >>> 0;
  return () => {
    t = (t + 0x6d2b79f5) >>> 0;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

/**
 * The graph of a shape: `units` units of `unitMs` each (tiny: no cost). forest: trees of 5, each
 * unit's parent a random earlier unit of its tree; tree: one tree, each unit's parent a random
 * earlier unit (a random recursive tree, depth about e·ln(units)); chain: each unit's parent the
 * one before.
 */
export function graphOf(shape: Shape, units: number, unitMs: number, itersPerMs: number, run: string, bytes = 0) {
  if (shape === "slices" || shape === "bodies") return slicesOf(shape, units, unitMs, itersPerMs, run, bytes);
  const rand = random(7);
  const g = workGraphOf(TASKS);
  const rounds = shape === "tiny" ? 0 : Math.round(unitMs * itersPerMs);
  const cost = shape === "tiny" ? 0 : unitMs;
  const nodes: NodeRef<string, string>[] = [];
  for (let i = 0; i < units; i++) {
    // Salted by run: every run computes from scratch.
    const input = { salt: `${run}:${i}`, rounds };
    let parent: NodeRef<string, string> | null = null;
    if (shape === "forest" && i % 5 !== 0) parent = nodes[i - 1 - Math.floor(rand() * (i % 5))]!;
    if (shape === "tree" && i > 0) parent = nodes[Math.floor(rand() * i)]!;
    if (shape === "chain" && i > 0) parent = nodes[i - 1]!;
    nodes.push(parent ? g.add(child, `u${i}`, input, { cost, parent }) : g.add(root, `u${i}`, input, { cost }));
  }
  return g;
}


/** Deterministic bytes, salted: `entries` of 20 bytes each. */
function entriesOf(bytes: number, salt: string): Uint8Array {
  const out = new Uint8Array(bytes);
  let h = createHash("sha256").update(salt).digest();
  for (let at = 0; at < bytes; at += 32) {
    out.set(h.subarray(0, Math.min(32, bytes - at)), at);
    if (at % 4096 === 0) h = createHash("sha256").update(h).digest();
  }
  return out;
}

/**
 * slices: `units` independent slices of 0.8 to 1.2 × `unitMs`, each with `bytes` of entries, and 6
 * big trees, each a root slice and 2 to 10 pieces below it of `unitMs` each, which load the root's
 * value. bodies: `units` slices of `unitMs`, for chunks whose bodies are mostly bytes.
 */
function slicesOf(shape: "slices" | "bodies", units: number, unitMs: number, itersPerMs: number, run: string, bytes: number) {
  const rand = random(11);
  const g = workGraphOf(TASKS);
  const unit = (key: string, ms: number) => ({ rounds: Math.round(ms * itersPerMs), entries: entriesOf(bytes, `${run}:${key}`) });
  for (let i = 0; i < units; i++) {
    const ms = shape === "slices" ? Math.round(unitMs * (0.8 + 0.4 * rand())) : unitMs;
    g.add(slice, `slice-${i}`, unit(`slice-${i}`, ms), { cost: ms });
  }
  if (shape === "slices")
    for (let t = 0; t < 6; t++) {
      const top = g.add(slice, `tree-${t}`, unit(`tree-${t}`, unitMs), { cost: unitMs });
      const pieces = 2 + Math.floor(rand() * 9);
      for (let p = 0; p < pieces; p++) g.add(piece, `tree-${t}/${p}`, unit(`tree-${t}/${p}`, unitMs), { cost: unitMs, parent: top });
    }
  return g;
}
