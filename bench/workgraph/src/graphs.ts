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
export const TASKS = { root, child };

export type Shape = "forest" | "tree" | "chain" | "tiny";

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
export function graphOf(shape: Shape, units: number, unitMs: number, itersPerMs: number, run: string) {
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

