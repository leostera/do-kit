// Planning a work graph (RFD0005): the graph's units, hashed by their inputs, assigned to rounds
// by their cross edges, and cut into chunks that each fit one invocation. Whole trees are packed
// together; a tree too big for a chunk is cut at subtrees, and each cut either replays the path
// to it or loads the value its parent's chunk stored. Pure: it runs wherever the graph is.
import { createHash } from "node:crypto";
import { jsonWithBinaries } from "./body.js";
import type { AnyTask, GraphUnit, NodeRef, WorkGraph } from "./graph.js";

export interface PlanOptions {
  /** The cost a chunk is packed to (default 10,000: a third of the default 30 s CPU limit). */
  readonly chunkMs?: number;
  /** At most this many units in a chunk, replayed ones included (default 5,000). */
  readonly maxUnitsPerChunk?: number;
  /** A cut replays the path to it if that path costs at most this (default chunkMs / 4). */
  readonly replayMs?: number;
  /**
   * What identifies a unit's result. "inputs" (the default): a hash of its task, version, input,
   * and its inputs' hashes, so a later run reuses every result whose inputs did not change.
   * "key": a hash of its task, version and key only, with no input hashed. Retries stay
   * idempotent, and a later run with the same key finds the result already there whatever the
   * input: name keys by what they compute (`pack-1234/slice-17`), and use a new store or new keys
   * for new work.
   */
  readonly identity?: "inputs" | "key";
}

/** A unit as a chunk holds it. */
export interface PlannedUnit {
  readonly key: string;
  readonly task: string;
  readonly hash: string;
  readonly input: unknown;
  /** The parent's key, when the parent is in this chunk (computed first, or replayed). */
  readonly parent: string | null;
  /** The parent's hash, when its value is loaded: the parent is in a chunk this one needs. */
  readonly load: string | null;
  /** The units whose results this one reads, by the name its task reads each under. */
  readonly after: readonly { readonly name: string; readonly key: string; readonly hash: string }[];
  /** Computed again only for its value: its result belongs to another chunk. */
  readonly replay: boolean;
}

/** A chunk: units that fit one invocation, parents before children. */
export interface Chunk {
  readonly id: number;
  readonly round: number;
  /** Estimated CPU milliseconds, replayed units included. */
  readonly cost: number;
  readonly units: readonly PlannedUnit[];
  /** Hashes of units whose values this chunk stores, for chunks cut below them. */
  readonly exports: readonly string[];
  /** Chunks that must be done before this one runs. */
  readonly needs: readonly number[];
}

export interface WorkGraphPlan {
  readonly chunks: readonly Chunk[];
  /** Each unit's hash, by key: the key of its result. */
  readonly hashes: ReadonlyMap<string, string>;
  /** A node's hash. */
  hashOf(node: NodeRef<unknown, unknown>): string;
  /** T₁: the cost of every unit. */
  readonly work: number;
  /** T∞: the cost of the most expensive chain of units that must run one after another. */
  readonly span: number;
  /**
   * The cost of the most expensive chain of chunks that must run one after another: the least time
   * a run can take, whatever the number of runners, since runners take whole chunks. At least the
   * cost of the biggest chunk, and at least `span`.
   */
  readonly chunkSpan: number;
  /** The cost added by replayed cuts. */
  readonly replayed: number;
  readonly rounds: number;
}

/**
 * The hash of a unit: its task and version, input, parent's hash, and its after units' hashes by
 * name. Uint8Arrays in the input are hashed as their bytes.
 */
export function unitHash(task: string, version: string, input: unknown, parent: string | null, after: Readonly<Record<string, string>>): string {
  const named = Object.keys(after)
    .sort()
    .map((name) => [name, after[name]]);
  const binaries: Uint8Array[] = [];
  const hash = createHash("sha256").update(jsonWithBinaries([task, version, input, parent, named], binaries));
  for (const b of binaries) hash.update(`\0${b.length}\0`).update(b);
  return hash.digest("hex");
}

/** The hash of a unit identified by its key: its task, version and key. */
export function unitKeyHash(task: string, version: string, key: string): string {
  return createHash("sha256").update(JSON.stringify(["key", task, version, key])).digest("hex");
}

interface Node {
  readonly unit: GraphUnit;
  readonly index: number;
  parent: Node | null;
  /** By the name its task reads each result under. */
  readonly after: { readonly name: string; readonly node: Node }[];
  /** Children in the same round (a tree edge that is not cut by a round). */
  readonly children: Node[];
  hash: string;
  round: number;
  /** The node's chunk (for its own result), once packed. */
  chunk: number;
}

/** A piece of a tree to pack: its root, the cost of what it holds, and how it gets its root's parent value. */
interface Piece {
  readonly root: Node;
  readonly members: Node[];
  readonly cost: number;
  /** Ancestors to replay, top first (empty when the parent's value is loaded, or there is none). */
  readonly replay: Node[];
  /** Loads its root's parent value from the chunk that stores it. */
  readonly loads: boolean;
}

/**
 * Plans a graph into chunks. Throws on a duplicate key, a parent or after unit that is not in the
 * graph, or a cycle (a graph built with `add` has none of these).
 */
export function planWorkGraph(graph: WorkGraph<Record<string, AnyTask>> | readonly GraphUnit[], options: PlanOptions = {}): WorkGraphPlan {
  const units: readonly GraphUnit[] = "units" in graph ? graph.units : graph;
  const chunkMs = options.chunkMs ?? 10_000;
  const maxUnits = options.maxUnitsPerChunk ?? 5_000;
  const replayMs = options.replayMs ?? chunkMs / 4;
  if (!(chunkMs > 0) || !(maxUnits > 1) || !(replayMs >= 0) || !(replayMs < chunkMs)) throw new Error("chunkMs must be positive, maxUnitsPerChunk over 1, and replayMs at least 0 and under chunkMs");

  const byKey = options.identity === "key";
  if (options.identity !== undefined && options.identity !== "inputs" && !byKey) throw new Error('identity must be "inputs" or "key"');
  const nodes = indexUnits(units);
  const order = topological(nodes);

  // Hashes, rounds and the span, parents and after units first.
  const chainCost = new Map<Node, number>();
  let span = 0;
  for (const n of order) {
    n.hash = byKey ? unitKeyHash(n.unit.task, n.unit.version, n.unit.key) : unitHash(n.unit.task, n.unit.version, n.unit.input, n.parent?.hash ?? null, Object.fromEntries(n.after.map((a) => [a.name, a.node.hash])));
    n.round = Math.max(n.parent?.round ?? 0, ...n.after.map((a) => a.node.round + 1));
    const before = Math.max(n.parent ? chainCost.get(n.parent)! : 0, ...n.after.map((a) => chainCost.get(a.node)!));
    chainCost.set(n, before + n.unit.cost);
    span = Math.max(span, before + n.unit.cost);
  }
  for (const n of order) if (n.parent && n.parent.round === n.round) n.parent.children.push(n);

  // Pieces: whole trees within a round, or subtrees cut from trees too big for one chunk.
  const pieces: Piece[] = [];
  for (const n of order) {
    if (n.parent && n.parent.round === n.round) continue; // not a tree's root in its round
    // A tree that fits is one piece. One that does not is cut into pieces that leave room for a
    // replayed path of up to replayMs (and half the units), so a piece and its path fit a chunk.
    const whole = fits(n, chunkMs, maxUnits);
    const cuts = whole ? [members(n, null)] : cutTree(n, chunkMs - replayMs, Math.max(1, Math.floor(maxUnits / 2)));
    for (const m of cuts) pieces.push(pieceOf(m, replayMs, chunkMs, maxUnits));
  }

  // Packing, per round: pieces that load are chunks of their own; the rest, first-fit decreasing.
  const byRound = new Map<number, Piece[]>();
  for (const p of pieces) byRound.set(p.root.round, [...(byRound.get(p.root.round) ?? []), p]);
  const bins: { round: number; pieces: Piece[]; cost: number; units: number }[] = [];
  for (const round of [...byRound.keys()].sort((a, b) => a - b)) {
    const open: (typeof bins)[number][] = [];
    const sorted = [...byRound.get(round)!].sort((a, b) => b.cost - a.cost || a.root.index - b.root.index);
    for (const p of sorted) {
      const size = p.members.length + p.replay.length;
      if (p.loads) {
        bins.push({ round, pieces: [p], cost: p.cost, units: size });
        continue;
      }
      let bin = open.find((b) => b.cost + p.cost <= chunkMs && b.units + size <= maxUnits);
      if (!bin) {
        bin = { round, pieces: [], cost: 0, units: 0 };
        open.push(bin);
        bins.push(bin);
      }
      bin.pieces.push(p);
      bin.cost += p.cost;
      bin.units += size;
    }
  }
  bins.forEach((bin, id) => bin.pieces.forEach((p) => p.members.forEach((m) => (m.chunk = id))));

  // Each chunk's units, exports and needs.
  const exports = new Map<number, Set<string>>();
  for (const bin of bins) for (const p of bin.pieces) if (p.loads) {
    const parent = p.root.parent!;
    exports.set(parent.chunk, (exports.get(parent.chunk) ?? new Set()).add(parent.hash));
  }
  let replayed = 0;
  const chunks: Chunk[] = bins.map((bin, id) => {
    const planned: PlannedUnit[] = [];
    const needs = new Set<number>();
    const inChunk = new Set<Node>();
    const add = (n: Node, replay: boolean) => {
      const parentHere = n.parent !== null && inChunk.has(n.parent);
      const load = n.parent !== null && !parentHere ? n.parent.hash : null;
      if (load) needs.add(n.parent!.chunk);
      for (const a of n.after) needs.add(a.node.chunk);
      planned.push({ key: n.unit.key, task: n.unit.task, hash: n.hash, input: n.unit.input, parent: parentHere ? n.parent!.unit.key : null, load, after: n.after.map((a) => ({ name: a.name, key: a.node.unit.key, hash: a.node.hash })), replay });
      inChunk.add(n);
    };
    for (const p of bin.pieces) {
      for (const r of p.replay) if (!inChunk.has(r)) {
        add(r, true);
        replayed += r.unit.cost;
      }
      for (const m of p.members) add(m, false);
    }
    needs.delete(id);
    return { id, round: bin.round, cost: bin.cost, units: planned, exports: [...(exports.get(id) ?? [])], needs: [...needs].sort((a, b) => a - b) };
  });
  assertAcyclic(chunks);

  const hashes = new Map(nodes.map((n) => [n.unit.key, n.hash]));
  const hashOf = (node: NodeRef<unknown, unknown>) => {
    const hash = hashes.get(node.key);
    if (!hash) throw new Error(`unit ${node.key} is not in this plan`);
    return hash;
  };
  return { chunks, hashes, hashOf, work: units.reduce((sum, u) => sum + u.cost, 0), span, chunkSpan: chunkSpanOf(chunks), replayed, rounds: Math.max(0, ...nodes.map((n) => n.round)) + 1 };
}

function indexUnits(units: readonly GraphUnit[]): Node[] {
  const byKey = new Map<string, Node>();
  const nodes = units.map((unit, index) => {
    if (byKey.has(unit.key)) throw new Error(`unit ${unit.key} appears twice`);
    if (!(unit.cost >= 0)) throw new Error(`unit ${unit.key} has a negative cost`);
    const node: Node = { unit, index, parent: null, after: [], children: [], hash: "", round: 0, chunk: -1 };
    byKey.set(unit.key, node);
    return node;
  });
  const find = (key: string, from: string) => {
    const n = byKey.get(key);
    if (!n) throw new Error(`unit ${from} refers to ${key}, which is not in the graph`);
    return n;
  };
  for (const n of nodes) {
    if (n.unit.parent !== null) n.parent = find(n.unit.parent, n.unit.key);
    for (const [name, key] of Object.entries(n.unit.after)) n.after.push({ name, node: find(key, n.unit.key) });
  }
  return nodes;
}

/** Every node after its parent and after units, or a throw naming a unit on a cycle. */
function topological(nodes: Node[]): Node[] {
  const waiting = new Map<Node, number>();
  const dependents = new Map<Node, Node[]>();
  for (const n of nodes) {
    const deps = [...(n.parent ? [n.parent] : []), ...n.after.map((a) => a.node)];
    waiting.set(n, deps.length);
    for (const d of deps) dependents.set(d, [...(dependents.get(d) ?? []), n]);
  }
  const ready = nodes.filter((n) => waiting.get(n) === 0);
  const order: Node[] = [];
  while (ready.length) {
    const n = ready.pop()!;
    order.push(n);
    for (const d of dependents.get(n) ?? []) {
      const left = waiting.get(d)! - 1;
      waiting.set(d, left);
      if (left === 0) ready.push(d);
    }
  }
  if (order.length < nodes.length) throw new Error(`the graph has a cycle through ${nodes.find((n) => waiting.get(n)! > 0)!.unit.key}`);
  return order;
}

/**
 * Cuts the tree under `root` (within its round) into pieces that fit: bottom-up, a unit whose
 * remaining subtree is over the limits gives its heaviest remaining child subtree a piece of its
 * own, until what is left fits. Each piece lists its members, parents first.
 */
function cutTree(root: Node, chunkMs: number, maxUnits: number): Node[][] {
  const pieces: Node[][] = [];
  const kept = new Map<Node, Node[]>(); // the children a node keeps in its piece
  const weight = new Map<Node, { cost: number; units: number }>();
  // Iterative post-order: trees may be deep (a chain of a million units).
  const stack: [Node, boolean][] = [[root, false]];
  while (stack.length) {
    const [n, visited] = stack.pop()!;
    if (!visited) {
      stack.push([n, true]);
      for (const c of n.children) stack.push([c, false]);
      continue;
    }
    const children = [...n.children];
    let cost = n.unit.cost + children.reduce((s, c) => s + weight.get(c)!.cost, 0);
    let units = 1 + children.reduce((s, c) => s + weight.get(c)!.units, 0);
    while ((cost > chunkMs || units > maxUnits) && children.length) {
      let heaviest = 0;
      for (let i = 1; i < children.length; i++) if (weight.get(children[i]!)!.cost > weight.get(children[heaviest]!)!.cost) heaviest = i;
      const [cut] = children.splice(heaviest, 1);
      pieces.push(members(cut!, kept));
      cost -= weight.get(cut!)!.cost;
      units -= weight.get(cut!)!.units;
    }
    kept.set(n, children);
    weight.set(n, { cost, units });
  }
  pieces.push(members(root, kept));
  return pieces;
}

/** A piece's members, parents first: the children each keeps, or (kept null) the whole subtree. */
function members(root: Node, kept: Map<Node, Node[]> | null): Node[] {
  const out: Node[] = [];
  const stack = [root];
  while (stack.length) {
    const n = stack.pop()!;
    out.push(n);
    const children = kept ? kept.get(n)! : n.children;
    for (let i = children.length - 1; i >= 0; i--) stack.push(children[i]!);
  }
  return out;
}

/** Whether the tree under `root` fits one chunk as it is. */
function fits(root: Node, chunkMs: number, maxUnits: number): boolean {
  let cost = 0;
  let units = 0;
  const stack = [root];
  while (stack.length) {
    const n = stack.pop()!;
    cost += n.unit.cost;
    units++;
    if (cost > chunkMs || units > maxUnits) return false;
    for (const c of n.children) stack.push(c);
  }
  return true;
}

/**
 * How a piece gets its root's parent value: replaying the path above it, if that costs at most
 * `replayMs` and the piece still fits a chunk, or else loading the value its parent's chunk stores.
 */
function pieceOf(members: Node[], replayMs: number, chunkMs: number, maxUnits: number): Piece {
  const root = members[0]!;
  const own = members.reduce((s, m) => s + m.unit.cost, 0);
  if (!root.parent) return { root, members, cost: own, replay: [], loads: false };
  const path: Node[] = [];
  let pathCost = 0;
  for (let a: Node | null = root.parent; a; a = a.parent) {
    path.push(a);
    pathCost += a.unit.cost;
    if (pathCost > replayMs || own + pathCost > chunkMs || members.length + path.length > maxUnits) return { root, members, cost: own, replay: [], loads: true };
  }
  return { root, members, cost: own + pathCost, replay: path.reverse(), loads: false };
}

/** The most expensive path along chunk needs, in Kahn order (the needs have no cycles). */
function chunkSpanOf(chunks: readonly Chunk[]): number {
  const byId = new Map(chunks.map((c) => [c.id, c]));
  const neededBy = new Map<number, number[]>();
  const left = new Map<number, number>();
  for (const c of chunks) {
    left.set(c.id, c.needs.length);
    for (const n of c.needs) neededBy.set(n, [...(neededBy.get(n) ?? []), c.id]);
  }
  const finish = new Map<number, number>();
  const ready = chunks.filter((c) => c.needs.length === 0).map((c) => c.id);
  let span = 0;
  while (ready.length) {
    const c = byId.get(ready.pop()!)!;
    const end = Math.max(0, ...c.needs.map((n) => finish.get(n)!)) + c.cost;
    finish.set(c.id, end);
    span = Math.max(span, end);
    for (const d of neededBy.get(c.id) ?? []) {
      left.set(d, left.get(d)! - 1);
      if (left.get(d) === 0) ready.push(d);
    }
  }
  return span;
}

function assertAcyclic(chunks: readonly Chunk[]): void {
  const state = new Map<number, "visiting" | "done">();
  for (const start of chunks) {
    const stack: [number, number][] = [[start.id, 0]];
    while (stack.length) {
      const top = stack[stack.length - 1]!;
      const [id, i] = top;
      if (i === 0) {
        if (state.get(id) === "done") {
          stack.pop();
          continue;
        }
        state.set(id, "visiting");
      }
      const needs = chunks[id]!.needs;
      if (i < needs.length) {
        top[1] = i + 1;
        const next = needs[i]!;
        if (state.get(next) === "visiting") throw new Error(`chunks ${id} and ${next} need each other: a planner bug`);
        if (state.get(next) !== "done") stack.push([next, 0]);
      } else {
        state.set(id, "done");
        stack.pop();
      }
    }
  }
}
