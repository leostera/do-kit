// Planning a work graph (RFD0005): the graph's units, hashed by their inputs, assigned to rounds
// by their cross edges, and cut into chunks that each fit one invocation. Whole trees are packed
// together; a tree too big for a chunk is cut at subtrees, and each cut either replays the path
// to it or loads the value its parent's chunk stored. Pure: it runs wherever the graph is.
import { createHash } from "node:crypto";
import { jsonWithBinaries } from "./body.js";
/**
 * The hash of a unit: its task and version, input, parent's hash, and its after units' hashes by
 * name. Uint8Arrays in the input are hashed as their bytes.
 */
export function unitHash(task, version, input, parent, after) {
    const named = Object.keys(after)
        .sort()
        .map((name) => [name, after[name]]);
    const binaries = [];
    const hash = createHash("sha256").update(jsonWithBinaries([task, version, input, parent, named], binaries));
    for (const b of binaries)
        hash.update(`\0${b.length}\0`).update(b);
    return hash.digest("hex");
}
/** The hash of a unit identified by its key: its task, version and key. */
export function unitKeyHash(task, version, key) {
    return createHash("sha256").update(JSON.stringify(["key", task, version, key])).digest("hex");
}
/**
 * Plans a graph into chunks. Throws on a duplicate key, a parent or after unit that is not in the
 * graph, or a cycle (a graph built with `add` has none of these).
 */
export function planWorkGraph(graph, options = {}) {
    const units = "units" in graph ? graph.units : graph;
    const chunkMs = options.chunkMs ?? 10_000;
    const maxUnits = options.maxUnitsPerChunk ?? 5_000;
    const replayMs = options.replayMs ?? chunkMs / 4;
    if (!(chunkMs > 0) || !(maxUnits > 1) || !(replayMs >= 0) || !(replayMs < chunkMs))
        throw new Error("chunkMs must be positive, maxUnitsPerChunk over 1, and replayMs at least 0 and under chunkMs");
    const byKey = options.identity === "key";
    if (options.identity !== undefined && options.identity !== "inputs" && !byKey)
        throw new Error('identity must be "inputs" or "key"');
    const nodes = indexUnits(units);
    const order = topological(nodes);
    // Hashes, rounds and the span, parents and after units first.
    const chainCost = new Map();
    let span = 0;
    let rounds = 0;
    for (const n of order) {
        if (n.unit.cost > chunkMs)
            throw new Error(`unit ${n.unit.key} costs ${n.unit.cost}, over chunkMs (${chunkMs}): a unit is never cut, so its chunk could not fit one invocation. Raise chunkMs, or make the unit smaller`);
        n.hash = byKey ? unitKeyHash(n.unit.task, n.unit.version, n.unit.key) : unitHash(n.unit.task, n.unit.version, n.unit.input, n.parent?.hash ?? null, Object.fromEntries(n.after.map((a) => [a.name, a.node.hash])));
        // Loops, not spreads into Math.max: a unit may run after more units than a call takes arguments.
        n.round = n.parent?.round ?? 0;
        let before = n.parent ? chainCost.get(n.parent) : 0;
        for (const a of n.after) {
            n.round = Math.max(n.round, a.node.round + 1);
            before = Math.max(before, chainCost.get(a.node));
        }
        rounds = Math.max(rounds, n.round + 1);
        chainCost.set(n, before + n.unit.cost);
        span = Math.max(span, before + n.unit.cost);
    }
    for (const n of order)
        if (n.parent && n.parent.round === n.round)
            n.parent.children.push(n);
    // Pieces: whole trees within a round, or subtrees cut from trees too big for one chunk.
    const pieces = [];
    for (const n of order) {
        if (n.parent && n.parent.round === n.round)
            continue; // not a tree's root in its round
        // A tree that fits is one piece. One that does not is cut into pieces that leave room for a
        // replayed path of up to replayMs (and half the units), so a piece and its path fit a chunk.
        const whole = fits(n, chunkMs, maxUnits);
        const cuts = whole ? [members(n, null)] : cutTree(n, chunkMs - replayMs, Math.max(1, Math.floor(maxUnits / 2)));
        for (const m of cuts)
            pieces.push(pieceOf(m, replayMs, chunkMs, maxUnits));
    }
    // Packing, per round: pieces that load are chunks of their own; the rest, first-fit decreasing.
    const byRound = new Map();
    for (const p of pieces) {
        const list = byRound.get(p.root.round);
        if (list)
            list.push(p);
        else
            byRound.set(p.root.round, [p]);
    }
    const bins = [];
    for (const round of [...byRound.keys()].sort((a, b) => a - b)) {
        const open = [];
        const sorted = [...byRound.get(round)].sort((a, b) => b.cost - a.cost || a.root.index - b.root.index);
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
    const exports = new Map();
    for (const bin of bins)
        for (const p of bin.pieces)
            if (p.loads) {
                const parent = p.root.parent;
                exports.set(parent.chunk, (exports.get(parent.chunk) ?? new Set()).add(parent.hash));
            }
    let replayed = 0;
    const chunks = bins.map((bin, id) => {
        const planned = [];
        const needs = new Set();
        const inChunk = new Set();
        const add = (n, replay) => {
            const parentHere = n.parent !== null && inChunk.has(n.parent);
            const load = n.parent !== null && !parentHere ? n.parent.hash : null;
            if (load)
                needs.add(n.parent.chunk);
            for (const a of n.after)
                needs.add(a.node.chunk);
            planned.push({ key: n.unit.key, task: n.unit.task, hash: n.hash, input: n.unit.input, parent: parentHere ? n.parent.unit.key : null, load, after: n.after.map((a) => ({ name: a.name, key: a.node.unit.key, hash: a.node.hash })), replay });
            inChunk.add(n);
        };
        for (const p of bin.pieces) {
            for (const r of p.replay)
                if (!inChunk.has(r)) {
                    add(r, true);
                    replayed += r.unit.cost;
                }
            for (const m of p.members)
                add(m, false);
        }
        needs.delete(id);
        return { id, round: bin.round, cost: bin.cost, units: planned, exports: [...(exports.get(id) ?? [])], needs: [...needs].sort((a, b) => a - b) };
    });
    assertAcyclic(chunks);
    const hashes = new Map(nodes.map((n) => [n.unit.key, n.hash]));
    const hashOf = (node) => {
        const hash = hashes.get(node.key);
        if (!hash)
            throw new Error(`unit ${node.key} is not in this plan`);
        return hash;
    };
    return { chunks, hashes, hashOf, work: units.reduce((sum, u) => sum + u.cost, 0), span, chunkSpan: chunkSpanOf(chunks), replayed, rounds: Math.max(1, rounds) };
}
function indexUnits(units) {
    const byKey = new Map();
    const nodes = units.map((unit, index) => {
        if (byKey.has(unit.key))
            throw new Error(`unit ${unit.key} appears twice`);
        if (!(unit.cost >= 0))
            throw new Error(`unit ${unit.key} has a negative cost`);
        const node = { unit, index, parent: null, after: [], children: [], hash: "", round: 0, chunk: -1 };
        byKey.set(unit.key, node);
        return node;
    });
    const find = (key, from) => {
        const n = byKey.get(key);
        if (!n)
            throw new Error(`unit ${from} refers to ${key}, which is not in the graph`);
        return n;
    };
    for (const n of nodes) {
        if (n.unit.parent !== null)
            n.parent = find(n.unit.parent, n.unit.key);
        for (const [name, key] of Object.entries(n.unit.after))
            n.after.push({ name, node: find(key, n.unit.key) });
    }
    return nodes;
}
/** Every node after its parent and after units, or a throw naming a unit on a cycle. */
function topological(nodes) {
    const waiting = new Map();
    const dependents = new Map();
    for (const n of nodes) {
        const deps = [...(n.parent ? [n.parent] : []), ...n.after.map((a) => a.node)];
        waiting.set(n, deps.length);
        for (const d of deps) {
            const list = dependents.get(d);
            if (list)
                list.push(n);
            else
                dependents.set(d, [n]);
        }
    }
    const ready = nodes.filter((n) => waiting.get(n) === 0);
    const order = [];
    while (ready.length) {
        const n = ready.pop();
        order.push(n);
        for (const d of dependents.get(n) ?? []) {
            const left = waiting.get(d) - 1;
            waiting.set(d, left);
            if (left === 0)
                ready.push(d);
        }
    }
    if (order.length < nodes.length)
        throw new Error(`the graph has a cycle through ${nodes.find((n) => waiting.get(n) > 0).unit.key}`);
    return order;
}
/**
 * Cuts the tree under `root` (within its round) into pieces that fit: bottom-up, a unit whose
 * remaining subtree is over the limits gives its heaviest remaining child subtree a piece of its
 * own, until what is left fits. Each piece lists its members, parents first.
 */
function cutTree(root, chunkMs, maxUnits) {
    const pieces = [];
    const kept = new Map(); // the children a node keeps in its piece
    const weight = new Map();
    // Iterative post-order: trees may be deep (a chain of a million units).
    const stack = [[root, false]];
    while (stack.length) {
        const [n, visited] = stack.pop();
        if (!visited) {
            stack.push([n, true]);
            for (const c of n.children)
                stack.push([c, false]);
            continue;
        }
        let cost = n.unit.cost + n.children.reduce((s, c) => s + weight.get(c).cost, 0);
        let units = 1 + n.children.reduce((s, c) => s + weight.get(c).units, 0);
        let children = n.children;
        if (cost > chunkMs || units > maxUnits) {
            // Heaviest first (the first of equals), sorted once: a unit may have very many children.
            const byWeight = [...n.children].sort((a, b) => weight.get(b).cost - weight.get(a).cost);
            const cut = new Set();
            for (const c of byWeight) {
                if (cost <= chunkMs && units <= maxUnits)
                    break;
                cut.add(c);
                pieces.push(members(c, kept));
                cost -= weight.get(c).cost;
                units -= weight.get(c).units;
            }
            children = n.children.filter((c) => !cut.has(c));
        }
        kept.set(n, children);
        weight.set(n, { cost, units });
    }
    pieces.push(members(root, kept));
    return pieces;
}
/** A piece's members, parents first: the children each keeps, or (kept null) the whole subtree. */
function members(root, kept) {
    const out = [];
    const stack = [root];
    while (stack.length) {
        const n = stack.pop();
        out.push(n);
        const children = kept ? kept.get(n) : n.children;
        for (let i = children.length - 1; i >= 0; i--)
            stack.push(children[i]);
    }
    return out;
}
/** Whether the tree under `root` fits one chunk as it is. */
function fits(root, chunkMs, maxUnits) {
    let cost = 0;
    let units = 0;
    const stack = [root];
    while (stack.length) {
        const n = stack.pop();
        cost += n.unit.cost;
        units++;
        if (cost > chunkMs || units > maxUnits)
            return false;
        for (const c of n.children)
            stack.push(c);
    }
    return true;
}
/**
 * How a piece gets its root's parent value: replaying the path above it, if that costs at most
 * `replayMs` and the piece still fits a chunk, or else loading the value its parent's chunk stores.
 */
function pieceOf(members, replayMs, chunkMs, maxUnits) {
    const root = members[0];
    const own = members.reduce((s, m) => s + m.unit.cost, 0);
    if (!root.parent)
        return { root, members, cost: own, replay: [], loads: false };
    const path = [];
    let pathCost = 0;
    for (let a = root.parent; a; a = a.parent) {
        path.push(a);
        pathCost += a.unit.cost;
        if (pathCost > replayMs || own + pathCost > chunkMs || members.length + path.length > maxUnits)
            return { root, members, cost: own, replay: [], loads: true };
    }
    return { root, members, cost: own + pathCost, replay: path.reverse(), loads: false };
}
/** The most expensive path along chunk needs, in Kahn order (the needs have no cycles). */
function chunkSpanOf(chunks) {
    const byId = new Map(chunks.map((c) => [c.id, c]));
    const neededBy = new Map();
    const left = new Map();
    for (const c of chunks) {
        left.set(c.id, c.needs.length);
        for (const n of c.needs) {
            const list = neededBy.get(n);
            if (list)
                list.push(c.id);
            else
                neededBy.set(n, [c.id]);
        }
    }
    const finish = new Map();
    const ready = chunks.filter((c) => c.needs.length === 0).map((c) => c.id);
    let span = 0;
    while (ready.length) {
        const c = byId.get(ready.pop());
        let end = 0;
        for (const n of c.needs)
            end = Math.max(end, finish.get(n));
        end += c.cost;
        finish.set(c.id, end);
        span = Math.max(span, end);
        for (const d of neededBy.get(c.id) ?? []) {
            left.set(d, left.get(d) - 1);
            if (left.get(d) === 0)
                ready.push(d);
        }
    }
    return span;
}
function assertAcyclic(chunks) {
    const state = new Map();
    for (const start of chunks) {
        const stack = [[start.id, 0]];
        while (stack.length) {
            const top = stack[stack.length - 1];
            const [id, i] = top;
            if (i === 0) {
                if (state.get(id) === "done") {
                    stack.pop();
                    continue;
                }
                state.set(id, "visiting");
            }
            const needs = chunks[id].needs;
            if (i < needs.length) {
                top[1] = i + 1;
                const next = needs[i];
                if (state.get(next) === "visiting")
                    throw new Error(`chunks ${id} and ${next} need each other: a planner bug`);
                if (state.get(next) !== "done")
                    stack.push([next, 0]);
            }
            else {
                state.set(id, "done");
                stack.pop();
            }
        }
    }
}
//# sourceMappingURL=plan.js.map