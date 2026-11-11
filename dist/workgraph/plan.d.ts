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
    readonly after: readonly {
        readonly name: string;
        readonly key: string;
        readonly hash: string;
    }[];
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
export declare function unitHash(task: string, version: string, input: unknown, parent: string | null, after: Readonly<Record<string, string>>): string;
/** The hash of a unit identified by its key: its task, version and key. */
export declare function unitKeyHash(task: string, version: string, key: string): string;
/**
 * Plans a graph into chunks. Throws on a duplicate key, a parent or after unit that is not in the
 * graph, or a cycle (a graph built with `add` has none of these).
 */
export declare function planWorkGraph(graph: WorkGraph<Record<string, AnyTask>> | readonly GraphUnit[], options?: PlanOptions): WorkGraphPlan;
//# sourceMappingURL=plan.d.ts.map