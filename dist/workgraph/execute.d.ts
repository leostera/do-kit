import type { AnyTask } from "./graph.js";
import type { PlannedUnit } from "./plan.js";
/** A chunk as the coordinator hands it out. */
export interface ChunkBody {
    readonly units: readonly PlannedUnit[];
    readonly exports: readonly string[];
}
/**
 * Why a unit cannot be computed, the same way on every attempt: its compute threw (a pure unit
 * throws again), its task is not the runner's, or what it returned cannot be stored. The chunk
 * fails with it, and after maxAttempts so does the run.
 */
export declare class ComputeFailed extends Error {
    readonly unit: string;
    constructor(unit: string, cause: unknown);
}
/** A chunk computed: the store's writes, and counts. */
export interface Executed {
    readonly writes: Map<string, string>;
    /** Units of this chunk computed for a result that was not stored. */
    readonly computed: number;
    /** Units of this chunk whose results were already stored. */
    readonly reused: number;
    /** Units computed only for their values: replayed ones, and stored ones above a unit computed. */
    readonly replayed: number;
}
/** The store keys a chunk reads before computing: its results, its exports, its loads and after results. */
export declare function keysToRead(body: ChunkBody): string[];
export declare function executeChunk<Env>(body: ChunkBody, tasks: Readonly<Record<string, AnyTask<Env>>>, stored: ReadonlyMap<string, string>, env: Env): Promise<Executed>;
//# sourceMappingURL=execute.d.ts.map