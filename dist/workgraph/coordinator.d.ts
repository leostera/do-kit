import { DurableObject } from "cloudflare:workers";
import type { Chunk } from "./plan.js";
import type { WorkGraphRunner } from "./runner.js";
/** A finished chunk, as its runner reports it. */
export type Finished = {
    readonly chunk: number;
    readonly attempt: number;
    readonly computed: number;
    readonly reused: number;
    readonly replayed: number;
    readonly wallMs: number;
} | {
    readonly chunk: number;
    readonly attempt: number;
    readonly error: string;
};
/** What a runner is told to do next. */
export type NextAnswer = {
    readonly chunk: number;
    readonly attempt: number;
    readonly body: string;
} | {
    readonly wait: number;
} | {
    readonly done: true;
} | {
    readonly failed: string;
};
export interface StartOptions {
    /** How many runners pull chunks. */
    readonly runners: number;
    /** The name of the results store (a WorkGraphResults object): runs sharing it reuse results. */
    readonly results: string;
    /** How long a lease lasts before the chunk goes back to ready (default 120 s). */
    readonly leaseMs?: number;
    /** A chunk whose compute fails this many times fails the run (default 3). */
    readonly maxAttempts?: number;
    /** A runner that has not called for this long is woken while work remains (default 30 s). */
    readonly wakeAfterMs?: number;
    /** How often the alarm checks leases and runners (default 5 s). */
    readonly tickMs?: number;
    /** Log one JSON line, `graph-chunk`, per chunk completed (off by default). */
    readonly logChunks?: boolean;
}
export type RunState = "submitting" | "ready" | "running" | "done" | "failed";
export interface RunStatus {
    readonly name: string;
    readonly state: RunState;
    readonly runners: number;
    readonly chunks: {
        readonly total: number;
        readonly waiting: number;
        readonly ready: number;
        readonly leased: number;
        readonly done: number;
    };
    readonly units: {
        readonly computed: number;
        readonly reused: number;
        readonly replayed: number;
    };
    /** Leases that ran out and went back to ready. */
    readonly expired: number;
    /** Runners woken after going quiet. */
    readonly woken: number;
    /** Chunk attempts that failed in compute. */
    readonly failures: number;
    readonly error: string | null;
    readonly startedAt: number | null;
    readonly finishedAt: number | null;
    readonly elapsedMs: number | null;
}
/** The object name of runner `index` of a run. */
export declare const runnerName: (run: string, index: number) => string;
/**
 * A run's coordinator. A subclass gives `runners()`, the binding of its runner class. Name each
 * coordinator object by its run.
 */
export declare abstract class WorkGraphCoordinator<Env = unknown> extends DurableObject<Env> {
    private readonly sql;
    private meta;
    protected abstract runners(): DurableObjectNamespace<WorkGraphRunner<any>>;
    constructor(ctx: DurableObjectState, env: Env);
    /** Adds chunks to a run being submitted (idempotent per chunk id). */
    graphSubmit(name: string, chunks: readonly Chunk[]): Promise<void>;
    /** Ends submission: every needed chunk must exist. Chunks that need nothing are ready. */
    graphSeal(name: string): Promise<RunStatus>;
    /** Starts the runners, and the alarm that watches them. */
    graphStart(name: string, options: StartOptions): Promise<RunStatus>;
    /** Records what runner `runner` finished, if anything, and tells it what to do next. */
    graphNext(runner: number, finished: Finished | null): Promise<NextAnswer>;
    graphStatus(): Promise<RunStatus | null>;
    /** The runner's own lease back (it was evicted while computing it), or the next ready chunk. */
    private lease;
    /**
     * Marks a chunk done, from whichever runner finished it first (a later finish of the same chunk
     * wrote the same results), and readies the chunks that needed only it.
     */
    private complete;
    /** A chunk whose compute threw: ready again, or the run fails once it has failed maxAttempts times. */
    private fail;
    private finishIfDone;
    /** Leases that ran out go back to ready; quiet runners are woken; the alarm stops once the run ends. */
    alarm(): Promise<void>;
    private waitingRunners;
    /** Wakes the runners told to wait, so a chunk that just became ready does not wait for their poll. */
    private wakeWaiting;
    private runner;
    private metaFor;
    private save;
    private status;
}
//# sourceMappingURL=coordinator.d.ts.map