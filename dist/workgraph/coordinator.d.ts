import { DurableObject } from "cloudflare:workers";
import type { WorkGraphResults } from "./results.js";
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
    readonly body: Uint8Array;
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
    /**
     * The name of a results store (a WorkGraphResults object) that runs share, to reuse results:
     * the app's to keep and to wipe. Without it, the run gets a store of its own, named
     * `<run>/results`, which is wiped with the run's status.
     */
    readonly results?: string;
    /** The run fails once this long has passed since it started, and is wiped (default 24 h). */
    readonly deadlineMs?: number;
    /** How long the run's status (and its own results store) is kept once it ends (default 1 h). */
    readonly keepStatusMs?: number;
    /** How long a lease lasts before the chunk goes back to ready (default 120 s). */
    readonly leaseMs?: number;
    /** A chunk whose compute fails this many times fails the run (default 3). */
    readonly maxAttempts?: number;
    /**
     * A chunk whose lease runs out after it was leased this many times fails the run (default 5): its
     * runners never report, as when the chunk needs more CPU than an invocation has.
     */
    readonly maxLeases?: number;
    /** A runner that has not called for this long is woken while work remains (default 30 s). */
    readonly wakeAfterMs?: number;
    /** How often the alarm checks leases and runners (default 5 s). */
    readonly tickMs?: number;
    /** Log one JSON line, `graph-chunk`, per chunk completed (off by default). */
    readonly logChunks?: boolean;
}
export type RunState = "submitting" | "ready" | "running" | "done" | "failed" | "cancelled";
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
    /**
     * The runners' time on the chunks done: each chunk's wall time, from lease to results written, on
     * its first completion. Over `elapsedMs`, how many runners were busy on average.
     */
    readonly busyMs: number;
    /** The run's results store, and whether it is the run's own (wiped with it). */
    readonly results: string;
    readonly ownResults: boolean;
    /**
     * After the run ends: "stopping" while its runners are told to stop, then "kept" while only
     * this status is kept, until `keptUntil`; after that, there is no run (status is null).
     */
    readonly cleanup: "stopping" | "kept" | null;
    readonly keptUntil: number | null;
    readonly deadlineAt: number | null;
}
/** A chunk as submitted: its body encoded (encodeBody). */
export interface SubmittedChunk {
    readonly id: number;
    readonly round: number;
    readonly cost: number;
    readonly needs: readonly number[];
    readonly body: Uint8Array;
}
/** The object name of runner `index` of a run. */
export declare const runnerName: (run: string, index: number) => string;
/** The name of a run's own results store. */
export declare const ownResultsName: (run: string) => string;
/**
 * A run's coordinator. A subclass gives `runners()` and `results()`, the bindings of its runner
 * and results classes. Name each coordinator object by its run.
 */
export declare abstract class WorkGraphCoordinator<Env = unknown> extends DurableObject<Env> {
    private readonly sql;
    private meta;
    private cleaning;
    protected abstract runners(): DurableObjectNamespace<WorkGraphRunner<any>>;
    protected abstract results(): DurableObjectNamespace<WorkGraphResults<any>>;
    constructor(ctx: DurableObjectState, env: Env);
    /** Adds chunks to a run being submitted (idempotent per chunk id, and once sealed, a no-op). */
    graphSubmit(name: string, chunks: readonly SubmittedChunk[]): Promise<void>;
    /** Ends submission: every needed chunk must exist. Chunks that need nothing are ready. */
    graphSeal(name: string): Promise<RunStatus>;
    /** Starts the runners, and the alarm that watches them. */
    graphStart(name: string, options: StartOptions): Promise<RunStatus | null>;
    /**
     * Cancels the run if it has not ended, stops its runners, and wipes it: its chunks, its status,
     * its own results store, and this object. Idempotent. Null once nothing is left; a status while
     * a runner has not stopped yet, in which case the alarm goes on (call again to wait).
     */
    graphDelete(name: string): Promise<RunStatus | null>;
    /** Records what runner `runner` finished, if anything, and tells it what to do next. */
    graphNext(runner: number, finished: Finished | null): Promise<NextAnswer>;
    graphStatus(): Promise<RunStatus | null>;
    /** A chunk's body, from its parts. */
    private body;
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
    private ended;
    /** The run ends: what its chunks were is kept for its status, and its cleanup begins. */
    private end;
    /**
     * One step of a run's cleanup. Stopping: tells every runner that has not said so to stop (each
     * wipes itself); once all have, drops the chunks and keeps the status. Kept: once its time is up
     * (or on delete), wipes the run's own results store, then this object. Re-arms the alarm for
     * whatever is left.
     */
    private cleanUp;
    private cleanUpOnce;
    /** The run's own results store, then this object: afterwards it holds nothing, and no alarm. */
    private wipe;
    /** Leases that ran out go back to ready, quiet runners are woken, and the deadline is kept; once the run ends, its cleanup. */
    alarm(): Promise<void>;
    /** Up to `count` runners told to wait, no longer marked waiting: one for each chunk there is to take. */
    private takeWaiting;
    /**
     * Wakes as many waiting runners as chunks just became ready, so a ready chunk does not wait for
     * a poll, and a runner with nothing to take is not woken for it. (The others poll.)
     */
    private wakeWaiting;
    private resultsStore;
    private runner;
    /** The run named `name`, if this object has one. */
    private existing;
    /** The run named `name`, made by its first submission. */
    private metaFor;
    private save;
    private liveCounts;
    private status;
}
//# sourceMappingURL=coordinator.d.ts.map