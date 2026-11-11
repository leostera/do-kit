import { type ShardedClientOptions } from "../sharded/client.js";
import type { RunStatus, StartOptions, WorkGraphCoordinator } from "./coordinator.js";
import type { NodeRef } from "./graph.js";
import type { WorkGraphPlan } from "./plan.js";
import type { WorkGraphResults } from "./results.js";
export declare class WorkGraphRun {
    private readonly coordinators;
    readonly name: string;
    constructor(coordinators: DurableObjectNamespace<WorkGraphCoordinator<any>>, name: string);
    private get coordinator();
    /**
     * Submits the plan's chunks and seals the run. Submitting again is harmless. Each chunk's body
     * (its units' inputs, mostly) must encode to at most MAX_BODY_BYTES (16 MiB).
     */
    submit(plan: WorkGraphPlan): Promise<RunStatus>;
    /** Starts `runners` runners. */
    start(options: StartOptions): Promise<RunStatus>;
    /**
     * The run's status: kept for `keepStatusMs` after it ends, then null (as for a run never
     * submitted).
     */
    status(): Promise<RunStatus | null>;
    /**
     * Cancels the run if it has not ended, and wipes it: its runners, chunks, status, its own
     * results store (not a shared one), and its coordinator. Idempotent. Waits up to `waitMs`
     * (default 60 s) for its runners to stop, a runner finishing the chunk it is writing first; past
     * that, it returns the status, and the run's alarm finishes the job.
     */
    delete(options?: {
        readonly waitMs?: number;
    }): Promise<RunStatus | null>;
}
/** A run of a work graph, by name: one coordinator object. */
export declare function workGraphRun(coordinators: DurableObjectNamespace<WorkGraphCoordinator<any>>, name: string): WorkGraphRun;
export declare class WorkGraphResultsClient {
    private readonly client;
    constructor(namespace: DurableObjectNamespace<WorkGraphResults<any>>, name: string, options?: ShardedClientOptions);
    /** A node's result, if it is stored. */
    get<R>(plan: WorkGraphPlan, node: NodeRef<R, unknown>): Promise<R | undefined>;
    /** Nodes' results, in order (undefined where none is stored). */
    many<R>(plan: WorkGraphPlan, nodes: readonly NodeRef<R, unknown>[]): Promise<(R | undefined)[]>;
}
/** The results store named `name`: runs that write to it share their results. */
export declare function workGraphResults(namespace: DurableObjectNamespace<WorkGraphResults<any>>, name: string, options?: ShardedClientOptions): WorkGraphResultsClient;
//# sourceMappingURL=client.d.ts.map