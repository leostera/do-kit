import { DurableObject } from "cloudflare:workers";
import type { WorkGraphCoordinator } from "./coordinator.js";
import type { AnyTask } from "./graph.js";
import type { WorkGraphResults } from "./results.js";
/**
 * A runner. A subclass gives its tasks (every task its runs' graphs use, by name) and the bindings
 * of the coordinator and results classes.
 */
export declare abstract class WorkGraphRunner<Env = unknown> extends DurableObject<Env> {
    protected abstract tasks(): Readonly<Record<string, AnyTask<Env>>>;
    protected abstract coordinators(): DurableObjectNamespace<WorkGraphCoordinator<any>>;
    protected abstract results(): DurableObjectNamespace<WorkGraphResults<any>>;
    /**
     * How long one alarm invocation goes on taking chunks. Chunks that are small next to it run one
     * after another in the same invocation; a chunk that costs more than what is left of it runs
     * in the next. Keep it well under the CPU an invocation has, less a chunk.
     */
    protected invocationMs: number;
    /** Told to stop (in memory: a stop also wipes storage, so a later instance finds nothing). */
    private stopping;
    /** The alarm's work in progress, which a stop waits for. */
    private inflight;
    /** Before computing a chunk: "abandon" stops this runner holding its lease (tests use it). */
    protected chunkStarting(_run: string, _index: number, _chunk: number, _attempt: number): Promise<"run" | "abandon">;
    /** After computing a chunk, before writing its results (tests throw here, as an eviction would). */
    protected chunkComputed(_run: string, _index: number, _chunk: number, _attempt: number): Promise<void>;
    /**
     * Starts pulling chunks of run `run` as runner `index`, writing results to the store `results`.
     * With `ensure` (the coordinator's alarm, for a runner gone quiet): starts it only if its start
     * never reached it, and otherwise wakes it, keeping the chunk it has to report.
     */
    graphRun(run: string, index: number, results: string, ensure?: boolean): Promise<void>;
    /**
     * Stops for good: waits for the chunk in hand to stop (it writes nothing more), then deletes this
     * runner's storage and alarm. Idempotent, and it makes nothing on a runner that holds nothing.
     */
    graphStop(): Promise<void>;
    /** Asks again now: a chunk may be ready, or this runner's alarm retries ran out. */
    graphWake(): Promise<void>;
    alarm(): Promise<void>;
    private wipe;
    /** Throws Stopping once the runner is told to stop: called before each write. */
    private live;
    private step;
    /** The run's coordinator, through one stub for as long as its calls succeed. */
    private coordinator;
    private coordinatorStub;
    /** The results store's client, kept: it keeps its stubs of the store's shards. */
    private resultsStore;
    private store;
    /**
     * Computes a chunk and makes its results durable; the report is recorded only after. A compute
     * that throws is reported as the chunk's failure. Anything else throws, and the alarm is retried.
     */
    private runChunk;
}
//# sourceMappingURL=runner.d.ts.map