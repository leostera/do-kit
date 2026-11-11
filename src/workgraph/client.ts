// The Worker side of work graphs (RFD0005): submitting a plan as a run, starting its runners,
// watching it, and reading results, typed by the graph's nodes.
import { sharded, type ShardedClientOptions } from "../sharded/client.js";
import type { RunStatus, StartOptions, WorkGraphCoordinator } from "./coordinator.js";
import type { NodeRef } from "./graph.js";
import type { WorkGraphPlan } from "./plan.js";
import type { WorkGraphResults } from "./results.js";
import { decodeStored, resultKey } from "./stored.js";

/** Chunk bytes per submit call (RPC arguments are limited to 32 MiB). */
const SUBMIT_BATCH_BYTES = 8 * 1024 * 1024;

export class WorkGraphRun {
  constructor(
    private readonly coordinators: DurableObjectNamespace<WorkGraphCoordinator<any>>,
    readonly name: string,
  ) {}

  private get coordinator() {
    return this.coordinators.get(this.coordinators.idFromName(this.name));
  }

  /** Submits the plan's chunks and seals the run. Submitting again is harmless. */
  async submit(plan: WorkGraphPlan): Promise<RunStatus> {
    let batch: (typeof plan.chunks)[number][] = [];
    let bytes = 0;
    for (const chunk of plan.chunks) {
      const size = JSON.stringify(chunk).length;
      if (batch.length > 0 && bytes + size > SUBMIT_BATCH_BYTES) {
        await this.coordinator.graphSubmit(this.name, batch);
        batch = [];
        bytes = 0;
      }
      batch.push(chunk);
      bytes += size;
    }
    if (batch.length > 0) await this.coordinator.graphSubmit(this.name, batch);
    return (await this.coordinator.graphSeal(this.name)) as RunStatus;
  }

  /** Starts `runners` runners. */
  async start(options: StartOptions): Promise<RunStatus> {
    return (await this.coordinator.graphStart(this.name, options)) as RunStatus;
  }

  async status(): Promise<RunStatus | null> {
    return (await this.coordinator.graphStatus()) as RunStatus | null;
  }
}

/** A run of a work graph, by name: one coordinator object. */
export function workGraphRun(coordinators: DurableObjectNamespace<WorkGraphCoordinator<any>>, name: string): WorkGraphRun {
  return new WorkGraphRun(coordinators, name);
}

export class WorkGraphResultsClient {
  private readonly client;

  constructor(namespace: DurableObjectNamespace<WorkGraphResults<any>>, name: string, options: ShardedClientOptions = {}) {
    this.client = sharded(namespace, name, options);
  }

  /** A node's result, if it is stored. */
  async get<R>(plan: WorkGraphPlan, node: NodeRef<R, unknown>): Promise<R | undefined> {
    return (await this.many(plan, [node]))[0];
  }

  /** Nodes' results, in order (undefined where none is stored). */
  async many<R>(plan: WorkGraphPlan, nodes: readonly NodeRef<R, unknown>[]): Promise<(R | undefined)[]> {
    const keys = nodes.map((n) => resultKey(plan.hashOf(n)));
    const out: (R | undefined)[] = new Array(nodes.length).fill(undefined);
    for (const shard of await this.client.call(keys, "graphGet", null)) shard.positions.forEach((p, j) => shard.result[j] !== null && (out[p] = decodeStored<R>(shard.result[j]!)));
    return out;
  }
}

/** The results store named `name`: runs that write to it share their results. */
export function workGraphResults(namespace: DurableObjectNamespace<WorkGraphResults<any>>, name: string, options: ShardedClientOptions = {}): WorkGraphResultsClient {
  return new WorkGraphResultsClient(namespace, name, options);
}
