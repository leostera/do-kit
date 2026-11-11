// Work graphs in the test Worker (RFD0005): a coordinator, a runner with the fixture's tasks, and a
// results store. Runs named with "evict" throw after computing each chunk once (as an eviction
// would, before the results are written); in runs named with "lose", runner 0 abandons the first
// chunk it leases and never runs again.
import { encodeBody } from "../../../src/workgraph/body.ts";
import { planWorkGraph } from "../../../src/workgraph/plan.ts";
import { WorkGraphCoordinator } from "../../../src/workgraph/coordinator.ts";
import { WorkGraphRunner } from "../../../src/workgraph/runner.ts";
import { WorkGraphResults } from "../../../src/workgraph/results.ts";
import { workGraphResults, workGraphRun } from "../../../src/workgraph/client.ts";
import { forest, slices, TASKS, type Shape, type Slices } from "../../workgraph-fixture.ts";

export interface GraphEnv {
  readonly GRAPH_COORDINATORS: DurableObjectNamespace<GraphCoordinators>;
  readonly GRAPH_RUNNERS: DurableObjectNamespace<GraphRunners>;
  readonly GRAPH_RESULTS: DurableObjectNamespace<GraphResults>;
}

export class GraphCoordinators extends WorkGraphCoordinator<GraphEnv> {
  protected runners() {
    return this.env.GRAPH_RUNNERS;
  }
}

export class GraphRunners extends WorkGraphRunner<GraphEnv> {
  protected tasks() {
    return TASKS;
  }
  protected coordinators() {
    return this.env.GRAPH_COORDINATORS;
  }
  protected results() {
    return this.env.GRAPH_RESULTS;
  }
  protected override async chunkStarting(run: string, index: number): Promise<"run" | "abandon"> {
    return run.includes("lose") && index === 0 ? "abandon" : "run";
  }
  protected override async chunkComputed(run: string, _index: number, chunk: number): Promise<void> {
    if (!run.includes("evict")) return;
    const key = `evicted:${run}:${chunk}`;
    if (await this.ctx.storage.get(key)) return;
    await this.ctx.storage.put(key, true);
    throw new Error("simulated eviction");
  }
}

export class GraphResults extends WorkGraphResults<GraphEnv> {
  protected nodes() {
    return this.env.GRAPH_RESULTS;
  }
}

export interface GraphRequest {
  op: "start" | "status" | "results";
  run: string;
  results: string;
  shape?: Shape;
  /** Instead of a forest: slices of bytes. */
  slices?: Slices;
  identity?: "inputs" | "key";
  runners?: number;
  chunkMs?: number;
  replayMs?: number;
  leaseMs?: number;
  tickMs?: number;
  wakeAfterMs?: number;
}

export async function graphRoute(env: GraphEnv, body: GraphRequest): Promise<unknown> {
  const run = workGraphRun(env.GRAPH_COORDINATORS, body.run);
  if (body.op === "status") return run.status();
  const { g } = body.slices ? slices(body.slices) : forest(body.shape!);
  const plan = planWorkGraph(g, { ...(body.chunkMs ? { chunkMs: body.chunkMs } : {}), ...(body.replayMs !== undefined ? { replayMs: body.replayMs } : {}), ...(body.identity ? { identity: body.identity } : {}) });
  if (body.op === "results") {
    const keys = g.units.map((u) => u.key);
    const values = await workGraphResults(env.GRAPH_RESULTS, body.results).many(plan, keys.map((key) => ({ key, task: "" })));
    return Object.fromEntries(keys.map((k, i) => [k, values[i] ?? null]));
  }
  await run.submit(plan);
  const status = await run.start({ runners: body.runners ?? 4, results: body.results, ...(body.leaseMs ? { leaseMs: body.leaseMs } : {}), ...(body.tickMs ? { tickMs: body.tickMs } : {}), ...(body.wakeAfterMs ? { wakeAfterMs: body.wakeAfterMs } : {}) });
  return { chunks: plan.chunks.length, work: plan.work, span: plan.span, units: g.units.length, bodyBytes: plan.chunks.map((c) => encodeBody(c).byteLength), status };
}
