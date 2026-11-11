// Work graphs in the test Worker (RFD0005): a coordinator, a runner with the fixture's tasks, and a
// results store. Runs named with "evict" throw after computing each chunk once (as an eviction
// would, before the results are written); in runs named with "lose", runner 0 abandons the first
// chunk it leases and never runs again.
import { encodeBody } from "../../../src/workgraph/body.ts";
import { planWorkGraph } from "../../../src/workgraph/plan.ts";
import { checkSplitPolicy, DEFAULT_SPLIT_POLICY, shardObjectName } from "../../../src/sharded/index.ts";
import { ownResultsName, runnerName, WorkGraphCoordinator } from "../../../src/workgraph/coordinator.ts";
import { storageReport, type StorageReport } from "./storage-report.ts";
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
  protected results() {
    return this.env.GRAPH_RESULTS;
  }
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }
  /** Ends this instance: the next call makes a new one, which runs the constructor again. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
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
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }
  /** Ends this instance: the next call makes a new one, which runs the constructor again. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
  protected override async chunkStarting(run: string, index: number): Promise<"run" | "abandon"> {
    // "poison": every runner dies holding its lease, as on a chunk that needs more CPU than an invocation has.
    if (run.includes("poison")) throw new Error("test: the runner died on this chunk");
    return run.includes("lose") && index === 0 ? "abandon" : "run";
  }
  /** "nostart": the start's own call never reaches the runners; only the coordinator's alarm does. */
  override async graphRun(run: string, index: number, results: string, ensure = false): Promise<void> {
    if (run.includes("nostart") && !ensure) throw new Error("test: the start did not reach this runner");
    return super.graphRun(run, index, results, ensure);
  }
  protected override async chunkComputed(run: string, _index: number, chunk: number): Promise<void> {
    if (!run.includes("evict")) return;
    const key = `evicted:${run}:${chunk}`;
    if (await this.ctx.storage.get(key)) return;
    await this.ctx.storage.put(key, true);
    throw new Error("simulated eviction");
  }
}

/** Presplit into four shards, so a run's own store is several objects to wipe. */
export class GraphResults extends WorkGraphResults<GraphEnv> {
  protected override splitPolicy = checkSplitPolicy({ ...DEFAULT_SPLIT_POLICY, initialDepth: 2 });
  protected nodes() {
    return this.env.GRAPH_RESULTS;
  }
  async storage(): Promise<StorageReport> {
    return storageReport(this.ctx.storage);
  }
  /** Ends this instance: the next call makes a new one, which runs the constructor again. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
}

/** The objects a results store's four shards live in. */
const RESULT_SHARDS = ["", "01", "10", "11"];

export interface GraphRequest {
  op: "start" | "status" | "results" | "delete" | "storage" | "restart";
  run: string;
  /** A shared results store; without it, the run's own. */
  results?: string;
  deadlineMs?: number;
  keepStatusMs?: number;
  shape?: Shape;
  /** Instead of a forest: slices of bytes. */
  slices?: Slices;
  identity?: "inputs" | "key";
  runners?: number;
  chunkMs?: number;
  replayMs?: number;
  leaseMs?: number;
  maxLeases?: number;
  tickMs?: number;
  wakeAfterMs?: number;
}

export async function graphRoute(env: GraphEnv, body: GraphRequest): Promise<unknown> {
  const run = workGraphRun(env.GRAPH_COORDINATORS, body.run);
  const resultsName = body.results ?? ownResultsName(body.run);
  if (body.op === "status") return run.status();
  if (body.op === "delete") return run.delete();
  if (body.op === "restart") {
    const stubs = [
      env.GRAPH_COORDINATORS.get(env.GRAPH_COORDINATORS.idFromName(body.run)),
      ...Array.from({ length: body.runners ?? 4 }, (_, i) => env.GRAPH_RUNNERS.get(env.GRAPH_RUNNERS.idFromName(runnerName(body.run, i)))),
      ...RESULT_SHARDS.map((bits) => env.GRAPH_RESULTS.get(env.GRAPH_RESULTS.idFromName(shardObjectName(resultsName, bits)))),
    ];
    await Promise.allSettled(stubs.map((stub) => stub.restart()));
    return null;
  }
  if (body.op === "storage") {
    // Everything the run may have touched: its coordinator, each runner, each results shard.
    const coordinator = await env.GRAPH_COORDINATORS.get(env.GRAPH_COORDINATORS.idFromName(body.run)).storage();
    const runners = await Promise.all(Array.from({ length: body.runners ?? 4 }, (_, i) => env.GRAPH_RUNNERS.get(env.GRAPH_RUNNERS.idFromName(runnerName(body.run, i))).storage()));
    const results = await Promise.all(RESULT_SHARDS.map((bits) => env.GRAPH_RESULTS.get(env.GRAPH_RESULTS.idFromName(shardObjectName(resultsName, bits))).storage()));
    return { coordinator, runners, results };
  }
  const { g } = body.slices ? slices(body.slices) : forest(body.shape!);
  const plan = planWorkGraph(g, { ...(body.chunkMs ? { chunkMs: body.chunkMs } : {}), ...(body.replayMs !== undefined ? { replayMs: body.replayMs } : {}), ...(body.identity ? { identity: body.identity } : {}) });
  if (body.op === "results") {
    const keys = g.units.map((u) => u.key);
    const values = await workGraphResults(env.GRAPH_RESULTS, resultsName).many(plan, keys.map((key) => ({ key, task: "" })));
    return Object.fromEntries(keys.map((k, i) => [k, values[i] ?? null]));
  }
  await run.submit(plan);
  const status = await run.start({ runners: body.runners ?? 4, ...(body.results ? { results: body.results } : {}), ...(body.deadlineMs ? { deadlineMs: body.deadlineMs } : {}), ...(body.keepStatusMs !== undefined ? { keepStatusMs: body.keepStatusMs } : {}), ...(body.leaseMs ? { leaseMs: body.leaseMs } : {}), ...(body.maxLeases ? { maxLeases: body.maxLeases } : {}), ...(body.tickMs ? { tickMs: body.tickMs } : {}), ...(body.wakeAfterMs ? { wakeAfterMs: body.wakeAfterMs } : {}) });
  return { chunks: plan.chunks.length, work: plan.work, span: plan.span, units: g.units.length, bodyBytes: plan.chunks.map((c) => encodeBody(c).byteLength), status };
}
