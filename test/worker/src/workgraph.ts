// Work graphs in the test Worker (RFD0005): a coordinator, a runner with the fixture's tasks, and a
// results store. Runs named with "evict" throw after computing each chunk once (as an eviction
// would, before the results are written); in runs named with "lose", runner 0 abandons the first
// chunk it leases and never runs again. Every runner keeps a log of its storage writes, syncs and
// units computed (`ops`); in runs named with "slow", each unit takes 600 ms, so a chunk outlasts a
// short wakeAfterMs and the coordinator wakes its runner while it computes.
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
  /** Runner starts (graphRun) still to fail, as a first call to a new object sometimes does. */
  private failingStarts = 0;
  protected runners() {
    if (this.failingStarts === 0) return this.env.GRAPH_RUNNERS;
    const ns = this.env.GRAPH_RUNNERS;
    return new Proxy(ns, {
      get: (target, prop) => {
        if (prop !== "get") return (Reflect.get(target, prop) as (...a: unknown[]) => unknown).bind(target);
        return (id: DurableObjectId) => {
          const stub = target.get(id);
          return new Proxy(stub, {
            get: (t, p) => {
              if (p === "graphRun" && this.failingStarts > 0) {
                this.failingStarts--;
                return async () => {
                  throw new Error("test: runner start failed");
                };
              }
              return Reflect.get(t, p);
            },
          });
        };
      },
    });
  }
  /** The next `n` runner starts fail; and the next setAlarm for within a minute (a tick), once. */
  async failStarts(n: number): Promise<void> {
    this.failingStarts = n;
    const storage = this.ctx.storage;
    const setAlarm = storage.setAlarm.bind(storage);
    storage.setAlarm = (async (time: number | Date, options?: DurableObjectSetAlarmOptions) => {
      if (Number(time) > Date.now() + 60_000) return setAlarm(time, options);
      storage.setAlarm = setAlarm;
      throw new Error("test: setAlarm failed");
    }) as typeof storage.setAlarm;
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
  /** Loses this coordinator's alarm, as a failed setAlarm or an alarm the runtime gave up on would. */
  async dropAlarm(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
  }
  /** The next SQL statement containing `text` (and a bound value containing `binding`) throws, once. */
  async failStatementOnce(text: string, binding?: string): Promise<void> {
    const sql = this.ctx.storage.sql;
    const exec = sql.exec.bind(sql);
    sql.exec = ((query: string, ...bindings: unknown[]) => {
      if (!query.includes(text) || (binding !== undefined && !bindings.some((b) => typeof b === "string" && b.includes(binding)))) return exec(query, ...bindings);
      sql.exec = exec;
      throw new Error("test: storage error");
    }) as typeof sql.exec;
  }
}

/** The storage calls a runner's log records: the writes, and sync. */
const WRITES = ["put", "delete", "deleteAll", "setAlarm", "deleteAlarm"] as const;

export class GraphRunners extends WorkGraphRunner<GraphEnv> {
  /** This instance's storage writes, syncs and units computed, in order. */
  private readonly ops: string[] = [];
  private run: string | null = null;
  constructor(ctx: DurableObjectState, env: GraphEnv) {
    super(ctx, env);
    const storage = ctx.storage as unknown as Record<string, (...args: unknown[]) => unknown>;
    for (const name of [...WRITES, "sync"]) {
      const original = storage[name]!.bind(storage);
      storage[name] = (...args: unknown[]) => {
        this.ops.push(name);
        return original(...args);
      };
    }
  }
  protected tasks() {
    const slow = this.run?.includes("slow") ?? false;
    return Object.fromEntries(
      Object.entries(TASKS).map(([name, task]) => [
        name,
        {
          ...task,
          compute: async (...args: Parameters<(typeof task)["compute"]>) => {
            this.ops.push("compute");
            if (slow) await new Promise((resolve) => setTimeout(resolve, 600));
            const out = await (task.compute as (...a: unknown[]) => unknown)(...args);
            this.ops.push("computed");
            return out;
          },
        },
      ]),
    ) as typeof TASKS;
  }
  async opLog(): Promise<string[]> {
    return this.ops;
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
    this.run = run;
    // "each": every chunk in an alarm invocation of its own, its report stored between them.
    this.invocationMs = run.includes("each") ? 0 : 5_000;
    // "poison": every runner dies holding its lease, as on a chunk that needs more CPU than an invocation has.
    if (run.includes("poison")) throw new Error("test: the runner died on this chunk");
    return run.includes("lose") && index === 0 ? "abandon" : "run";
  }
  /** "nostart": the start's own call never reaches the runners; only the coordinator's alarm does. */
  override async graphRun(run: string, index: number, results: string, ensure = false): Promise<void> {
    if (run.includes("nostart") && !ensure) throw new Error("test: the start did not reach this runner");
    // "idle": the runner never starts; the test calls graphNext itself, as the runners it names.
    if (run.includes("idle")) return;
    return super.graphRun(run, index, results, ensure);
  }
  /** "strand": after its first alarm, this runner's alarm is stranded (getAlarm answers a past time that never fires). */
  private stranded = false;
  override async alarm(): Promise<void> {
    await super.alarm();
    if (!this.run?.includes("strand") || this.stranded) return;
    this.stranded = true;
    const storage = this.ctx.storage;
    await storage.deleteAlarm();
    const at = Date.now() - 10 * 60_000;
    const getAlarm = storage.getAlarm.bind(storage);
    const setAlarm = storage.setAlarm.bind(storage);
    let on = true;
    storage.getAlarm = (async (options?: DurableObjectGetAlarmOptions) => (on ? at : getAlarm(options))) as typeof storage.getAlarm;
    storage.setAlarm = ((time: number | Date, options?: DurableObjectSetAlarmOptions) => {
      on = false;
      return setAlarm(time, options);
    }) as typeof storage.setAlarm;
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
  op: "start" | "status" | "results" | "delete" | "storage" | "restart" | "ops" | "dropCoordinatorAlarm" | "next" | "failCoordinatorStatementOnce" | "failStarts";
  /** For "failCoordinatorStatementOnce": text of the statement to fail, and of one of its bound values. */
  text?: string;
  binding?: string;
  /** For "next": the runner that calls, and what it reports. */
  runner?: number;
  finished?: unknown;
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
  maxAttempts?: number;
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
  if (body.op === "next") {
    const answer = await env.GRAPH_COORDINATORS.get(env.GRAPH_COORDINATORS.idFromName(body.run)).graphNext(body.runner ?? 0, (body.finished ?? null) as never);
    // The body is bytes: only whether there is one.
    return { ...answer, ...("body" in answer ? { body: answer.body.byteLength } : {}) };
  }
  if (body.op === "failStarts") return env.GRAPH_COORDINATORS.get(env.GRAPH_COORDINATORS.idFromName(body.run)).failStarts(body.runners ?? 2).then(() => null);
  if (body.op === "failCoordinatorStatementOnce") return env.GRAPH_COORDINATORS.get(env.GRAPH_COORDINATORS.idFromName(body.run)).failStatementOnce(body.text ?? "", body.binding).then(() => null);
  if (body.op === "dropCoordinatorAlarm") return env.GRAPH_COORDINATORS.get(env.GRAPH_COORDINATORS.idFromName(body.run)).dropAlarm().then(() => null);
  if (body.op === "ops") return Promise.all(Array.from({ length: body.runners ?? 4 }, (_, i) => env.GRAPH_RUNNERS.get(env.GRAPH_RUNNERS.idFromName(runnerName(body.run, i))).opLog()));
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
  const status = await run.start({ runners: body.runners ?? 4, ...(body.results ? { results: body.results } : {}), ...(body.deadlineMs ? { deadlineMs: body.deadlineMs } : {}), ...(body.keepStatusMs !== undefined ? { keepStatusMs: body.keepStatusMs } : {}), ...(body.leaseMs ? { leaseMs: body.leaseMs } : {}), ...(body.maxLeases ? { maxLeases: body.maxLeases } : {}), ...(body.tickMs ? { tickMs: body.tickMs } : {}), ...(body.wakeAfterMs ? { wakeAfterMs: body.wakeAfterMs } : {}), ...(body.maxAttempts ? { maxAttempts: body.maxAttempts } : {}) });
  return { chunks: plan.chunks.length, work: plan.work, span: plan.span, units: g.units.length, bodyBytes: plan.chunks.map((c) => encodeBody(c).byteLength), status };
}
