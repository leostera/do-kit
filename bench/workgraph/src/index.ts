// Work graph benchmarks (RFD0005). Units burn a known amount of CPU (SHA-256 rounds, calibrated
// per millisecond on the edge), in four shapes: many small trees, one huge tree, one chain, and
// tiny chunks that cost nothing (the coordinator's own limit). Runs may inject faults: "evict"
// throws after computing a share of chunks once, before writing (as an eviction would); "lose"
// makes some runners abandon their first chunk and never run again.
//
// POST /calibrate {rounds}  POST /start {run, shape, units, unitMs, runners, chunkMs, itersPerMs, faults?}
// POST /status {run}
import { where, type Where } from "../../worker/src/where.ts";
import { createHash } from "node:crypto";
import { burn, graphOf, TASKS, type Shape } from "./graphs.ts";
import { checkSplitPolicy, DEFAULT_SPLIT_POLICY } from "../../../src/sharded/index.ts";
import { encodeBody, planWorkGraph, runnerName, workGraphRun, WorkGraphCoordinator, WorkGraphResults, WorkGraphRunner } from "../../../src/workgraph/index.ts";

interface Env {
  readonly COORDINATORS: DurableObjectNamespace<BenchCoordinator>;
  readonly RUNNERS: DurableObjectNamespace<BenchRunner>;
  readonly RESULTS: DurableObjectNamespace<BenchResults>;
  readonly BENCH_TOKEN: string;
}

export class BenchCoordinator extends WorkGraphCoordinator<Env> {
  protected runners() {
    return this.env.RUNNERS;
  }
  protected results() {
    return this.env.RESULTS;
  }
}

interface Faults {
  /** The share of chunks whose first computation is thrown away, as by an eviction. */
  readonly evict?: number;
  /** How many runners abandon their first chunk and stop. */
  readonly lose?: number;
}

export class BenchRunner extends WorkGraphRunner<Env> {
  protected tasks() {
    return TASKS;
  }
  protected coordinators() {
    return this.env.COORDINATORS;
  }
  protected results() {
    return this.env.RESULTS;
  }
  private async faults(run: string): Promise<Faults> {
    return (await this.ctx.storage.get<Faults>(`faults:${run}`)) ?? {};
  }
  /** What this runner did when, in this isolate: alarm invocations, chunks started and computed. */
  private timeline: { event: string; at: number; chunk?: number }[] = [];
  /** The clock moves only with I/O: a storage read brings it up to date. */
  private async mark(event: string, chunk?: number): Promise<void> {
    await this.ctx.storage.get("mark");
    if (this.timeline.length < 10_000) this.timeline.push({ event, at: Date.now(), ...(chunk === undefined ? {} : { chunk }) });
  }
  override async graphRun(run: string, index: number, results: string, ensure = false): Promise<void> {
    await this.mark(ensure ? "run-ensure" : "run");
    return super.graphRun(run, index, results, ensure);
  }
  override async graphWake(): Promise<void> {
    await this.mark("wake");
    return super.graphWake();
  }
  override async alarm(): Promise<void> {
    await this.mark("alarm");
    try {
      await super.alarm();
    } finally {
      await this.mark("alarm-end");
    }
  }
  async benchTimeline(): Promise<{ event: string; at: number; chunk?: number }[]> {
    return this.timeline;
  }
  /** Which isolate and colo this runner is in (bench/worker/src/where.ts). */
  async benchWhere(burnIterations: number): Promise<Where> {
    return where(burnIterations);
  }
  /** Burns `rounds` rounds: the caller times the call. */
  async benchBurn(rounds: number): Promise<string> {
    return burn("calibrate", rounds);
  }
  async benchFaults(run: string, faults: Faults): Promise<void> {
    await this.ctx.storage.put(`faults:${run}`, faults);
  }
  protected override async chunkStarting(run: string, index: number, chunk: number): Promise<"run" | "abandon"> {
    await this.mark("start", chunk);
    const lose = (await this.faults(run)).lose ?? 0;
    return index < lose ? "abandon" : "run";
  }
  protected override async chunkComputed(run: string, _index: number, chunk: number): Promise<void> {
    await this.mark("computed", chunk);
    const evict = (await this.faults(run)).evict ?? 0;
    if (evict === 0) return;
    // The same chunks in every run with the same share: a hash of the chunk id.
    const draw = parseInt(createHash("sha256").update(String(chunk)).digest("hex").slice(0, 8), 16) / 2 ** 32;
    if (draw >= evict) return;
    const key = `evicted:${run}:${chunk}`;
    if (await this.ctx.storage.get(key)) return;
    await this.ctx.storage.put(key, true);
    throw new Error("simulated eviction");
  }
}

export class BenchResults extends WorkGraphResults<Env> {
  // 16 shards from the start, so the store is not what limits the coordinator's measurements.
  protected override splitPolicy = checkSplitPolicy({ ...DEFAULT_SPLIT_POLICY, initialDepth: 4 });
  protected nodes() {
    return this.env.RESULTS;
  }
}

interface StartRequest {
  run: string;
  shape: Shape;
  units: number;
  unitMs: number;
  itersPerMs: number;
  runners: number;
  chunkMs: number;
  replayMs?: number;
  maxUnitsPerChunk?: number;
  /** Bytes of entries per slice (slices, bodies). */
  bytes?: number;
  identity?: "inputs" | "key";
  leaseMs?: number;
  /** How long a runner may go without calling before the coordinator wakes it. */
  wakeAfterMs?: number;
  faults?: Faults;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`) return new Response("unauthorized", { status: 401 });
    const url = new URL(request.url);
    const body = (await request.json().catch(() => ({}))) as Record<string, unknown>;
    try {
      switch (url.pathname) {
        // Which isolate and colo each runner of a run is in: runners that share an isolate share
        // its one thread, and compute one at a time.
        case "/placement": {
          const run = String(body.run);
          const runners = Number(body.runners ?? 16);
          const asked = Date.now();
          const objects = await Promise.all(Array.from({ length: runners }, async (_, i) => ({ runner: i, ...(await env.RUNNERS.get(env.RUNNERS.idFromName(runnerName(run, i))).benchWhere(Number(body.burnIterations ?? 0))), answeredAt: Date.now() - asked })));
          return Response.json({ asked, objects });
        }
        // Each runner's timeline of a run (from memory: a runner evicted meanwhile lost its own).
        case "/timeline": {
          const run = String(body.run);
          const runners = Number(body.runners ?? 16);
          const timelines = await Promise.all(Array.from({ length: runners }, (_, i) => env.RUNNERS.get(env.RUNNERS.idFromName(runnerName(run, i))).benchTimeline()));
          return Response.json({ timelines });
        }
        case "/calibrate": {
          // Timed around a call to a runner object (Date.now() moves only across I/O), against an
          // empty call to the same object: the difference is the burn.
          const rounds = Number(body.rounds ?? 1_000_000);
          const stub = env.RUNNERS.get(env.RUNNERS.idFromName("calibrate"));
          await stub.benchBurn(0);
          const empty = Date.now();
          await stub.benchBurn(0);
          const overhead = Date.now() - empty;
          const started = Date.now();
          await stub.benchBurn(rounds);
          const ms = Date.now() - started - overhead;
          return Response.json({ rounds, ms, overheadMs: overhead, itersPerMs: rounds / ms });
        }
        case "/start": {
          const r = body as unknown as StartRequest;
          const planned = Date.now();
          const g = graphOf(r.shape, r.units, r.unitMs, r.itersPerMs, r.run, r.bytes ?? 0);
          const plan = planWorkGraph(g, { chunkMs: r.chunkMs, ...(r.replayMs !== undefined ? { replayMs: r.replayMs } : {}), ...(r.maxUnitsPerChunk ? { maxUnitsPerChunk: r.maxUnitsPerChunk } : {}), ...(r.identity ? { identity: r.identity } : {}) });
          const planMs = Date.now() - planned;
          if (r.faults) await Promise.all(Array.from({ length: r.runners }, (_, i) => env.RUNNERS.get(env.RUNNERS.idFromName(runnerName(r.run, i))).benchFaults(r.run, r.faults!)));
          const run = workGraphRun(env.COORDINATORS, r.run);
          await run.submit(plan);
          const status = await run.start({ runners: r.runners, ...(r.leaseMs ? { leaseMs: r.leaseMs } : {}), ...(r.wakeAfterMs ? { wakeAfterMs: r.wakeAfterMs } : {}) });
          return Response.json({ chunks: plan.chunks.length, units: g.units.length, work: plan.work, span: plan.span, chunkSpan: plan.chunkSpan, bodyBytes: Math.max(...plan.chunks.map((c) => encodeBody(c).byteLength)), replayed: plan.replayed, planMs, status });
        }
        case "/status":
          return Response.json(await workGraphRun(env.COORDINATORS, String(body.run)).status());
        default:
          return new Response("not found", { status: 404 });
      }
    } catch (error) {
      return Response.json({ error: String(error), stack: error instanceof Error ? error.stack : undefined }, { status: 500 });
    }
  },
};
