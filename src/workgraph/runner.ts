// A work graph runner (RFD0005; specs/work-graph-run.ts): one chunk per alarm invocation, so each
// chunk has an invocation's whole CPU budget. The alarm reports the chunk the runner finished and
// leases the next, in one call; reads what is already stored; computes what is missing; writes
// the results; and only then records the chunk finished, for the next alarm to report. Evicted
// partway, the platform retries its alarm, which leases the same chunk again.
//
// A runner leaves nothing behind: told the run is over (by its answer, or by the coordinator's
// stop), it deletes its storage and its alarm. A stop waits for the chunk in hand to stop writing.
import { DurableObject } from "cloudflare:workers";
import { sharded } from "../sharded/client.js";
import { decodeBody } from "./body.js";
import type { Finished, NextAnswer, WorkGraphCoordinator } from "./coordinator.js";
import { ComputeFailed, executeChunk, keysToRead, type ChunkBody } from "./execute.js";
import type { AnyTask } from "./graph.js";
import type { WorkGraphResults } from "./results.js";

/** Keys per call to the results store, and bytes per write. */
const READ_BATCH = 2_000;
const WRITE_BATCH_BYTES = 4 * 1024 * 1024;

/** Thrown inside the alarm's work once the runner is stopping: it ends without writing. */
class Stopping extends Error {}

interface RunnerState {
  run: string;
  index: number;
  results: string;
  stopped: boolean;
}

/**
 * A runner. A subclass gives its tasks (every task its runs' graphs use, by name) and the bindings
 * of the coordinator and results classes.
 */
export abstract class WorkGraphRunner<Env = unknown> extends DurableObject<Env> {
  protected abstract tasks(): Readonly<Record<string, AnyTask<Env>>>;
  protected abstract coordinators(): DurableObjectNamespace<WorkGraphCoordinator<any>>;
  protected abstract results(): DurableObjectNamespace<WorkGraphResults<any>>;

  /** Told to stop (in memory: a stop also wipes storage, so a later instance finds nothing). */
  private stopping = false;
  /** The alarm's work in progress, which a stop waits for. */
  private inflight: Promise<void> | null = null;

  /** Before computing a chunk: "abandon" stops this runner holding its lease (tests use it). */
  protected async chunkStarting(_run: string, _index: number, _chunk: number, _attempt: number): Promise<"run" | "abandon"> {
    return "run";
  }

  /** After computing a chunk, before writing its results (tests throw here, as an eviction would). */
  protected async chunkComputed(_run: string, _index: number, _chunk: number, _attempt: number): Promise<void> {}

  /** Starts pulling chunks of run `run` as runner `index`, writing results to the store `results`. */
  async graphRun(run: string, index: number, results: string): Promise<void> {
    await this.ctx.storage.put<RunnerState>("state", { run, index, results, stopped: false });
    await this.ctx.storage.delete("finished");
    await this.ctx.storage.setAlarm(Date.now());
  }

  /**
   * Stops for good: waits for the chunk in hand to stop (it writes nothing more), then deletes this
   * runner's storage and alarm. Idempotent, and it makes nothing on a runner that holds nothing.
   */
  async graphStop(): Promise<void> {
    this.stopping = true;
    await this.inflight?.catch(() => {});
    await this.wipe();
  }

  /** Asks again now: a chunk may be ready, or this runner's alarm retries ran out. */
  async graphWake(): Promise<void> {
    if (this.stopping) return;
    const state = await this.ctx.storage.get<RunnerState>("state");
    if (!state || state.stopped) return;
    const at = await this.ctx.storage.getAlarm();
    if (at === null || at > Date.now()) await this.ctx.storage.setAlarm(Date.now());
  }

  override async alarm(): Promise<void> {
    if (this.stopping) return;
    const work = this.step();
    this.inflight = work;
    try {
      await work;
    } catch (error) {
      if (!(error instanceof Stopping)) throw error;
    } finally {
      if (this.inflight === work) this.inflight = null;
    }
  }

  private async wipe(): Promise<void> {
    await this.ctx.storage.deleteAlarm();
    await this.ctx.storage.deleteAll();
  }

  /** Throws Stopping once the runner is told to stop: called before each write. */
  private live(): void {
    if (this.stopping) throw new Stopping();
  }

  private async step(): Promise<void> {
    const state = await this.ctx.storage.get<RunnerState>("state");
    if (!state || state.stopped) return;
    const finished = (await this.ctx.storage.get<Finished>("finished")) ?? null;
    const coordinators = this.coordinators();
    const jurisdiction = this.ctx.id.jurisdiction;
    const ns = jurisdiction ? coordinators.jurisdiction(jurisdiction as DurableObjectJurisdiction) : coordinators;
    const answer = (await ns.get(ns.idFromName(state.run)).graphNext(state.index, finished)) as NextAnswer;
    // The run is over: nothing of it stays here.
    if ("done" in answer || "failed" in answer) return this.wipe();
    this.live();
    // The coordinator has the report: it is not sent again.
    if (finished) await this.ctx.storage.delete("finished");
    if ("wait" in answer) return void (await this.ctx.storage.setAlarm(Date.now() + answer.wait));
    // (Tests: a runner lost holding its lease. It keeps its state, as a lost object would, until stopped.)
    if ((await this.chunkStarting(state.run, state.index, answer.chunk, answer.attempt)) === "abandon") return void (await this.ctx.storage.put<RunnerState>("state", { ...state, stopped: true }));
    const report = await this.runChunk(state, answer.chunk, answer.attempt, decodeBody(answer.body));
    this.live();
    await this.ctx.storage.put<Finished>("finished", report);
    await this.ctx.storage.setAlarm(Date.now());
  }

  /**
   * Computes a chunk and makes its results durable; the report is recorded only after. A compute
   * that throws is reported as the chunk's failure. Anything else throws, and the alarm is retried.
   */
  private async runChunk(state: RunnerState, chunk: number, attempt: number, body: ChunkBody): Promise<Finished> {
    const started = Date.now();
    const store = sharded(this.results(), state.results);
    const stored = new Map<string, string>();
    const keys = keysToRead(body);
    for (let i = 0; i < keys.length; i += READ_BATCH) {
      const part = keys.slice(i, i + READ_BATCH);
      for (const shard of await store.call(part, "graphGet", null)) shard.keys.forEach((k, j) => shard.result[j] !== null && stored.set(k, shard.result[j]!));
    }
    this.live();
    let executed;
    try {
      executed = await executeChunk(body, this.tasks(), stored, this.env);
    } catch (error) {
      if (error instanceof ComputeFailed) return { chunk, attempt, error: error.message };
      throw error;
    }
    await this.chunkComputed(state.run, state.index, chunk, attempt);
    let batch: [string, string][] = [];
    let bytes = 0;
    const flush = async () => {
      if (batch.length === 0) return;
      this.live();
      await store.call(
        batch.map(([k]) => k),
        "graphPut",
        batch.map(([, v]) => v),
      );
      batch = [];
      bytes = 0;
    };
    for (const entry of executed.writes) {
      batch.push(entry);
      bytes += entry[0].length + entry[1].length;
      if (bytes >= WRITE_BATCH_BYTES) await flush();
    }
    await flush();
    return { chunk, attempt, computed: executed.computed, reused: executed.reused, replayed: executed.replayed, wallMs: Date.now() - started };
  }
}
