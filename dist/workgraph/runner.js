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
import { ComputeFailed, executeChunk, keysToRead } from "./execute.js";
/** Keys per call to the results store, and bytes per write. */
const READ_BATCH = 2_000;
const WRITE_BATCH_BYTES = 4 * 1024 * 1024;
/** Thrown inside the alarm's work once the runner is stopping: it ends without writing. */
class Stopping extends Error {
}
/**
 * A runner. A subclass gives its tasks (every task its runs' graphs use, by name) and the bindings
 * of the coordinator and results classes.
 */
export class WorkGraphRunner extends DurableObject {
    /**
     * How long one alarm invocation goes on taking chunks. Chunks that are small next to it run one
     * after another in the same invocation; a chunk that costs more than what is left of it runs
     * in the next. Keep it well under the CPU an invocation has, less a chunk.
     */
    invocationMs = 5_000;
    /** Told to stop (in memory: a stop also wipes storage, so a later instance finds nothing). */
    stopping = false;
    /** The alarm's work in progress, which a stop waits for. */
    inflight = null;
    /** Before computing a chunk: "abandon" stops this runner holding its lease (tests use it). */
    async chunkStarting(_run, _index, _chunk, _attempt) {
        return "run";
    }
    /** After computing a chunk, before writing its results (tests throw here, as an eviction would). */
    async chunkComputed(_run, _index, _chunk, _attempt) { }
    /**
     * Starts pulling chunks of run `run` as runner `index`, writing results to the store `results`.
     * With `ensure` (the coordinator's alarm, for a runner gone quiet): starts it only if its start
     * never reached it, and otherwise wakes it, keeping the chunk it has to report.
     */
    async graphRun(run, index, results, ensure = false) {
        // A run's name may be used again after it was stopped and wiped here.
        this.stopping = false;
        const state = await this.ctx.storage.get("state");
        if (ensure && state?.run === run)
            return this.graphWake();
        await this.ctx.storage.put("state", { run, index, results, stopped: false });
        await this.ctx.storage.delete("finished");
        await this.ctx.storage.setAlarm(Date.now());
    }
    /**
     * Stops for good: waits for the chunk in hand to stop (it writes nothing more), then deletes this
     * runner's storage and alarm. Idempotent, and it makes nothing on a runner that holds nothing.
     */
    async graphStop() {
        this.stopping = true;
        await this.inflight?.catch(() => { });
        await this.wipe();
    }
    /** Asks again now: a chunk may be ready, or this runner's alarm retries ran out. */
    async graphWake() {
        if (this.stopping)
            return;
        const state = await this.ctx.storage.get("state");
        if (!state || state.stopped)
            return;
        const at = await this.ctx.storage.getAlarm();
        if (at === null || at > Date.now())
            await this.ctx.storage.setAlarm(Date.now());
    }
    async alarm() {
        if (this.stopping)
            return;
        const work = this.step();
        this.inflight = work;
        try {
            await work;
        }
        catch (error) {
            if (!(error instanceof Stopping))
                throw error;
        }
        finally {
            if (this.inflight === work)
                this.inflight = null;
        }
    }
    async wipe() {
        await this.ctx.storage.deleteAlarm();
        await this.ctx.storage.deleteAll();
    }
    /** Throws Stopping once the runner is told to stop: called before each write. */
    live() {
        if (this.stopping)
            throw new Stopping();
    }
    async step() {
        const state = await this.ctx.storage.get("state");
        if (!state || state.stopped)
            return;
        const started = Date.now();
        let finished = (await this.ctx.storage.get("finished")) ?? null;
        // Whether `finished` is in storage (from the invocation before) or only in hand (from this one).
        let stored = finished !== null;
        for (let chunks = 0;; chunks++) {
            let answer;
            try {
                answer = (await this.coordinator(state.run).graphNext(state.index, finished));
            }
            catch (error) {
                this.coordinatorStub = null;
                throw error;
            }
            // The run is over: nothing of it stays here.
            if ("done" in answer || "failed" in answer)
                return this.wipe();
            this.live();
            // The coordinator has the report: it is not sent again.
            if (stored)
                await this.ctx.storage.delete("finished");
            stored = false;
            finished = null;
            if ("wait" in answer)
                return void (await this.ctx.storage.setAlarm(Date.now() + answer.wait));
            // After this invocation's first chunk: one that may not fit what is left of it goes to the
            // next invocation, which is given this lease back.
            if (chunks > 0 && Date.now() - started + (answer.cost ?? Number.POSITIVE_INFINITY) > this.invocationMs)
                return void (await this.ctx.storage.setAlarm(Date.now()));
            // (Tests: a runner lost holding its lease. It keeps its state, as a lost object would, until stopped.)
            if ((await this.chunkStarting(state.run, state.index, answer.chunk, answer.attempt)) === "abandon")
                return void (await this.ctx.storage.put("state", { ...state, stopped: true }));
            const report = await this.runChunk(state, answer.chunk, answer.attempt, decodeBody(answer.body), answer.fresh === true);
            this.live();
            // Small chunks go on in this invocation: the report goes with the next lease, with no alarm
            // between. (Lost with a crash, the chunk's lease runs out, and its results are found stored.)
            finished = report;
            if (Date.now() - started < this.invocationMs)
                continue;
            await this.ctx.storage.put("finished", report);
            await this.ctx.storage.setAlarm(Date.now());
            return;
        }
    }
    /** The run's coordinator, through one stub for as long as its calls succeed. */
    coordinator(run) {
        if (this.coordinatorStub?.run !== run) {
            const coordinators = this.coordinators();
            const jurisdiction = this.ctx.id.jurisdiction;
            const ns = jurisdiction ? coordinators.jurisdiction(jurisdiction) : coordinators;
            this.coordinatorStub = { run, stub: ns.get(ns.idFromName(run)) };
        }
        return this.coordinatorStub.stub;
    }
    coordinatorStub = null;
    /** The results store's client, kept: it keeps its stubs of the store's shards. */
    resultsStore(name) {
        if (this.store?.name !== name)
            this.store = { name, client: sharded(this.results(), name) };
        return this.store.client;
    }
    store = null;
    /**
     * Computes a chunk and makes its results durable; the report is recorded only after. A compute
     * that throws is reported as the chunk's failure. Anything else throws, and the alarm is retried.
     */
    async runChunk(state, chunk, attempt, body, fresh) {
        const started = Date.now();
        const store = this.resultsStore(state.results);
        const stored = new Map();
        // A fresh chunk has nothing stored of its own: only what it takes from other chunks is read.
        const keys = keysToRead(body, fresh);
        for (let i = 0; i < keys.length; i += READ_BATCH) {
            const part = keys.slice(i, i + READ_BATCH);
            for (const shard of await store.call(part, "graphGet", null))
                shard.keys.forEach((k, j) => shard.result[j] !== null && stored.set(k, shard.result[j]));
        }
        this.live();
        let executed;
        try {
            executed = await executeChunk(body, this.tasks(), stored, this.env);
        }
        catch (error) {
            if (error instanceof ComputeFailed)
                return { chunk, attempt, error: error.message };
            throw error;
        }
        await this.chunkComputed(state.run, state.index, chunk, attempt);
        let batch = [];
        let bytes = 0;
        const flush = async () => {
            if (batch.length === 0)
                return;
            this.live();
            await store.call(batch.map(([k]) => k), "graphPut", batch.map(([, v]) => v));
            batch = [];
            bytes = 0;
        };
        for (const entry of executed.writes) {
            batch.push(entry);
            bytes += entry[0].length + entry[1].length;
            if (bytes >= WRITE_BATCH_BYTES)
                await flush();
        }
        await flush();
        return { chunk, attempt, computed: executed.computed, reused: executed.reused, replayed: executed.replayed, wallMs: Date.now() - started };
    }
}
//# sourceMappingURL=runner.js.map