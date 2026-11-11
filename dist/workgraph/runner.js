// A work graph runner (RFD0005; specs/work-graph-run.ts): one chunk per alarm invocation, so each
// chunk has an invocation's whole CPU budget. The alarm reports the chunk the runner finished and
// leases the next, in one call; reads what is already stored; computes what is missing; writes
// the results; and only then records the chunk finished, for the next alarm to report. Evicted
// partway, the platform retries its alarm, which leases the same chunk again.
import { DurableObject } from "cloudflare:workers";
import { sharded } from "../sharded/client.js";
import { decodeBody } from "./body.js";
import { ComputeFailed, executeChunk, keysToRead } from "./execute.js";
/** Keys per call to the results store, and bytes per write. */
const READ_BATCH = 2_000;
const WRITE_BATCH_BYTES = 4 * 1024 * 1024;
/**
 * A runner. A subclass gives its tasks (every task its runs' graphs use, by name) and the bindings
 * of the coordinator and results classes.
 */
export class WorkGraphRunner extends DurableObject {
    /** Before computing a chunk: "abandon" stops this runner holding its lease (tests use it). */
    async chunkStarting(_run, _index, _chunk, _attempt) {
        return "run";
    }
    /** After computing a chunk, before writing its results (tests throw here, as an eviction would). */
    async chunkComputed(_run, _index, _chunk, _attempt) { }
    /** Starts pulling chunks of run `run` as runner `index`, writing results to the store `results`. */
    async graphRun(run, index, results) {
        await this.ctx.storage.put("state", { run, index, results, stopped: false });
        await this.ctx.storage.delete("finished");
        await this.ctx.storage.setAlarm(Date.now());
    }
    /** Asks again now: a chunk may be ready, or this runner's alarm retries ran out. */
    async graphWake() {
        const state = await this.ctx.storage.get("state");
        if (!state || state.stopped)
            return;
        const at = await this.ctx.storage.getAlarm();
        if (at === null || at > Date.now())
            await this.ctx.storage.setAlarm(Date.now());
    }
    async alarm() {
        const state = await this.ctx.storage.get("state");
        if (!state || state.stopped)
            return;
        const finished = (await this.ctx.storage.get("finished")) ?? null;
        const coordinators = this.coordinators();
        const jurisdiction = this.ctx.id.jurisdiction;
        const ns = jurisdiction ? coordinators.jurisdiction(jurisdiction) : coordinators;
        const answer = (await ns.get(ns.idFromName(state.run)).graphNext(state.index, finished));
        // The coordinator has the report: it is not sent again.
        if (finished)
            await this.ctx.storage.delete("finished");
        if ("wait" in answer)
            return void (await this.ctx.storage.setAlarm(Date.now() + answer.wait));
        if ("done" in answer || "failed" in answer)
            return void (await this.ctx.storage.put("state", { ...state, stopped: true }));
        if ((await this.chunkStarting(state.run, state.index, answer.chunk, answer.attempt)) === "abandon")
            return void (await this.ctx.storage.put("state", { ...state, stopped: true }));
        const report = await this.runChunk(state, answer.chunk, answer.attempt, decodeBody(answer.body));
        await this.ctx.storage.put("finished", report);
        await this.ctx.storage.setAlarm(Date.now());
    }
    /**
     * Computes a chunk and makes its results durable; the report is recorded only after. A compute
     * that throws is reported as the chunk's failure. Anything else throws, and the alarm is retried.
     */
    async runChunk(state, chunk, attempt, body) {
        const started = Date.now();
        const store = sharded(this.results(), state.results);
        const stored = new Map();
        const keys = keysToRead(body);
        for (let i = 0; i < keys.length; i += READ_BATCH) {
            const part = keys.slice(i, i + READ_BATCH);
            for (const shard of await store.call(part, "graphGet", null))
                shard.keys.forEach((k, j) => shard.result[j] !== null && stored.set(k, shard.result[j]));
        }
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