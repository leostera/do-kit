// A work graph run's coordinator (RFD0005; specs/work-graph-run.ts): one object per run, holding
// its chunks, what each needs, and their leases, in SQLite. Runners pull: each call reports the
// chunk the runner finished and leases it the next. A lease that runs out goes back to ready, and a
// runner that has gone quiet is woken, from the alarm.
//
// A run leaves nothing behind. Once it ends (done, failed, cancelled, or past its deadline), the
// alarm stops every runner, which wipes itself; once each has said so, the coordinator drops its
// chunks and keeps only a status row for `keepStatusMs`, then wipes the run's own results store
// and itself. `delete` does all of it at once.
import { DurableObject } from "cloudflare:workers";
import { sharded } from "../sharded/client.js";
import { MAX_BODY_BYTES } from "./body.js";
const SCHEMA = `
CREATE TABLE IF NOT EXISTS graph_chunks (
  id INTEGER PRIMARY KEY, round INTEGER NOT NULL, cost REAL NOT NULL,
  state TEXT NOT NULL DEFAULT 'waiting', needs INTEGER NOT NULL DEFAULT 0,
  runner INTEGER, lease_until INTEGER, attempts INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS graph_chunks_ready ON graph_chunks (state, round, id);
CREATE INDEX IF NOT EXISTS graph_chunks_runner ON graph_chunks (runner) WHERE state = 'leased';
-- (needed chunk, chunk that needs it)
-- Each chunk's body (body.ts), in parts: a body may be bigger than a row (8 MB at most).
CREATE TABLE IF NOT EXISTS graph_bodies (chunk INTEGER NOT NULL, part INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (chunk, part)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS graph_needs (needed INTEGER NOT NULL, chunk INTEGER NOT NULL, PRIMARY KEY (needed, chunk)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS graph_runners (runner INTEGER PRIMARY KEY, seen_at INTEGER NOT NULL, waiting INTEGER NOT NULL DEFAULT 0);
-- Runners that said they stopped (and wiped themselves), once the run ended.
CREATE TABLE IF NOT EXISTS graph_stopped (runner INTEGER PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS graph_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
`;
/** Bytes per row of a chunk's body. */
const BODY_PART_BYTES = 1024 * 1024;
/** The tables of a run's work, dropped once its runners have stopped: only graph_meta is kept. */
const WORK_TABLES = ["graph_chunks", "graph_bodies", "graph_needs", "graph_runners", "graph_stopped"];
/** How long a stop waits on a runner (which finishes the chunk it is writing first), per try. */
const STOP_WAIT_MS = 60_000;
/** How long a run submitted but never started is kept: then it fails, and is wiped. */
const UNSTARTED_MS = 24 * 60 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
const HOUR_MS = 60 * 60 * 1000;
/** The object name of runner `index` of a run. */
export const runnerName = (run, index) => `${run}/runner/${index}`;
/** The name of a run's own results store. */
export const ownResultsName = (run) => `${run}/results`;
/**
 * A run's coordinator. A subclass gives `runners()` and `results()`, the bindings of its runner
 * and results classes. Name each coordinator object by its run.
 */
export class WorkGraphCoordinator extends DurableObject {
    sql;
    meta;
    cleaning = null;
    constructor(ctx, env) {
        super(ctx, env);
        this.sql = ctx.storage.sql;
        // Tables are made by a run's first submission: an object only asked about (a status after the
        // run is gone, a late runner) stays empty.
        const exists = this.sql.exec("SELECT COUNT(*) AS n FROM sqlite_master WHERE type = 'table' AND name = 'graph_meta'").one().n > 0;
        const row = exists ? this.sql.exec("SELECT value FROM graph_meta WHERE key = 'meta'").toArray()[0] : undefined;
        this.meta = row ? JSON.parse(row.value) : null;
    }
    /** Adds chunks to a run being submitted (idempotent per chunk id, and once sealed, a no-op). */
    async graphSubmit(name, chunks) {
        const fresh = !this.existing(name);
        const meta = this.metaFor(name);
        // A run never started still ends: its alarm wipes it once UNSTARTED_MS is up.
        if (fresh)
            await this.ctx.storage.setAlarm(meta.deadlineAt);
        // Sealed: every chunk is in already, and this is a retry of the same submission (a run's name
        // stands for its plan). Submitting again is harmless.
        if (meta.state !== "submitting")
            return;
        this.ctx.storage.transactionSync(() => {
            for (const c of chunks) {
                if (c.body.byteLength > MAX_BODY_BYTES)
                    throw new Error(`chunk ${c.id}'s body is ${c.body.byteLength} bytes, over ${MAX_BODY_BYTES}`);
                if (this.sql.exec("INSERT OR IGNORE INTO graph_chunks (id, round, cost) VALUES (?, ?, ?)", c.id, c.round, c.cost).rowsWritten === 0)
                    continue;
                for (let at = 0, part = 0; at < c.body.byteLength || part === 0; at += BODY_PART_BYTES, part++)
                    this.sql.exec("INSERT INTO graph_bodies (chunk, part, data) VALUES (?, ?, ?)", c.id, part, c.body.subarray(at, at + BODY_PART_BYTES));
                for (const needed of c.needs)
                    this.sql.exec("INSERT OR IGNORE INTO graph_needs (needed, chunk) VALUES (?, ?)", needed, c.id);
            }
        });
    }
    /** Ends submission: every needed chunk must exist. Chunks that need nothing are ready. */
    async graphSeal(name) {
        const meta = this.metaFor(name);
        if (meta.state !== "submitting")
            return this.status();
        const missing = this.sql.exec("SELECT needed FROM graph_needs WHERE needed NOT IN (SELECT id FROM graph_chunks) LIMIT 1").toArray()[0];
        if (missing)
            throw new Error(`run ${name}: chunk ${missing.needed} is needed but was not submitted`);
        this.ctx.storage.transactionSync(() => {
            this.sql.exec("UPDATE graph_chunks SET needs = (SELECT COUNT(*) FROM graph_needs WHERE graph_needs.chunk = graph_chunks.id)");
            this.sql.exec("UPDATE graph_chunks SET state = 'ready' WHERE needs = 0");
            this.save({ ...meta, state: "ready" });
        });
        return this.status();
    }
    /** Starts the runners, and the alarm that watches them. */
    async graphStart(name, options) {
        const meta = this.existing(name);
        if (!meta)
            throw new Error(`run ${name} was not submitted (or is gone)`);
        if (meta.state === "submitting")
            throw new Error(`run ${name} is not sealed`);
        if (meta.state !== "ready")
            return this.status();
        if (!(options.runners >= 1))
            throw new Error("a run needs at least one runner");
        const now = Date.now();
        const own = options.results === undefined;
        const results = options.results ?? ownResultsName(name);
        const started = { ...meta, state: "running", runners: options.runners, results, ownResults: own, leaseMs: options.leaseMs ?? 120_000, maxAttempts: options.maxAttempts ?? 3, maxLeases: options.maxLeases ?? 5, wakeAfterMs: options.wakeAfterMs ?? 30_000, tickMs: options.tickMs ?? 5_000, logChunks: options.logChunks ?? false, startedAt: now, deadlineAt: now + (options.deadlineMs ?? DAY_MS), keepStatusMs: options.keepStatusMs ?? HOUR_MS };
        this.save(started);
        this.finishIfDone(); // an empty run
        for (let i = 0; i < options.runners; i++)
            this.sql.exec("INSERT OR REPLACE INTO graph_runners (runner, seen_at, waiting) VALUES (?, ?, 0)", i, now);
        await this.ctx.storage.setAlarm(now + (this.meta.cleanup ? 0 : started.tickMs));
        // A runner this call fails to start is started by the alarm, like one that went quiet.
        if (this.meta.state === "running")
            await Promise.allSettled(Array.from({ length: options.runners }, (_, i) => this.runner(i).graphRun(name, i, results)));
        return this.status();
    }
    /**
     * Cancels the run if it has not ended, stops its runners, and wipes it: its chunks, its status,
     * its own results store, and this object. Idempotent. Null once nothing is left; a status while
     * a runner has not stopped yet, in which case the alarm goes on (call again to wait).
     */
    async graphDelete(name) {
        const meta = this.existing(name);
        if (!meta)
            return null;
        if (!this.ended(meta))
            this.end("cancelled", "the run was deleted");
        this.save({ ...this.meta, deleting: true });
        await this.cleanUp(Date.now());
        return this.meta ? this.status() : null;
    }
    /** Records what runner `runner` finished, if anything, and tells it what to do next. */
    async graphNext(runner, finished) {
        const meta = this.meta;
        // Gone, or ended: the runner stops (and wipes itself). Nothing is written here.
        if (!meta)
            return { failed: "the run is gone" };
        if (this.ended(meta))
            return meta.state === "done" ? { done: true } : { failed: meta.error ?? `the run is ${meta.state}` };
        const now = Date.now();
        this.sql.exec("INSERT INTO graph_runners (runner, seen_at, waiting) VALUES (?, ?, 0) ON CONFLICT (runner) DO UPDATE SET seen_at = excluded.seen_at, waiting = 0", runner, now);
        if (finished) {
            if ("error" in finished)
                this.fail(finished.chunk, finished.error);
            else
                this.complete(finished);
        }
        const m = this.meta;
        if (this.ended(m)) {
            // It ended just now: the alarm stops the other runners.
            await this.ctx.storage.setAlarm(now);
            return m.state === "done" ? { done: true } : { failed: m.error ?? `the run is ${m.state}` };
        }
        return this.lease(runner, now);
    }
    async graphStatus() {
        return this.meta ? this.status() : null;
    }
    /** A chunk's body, from its parts. */
    body(chunk) {
        const parts = this.sql.exec("SELECT data FROM graph_bodies WHERE chunk = ? ORDER BY part", chunk).toArray().map((r) => new Uint8Array(r.data));
        if (parts.length === 1)
            return parts[0];
        const out = new Uint8Array(parts.reduce((sum, p) => sum + p.byteLength, 0));
        let at = 0;
        for (const p of parts) {
            out.set(p, at);
            at += p.byteLength;
        }
        return out;
    }
    /** The runner's own lease back (it was evicted while computing it), or the next ready chunk. */
    lease(runner, now) {
        const meta = this.meta;
        const own = this.sql.exec("SELECT id, attempts, cost FROM graph_chunks WHERE state = 'leased' AND runner = ? LIMIT 1", runner).toArray()[0];
        if (own) {
            this.sql.exec("UPDATE graph_chunks SET lease_until = ? WHERE id = ?", now + meta.leaseMs, own.id);
            return { chunk: own.id, attempt: own.attempts, body: this.body(own.id), cost: own.cost };
        }
        const next = this.sql.exec("SELECT id, attempts, cost FROM graph_chunks WHERE state = 'ready' ORDER BY round, id LIMIT 1").toArray()[0];
        if (next) {
            this.sql.exec("UPDATE graph_chunks SET state = 'leased', runner = ?, lease_until = ?, attempts = attempts + 1 WHERE id = ?", runner, now + meta.leaseMs, next.id);
            // A chunk leased for the first time, writing to a store only this run uses, has no results
            // there yet: its runner need not look. (Leased again, or the same lease given back to a
            // runner that was evicted, it may have written some.)
            return { chunk: next.id, attempt: next.attempts + 1, body: this.body(next.id), fresh: meta.ownResults && next.attempts === 0, cost: next.cost };
        }
        this.sql.exec("UPDATE graph_runners SET waiting = 1 WHERE runner = ?", runner);
        return { wait: Math.min(2_000, meta.tickMs) };
    }
    /**
     * Marks a chunk done, from whichever runner finished it first (a later finish of the same chunk
     * wrote the same results), and readies the chunks that needed only it.
     */
    complete(f) {
        const row = this.sql.exec("SELECT state FROM graph_chunks WHERE id = ?", f.chunk).toArray()[0];
        if (!row || row.state === "done")
            return;
        let readied = 0;
        this.ctx.storage.transactionSync(() => {
            this.sql.exec("UPDATE graph_chunks SET state = 'done', runner = NULL, lease_until = NULL WHERE id = ?", f.chunk);
            this.sql.exec("UPDATE graph_chunks SET needs = needs - 1 WHERE id IN (SELECT chunk FROM graph_needs WHERE needed = ?)", f.chunk);
            // Counted by the rows returned: rowsWritten also counts the writes to the indexes.
            readied = this.sql.exec("UPDATE graph_chunks SET state = 'ready' WHERE state = 'waiting' AND needs = 0 AND id IN (SELECT chunk FROM graph_needs WHERE needed = ?) RETURNING id", f.chunk).toArray().length;
            const m = this.meta;
            this.save({ ...m, computed: m.computed + f.computed, reused: m.reused + f.reused, replayed: m.replayed + f.replayed, busyMs: (m.busyMs ?? 0) + f.wallMs });
            this.finishIfDone();
        });
        if (this.meta.logChunks)
            console.log(JSON.stringify({ event: "graph-chunk", run: this.meta.name, chunk: f.chunk, attempt: f.attempt, computed: f.computed, reused: f.reused, replayed: f.replayed, wallMs: f.wallMs }));
        if (readied > 0)
            this.wakeWaiting(readied);
    }
    /** A chunk whose compute threw: ready again, or the run fails once it has failed maxAttempts times. */
    fail(chunk, error) {
        const row = this.sql.exec("SELECT state, failures FROM graph_chunks WHERE id = ?", chunk).toArray()[0];
        if (!row || row.state === "done")
            return;
        const m = this.meta;
        const failures = row.failures + 1;
        this.ctx.storage.transactionSync(() => {
            this.sql.exec("UPDATE graph_chunks SET state = 'ready', runner = NULL, lease_until = NULL, failures = ? WHERE id = ?", failures, chunk);
            this.save({ ...m, failures: m.failures + 1 });
            if (failures >= m.maxAttempts)
                this.end("failed", `chunk ${chunk} failed ${failures} times: ${error}`);
        });
    }
    finishIfDone() {
        const m = this.meta;
        if (m.state !== "running")
            return;
        // By state, so the index answers it: `state != 'done'` would read every chunk done so far.
        const left = this.sql.exec("SELECT 1 FROM graph_chunks WHERE state IN ('waiting', 'ready', 'leased') LIMIT 1").toArray().length;
        if (left === 0)
            this.end("done", null);
    }
    ended(m) {
        return m.state === "done" || m.state === "failed" || m.state === "cancelled";
    }
    /** The run ends: what its chunks were is kept for its status, and its cleanup begins. */
    end(state, error) {
        const m = this.meta;
        this.save({ ...m, state, error: m.error ?? error, finishedAt: Date.now(), final: this.liveCounts(), cleanup: "stopping" });
    }
    /**
     * One step of a run's cleanup. Stopping: tells every runner that has not said so to stop (each
     * wipes itself); once all have, drops the chunks and keeps the status. Kept: once its time is up
     * (or on delete), wipes the run's own results store, then this object. Re-arms the alarm for
     * whatever is left.
     */
    cleanUp(now) {
        // One at a time (the alarm's and a delete's): a second waits, then looks again.
        const previous = this.cleaning ?? Promise.resolve();
        const next = previous.then(() => (this.meta?.cleanup ? this.cleanUpOnce(now) : undefined));
        this.cleaning = next.catch(() => { });
        return next;
    }
    async cleanUpOnce(now) {
        let m = this.meta;
        if (m.cleanup === "stopping") {
            const stopped = new Set(this.sql.exec("SELECT runner FROM graph_stopped").toArray().map((r) => r.runner));
            const left = Array.from({ length: m.runners }, (_, i) => i).filter((i) => !stopped.has(i));
            const answers = await Promise.allSettled(left.map((i) => withTimeout(this.runner(i).graphStop(), STOP_WAIT_MS)));
            answers.forEach((a, j) => a.status === "fulfilled" && this.sql.exec("INSERT OR IGNORE INTO graph_stopped (runner) VALUES (?)", left[j]));
            if (answers.some((a) => a.status === "rejected"))
                return void (await this.ctx.storage.setAlarm(Date.now() + (m.tickMs || 5_000)));
            // Every runner has stopped: nothing can lease or report any more. Only the status stays.
            this.ctx.storage.transactionSync(() => {
                for (const t of WORK_TABLES)
                    this.sql.exec(`DROP TABLE IF EXISTS ${t}`);
                this.save({ ...m, cleanup: "kept", keptUntil: now + m.keepStatusMs });
            });
            m = this.meta;
        }
        if (m.deleting || m.keptUntil === null || Date.now() >= m.keptUntil)
            return this.wipe();
        await this.ctx.storage.setAlarm(m.keptUntil);
    }
    /** The run's own results store, then this object: afterwards it holds nothing, and no alarm. */
    async wipe() {
        const m = this.meta;
        if (m.ownResults) {
            try {
                await this.resultsStore(m.results).wipe();
            }
            catch (error) {
                console.error(JSON.stringify({ event: "graph-results-wipe-failed", run: m.name, error: String(error) }));
                return void (await this.ctx.storage.setAlarm(Date.now() + (m.tickMs || 5_000)));
            }
        }
        await this.ctx.storage.deleteAlarm();
        await this.ctx.storage.deleteAll();
        this.meta = null;
    }
    /** Leases that ran out go back to ready, quiet runners are woken, and the deadline is kept; once the run ends, its cleanup. */
    async alarm() {
        const m = this.meta;
        if (!m)
            return;
        if (m.cleanup)
            return this.cleanUp(Date.now());
        const now = Date.now();
        if (m.state !== "running") {
            // Submitted, never started: it ends at its deadline, like a run that started.
            if (m.deadlineAt !== null && now >= m.deadlineAt && !this.ended(m)) {
                this.end("failed", `the run was never started, ${UNSTARTED_MS} ms after it was submitted`);
                return this.cleanUp(now);
            }
            if (m.deadlineAt !== null && !this.ended(m))
                await this.ctx.storage.setAlarm(m.deadlineAt);
            return;
        }
        if (m.deadlineAt !== null && now >= m.deadlineAt) {
            this.end("failed", `the run passed its deadline, ${m.deadlineAt - (m.startedAt ?? m.deadlineAt)} ms after it started`);
            return this.cleanUp(now);
        }
        const lapsed = this.sql.exec("UPDATE graph_chunks SET state = 'ready', runner = NULL, lease_until = NULL WHERE state = 'leased' AND lease_until < ? RETURNING id, attempts", now).toArray();
        const expired = lapsed.length;
        const quiet = this.sql.exec("SELECT runner FROM graph_runners WHERE seen_at < ? AND runner < ?", now - m.wakeAfterMs, m.runners).toArray();
        this.save({ ...m, expired: m.expired + expired, woken: m.woken + quiet.length });
        // A chunk nobody ever reports on would be leased until the deadline, at an invocation's CPU each time.
        const stuck = lapsed.find((c) => c.attempts >= (m.maxLeases ?? 5));
        if (stuck) {
            this.end("failed", `chunk ${stuck.id} was leased ${stuck.attempts} times and never reported: its runners were lost each time (a chunk that needs more CPU than one invocation has does this)`);
            return this.cleanUp(now);
        }
        // A quiet runner is started again, not only woken: its start may never have reached it.
        const quietSet = new Set(quiet.map((r) => r.runner));
        const wakes = expired > 0 ? this.takeWaiting(expired).filter((i) => !quietSet.has(i)) : [];
        await Promise.allSettled([...[...quietSet].map((i) => this.runner(i).graphRun(m.name, i, m.results, true)), ...wakes.map((i) => this.runner(i).graphWake())]);
        await this.ctx.storage.setAlarm(now + m.tickMs);
    }
    /** Up to `count` runners told to wait, no longer marked waiting: one for each chunk there is to take. */
    takeWaiting(count) {
        return this.sql
            .exec("UPDATE graph_runners SET waiting = 0 WHERE runner IN (SELECT runner FROM graph_runners WHERE waiting = 1 LIMIT ?) RETURNING runner", count)
            .toArray()
            .map((r) => r.runner);
    }
    /**
     * Wakes as many waiting runners as chunks just became ready, so a ready chunk does not wait for
     * a poll, and a runner with nothing to take is not woken for it. (The others poll.)
     */
    wakeWaiting(readied) {
        const runners = this.takeWaiting(readied);
        if (runners.length === 0)
            return;
        this.ctx.waitUntil(Promise.allSettled(runners.map((i) => this.runner(i).graphWake())));
    }
    resultsStore(name) {
        const ns = this.results();
        const jurisdiction = this.ctx.id.jurisdiction;
        return sharded(jurisdiction ? ns.jurisdiction(jurisdiction) : ns, name);
    }
    runner(index) {
        const ns = this.runners();
        const jurisdiction = this.ctx.id.jurisdiction;
        const scoped = jurisdiction ? ns.jurisdiction(jurisdiction) : ns;
        return scoped.get(scoped.idFromName(runnerName(this.meta.name, index)));
    }
    /** The run named `name`, if this object has one. */
    existing(name) {
        if (this.meta && this.meta.name !== name)
            throw new Error(`this coordinator runs ${this.meta.name}, not ${name}`);
        return this.meta;
    }
    /** The run named `name`, made by its first submission. */
    metaFor(name) {
        const existing = this.existing(name);
        if (existing)
            return existing;
        this.sql.exec(SCHEMA);
        const meta = { name, state: "submitting", runners: 0, results: "", leaseMs: 0, maxAttempts: 0, wakeAfterMs: 0, tickMs: 0, logChunks: false, computed: 0, reused: 0, replayed: 0, busyMs: 0, expired: 0, woken: 0, failures: 0, error: null, startedAt: null, finishedAt: null, ownResults: false, deadlineAt: Date.now() + UNSTARTED_MS, keepStatusMs: HOUR_MS, cleanup: null, keptUntil: null, final: null, deleting: false };
        this.save(meta);
        return meta;
    }
    save(meta) {
        this.meta = meta;
        this.sql.exec("INSERT INTO graph_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
    }
    liveCounts() {
        const counts = { waiting: 0, ready: 0, leased: 0, done: 0 };
        for (const row of this.sql.exec("SELECT state, COUNT(*) AS n FROM graph_chunks GROUP BY state"))
            counts[row.state] = row.n;
        return { total: counts.waiting + counts.ready + counts.leased + counts.done, ...counts };
    }
    status() {
        const m = this.meta;
        return {
            name: m.name,
            state: m.state,
            runners: m.runners,
            chunks: m.final ?? this.liveCounts(),
            units: { computed: m.computed, reused: m.reused, replayed: m.replayed },
            expired: m.expired,
            woken: m.woken,
            failures: m.failures,
            error: m.error,
            startedAt: m.startedAt,
            finishedAt: m.finishedAt,
            elapsedMs: m.startedAt !== null ? (m.finishedAt ?? Date.now()) - m.startedAt : null,
            busyMs: m.busyMs ?? 0,
            results: m.results,
            ownResults: m.ownResults,
            cleanup: m.cleanup,
            keptUntil: m.keptUntil,
            deadlineAt: m.deadlineAt,
        };
    }
}
/** `promise`, or a rejection after `ms`. */
function withTimeout(promise, ms) {
    let timer;
    return Promise.race([promise, new Promise((_, reject) => (timer = setTimeout(() => reject(new Error(`no answer in ${ms} ms`)), ms)))]).finally(() => clearTimeout(timer));
}
//# sourceMappingURL=coordinator.js.map