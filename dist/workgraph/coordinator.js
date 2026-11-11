// A work graph run's coordinator (RFD0005; specs/work-graph-run.ts): one object per run, holding
// its chunks, what each needs, and their leases, in SQLite. Runners pull: each call reports the
// chunk the runner finished and leases it the next. A lease that runs out goes back to ready, and a
// runner that has gone quiet is woken, from the alarm.
import { DurableObject } from "cloudflare:workers";
const SCHEMA = `
CREATE TABLE IF NOT EXISTS graph_chunks (
  id INTEGER PRIMARY KEY, round INTEGER NOT NULL, cost REAL NOT NULL, body TEXT NOT NULL,
  state TEXT NOT NULL DEFAULT 'waiting', needs INTEGER NOT NULL DEFAULT 0,
  runner INTEGER, lease_until INTEGER, attempts INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS graph_chunks_ready ON graph_chunks (state, round, id);
CREATE INDEX IF NOT EXISTS graph_chunks_runner ON graph_chunks (runner) WHERE state = 'leased';
-- (needed chunk, chunk that needs it)
CREATE TABLE IF NOT EXISTS graph_needs (needed INTEGER NOT NULL, chunk INTEGER NOT NULL, PRIMARY KEY (needed, chunk)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS graph_runners (runner INTEGER PRIMARY KEY, seen_at INTEGER NOT NULL, waiting INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS graph_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
`;
/** The object name of runner `index` of a run. */
export const runnerName = (run, index) => `${run}/runner/${index}`;
/**
 * A run's coordinator. A subclass gives `runners()`, the binding of its runner class. Name each
 * coordinator object by its run.
 */
export class WorkGraphCoordinator extends DurableObject {
    sql;
    meta;
    constructor(ctx, env) {
        super(ctx, env);
        this.sql = ctx.storage.sql;
        this.sql.exec(SCHEMA);
        const row = this.sql.exec("SELECT value FROM graph_meta WHERE key = 'meta'").toArray()[0];
        this.meta = row ? JSON.parse(row.value) : null;
    }
    /** Adds chunks to a run being submitted (idempotent per chunk id). */
    async graphSubmit(name, chunks) {
        const meta = this.metaFor(name);
        if (meta.state !== "submitting")
            throw new Error(`run ${name} is ${meta.state}: it takes no more chunks`);
        this.ctx.storage.transactionSync(() => {
            for (const c of chunks) {
                this.sql.exec("INSERT OR IGNORE INTO graph_chunks (id, round, cost, body) VALUES (?, ?, ?, ?)", c.id, c.round, c.cost, JSON.stringify({ units: c.units, exports: c.exports }));
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
        const meta = this.metaFor(name);
        if (meta.state === "submitting")
            throw new Error(`run ${name} is not sealed`);
        if (meta.state !== "ready")
            return this.status();
        if (!(options.runners >= 1))
            throw new Error("a run needs at least one runner");
        const now = Date.now();
        const started = { ...meta, state: "running", runners: options.runners, results: options.results, leaseMs: options.leaseMs ?? 120_000, maxAttempts: options.maxAttempts ?? 3, wakeAfterMs: options.wakeAfterMs ?? 30_000, tickMs: options.tickMs ?? 5_000, logChunks: options.logChunks ?? false, startedAt: now };
        this.save(started);
        this.finishIfDone(); // an empty run
        for (let i = 0; i < options.runners; i++)
            this.sql.exec("INSERT OR REPLACE INTO graph_runners (runner, seen_at, waiting) VALUES (?, ?, 0)", i, now);
        await this.ctx.storage.setAlarm(now + started.tickMs);
        await Promise.all(Array.from({ length: options.runners }, (_, i) => this.runner(i).graphRun(name, i, options.results)));
        return this.status();
    }
    /** Records what runner `runner` finished, if anything, and tells it what to do next. */
    async graphNext(runner, finished) {
        const meta = this.meta;
        if (!meta)
            return { failed: "no such run" };
        const now = Date.now();
        this.sql.exec("INSERT INTO graph_runners (runner, seen_at, waiting) VALUES (?, ?, 0) ON CONFLICT (runner) DO UPDATE SET seen_at = excluded.seen_at, waiting = 0", runner, now);
        if (finished) {
            if ("error" in finished)
                this.fail(finished.chunk, finished.error);
            else
                this.complete(finished);
        }
        const m = this.meta;
        if (m.state === "failed")
            return { failed: m.error ?? "the run failed" };
        if (m.state === "done")
            return { done: true };
        return this.lease(runner, now);
    }
    async graphStatus() {
        return this.meta ? this.status() : null;
    }
    /** The runner's own lease back (it was evicted while computing it), or the next ready chunk. */
    lease(runner, now) {
        const meta = this.meta;
        const own = this.sql.exec("SELECT id, attempts, body FROM graph_chunks WHERE state = 'leased' AND runner = ? LIMIT 1", runner).toArray()[0];
        if (own) {
            this.sql.exec("UPDATE graph_chunks SET lease_until = ? WHERE id = ?", now + meta.leaseMs, own.id);
            return { chunk: own.id, attempt: own.attempts, body: own.body };
        }
        const next = this.sql.exec("SELECT id, attempts, body FROM graph_chunks WHERE state = 'ready' ORDER BY round, id LIMIT 1").toArray()[0];
        if (next) {
            this.sql.exec("UPDATE graph_chunks SET state = 'leased', runner = ?, lease_until = ?, attempts = attempts + 1 WHERE id = ?", runner, now + meta.leaseMs, next.id);
            return { chunk: next.id, attempt: next.attempts + 1, body: next.body };
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
            this.save({ ...m, computed: m.computed + f.computed, reused: m.reused + f.reused, replayed: m.replayed + f.replayed });
            this.finishIfDone();
        });
        if (this.meta.logChunks)
            console.log(JSON.stringify({ event: "graph-chunk", run: this.meta.name, chunk: f.chunk, attempt: f.attempt, computed: f.computed, reused: f.reused, replayed: f.replayed, wallMs: f.wallMs }));
        if (readied > 0)
            this.wakeWaiting();
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
            if (failures >= m.maxAttempts)
                this.save({ ...m, failures: m.failures + 1, state: "failed", error: `chunk ${chunk} failed ${failures} times: ${error}`, finishedAt: Date.now() });
            else
                this.save({ ...m, failures: m.failures + 1 });
        });
    }
    finishIfDone() {
        const m = this.meta;
        if (m.state !== "running")
            return;
        const left = this.sql.exec("SELECT COUNT(*) AS n FROM graph_chunks WHERE state != 'done'").one().n;
        if (left === 0)
            this.save({ ...m, state: "done", finishedAt: Date.now() });
    }
    /** Leases that ran out go back to ready; quiet runners are woken; the alarm stops once the run ends. */
    async alarm() {
        const m = this.meta;
        if (!m || m.state !== "running")
            return;
        const now = Date.now();
        const expired = this.sql.exec("UPDATE graph_chunks SET state = 'ready', runner = NULL, lease_until = NULL WHERE state = 'leased' AND lease_until < ? RETURNING id", now).toArray().length;
        const quiet = this.sql.exec("SELECT runner FROM graph_runners WHERE seen_at < ? AND runner < ?", now - m.wakeAfterMs, m.runners).toArray();
        this.save({ ...m, expired: m.expired + expired, woken: m.woken + quiet.length });
        const wakes = quiet.map((r) => r.runner);
        if (expired > 0)
            wakes.push(...this.waitingRunners());
        await Promise.allSettled([...new Set(wakes)].map((i) => this.runner(i).graphWake()));
        await this.ctx.storage.setAlarm(now + m.tickMs);
    }
    waitingRunners() {
        return this.sql
            .exec("SELECT runner FROM graph_runners WHERE waiting = 1")
            .toArray()
            .map((r) => r.runner);
    }
    /** Wakes the runners told to wait, so a chunk that just became ready does not wait for their poll. */
    wakeWaiting() {
        const runners = this.waitingRunners();
        if (runners.length === 0)
            return;
        this.sql.exec("UPDATE graph_runners SET waiting = 0 WHERE waiting = 1");
        this.ctx.waitUntil(Promise.allSettled(runners.map((i) => this.runner(i).graphWake())));
    }
    runner(index) {
        const ns = this.runners();
        const jurisdiction = this.ctx.id.jurisdiction;
        const scoped = jurisdiction ? ns.jurisdiction(jurisdiction) : ns;
        return scoped.get(scoped.idFromName(runnerName(this.meta.name, index)));
    }
    metaFor(name) {
        if (this.meta) {
            if (this.meta.name !== name)
                throw new Error(`this coordinator runs ${this.meta.name}, not ${name}`);
            return this.meta;
        }
        const meta = { name, state: "submitting", runners: 0, results: "", leaseMs: 0, maxAttempts: 0, wakeAfterMs: 0, tickMs: 0, logChunks: false, computed: 0, reused: 0, replayed: 0, expired: 0, woken: 0, failures: 0, error: null, startedAt: null, finishedAt: null };
        this.save(meta);
        return meta;
    }
    save(meta) {
        this.meta = meta;
        this.sql.exec("INSERT INTO graph_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
    }
    status() {
        const m = this.meta;
        const counts = { waiting: 0, ready: 0, leased: 0, done: 0 };
        for (const row of this.sql.exec("SELECT state, COUNT(*) AS n FROM graph_chunks GROUP BY state"))
            counts[row.state] = row.n;
        const total = counts.waiting + counts.ready + counts.leased + counts.done;
        return {
            name: m.name,
            state: m.state,
            runners: m.runners,
            chunks: { total, ...counts },
            units: { computed: m.computed, reused: m.reused, replayed: m.replayed },
            expired: m.expired,
            woken: m.woken,
            failures: m.failures,
            error: m.error,
            startedAt: m.startedAt,
            finishedAt: m.finishedAt,
            elapsedMs: m.startedAt !== null ? (m.finishedAt ?? Date.now()) - m.startedAt : null,
        };
    }
}
//# sourceMappingURL=coordinator.js.map