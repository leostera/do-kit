// The index side of an IndexedTable (RFD0003, specs/indexed-write.ts): the entries of every global
// index of one table, on a sharded object of their own, routed by index name and partition values.
// Entries are derived from the rows: a row shard delivers them, and settles any reservation the
// index cannot decide alone.
import { keyHash, sharded, ShardedObject } from "../sharded/index.js";
const ENTRIES = "global_index_entries";
const META = "global_index_meta";
const COLUMN = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
/** Rounds of resolving one reservation before a reserve answers "contended". */
const RESOLVE_ROUNDS = 2;
const CLEANUP_BATCH = 250;
/** Stale reservations resolved per alarm. */
const RESOLVE_BATCH = 50;
/**
 * Extend with `nodes()` (this class's binding) and `rowNodes()` (the IndexedTable's binding):
 *
 *     export class UserIndexes extends GlobalIndex<Env> {
 *       protected nodes() { return this.env.USER_INDEXES; }
 *       protected rowNodes() { return this.env.USERS; }
 *     }
 *
 * Subclasses that override `onAlarm` or `cleanupForeign` must call `super`.
 */
export class GlobalIndex extends ShardedObject {
    /** How long a reservation may stay pending before the alarm asks its row shard. */
    reservationTimeoutMs = 5_000;
    /** How long an entry a row let go of is kept, so a late older change cannot bring it back. */
    tombstoneMs = 60 * 60_000;
    migrated = false;
    rowClient = null;
    /** Reservations being resolved: one question per entry at a time. */
    resolving = new Map();
    wakeAt = null;
    stats = { committed: 0, aborted: 0, applied: 0, ignored: 0 };
    // ---- Called by the client and by row shards ----
    /** Reserves unique values for their rows: a token, or the row that has the value, or "contended". */
    async indexReserve(routes, payloads) {
        this.migrate();
        return Promise.all(routes.map((route, i) => this.reserve(route, payloads[i].entry, payloads[i].owner)));
    }
    /** Applies changes delivered by row shards, idempotently and by version. */
    async indexApply(routes, ops) {
        this.migrate();
        const now = Date.now();
        await this.transact(routes, () => {
            for (const op of ops)
                this.apply(op, now);
        });
        if (ops.some((op) => op.kind === "remove"))
            await this.wake(now + this.tombstoneMs);
        return ops.map(() => null);
    }
    /** The rows of unique entries (null: none, or only reserved). */
    async indexLookup(routes, payloads) {
        this.migrate();
        this.assertOwns(routes);
        return routes.map((route, i) => {
            const e = this.get(route, payloads[i].entry);
            return e && !e.removed && e.version > 0 ? { row: JSON.parse(e.row), version: e.version } : null;
        });
    }
    /** One page of the entries of one partition, in entry order. */
    async indexPage(routes, payloads) {
        this.migrate();
        this.assertOwns(routes);
        return routes.map((route, i) => this.page(route, payloads[i]));
    }
    /** Counts, for tests and operators. */
    async indexStats() {
        this.migrate();
        this.assertOwns([]);
        const [lo, hi] = this.ownedHashRange();
        const n = (where) => this.db.exec(`SELECT COUNT(*) AS n FROM ${ENTRIES} WHERE hash >= ? AND hash <= ? AND ${where}`, lo, hi).one().n;
        const { committed, aborted, applied, ignored } = this.stats;
        return { entries: n("removed = 0 AND version > 0"), pending: n("pending IS NOT NULL"), removed: n("removed = 1"), resolved: { committed, aborted }, applied, ignored };
    }
    // ---- Reservations ----
    async reserve(route, entry, owner) {
        for (let round = 0;; round++) {
            const now = Date.now();
            const decided = await this.transact([route], () => {
                const e = this.get(route, entry);
                const fresh = () => {
                    const token = this.nextToken();
                    this.db.exec(`INSERT INTO ${ENTRIES} (route, entry, hash, owner, version, row, pending, issued, removed, floor, fenced, at) VALUES (?, ?, ?, ?, 0, NULL, ?, ?, 0, 0, 0, ?) ` +
                        `ON CONFLICT (route, entry) DO UPDATE SET owner = excluded.owner, version = 0, row = NULL, pending = excluded.pending, issued = excluded.issued, removed = 0, floor = 0, fenced = 0, at = excluded.at`, route, entry, keyHash(route), owner, token, token, now);
                    return { reserved: token };
                };
                if (!e)
                    return fresh();
                if (e.owner === owner) {
                    const token = this.nextToken();
                    this.db.exec(`UPDATE ${ENTRIES} SET pending = ?, issued = ?, fenced = 0, at = ? WHERE route = ? AND entry = ?`, token, token, now, route, entry);
                    return { reserved: token };
                }
                // Another row's. It passes on only once that row let go of it and every token it was
                // issued is fenced: a write still in flight with one must not land after the hand-over.
                if (e.removed && e.pending === null && e.fenced)
                    return fresh();
                if (!e.removed && e.pending === null && e.version > 0)
                    return { taken: e.owner };
                return { resolve: { owner: e.owner, token: e.issued } };
            });
            if (!("resolve" in decided)) {
                if ("reserved" in decided)
                    await this.wake(now + this.reservationTimeoutMs);
                return decided;
            }
            if (round >= RESOLVE_ROUNDS)
                return { contended: true };
            await this.resolve(route, entry, decided.resolve.owner, decided.resolve.token);
        }
    }
    /** Asks the row shard of `owner` whether its row has the entry, and applies the answer. */
    resolve(route, entry, owner, token) {
        const id = `${route} ${entry}`;
        const running = this.resolving.get(id);
        if (running)
            return running;
        const done = (async () => {
            const [answer] = (await this.rows().call([owner], "indexResolve", [{ route, entry, token }]))[0].result;
            await this.transact([route], () => this.settle(route, entry, owner, token, answer));
        })().finally(() => this.resolving.delete(id));
        this.resolving.set(id, done);
        return done;
    }
    settle(route, entry, owner, token, answer) {
        const e = this.get(route, entry);
        if (!e || e.owner !== owner)
            return;
        if (answer.committed) {
            this.stats.committed++;
            const pending = e.pending !== null && e.pending <= token ? null : e.pending;
            if (answer.version > e.version && answer.version > e.floor)
                this.db.exec(`UPDATE ${ENTRIES} SET pending = ?, version = ?, row = ?, removed = 0, at = ? WHERE route = ? AND entry = ?`, pending, answer.version, JSON.stringify(answer.row), Date.now(), route, entry);
            else
                this.db.exec(`UPDATE ${ENTRIES} SET pending = ? WHERE route = ? AND entry = ?`, pending, route, entry);
            return;
        }
        // Every token up to `token` is fenced; one issued since is still to be resolved.
        if (token < e.issued)
            return;
        this.stats.aborted++;
        this.db.exec(`UPDATE ${ENTRIES} SET pending = NULL, removed = 1, fenced = 1, floor = max(floor, ?), at = ? WHERE route = ? AND entry = ?`, answer.version, Date.now(), route, entry);
    }
    // ---- Delivered changes ----
    apply(op, now) {
        const e = this.get(op.route, op.entry);
        if (op.kind === "put") {
            if (!e) {
                this.db.exec(`INSERT INTO ${ENTRIES} (route, entry, hash, owner, version, row, pending, issued, removed, floor, fenced, at) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 0, 0, 0, ?)`, op.route, op.entry, keyHash(op.route), op.owner, op.version, JSON.stringify(op.row), op.token ?? 0, now);
                this.stats.applied++;
                return;
            }
            // Another row's entry: this change is older than the hand-over (its tokens are fenced).
            if (e.owner !== op.owner)
                return void this.stats.ignored++;
            const newer = op.version > e.version && op.version > e.floor;
            const pending = op.token !== null && e.pending !== null && op.token >= e.pending ? null : e.pending;
            if (newer)
                this.db.exec(`UPDATE ${ENTRIES} SET version = ?, row = ?, removed = 0, pending = ?, at = ? WHERE route = ? AND entry = ?`, op.version, JSON.stringify(op.row), pending, now, op.route, op.entry);
            else if (pending !== e.pending)
                this.db.exec(`UPDATE ${ENTRIES} SET pending = ? WHERE route = ? AND entry = ?`, pending, op.route, op.entry);
            if (newer)
                this.stats.applied++;
            else
                this.stats.ignored++;
            return;
        }
        if (!e) {
            // A remove before its put: a tombstone, so the put is ignored when it comes.
            this.db.exec(`INSERT INTO ${ENTRIES} (route, entry, hash, owner, version, row, pending, issued, removed, floor, fenced, at) VALUES (?, ?, ?, ?, 0, NULL, NULL, 0, 1, ?, 0, ?)`, op.route, op.entry, keyHash(op.route), op.owner, op.version, now);
            this.stats.applied++;
            return;
        }
        if (e.owner !== op.owner || op.version <= e.version)
            return void this.stats.ignored++;
        this.db.exec(`UPDATE ${ENTRIES} SET removed = 1, floor = max(floor, ?), at = ? WHERE route = ? AND entry = ?`, op.version, now, op.route, op.entry);
        this.stats.applied++;
    }
    // ---- Listings ----
    page(route, request) {
        const params = [];
        const matches = request.where.length ? request.where.map((c) => condition(c, params)).join(" AND ") : "1";
        const after = request.after === null ? "" : `AND entry ${request.direction === "asc" ? ">" : "<"} ? `;
        const sql = `SELECT entry, row, version, (${matches}) AS _m FROM ${ENTRIES} WHERE route = ? AND removed = 0 AND version > 0 ${after}` +
            `ORDER BY entry ${request.direction === "asc" ? "ASC" : "DESC"} LIMIT ?`;
        const rows = [];
        let last = null;
        let scanned = 0;
        for (const found of this.db.exec(sql, ...params, route, ...(request.after === null ? [] : [request.after]), request.budget)) {
            scanned++;
            last = found.entry;
            if (found._m) {
                rows.push({ row: JSON.parse(found.row), version: found.version, entry: found.entry });
                if (rows.length === request.limit)
                    return { rows, frontier: last };
            }
        }
        return { rows, frontier: scanned === request.budget ? last : null };
    }
    // ---- Alarms and splits ----
    /** Resolves reservations left pending, and drops tombstones past their time. Call `super.onAlarm()`. */
    async onAlarm() {
        this.migrate();
        const now = Date.now();
        const [lo, hi] = this.ownedHashRange();
        const stale = this.db
            .exec(`SELECT * FROM ${ENTRIES} WHERE pending IS NOT NULL AND at <= ? AND hash >= ? AND hash <= ? LIMIT ?`, now - this.reservationTimeoutMs, lo, hi, RESOLVE_BATCH)
            .toArray();
        const results = await Promise.allSettled(stale.map((e) => this.resolve(e.route, e.entry, e.owner, e.issued)));
        for (const r of results)
            if (r.status === "rejected")
                console.error(JSON.stringify({ event: "index-resolve-failed", node: this.nodeLabel(), error: String(r.reason) }));
        this.db.exec(`DELETE FROM ${ENTRIES} WHERE rowid IN (SELECT rowid FROM ${ENTRIES} WHERE removed = 1 AND pending IS NULL AND at < ? LIMIT 1000)`, now - this.tombstoneMs);
        const next = this.db.exec(`SELECT (SELECT min(at) FROM ${ENTRIES} WHERE pending IS NOT NULL) AS pending, (SELECT min(at) FROM ${ENTRIES} WHERE removed = 1 AND pending IS NULL) AS removed`).one();
        this.wakeAt = null;
        const times = [next.pending === null ? null : Math.max(next.pending + this.reservationTimeoutMs, now + 1_000), next.removed === null ? null : next.removed + this.tombstoneMs].filter((t) => t !== null);
        if (times.length)
            await this.wake(Math.min(...times));
    }
    /** Deletes entries of routes this shard gave away. Call `super.cleanupForeign`. */
    cleanupForeign(owns, limit) {
        this.migrate();
        const [lo, hi] = this.ownedHashRange();
        const deleted = this.db.exec(`DELETE FROM ${ENTRIES} WHERE rowid IN (SELECT rowid FROM ${ENTRIES} WHERE hash < ? OR hash > ? LIMIT ?)`, lo, hi, Math.min(limit, CLEANUP_BATCH)).rowsWritten;
        return deleted + super.cleanupForeign(owns, limit);
    }
    // ---- Storage ----
    migrate() {
        if (this.migrated)
            return;
        // A rowid table: the cleanup and tombstone deletes page by rowid.
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${ENTRIES} (route TEXT NOT NULL, entry TEXT NOT NULL, hash TEXT NOT NULL, owner TEXT NOT NULL, version INTEGER NOT NULL, row TEXT, ` +
            `pending INTEGER, issued INTEGER NOT NULL, removed INTEGER NOT NULL, floor INTEGER NOT NULL, fenced INTEGER NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (route, entry))`);
        this.db.exec(`CREATE INDEX IF NOT EXISTS ${ENTRIES}_hash ON ${ENTRIES} (hash)`);
        this.db.exec(`CREATE INDEX IF NOT EXISTS ${ENTRIES}_pending ON ${ENTRIES} (at) WHERE pending IS NOT NULL`);
        this.db.exec(`CREATE INDEX IF NOT EXISTS ${ENTRIES}_removed ON ${ENTRIES} (at) WHERE removed = 1`);
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${META} (name TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL) WITHOUT ROWID`);
        this.migrated = true;
    }
    get(route, entry) {
        return this.db.exec(`SELECT * FROM ${ENTRIES} WHERE route = ? AND entry = ?`, route, entry).toArray()[0] ?? null;
    }
    /** Tokens only grow, on this shard and on any split from it (the counter is copied). */
    nextToken() {
        return this.db.exec(`INSERT INTO ${META} (name, value) VALUES ('token', 1) ON CONFLICT (name) DO UPDATE SET value = value + 1 RETURNING value`).one().value;
    }
    /** Asks for the alarm at `time`, unless it is asked for earlier already. */
    async wake(time) {
        if (this.wakeAt !== null && this.wakeAt <= time && this.wakeAt > Date.now())
            return;
        this.wakeAt = time;
        await this.scheduleAlarm(time);
    }
    rows() {
        if (!this.rowClient) {
            let ns = this.rowNodes();
            const jurisdiction = this.ctx.id.jurisdiction;
            if (jurisdiction)
                ns = ns.jurisdiction(jurisdiction);
            const options = this.stubOptions();
            this.rowClient = sharded(ns, this.objectName(), options ? { stubOptions: options } : {});
        }
        return this.rowClient;
    }
    get db() {
        return this.ctx.storage.sql;
    }
}
/** A condition on a row copy's column, its value encoded as the rows store it (booleans 0/1). */
function condition(c, params) {
    if (!COLUMN.test(c.column))
        throw new Error(`no column ${c.column}`);
    const col = `json_extract(row, '$.${c.column}')`;
    switch (c.op) {
        case "isNull":
            return `${col} IS NULL`;
        case "notNull":
            return `${col} IS NOT NULL`;
        case "prefix":
            params.push(String(c.value));
            return `instr(${col}, ?) = 1`;
        case "contains":
            params.push(String(c.value));
            return `instr(lower(${col}), lower(?)) > 0`;
        case "=":
        case "!=":
        case "<":
        case "<=":
        case ">":
        case ">=":
            params.push(c.value);
            return `${col} ${c.op} ?`;
        default:
            throw new Error(`unknown op ${String(c.op)}`);
    }
}
//# sourceMappingURL=global-index.js.map