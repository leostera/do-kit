// The row side of an IndexedTable (RFD0003, specs/indexed-write.ts): a sharded table that writes the
// index changes of every row write to an outbox in the same transaction, delivers them to the index
// shards before the write returns, and fences the reservation tokens of unique values.
import { keyHash, sharded } from "../sharded/index.js";
import { ShardedTable } from "../table/index.js";
import { addressKey, entriesOf, entryOf, globalIndexes } from "./entries.js";
const OUTBOX = "indexed_outbox";
const FENCES = "indexed_fences";
/** How soon the alarm delivers what a write could not, after it started. */
const DELIVERY_RETRY_MS = 2_000;
/** Outbox entries delivered per call to the index shards. */
const DELIVERY_BATCH = 200;
const CLEANUP_BATCH = 250;
/**
 * Extend with `protected table = defineTable({ ..., globalIndexes })`, `nodes()` (this class's
 * binding) and `indexNodes()` (the binding of its GlobalIndex class). The client
 * (`indexedTable()`) reserves unique values, writes, looks up and lists.
 *
 * Custom transactional methods that write rows use `indexedTransact`, not `transact`: it delivers
 * the writes' index changes before returning. A custom write that changes a unique column's value
 * is refused (`refused: index`): those go through the client, which reserves first.
 *
 * Subclasses that override `onAlarm`, `afterClone` or `cleanupForeign` must call `super`.
 */
export class IndexedTable extends ShardedTable {
    /** How long a write waits for its index entries before it returns `IndexPending`. */
    indexWaitMs = 5_000;
    indexMigrated = false;
    batch = null;
    indexClient = null;
    /** Outbox entries being delivered now. */
    delivering = new Set();
    wakeAt = null;
    // ---- Called by GlobalIndex ----
    /**
     * Whether row `key` has an entry a reservation is pending for. If not, fences `token`, so no
     * write carrying it (or an older one) lands afterwards.
     */
    async indexResolve(keys, payloads) {
        this.migrate();
        return this.transact(keys, () => keys.map((key, i) => {
            const { route, entry, token } = payloads[i];
            const current = this.getRow(key);
            if (current && entriesOf(this.table, current.row).has(addressKey({ route, entry })))
                return { committed: true, version: current.version, row: current.row };
            this.raiseFence(key, route, entry, token);
            return { committed: false, version: current?.version ?? 0 };
        }));
    }
    // ---- Writes ----
    /**
     * Like `transact`, for custom methods that write rows: delivers their index changes before it
     * returns, and throws `IndexPending` if that takes longer than `indexWaitMs` (the rows are
     * written; the alarm goes on delivering).
     */
    async indexedTransact(keys, fn) {
        this.migrate();
        await this.wake(Date.now() + DELIVERY_RETRY_MS);
        const { result, ops } = await this.transact(keys, () => this.inBatch(new Map(), fn));
        const ids = [...ops.values()].flat();
        if (ids.length && !(await this.deliverWithin(ids, this.indexWaitMs)))
            throw new Error("IndexPending: rows written, index entries still being delivered");
        return result;
    }
    async writeBatch(keys, payloads, write) {
        this.migrate();
        // Before the transaction: an outbox left by a crash right after it is delivered by the alarm.
        await this.wake(Date.now() + DELIVERY_RETRY_MS);
        const tokens = new Map();
        keys.forEach((key, i) => tokens.set(key, payloads?.[i]?.tokens ?? {}));
        const { result, ops } = await this.transact(keys, () => this.inBatch(tokens, write));
        const ids = [...ops.values()].flat();
        const delivered = ids.length === 0 || (await this.deliverWithin(ids, this.indexWaitMs));
        return result.map((r, i) => (r.ok && ops.has(keys[i]) ? { ...r, indexed: delivered } : r));
    }
    inBatch(tokens, fn) {
        this.batch = { tokens, ops: new Map() };
        try {
            return { result: fn(), ops: this.batch.ops };
        }
        finally {
            this.batch = null;
        }
    }
    watchesRows() {
        return true;
    }
    keepsGlobalIndexes() {
        return true;
    }
    /** A unique value the row takes needs a reservation token above the fence. */
    refuseRowWrite(key, before, after) {
        if (!after)
            return null;
        for (const ix of this.uniqueIndexes()) {
            const next = entryOf(this.table, ix, after);
            if (!next)
                continue;
            const prev = before ? entryOf(this.table, ix, before) : null;
            if (prev && addressKey(prev) === addressKey(next))
                continue;
            const token = this.batch?.tokens.get(key)?.[ix.name];
            if (token === undefined || token <= this.fence(key, next.route, next.entry))
                return ix.name;
        }
        return null;
    }
    /** Appends the row's index changes to the outbox: a put for each entry it has, a remove for each it lost. */
    rowWritten(key, before, after, version) {
        const had = entriesOf(this.table, before);
        const has = entriesOf(this.table, after);
        const tokens = this.batch?.tokens.get(key) ?? {};
        const unique = new Set(this.uniqueIndexes().map((ix) => ix.name));
        const ops = [];
        for (const [id, e] of has) {
            let token = null;
            const given = tokens[e.index];
            if (unique.has(e.index) && given !== undefined && given > this.fence(key, e.route, e.entry)) {
                token = given;
                // Used: an older write carrying it, or an older token, cannot land after this one.
                this.raiseFence(key, e.route, e.entry, given);
            }
            ops.push({ kind: "put", route: e.route, entry: e.entry, owner: key, version, row: after, token });
            had.delete(id);
        }
        for (const e of had.values())
            ops.push({ kind: "remove", route: e.route, entry: e.entry, owner: key, version });
        const now = Date.now();
        const ids = ops.map((op) => this.store.exec(`INSERT INTO ${OUTBOX} (key, op, at) VALUES (?, ?, ?) RETURNING id`, key, JSON.stringify(op), now).one().id);
        if (this.batch && ids.length)
            this.batch.ops.set(key, [...(this.batch.ops.get(key) ?? []), ...ids]);
    }
    // ---- Delivery ----
    /** Delivers these outbox entries; true if they all were within `ms` (they go on being delivered either way). */
    async deliverWithin(ids, ms) {
        let timer;
        const delivery = this.deliver(ids).then(() => true, (error) => {
            console.error(JSON.stringify({ event: "index-delivery-failed", node: this.nodeLabel(), error: String(error) }));
            return false;
        });
        const timeout = new Promise((resolve) => (timer = setTimeout(() => resolve(false), ms)));
        const done = await Promise.race([delivery, timeout]);
        clearTimeout(timer);
        return done;
    }
    /**
     * Sends outbox entries to their index shards, and deletes them once applied. Entries being sent
     * by another delivery are waited for, not sent twice.
     */
    async deliver(ids) {
        const mine = ids.filter((id) => !this.delivering.has(id));
        if (mine.length === 0)
            return this.waitDelivered(ids);
        for (const id of mine)
            this.delivering.add(id);
        try {
            const rows = mine.length ? this.store.exec(`SELECT id, op FROM ${OUTBOX} WHERE id IN (${mine.map(() => "?").join(", ")})`, ...mine).toArray() : [];
            if (rows.length) {
                const ops = rows.map((r) => JSON.parse(r.op));
                await this.indexShards().call(ops.map((op) => op.route), "indexApply", ops);
                // Not fenced by a split: a copy drops the outbox it gets (see afterClone), so a delete
                // the copy misses changes nothing.
                this.store.exec(`DELETE FROM ${OUTBOX} WHERE id IN (${rows.map(() => "?").join(", ")})`, ...rows.map((r) => r.id));
            }
        }
        finally {
            for (const id of mine)
                this.delivering.delete(id);
        }
        await this.waitDelivered(ids);
    }
    /** Waits while another delivery sends some of these entries; throws if they are still in the outbox after. */
    async waitDelivered(ids) {
        while (ids.some((id) => this.delivering.has(id)))
            await new Promise((resolve) => setTimeout(resolve, 5));
        const left = this.store.exec(`SELECT COUNT(*) AS n FROM ${OUTBOX} WHERE id IN (${ids.map(() => "?").join(", ")})`, ...ids).one().n;
        if (left > 0)
            throw new Error(`${left} index change(s) not delivered`);
    }
    /** Delivers what is left in the outbox. Call `super.onAlarm()`. */
    async onAlarm() {
        this.migrate();
        this.wakeAt = null;
        for (;;) {
            const ids = this.store.exec(`SELECT id FROM ${OUTBOX} ORDER BY id LIMIT ?`, DELIVERY_BATCH).toArray().map((r) => r.id);
            if (ids.length === 0)
                return;
            try {
                await this.deliver(ids);
            }
            catch (error) {
                console.error(JSON.stringify({ event: "index-delivery-failed", node: this.nodeLabel(), error: String(error) }));
                await this.wake(Date.now() + DELIVERY_RETRY_MS);
                return;
            }
        }
    }
    // ---- Splits ----
    /** The copy drops the outbox: the original goes on delivering all of it. Call `super.afterClone`. */
    afterClone(owns) {
        this.migrate();
        this.store.exec(`DELETE FROM ${OUTBOX}`);
        super.afterClone(owns);
    }
    /** Drops the fences of keys given away (never the outbox: see afterClone). Call `super.cleanupForeign`. */
    cleanupForeign(owns, limit) {
        this.migrate();
        const [lo, hi] = this.ownedHashRange();
        const deleted = this.store.exec(`DELETE FROM ${FENCES} WHERE rowid IN (SELECT rowid FROM ${FENCES} WHERE hash < ? OR hash > ? LIMIT ?)`, lo, hi, Math.min(limit, CLEANUP_BATCH)).rowsWritten;
        return deleted + super.cleanupForeign(owns, limit);
    }
    // ---- Storage ----
    migrate() {
        super.migrate();
        if (this.indexMigrated)
            return;
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${OUTBOX} (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, op TEXT NOT NULL, at INTEGER NOT NULL)`);
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${FENCES} (key TEXT NOT NULL, route TEXT NOT NULL, entry TEXT NOT NULL, hash TEXT NOT NULL, token INTEGER NOT NULL, PRIMARY KEY (key, route, entry))`);
        this.store.exec(`CREATE INDEX IF NOT EXISTS ${FENCES}_hash ON ${FENCES} (hash)`);
        this.indexMigrated = true;
    }
    fence(key, route, entry) {
        return this.store.exec(`SELECT token FROM ${FENCES} WHERE key = ? AND route = ? AND entry = ?`, key, route, entry).toArray()[0]?.token ?? 0;
    }
    raiseFence(key, route, entry, token) {
        this.store.exec(`INSERT INTO ${FENCES} (key, route, entry, hash, token) VALUES (?, ?, ?, ?, ?) ON CONFLICT (key, route, entry) DO UPDATE SET token = max(token, excluded.token)`, key, route, entry, keyHash(key), token);
    }
    uniqueIndexes() {
        return globalIndexes(this.table).filter((ix) => ix.unique);
    }
    async wake(time) {
        if (this.wakeAt !== null && this.wakeAt <= time && this.wakeAt > Date.now())
            return;
        this.wakeAt = time;
        await this.scheduleAlarm(time);
    }
    indexShards() {
        if (!this.indexClient) {
            let ns = this.indexNodes();
            const jurisdiction = this.ctx.id.jurisdiction;
            if (jurisdiction)
                ns = ns.jurisdiction(jurisdiction);
            const options = this.stubOptions();
            this.indexClient = sharded(ns, this.objectName(), options ? { stubOptions: options } : {});
        }
        return this.indexClient;
    }
    get store() {
        return this.ctx.storage.sql;
    }
}
//# sourceMappingURL=table.js.map