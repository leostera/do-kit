// The row side of an IndexedTable (RFD0003, specs/indexed-write.ts): a sharded table that writes the
// index changes of every row write to an outbox in the same transaction, delivers them to the index
// shards before the write returns, and fences the reservation tokens of unique values.
import { keyHash, sharded } from "../sharded/index.js";
import { ShardedTable } from "../table/index.js";
import { addressKey, entriesOf, entryOf, globalIndexes, indexIdOfRoute, indexNameOfId, routeRangeOf } from "./entries.js";
const OUTBOX = "indexed_outbox";
/** The global indexes this shard has seen, and how far each is built over the rows it had before it. */
const GLOBALS = "indexed_globals";
/** Rows a backfill could not put in a unique index: another row has their value. */
const DUPLICATES = "indexed_duplicates";
/** Ids of global indexes the schema no longer has (dropped or redefined), until the index shards drop their entries. */
const DROPS = "indexed_drops";
/** Rows a backfill reads at a time, and pages per alarm. */
const BACKFILL_ROWS = 200;
const BACKFILL_PAGES = 10;
const FENCES = "indexed_fences";
/** How soon the alarm delivers what a write could not, after it started. */
const DELIVERY_RETRY_MS = 2_000;
/**
 * How far ahead a write asks for the alarm, in case it never gets to deliver what it wrote (a
 * crash right after its transaction). Far enough that a shard taking writes sets its alarm now
 * and then, not with every write; a delivery that fails asks for it DELIVERY_RETRY_MS ahead.
 */
const SAFETY_ALARM_MS = 30_000;
/** Outbox entries the alarm picks up at a time. */
const DELIVERY_BATCH = 200;
/**
 * Deliveries are coalesced: writes queue their outbox entries, and up to MAX_SENDS calls to the
 * index shards send what is queued, up to SEND_BATCH entries each. Under load, one call carries
 * the entries of many writes.
 */
const MAX_SENDS = 4;
const SEND_BATCH = 500;
function waiter() {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => ((resolve = res), (reject = rej)));
    // A write that stopped waiting (IndexPending) leaves nobody to see the failure: the alarm retries.
    promise.catch(() => { });
    return { promise, resolve, reject };
}
const CLEANUP_BATCH = 250;
/** Ids per `IN (…)`: SQLite in a Durable Object takes at most 100 bound parameters. */
const IDS_PER_QUERY = 90;
/** `ids` in slices that fit one query. */
function slices(ids) {
    const out = [];
    for (let i = 0; i < ids.length; i += IDS_PER_QUERY)
        out.push(ids.slice(i, i + IDS_PER_QUERY));
    return out;
}
const marks = (ids) => ids.map(() => "?").join(", ");
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
    constructor(ctx, env) {
        super(ctx, env);
        // Index shards reach row shards with a client of their own, which knows no routeOf.
        if (!this.routesByKey())
            throw new Error("an IndexedTable routes rows by their keys: routeOf is not supported");
    }
    /** How long a write waits for its index entries before it returns `IndexPending`. */
    indexWaitMs = 5_000;
    indexMigrated = false;
    batch = null;
    indexClient = null;
    /** Outbox entries queued or being sent, and who waits for them. */
    waiting = new Map();
    /** Outbox entries queued, not sent yet, oldest first. */
    queued = [];
    /** Calls to the index shards in flight. */
    sends = 0;
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
        await this.wake(Date.now() + SAFETY_ALARM_MS);
        const { result, ops } = await this.transact(keys, () => this.inBatch(new Map(), fn));
        const ids = [...ops.values()].flat();
        if (ids.length && !(await this.deliverWithin(ids, this.indexWaitMs))) {
            await this.wake(Date.now() + DELIVERY_RETRY_MS);
            throw new Error("IndexPending: rows written, index entries still being delivered");
        }
        return result;
    }
    async writeBatch(keys, payloads, write) {
        this.migrate();
        // Before the transaction: an outbox left by a crash right after it is delivered by the alarm.
        await this.wake(Date.now() + SAFETY_ALARM_MS);
        const tokens = new Map();
        keys.forEach((key, i) => tokens.set(key, payloads?.[i]?.tokens ?? {}));
        const { result, ops } = await this.transact(keys, () => this.inBatch(tokens, write));
        const ids = [...ops.values()].flat();
        const delivered = ids.length === 0 || (await this.deliverWithin(ids, this.indexWaitMs));
        // Not delivered in time: the alarm takes over soon, not at the safety alarm's time.
        if (!delivered)
            await this.wake(Date.now() + DELIVERY_RETRY_MS);
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
        // A write settles what a backfill found: its own reservations decide from here on.
        if (this.hasDuplicates) {
            this.store.exec(`DELETE FROM ${DUPLICATES} WHERE key = ?`, key);
            this.hasDuplicates = this.store.exec(`SELECT 1 FROM ${DUPLICATES} LIMIT 1`).toArray().length > 0;
        }
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
            // No copy of the row: it is read when the put is sent (see send).
            ops.push({ kind: "put", route: e.route, entry: e.entry, owner: key, version, token });
            had.delete(id);
        }
        for (const e of had.values())
            ops.push({ kind: "remove", route: e.route, entry: e.entry, owner: key, version });
        const now = Date.now();
        const hash = keyHash(key);
        const ids = ops.map((op) => this.store.exec(`INSERT INTO ${OUTBOX} (key, hash, op, at) VALUES (?, ?, ?, ?) RETURNING id`, key, hash, JSON.stringify(op), now).one().id);
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
    /** Delivers these outbox entries: queues them (once), and resolves when every one is applied. */
    deliver(ids) {
        const waits = ids.map((id) => {
            let w = this.waiting.get(id);
            if (!w) {
                this.waiting.set(id, (w = waiter()));
                this.queued.push(id);
            }
            return w.promise;
        });
        this.pump();
        return Promise.all(waits).then(() => undefined);
    }
    /** Starts sends while there is something queued and room for another call. */
    pump() {
        while (this.sends < MAX_SENDS && this.queued.length > 0) {
            const batch = this.queued.splice(0, SEND_BATCH);
            this.sends++;
            void this.send(batch).finally(() => {
                this.sends--;
                this.pump();
            });
        }
    }
    /** One call: sends these entries to their index shards, and deletes them once applied. */
    async send(ids) {
        try {
            const rows = slices(ids).flatMap((part) => this.store.exec(`SELECT id, op FROM ${OUTBOX} WHERE id IN (${marks(part)})`, ...part).toArray());
            if (rows.length) {
                const ops = this.onTheWire(rows.map((r) => JSON.parse(r.op)));
                if (ops.length) {
                    for (const group of await this.indexShards().call(ops.map((op) => op.route), "indexApply", ops)) {
                        group.result.forEach((answer, j) => {
                            const op = ops[group.positions[j]];
                            if (answer && op.kind === "put" && op.backfill)
                                this.hasDuplicates = true;
                            if (answer && op.kind === "put" && op.backfill)
                                this.store.exec(`INSERT OR REPLACE INTO ${DUPLICATES} (ix, key, owner) VALUES (?, ?, ?)`, indexNameOfId(indexIdOfRoute(op.route)), op.owner, answer.taken);
                        });
                    }
                }
                // Not fenced by a split: a copy drops the outbox it gets (see afterClone), so a delete
                // the copy misses changes nothing.
                for (const part of slices(rows.map((r) => r.id)))
                    this.store.exec(`DELETE FROM ${OUTBOX} WHERE id IN (${marks(part)})`, ...part);
            }
            // Entries not found were delivered already (by an earlier send).
            for (const id of ids)
                this.waiting.get(id)?.resolve();
        }
        catch (error) {
            for (const id of ids)
                this.waiting.get(id)?.reject(error);
        }
        finally {
            for (const id of ids)
                this.waiting.delete(id);
        }
    }
    /**
     * The changes to send for these outbox entries. A put goes with the row as it is now, if it is
     * still at the put's version. If a later write superseded it (or deleted the row), that write's
     * changes cover the entries: the put goes as a confirm of its token, or not at all.
     */
    onTheWire(stored) {
        const rows = new Map();
        const out = [];
        const live = this.liveIndexIds();
        for (const op of stored) {
            // Of an index the schema no longer has: its entries are being dropped, not changed.
            if (!live.has(indexIdOfRoute(op.route)))
                continue;
            if (op.kind !== "put") {
                out.push(op);
                continue;
            }
            if (!rows.has(op.owner))
                rows.set(op.owner, this.getRow(op.owner));
            const current = rows.get(op.owner);
            if (current && current.version === op.version)
                out.push({ ...op, row: current.row });
            else if (op.token !== null)
                out.push({ kind: "confirm", route: op.route, entry: op.entry, owner: op.owner, version: op.version, token: op.token });
        }
        return out;
    }
    /**
     * Delivers what writes left in the outbox: entries older than DELIVERY_RETRY_MS that no delivery
     * is sending (a write that crashed, or gave up waiting and failed). Younger entries belong to
     * writes still delivering them. Call `super.onAlarm()`.
     */
    async onAlarm() {
        this.migrate();
        this.wakeAt = null;
        // This shard's own keys only. Entries of keys a split gave away went with the copy, which
        // sends them from the rows as they are now; this shard's rows of those keys stopped changing
        // at the split, and a put sent from one much later could bring back an entry its row has let
        // go of since.
        const [lo, hi] = this.ownedHashRange();
        for (;;) {
            const old = Date.now() - DELIVERY_RETRY_MS;
            const ids = this.store
                .exec(`SELECT id FROM ${OUTBOX} WHERE at <= ? AND hash >= ? AND hash <= ? ORDER BY id LIMIT ?`, old, lo, hi, DELIVERY_BATCH + this.waiting.size)
                .toArray()
                .map((r) => r.id)
                .filter((id) => !this.waiting.has(id))
                .slice(0, DELIVERY_BATCH);
            if (ids.length === 0)
                break;
            try {
                await this.deliver(ids);
            }
            catch (error) {
                console.error(JSON.stringify({ event: "index-delivery-failed", node: this.nodeLabel(), error: String(error) }));
                break;
            }
        }
        if (!(await this.sendDrops()))
            await this.wake(Date.now() + DELIVERY_RETRY_MS);
        if (await this.backfill())
            return void (await this.wake(Date.now()));
        // Whatever is still there: due once it is old enough (and in flight now, most likely).
        const oldest = this.store.exec(`SELECT min(at) AS at FROM ${OUTBOX} WHERE hash >= ? AND hash <= ?`, lo, hi).one().at;
        if (oldest !== null)
            await this.wake(Math.max(oldest + DELIVERY_RETRY_MS, Date.now() + DELIVERY_RETRY_MS));
    }
    // ---- Global indexes added to a table that has rows ----
    /** How far this shard's global indexes are built, and the rows a unique one could not take. */
    async indexBuildStatus() {
        this.migrate();
        this.assertOwns([]);
        // Built once every page is read and delivered: a page still in the outbox may yet bring back
        // a duplicate, and the last page is read before it is delivered.
        const building = new Set(this.store.exec(`SELECT name FROM ${GLOBALS} WHERE done = 0`).toArray().map((r) => r.name));
        const [lo, hi] = this.ownedHashRange();
        for (const { op } of this.store.exec(`SELECT op FROM ${OUTBOX} WHERE hash >= ? AND hash <= ? AND op LIKE '%"backfill":true%'`, lo, hi)) {
            const stored = JSON.parse(op);
            if (stored.kind === "put" && stored.backfill)
                building.add(indexNameOfId(indexIdOfRoute(stored.route)));
        }
        const duplicates = this.store.exec(`SELECT ix, key, owner FROM ${DUPLICATES} ORDER BY ix, key LIMIT 100`).toArray().filter((d) => this.owns(d.key)).map((d) => ({ index: d.ix, key: d.key, owner: d.owner }));
        return { building: [...building].sort(), duplicates };
    }
    /**
     * Builds the indexes added since this shard last saw the schema, over the rows it owns: a few
     * pages of rows, each row's entry put in the outbox and delivered like a write's. True if more
     * is left. A split copies how far each index got, and each side goes on over its own rows.
     */
    async backfill() {
        const indexes = new Map(globalIndexes(this.table).map((ix) => [ix.name, ix]));
        for (let page = 0; page < BACKFILL_PAGES; page++) {
            const next = this.store.exec(`SELECT name, after FROM ${GLOBALS} WHERE done = 0 ORDER BY name LIMIT 1`).toArray()[0];
            if (!next)
                return false;
            const ix = indexes.get(next.name);
            const rows = this.ownedRowsAfter(next.after, BACKFILL_ROWS);
            const now = Date.now();
            const ids = [];
            for (const { row, version } of rows) {
                const e = entryOf(this.table, ix, row);
                if (!e)
                    continue;
                const key = row[this.table.primaryKey];
                const op = { kind: "put", route: e.route, entry: e.entry, owner: key, version, token: null, backfill: true };
                ids.push(this.store.exec(`INSERT INTO ${OUTBOX} (key, hash, op, at) VALUES (?, ?, ?, ?) RETURNING id`, key, keyHash(key), JSON.stringify(op), now).one().id);
            }
            if (rows.length < BACKFILL_ROWS)
                this.store.exec(`UPDATE ${GLOBALS} SET done = 1, after = NULL WHERE name = ?`, next.name);
            else
                this.store.exec(`UPDATE ${GLOBALS} SET after = ? WHERE name = ?`, rows.at(-1).row[this.table.primaryKey], next.name);
            // Delivered before the next page, so the outbox stays small. One that fails is left to the alarm.
            if (ids.length)
                await this.deliver(ids).catch((error) => console.error(JSON.stringify({ event: "index-backfill-delivery-failed", node: this.nodeLabel(), error: String(error) })));
        }
        return this.store.exec(`SELECT 1 FROM ${GLOBALS} WHERE done = 0 LIMIT 1`).toArray().length > 0;
    }
    /**
     * Records the schema's global indexes, by id. One this shard has not seen is built over its rows,
     * unless the shard has none yet. One it saw that the schema no longer has (dropped, or redefined:
     * another id) has its entries dropped from the index shards.
     */
    migrateGlobals(fresh) {
        const recorded = this.tableExists(GLOBALS);
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${GLOBALS} (name TEXT PRIMARY KEY NOT NULL, def TEXT NOT NULL, after TEXT, done INTEGER NOT NULL, id TEXT) WITHOUT ROWID`);
        if (!this.store.exec(`PRAGMA table_info(${GLOBALS})`).toArray().some((c) => c.name === "id"))
            this.store.exec(`ALTER TABLE ${GLOBALS} ADD COLUMN id TEXT`);
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${DUPLICATES} (ix TEXT NOT NULL, key TEXT NOT NULL, owner TEXT NOT NULL, PRIMARY KEY (ix, key)) WITHOUT ROWID`);
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${DROPS} (id TEXT PRIMARY KEY NOT NULL) WITHOUT ROWID`);
        const wanted = new Map(globalIndexes(this.table).map((ix) => [ix.name, ix]));
        // What this shard's entries were made with: recorded, or (a shard from before indexes were
        // recorded) the schema's indexes under their names, which were their ids then.
        const seen = recorded
            ? this.store.exec(`SELECT name, id FROM ${GLOBALS}`).toArray().map((r) => ({ name: r.name, id: r.id ?? r.name }))
            : fresh ? [] : [...wanted.keys()].map((name) => ({ name, id: name }));
        for (const { name, id } of seen) {
            if (wanted.get(name)?.id === id) {
                wanted.delete(name);
                continue;
            }
            // Dropped, or redefined (built again, as a new one, under another id): its entries are dropped.
            this.store.exec(`DELETE FROM ${GLOBALS} WHERE name = ?`, name);
            this.store.exec(`DELETE FROM ${DUPLICATES} WHERE ix = ?`, name);
            this.store.exec(`INSERT OR IGNORE INTO ${DROPS} (id) VALUES (?)`, id);
        }
        // A shard with no rows yet (its first call) takes the indexes as built: they are as old as its table.
        for (const ix of wanted.values())
            this.store.exec(`INSERT INTO ${GLOBALS} (name, def, after, done, id) VALUES (?, ?, NULL, ?, ?)`, ix.name, JSON.stringify([ix.columns, ix.unique, ix.partition]), fresh ? 1 : 0, ix.id);
        // The alarm builds what is new and drops what is gone (the first call of a deploy reaches every shard that is used).
        if (this.store.exec(`SELECT 1 FROM ${GLOBALS} WHERE done = 0 UNION ALL SELECT 1 FROM ${DROPS} LIMIT 1`).toArray().length > 0)
            void this.wake(Date.now()).catch(() => { });
    }
    /**
     * Tells every index shard to drop the entries of the indexes this shard saw go, and drops their
     * fences here. True if none is left. Every row shard that saw one go asks, and asking again
     * changes nothing. Sends of the index (from the outbox) are left out from here on (see onTheWire).
     */
    async sendDrops() {
        const ids = this.store.exec(`SELECT id FROM ${DROPS} ORDER BY id`).toArray().map((r) => r.id);
        for (const id of ids) {
            try {
                await this.indexShards().scan("indexDrop", id);
            }
            catch (error) {
                console.error(JSON.stringify({ event: "index-drop-failed", node: this.nodeLabel(), index: id, error: String(error) }));
                return false;
            }
            const [from, to] = routeRangeOf(id);
            this.store.exec(`DELETE FROM ${FENCES} WHERE route >= ? AND route < ?`, from, to);
            this.store.exec(`DELETE FROM ${DROPS} WHERE id = ?`, id);
        }
        return true;
    }
    /** The ids of the schema's global indexes. */
    liveIndexIds() {
        const schema = this.table;
        if (this.live?.schema !== schema)
            this.live = { schema, ids: new Set(globalIndexes(schema).map((ix) => ix.id)) };
        return this.live.ids;
    }
    live = null;
    tableExists(name) {
        return this.store.exec(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?`, name).toArray().length > 0;
    }
    // ---- Splits ----
    /**
     * The copy keeps the outbox entries of its own keys, and delivers them: it has those rows from
     * here on, and a put is sent with its row. The rest are the original's. Call `super.afterClone`.
     */
    afterClone(owns) {
        this.migrate();
        for (const { key } of this.store.exec(`SELECT DISTINCT key FROM ${OUTBOX}`).toArray())
            if (!owns(key))
                this.store.exec(`DELETE FROM ${OUTBOX} WHERE key = ?`, key);
        super.afterClone(owns);
    }
    /**
     * Drops the outbox entries and fences of keys given away: the copy has them (see afterClone). An
     * entry a write here is still sending stays until it is sent, and so do the rows given away, since
     * a put is sent with its row. Call `super.cleanupForeign`.
     */
    cleanupForeign(owns, limit) {
        this.migrate();
        const [lo, hi] = this.ownedHashRange();
        const given = this.store.exec(`SELECT id FROM ${OUTBOX} WHERE hash < ? OR hash > ? ORDER BY id LIMIT ?`, lo, hi, Math.min(limit, CLEANUP_BATCH)).toArray().map((r) => r.id);
        const idle = given.filter((id) => !this.waiting.has(id));
        for (const part of slices(idle))
            this.store.exec(`DELETE FROM ${OUTBOX} WHERE id IN (${marks(part)})`, ...part);
        if (given.length > 0)
            return given.length; // not done: more to drop, or some still being sent
        const deleted = this.store.exec(`DELETE FROM ${FENCES} WHERE rowid IN (SELECT rowid FROM ${FENCES} WHERE hash < ? OR hash > ? LIMIT ?)`, lo, hi, Math.min(limit, CLEANUP_BATCH)).rowsWritten;
        return deleted + super.cleanupForeign(owns, limit);
    }
    // ---- Storage ----
    migrate() {
        super.migrate();
        if (this.indexMigrated)
            return;
        // No outbox yet: this shard has never written a row with global indexes.
        const fresh = !this.tableExists(OUTBOX);
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${OUTBOX} (id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL, hash TEXT NOT NULL DEFAULT '', op TEXT NOT NULL, at INTEGER NOT NULL)`);
        // An outbox from before entries had their key's hash: filled in once.
        if (!this.store.exec(`PRAGMA table_info(${OUTBOX})`).toArray().some((c) => c.name === "hash"))
            this.store.exec(`ALTER TABLE ${OUTBOX} ADD COLUMN hash TEXT NOT NULL DEFAULT ''`);
        for (const { key } of this.store.exec(`SELECT DISTINCT key FROM ${OUTBOX} WHERE hash = ''`).toArray())
            this.store.exec(`UPDATE ${OUTBOX} SET hash = ? WHERE key = ? AND hash = ''`, keyHash(key), key);
        this.store.exec(`CREATE INDEX IF NOT EXISTS ${OUTBOX}_hash ON ${OUTBOX} (hash)`);
        this.store.exec(`CREATE TABLE IF NOT EXISTS ${FENCES} (key TEXT NOT NULL, route TEXT NOT NULL, entry TEXT NOT NULL, hash TEXT NOT NULL, token INTEGER NOT NULL, PRIMARY KEY (key, route, entry))`);
        this.store.exec(`CREATE INDEX IF NOT EXISTS ${FENCES}_hash ON ${FENCES} (hash)`);
        this.migrateGlobals(fresh);
        this.indexMigrated = true;
    }
    fence(key, route, entry) {
        return this.store.exec(`SELECT token FROM ${FENCES} WHERE key = ? AND route = ? AND entry = ?`, key, route, entry).toArray()[0]?.token ?? 0;
    }
    raiseFence(key, route, entry, token) {
        this.store.exec(`INSERT INTO ${FENCES} (key, route, entry, hash, token) VALUES (?, ?, ?, ?, ?) ON CONFLICT (key, route, entry) DO UPDATE SET token = max(token, excluded.token)`, key, route, entry, keyHash(key), token);
    }
    uniqueIndexes() {
        // Per schema object: every write asks.
        const schema = this.table;
        if (this.uniques?.schema !== schema)
            this.uniques = { schema, indexes: globalIndexes(schema).filter((ix) => ix.unique) };
        return this.uniques.indexes;
    }
    uniques = null;
    /** Whether a backfill left duplicates on record here: a write clears its key's only if so. */
    hasDuplicates = true;
    async wake(time) {
        if (this.wakeAt !== null && this.wakeAt <= time && this.wakeAt > Date.now())
            return;
        // Remembered only once it is asked for: a failed ask must not make later ones look done.
        await this.scheduleAlarm(time);
        if (this.wakeAt === null || time < this.wakeAt || this.wakeAt <= Date.now())
            this.wakeAt = time;
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