// Splittable objects: a Durable Object base class whose instances split themselves in two, by the
// next bit of their keys' hash, when their call rate stays high; and a client that routes each key
// to the leaf that owns it.
//
// A leaf owns the keys whose hash starts with its `bits`. It lives in the Durable Object named
// `${name}#${bits without trailing zeros}`, so when a leaf splits its left half (`bits0`) stays in
// the same object and only the right half (`bits1`) moves, to a new object, as one copy of the
// object's storage (`ctx.storage.snapshot()`, or a row copy where snapshots are not available).
// Right-half writes wait while the copy is made; reads and left-half writes go on. Splits are
// final, so a client's map of splits can only lack some: it stops too shallow, and the object it
// reaches answers "moved".
//
// The protocol is modelled in specs/snapshot-split.ts.
import { DurableObject } from "cloudflare:workers";
import { createHash } from "node:crypto";
export { SpansLeaves, SplittableClient, splittable } from "./client.js";
/**
 * Split at half of what one object takes: about 800 calls/s on the edge, so 400, sustained for
 * 5 s.
 */
export const DEFAULT_SPLIT_POLICY = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25 };
/** The first 64 bits of the key's SHA-256, as 16 hex digits: what ownership is decided on. */
export function keyHash(key) {
    return createHash("sha256").update(key).digest("hex").slice(0, 16);
}
/** Bit `i` (0-based, most significant first) of a 16-hex-digit hash. */
export function hashBit(hash, i) {
    const nibble = Number.parseInt(hash[i >> 2], 16);
    return (nibble >> (3 - (i & 3))) & 1 ? "1" : "0";
}
function ownsHash(bits, hash) {
    for (let i = 0; i < bits.length; i++)
        if (hashBit(hash, i) !== bits[i])
            return false;
    return true;
}
/** The hashes a leaf owns, inclusive, as 16-hex-digit strings (they sort like the numbers). */
function hashRange(bits) {
    const lo = BigInt(`0b0${bits}${"0".repeat(64 - bits.length)}`);
    const hi = BigInt(`0b0${bits}${"1".repeat(64 - bits.length)}`);
    return [lo.toString(16).padStart(16, "0"), hi.toString(16).padStart(16, "0")];
}
export function nodeName(name, bits) {
    return `${name}#${bits}`;
}
/** The bits of the object hosting a leaf: its bits without trailing zeros. */
export function hostBits(bits) {
    return bits.replace(/0+$/, "");
}
/** The name of the Durable Object hosting the leaf `bits` of `name`. */
export function leafObjectName(name, bits) {
    return nodeName(name, hostBits(bits));
}
/** Thrown inside a method when its keys left this leaf; the dispatcher answers "moved". */
class Moved extends Error {
}
const SCHEMA = `
CREATE TABLE IF NOT EXISTS splittable_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS splittable_rows (key TEXT PRIMARY KEY, hash TEXT NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS splittable_rows_by_hash ON splittable_rows (hash, key);
`;
/** Bytes of rows per logical-copy call, well under the RPC limit; a call always carries one row. */
const COPY_BATCH_BYTES = 4 * 1024 * 1024;
/** The reason a sibling restarts with after scheduling a restore (its caller may see it). */
const RESTORE_ABORT = "splittable: restart on the snapshot";
/** Rows read per page of the logical copy. */
const COPY_PAGE_ROWS = 1000;
/**
 * Clearing what a split gave away is background work: those rows are already invisible. A delete
 * blocks the object while it runs, for longer the more rows it deletes, so it goes in small
 * batches with a pause between them, leaving the object to its calls.
 */
const DELETE_BATCH_ROWS = 250;
const DELETE_PAUSE_MS = 25;
/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this leaf), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses that define `alarm` must call `super.alarm()`.
 */
export class SplittableObject extends DurableObject {
    splitPolicy = DEFAULT_SPLIT_POLICY;
    sql;
    meta;
    calls = new Map();
    quietUntil = Date.now();
    /** The split in progress here: writes to `right` wait for `released`. */
    hold = null;
    /** An empty leaf splitting to the initial depth; calls wait for it, then are moved. */
    presplitting = null;
    /** This leaf had rows when it could have presplit: it splits by load only. */
    presplitDeclined = false;
    constructor(ctx, env) {
        super(ctx, env);
        this.sql = ctx.storage.sql;
        this.sql.exec(SCHEMA);
        this.meta = this.loadMeta();
    }
    // ---- Reads and writes for subclasses ----
    /** The current rows of these keys (absent keys are missing from the map). */
    async read(keys) {
        this.assertOwner(keys);
        const out = new Map();
        for (const key of keys) {
            const row = this.sql.exec("SELECT value FROM splittable_rows WHERE key = ?", key).toArray()[0];
            if (row)
                out.set(key, JSON.parse(row.value));
        }
        return out;
    }
    /**
     * Reads these keys and writes what `decide` returns, in one transaction, if this leaf still owns
     * them. `decide` must be synchronous. While the right half is being copied, an update touching
     * it waits; once the copy serves, it answers "moved".
     */
    async update(keys, decide) {
        for (let hold = this.holding(keys); hold; hold = this.holding(keys))
            await hold;
        // No await from here on: the check and the transaction see the same split state.
        this.assertOwner(keys);
        return this.ctx.storage.transactionSync(() => {
            const current = new Map();
            for (const key of keys) {
                const row = this.sql.exec("SELECT value FROM splittable_rows WHERE key = ?", key).toArray()[0];
                if (row)
                    current.set(key, JSON.parse(row.value));
            }
            const { writes, result } = decide(current);
            for (const [key, value] of writes) {
                if (value === null)
                    this.sql.exec("DELETE FROM splittable_rows WHERE key = ?", key);
                else
                    this.sql.exec("INSERT INTO splittable_rows (key, hash, value) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, keyHash(key), JSON.stringify(value));
            }
            return result;
        });
    }
    /**
     * Runs before this leaf copies its storage for a split, while writes to the half that moves
     * wait: a subclass with work queued under rows it is giving away (an outbox) finishes it here,
     * so the new leaf continues after it. It may run more than once.
     */
    async beforeHandOut() { }
    /**
     * Runs in the new object of a split when it claims its copy, inside that transaction. A split
     * copies every table, the subclass's too, and the original keeps its subclass tables and the
     * work queued in them (an outbox it goes on delivering), so the copy drops its share of that
     * work here. `owns(key)` says whether a key is the new leaf's. Synchronous. The rows of
     * `read`/`update` are handled by the base class.
     */
    afterClone(_owns) { }
    /**
     * Deletes up to `limit` rows of the subclass's own tables that belong to keys this leaf no
     * longer owns, and returns how many it deleted. After a split, the alarm calls it on both
     * leaves, in paced batches, until it returns 0, as it does for the rows of `read`/`update`.
     * Until then a subclass that queries its tables in bulk (listings, counts) filters with
     * `owns`. The default deletes nothing: an original that keeps delivering an outbox for keys
     * that moved keeps those rows.
     */
    cleanupForeign(_owns, _limit) {
        return 0;
    }
    /** Whether `key` is this leaf's now. Keys of a split's moving half stay this leaf's until it commits. */
    owns(key) {
        if (!this.meta)
            throw new Error("node has no name yet");
        return ownsHash(this.meta.bits, keyHash(key));
    }
    /** This leaf's name (`${name}#${bits}`), once it has one. */
    nodeLabel() {
        if (!this.meta)
            throw new Error("node has no name yet");
        return nodeName(this.meta.name, this.meta.bits);
    }
    /** The leaf this one split from and when; null for the root. */
    parentNode() {
        if (!this.meta || this.meta.parent === null)
            return null;
        return { label: nodeName(this.meta.name, this.meta.parent), adoptedAt: this.meta.splitAt };
    }
    /** The name all leaves of this object share (what the client was created with). */
    objectName() {
        if (!this.meta)
            throw new Error("node has no name yet");
        return this.meta.name;
    }
    assertOwner(keys) {
        const meta = this.meta;
        if (meta?.state !== "leaf")
            throw new Moved();
        for (const key of keys)
            if (!ownsHash(meta.bits, keyHash(key)))
                throw new Moved();
    }
    /** What an update of these keys waits for, if any. */
    holding(keys) {
        const hold = this.hold;
        if (!hold)
            return null;
        for (const key of keys)
            if (ownsHash(hold.right, keyHash(key)))
                return hold.released;
        return null;
    }
    // ---- The routed call ----
    /** A call routed by the client: `method(keys, payloads, ...args)` if this leaf owns the keys. */
    async splittableCall(name, bits, method, keys, payloads, args) {
        const answer = await this.arriveSettled(name, bits);
        if (answer)
            return answer;
        for (const key of keys)
            if (!ownsHash(bits, keyHash(key)))
                throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);
        this.countCall(keys.length);
        const fn = this[method];
        if (typeof fn !== "function" || method.startsWith("splittable"))
            throw new Error(`no method ${method}`);
        try {
            return { ok: await fn.call(this, keys, payloads, ...args) };
        }
        catch (error) {
            if (error instanceof Moved)
                return this.moved(bits);
            throw error;
        }
    }
    /** A call to every leaf: `method(...args)`, with no keys. */
    async splittableScan(name, bits, method, args) {
        const answer = await this.arriveSettled(name, bits);
        if (answer)
            return answer;
        const fn = this[method];
        if (typeof fn !== "function" || method.startsWith("splittable"))
            throw new Error(`no method ${method}`);
        try {
            return { ok: await fn.call(this, ...args) };
        }
        catch (error) {
            if (error instanceof Moved)
                return this.moved(bits);
            throw error;
        }
    }
    /** Every row this leaf owns, for scans (a subclass's scan method may use it). */
    allRows() {
        this.assertOwner([]);
        const [lo, hi] = hashRange(this.meta.bits);
        const out = new Map();
        for (const r of this.sql.exec("SELECT key, value FROM splittable_rows WHERE hash >= ? AND hash <= ?", lo, hi))
            out.set(r.key, JSON.parse(r.value));
        return out;
    }
    /** "moved" for a call addressed to `bits`: this leaf went deeper, so `bits` and more split. */
    moved(bits) {
        const now = this.meta.bits;
        return { moved: now.length > bits.length ? now.slice(0, -1) : bits };
    }
    /**
     * Null if this object serves `bits` now; otherwise what to answer. Splits to the initial depth
     * first if this leaf is above it and empty: every call waits for that.
     */
    async arriveSettled(name, bits) {
        const answer = this.arrive(name, bits);
        if (answer)
            return answer;
        const meta = this.meta;
        if (this.presplitDeclined || meta.bits.length >= (this.splitPolicy.initialDepth ?? 0))
            return null;
        this.presplitting ??= this.presplit().finally(() => (this.presplitting = null));
        await this.presplitting;
        return this.arrive(name, bits);
    }
    /** Whether this object serves `bits`, initialising the root on its first call. */
    arrive(name, bits) {
        if (!this.meta) {
            if (bits !== "")
                return { notReady: bits }; // a sibling whose copy has not arrived
            this.saveMeta({ name, bits: "", state: "leaf", epoch: 0, splitting: null, activate: null, cleanup: [], parent: null, splitAt: 0 });
        }
        const meta = this.meta;
        // Storage copied from another object and not claimed yet: it keeps that object's meta.
        if (this.isUnclaimedCopy())
            return { notReady: bits };
        if (meta.name !== name || hostBits(meta.bits) !== hostBits(bits))
            throw new Error(`${nodeName(name, bits)} reached ${nodeName(meta.name, meta.bits)}`);
        if (meta.state === "copy")
            return { notReady: bits };
        // A split interrupted by a restart: abandoned (its copy is never activated).
        if (meta.splitting !== null && !this.hold) {
            this.saveMeta({ ...meta, splitting: null });
            if (meta.activate || meta.cleanup.length > 0 || meta.foreign)
                void this.ctx.storage.setAlarm(Date.now());
        }
        if (meta.bits === bits)
            return null;
        if (meta.bits.startsWith(bits))
            return this.moved(bits);
        throw new Error(`${nodeName(name, bits)} is not split, ${nodeName(meta.name, meta.bits)} is the leaf`);
    }
    async presplit() {
        const meta = this.meta;
        const rows = this.sql.exec("SELECT COUNT(*) AS n FROM splittable_rows").one().n;
        if (rows > 0 || meta.state !== "leaf" || this.hold) {
            this.presplitDeclined = true; // not empty: splits by load only
            return;
        }
        // Empty: the right halves of every level start empty and serving; they split further on
        // their own first call.
        const depth = this.splitPolicy.initialDepth ?? 0;
        let bits = meta.bits;
        for (; bits.length < depth; bits += "0")
            await this.node(`${bits}1`).splittableAdoptEmpty(meta.name, `${bits}1`, bits);
        this.saveMeta({ ...this.meta, bits, parent: meta.bits, splitAt: Date.now() });
    }
    loadMeta() {
        const row = this.sql.exec("SELECT value FROM splittable_meta WHERE key = 'meta'").toArray()[0];
        return row ? JSON.parse(row.value) : null;
    }
    saveMeta(meta) {
        this.meta = meta;
        this.sql.exec("INSERT INTO splittable_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
    }
    // ---- Load and splitting ----
    countCall(keys) {
        const second = Math.floor(Date.now() / 1000);
        this.calls.set(second, (this.calls.get(second) ?? 0) + 1 + keys / (this.splitPolicy.keysPerCall ?? 25));
        if (this.calls.size > this.splitPolicy.windowSeconds + 2) {
            for (const s of this.calls.keys())
                if (s < second - this.splitPolicy.windowSeconds - 1)
                    this.calls.delete(s);
        }
        if (Date.now() - this.quietUntil > this.splitPolicy.windowSeconds * 1000 && this.rate() > this.splitPolicy.callsPerSecond)
            void this.split();
    }
    /** Average calls/s over the last full `windowSeconds`. */
    rate() {
        const now = Math.floor(Date.now() / 1000);
        let sum = 0;
        for (let s = now - this.splitPolicy.windowSeconds; s < now; s++)
            sum += this.calls.get(s) ?? 0;
        return sum / this.splitPolicy.windowSeconds;
    }
    /** Splits now if this object hosts the leaf `bits` (also an operator's lever). */
    async splittableSplit(name, bits) {
        if (this.arrive(name, bits))
            return false;
        return this.split();
    }
    /**
     * Hands the right half to a new object: holds writes to it, copies this object's storage there,
     * and once the copy is claimed stops owning the right half and activates it. A copy that is not
     * claimed in time is abandoned, and the held writes go ahead here.
     */
    async split() {
        const meta = this.meta;
        if (!meta || this.hold || meta.state !== "leaf" || meta.bits.length >= this.splitPolicy.maxDepth)
            return false;
        const epoch = meta.epoch + 1;
        const right = `${meta.bits}1`;
        let release;
        const hold = { epoch, right, released: new Promise((resolve) => (release = resolve)) };
        this.hold = hold;
        const started = Date.now();
        const timeout = this.splitPolicy.splitTimeoutMs ?? 60_000;
        let timer;
        try {
            this.saveMeta({ ...meta, epoch, splitting: epoch, from: this.ctx.id.toString() });
            const live = () => this.hold === hold && Date.now() - started < timeout;
            const copied = this.copyAndClaim(right, epoch, live);
            copied.catch(() => { }); // after a timeout nobody waits for it
            const expired = new Promise((_, reject) => (timer = setTimeout(() => reject(new Error(`split did not finish in ${timeout} ms`)), timeout)));
            const method = await Promise.race([copied, expired]);
            // The copy is claimed: from this transaction on, the right half is the sibling's.
            const now = this.meta;
            this.saveMeta({ ...now, bits: `${meta.bits}0`, splitting: null, activate: { bits: right, epoch }, cleanup: [...now.cleanup, hashRange(right)], foreign: true, parent: meta.bits, splitAt: Date.now(), lastSplit: { copy: method, ms: Date.now() - started } });
            this.calls.clear();
            this.quietUntil = Date.now();
            console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), copy: method, ms: Date.now() - started }));
            this.hold = null;
            release();
            await this.ctx.storage.setAlarm(Date.now());
            await this.activateSibling().catch(() => { }); // the alarm retries
            return true;
        }
        catch (error) {
            const now = this.meta;
            if (now)
                this.saveMeta({ ...now, splitting: now.splitting === epoch ? null : now.splitting, lastSplitError: String(error).slice(0, 500) });
            this.quietUntil = Date.now();
            console.error(JSON.stringify({ event: "split-abandoned", node: nodeName(meta.name, meta.bits), error: String(error), ms: Date.now() - started }));
            return false;
        }
        finally {
            clearTimeout(timer);
            if (this.hold === hold) {
                this.hold = null;
                release();
            }
        }
    }
    /** Copies this object's storage to the right sibling, then claims the copy there. */
    async copyAndClaim(right, epoch, live) {
        await this.beforeHandOut();
        const method = await this.copyTo(right, epoch, live);
        const meta = this.meta;
        let lastError = null;
        for (let wait = 10; live(); wait = Math.min(wait * 2, 500)) {
            try {
                // A fresh stub each time: the sibling restarts to apply the restore, which breaks the old one.
                if (await this.node(right).splittableClaim(meta.name, right, meta.bits, epoch))
                    return method;
            }
            catch (error) {
                // The sibling restarting (or still starting on the copy). Claiming is idempotent.
                lastError = error;
            }
            await new Promise((resolve) => setTimeout(resolve, wait));
        }
        throw new Error(`split timed out claiming its copy${lastError ? `: ${String(lastError)}` : ""}`);
    }
    async copyTo(right, epoch, live) {
        const mode = this.splitPolicy.copy ?? "auto";
        if (mode !== "logical" && typeof this.ctx.storage.snapshot === "function") {
            let snapshot = null;
            try {
                snapshot = await this.ctx.storage.snapshot();
            }
            catch (error) {
                if (mode === "native")
                    throw error; // auto: no snapshots here (local development)
            }
            if (snapshot) {
                try {
                    await this.node(right).splittableRestore(snapshot);
                }
                catch (error) {
                    // The sibling restarts right after replying; under load the restart can overtake the
                    // reply. Claiming tells whether the restore took.
                    if (!String(error).includes(RESTORE_ABORT))
                        throw error;
                }
                return "native";
            }
        }
        else if (mode === "native") {
            throw new Error("storage snapshots need the durable_object_snapshots compatibility flag");
        }
        await this.copyRows(right, epoch, live);
        return "logical";
    }
    /**
     * The logical copy, where snapshots are unavailable: every table, as a snapshot would carry
     * them, in byte-bounded batches. Writes to the half that moves are held meanwhile; rows of the
     * other half may change under the copy, which is fine: the copy drops them.
     */
    async copyRows(right, epoch, live) {
        const meta = this.meta;
        const target = this.node(right);
        let first = true;
        for (const table of this.tables()) {
            for (let offset = 0;;) {
                if (!live())
                    throw new Error("split timed out copying rows");
                const cursor = this.sql.exec(`SELECT * FROM "${table}" LIMIT ? OFFSET ?`, COPY_PAGE_ROWS, offset);
                const rows = [];
                let bytes = 0;
                for (const row of cursor.raw()) {
                    rows.push(row);
                    for (const value of row)
                        bytes += value instanceof ArrayBuffer ? value.byteLength : String(value).length;
                    if (bytes >= COPY_BATCH_BYTES)
                        break;
                }
                if (rows.length === 0 && !first)
                    break;
                await target.splittableReceive(meta.name, right, first, table, cursor.columnNames, rows);
                first = false;
                offset += rows.length;
                if (rows.length === 0 || (rows.length < COPY_PAGE_ROWS && bytes < COPY_BATCH_BYTES))
                    break;
            }
        }
        if (!(await target.splittableReceiveDone(meta.name, right, epoch)))
            throw new Error("the copy did not take this split's meta");
    }
    /** This object's tables, the subclass's too (not SQLite's or the runtime's). */
    tables() {
        return this.sql
            .exec("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '_cf_%' ORDER BY name")
            .toArray()
            .map((t) => t.name);
    }
    node(bits) {
        const nodes = this.nodes();
        return nodes.get(nodes.idFromName(leafObjectName(this.meta.name, bits)));
    }
    async activateSibling() {
        const pending = this.meta?.activate;
        if (!pending)
            return;
        if (!(await this.node(pending.bits).splittableActivate(this.meta.name, pending.bits, pending.epoch))) {
            console.error(JSON.stringify({ event: "split-activate-refused", node: this.nodeLabel(), sibling: pending.bits }));
            return;
        }
        const now = this.meta;
        if (now.activate?.epoch === pending.epoch && now.activate.bits === pending.bits)
            this.saveMeta({ ...now, activate: null });
    }
    async alarm() {
        const meta = this.meta;
        // A verbatim copy not claimed yet carries the source's alarm: it is not this object's.
        if (!meta || this.isUnclaimedCopy())
            return;
        if (meta.activate)
            await this.activateSibling();
        const range = meta.cleanup[0];
        if (range) {
            const deleted = this.sql.exec("DELETE FROM splittable_rows WHERE key IN (SELECT key FROM splittable_rows WHERE hash >= ? AND hash <= ? LIMIT ?)", range[0], range[1], DELETE_BATCH_ROWS).rowsWritten;
            if (deleted === 0)
                this.saveMeta({ ...this.meta, cleanup: this.meta.cleanup.slice(1) });
        }
        else if (meta.foreign) {
            const bits = meta.bits;
            const deleted = this.ctx.storage.transactionSync(() => this.cleanupForeign((key) => ownsHash(bits, keyHash(key)), DELETE_BATCH_ROWS));
            if (deleted === 0)
                this.saveMeta({ ...this.meta, foreign: false });
        }
        if (this.meta.activate)
            await this.ctx.storage.setAlarm(Date.now() + 1000);
        else if (this.meta.cleanup.length > 0 || this.meta.foreign)
            await this.ctx.storage.setAlarm(Date.now() + DELETE_PAUSE_MS);
    }
    // ---- The sibling's side of a split ----
    /** Schedules this object's storage to be replaced by the snapshot, then restarts to apply it. */
    async splittableRestore(snapshot) {
        if (this.serving())
            throw new Error("refusing to overwrite a serving leaf");
        try {
            await this.ctx.storage.onNextSessionRestore(snapshot);
        }
        catch (error) {
            // One pending restore per object: an earlier attempt's. Claiming tells which it was.
            if (!String(error).includes("already been scheduled"))
                throw error;
        }
        // Reply first, then restart: the next session starts on the copy.
        this.ctx.waitUntil(new Promise((resolve) => setTimeout(() => {
            resolve();
            try {
                this.ctx.abort(RESTORE_ABORT);
            }
            catch {
                // aborting throws in the caller too
            }
        }, 50)));
    }
    /** One batch of the logical copy: rows of one table. The first batch wipes every table. */
    async splittableReceive(_name, _bits, first, table, columns, rows) {
        if (first && this.serving())
            throw new Error("refusing to overwrite a serving leaf");
        this.ctx.storage.transactionSync(() => {
            if (first) {
                for (const t of this.tables())
                    this.sql.exec(`DELETE FROM "${t}"`);
                this.meta = null;
            }
            if (rows.length === 0)
                return;
            // A subclass table the copy has not created yet: its constructor creates it on a restart,
            // but copies need it now.
            if (!this.tables().includes(table))
                throw new Error(`table ${table} does not exist in the copy`);
            const sql = `INSERT OR REPLACE INTO "${table}" (${columns.map((c) => `"${c}"`).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
            for (const row of rows)
                this.sql.exec(sql, ...row);
        });
    }
    /** The logical copy is complete: it holds the source's meta, from this split. */
    async splittableReceiveDone(name, bits, epoch) {
        this.meta = this.loadMeta();
        const meta = this.meta;
        return meta !== null && meta.name === name && meta.splitting === epoch && `${meta.bits}1` === bits && this.isUnclaimedCopy();
    }
    /** Storage copied verbatim from a splitting object and not claimed yet: it is not this object's. */
    isUnclaimedCopy() {
        const meta = this.meta;
        return meta !== null && meta.splitting !== null && meta.from !== this.ctx.id.toString();
    }
    /**
     * A leaf that serves and is not splitting. Anything else may be overwritten by a split's copy:
     * an unused object, a copy (claimed or not) from an abandoned attempt.
     */
    serving() {
        return this.meta?.state === "leaf" && !this.isUnclaimedCopy();
    }
    /**
     * Claims the copy for the leaf `bits`, if it is the copy made by attempt `epoch` of its parent:
     * becomes that leaf, not serving yet, and deletes the rows of the half it does not own.
     */
    async splittableClaim(name, bits, parent, epoch) {
        const meta = this.meta;
        if (!meta || meta.name !== name)
            return false;
        if (meta.bits === bits)
            return meta.epoch === epoch && meta.state === "copy"; // claimed already
        if (!this.isUnclaimedCopy() || meta.bits !== parent || meta.splitting !== epoch)
            return false; // not this copy
        this.ctx.storage.transactionSync(() => {
            this.afterClone((key) => ownsHash(bits, keyHash(key)));
            this.saveMeta({ name, bits, state: "copy", epoch, splitting: null, activate: null, cleanup: this.outside(bits), foreign: true, parent, splitAt: Date.now() });
        });
        await this.ctx.storage.setAlarm(Date.now());
        return true;
    }
    /** The hash ranges outside a leaf's. */
    outside(bits) {
        const [lo, hi] = hashRange(bits);
        const out = [];
        if (lo !== "0000000000000000")
            out.push(["0000000000000000", (BigInt(`0x${lo}`) - 1n).toString(16).padStart(16, "0")]);
        if (hi !== "ffffffffffffffff")
            out.push([(BigInt(`0x${hi}`) + 1n).toString(16).padStart(16, "0"), "ffffffffffffffff"]);
        return out;
    }
    /** Starts serving, if this is the copy of attempt `epoch`. */
    async splittableActivate(name, bits, epoch) {
        const meta = this.meta;
        if (!meta || meta.name !== name)
            return false;
        if (meta.bits !== bits)
            return meta.state === "leaf" && meta.splitting === null && meta.bits.startsWith(bits); // serving, and split since
        if (meta.epoch !== epoch)
            return false;
        if (meta.state === "copy")
            this.saveMeta({ ...meta, state: "leaf" });
        return true;
    }
    /** The right half of an empty leaf's initial split: empty, and serving at once. */
    async splittableAdoptEmpty(name, bits, parent) {
        if (this.meta)
            return;
        this.saveMeta({ name, bits, state: "leaf", epoch: 0, splitting: null, activate: null, cleanup: [], parent, splitAt: Date.now() });
    }
    async splittableDescribe() {
        const meta = this.meta;
        const storedRows = this.sql.exec("SELECT COUNT(*) AS n FROM splittable_rows").one().n;
        let rows = storedRows;
        if (meta) {
            const [lo, hi] = hashRange(meta.bits);
            rows = this.sql.exec("SELECT COUNT(*) AS n FROM splittable_rows WHERE hash >= ? AND hash <= ?", lo, hi).one().n;
        }
        const state = !meta ? "new" : this.hold ? "splitting" : meta.state === "copy" || this.isUnclaimedCopy() ? "copy" : "leaf";
        return { bits: meta?.bits ?? "", state, rows, storedRows, callsPerSecond: this.rate(), lastSplit: meta?.lastSplit ?? null, lastSplitError: meta?.lastSplitError ?? null };
    }
}
//# sourceMappingURL=index.js.map