// Sharded objects: a Durable Object base class whose instances split themselves in two, by the
// next bit of their keys' hash, when their call rate stays high; and a client that routes each key
// to the shard that owns it.
//
// A shard owns the keys whose hash starts with its `bits`. It lives in the Durable Object named
// `${name}#${bits without trailing zeros}`, so when a shard splits its left half (`bits0`) stays in
// the same object and only the right half (`bits1`) moves, to a new object, as one copy of the
// object's storage (`ctx.storage.snapshot()`, or a row copy where snapshots are not available).
// Right-half writes wait while the copy is made; reads and left-half writes go on. Splits are
// final, so a client's map of splits can only lack some: it stops too shallow, and the object it
// reaches answers "moved".
//
// The protocol is modelled in specs/snapshot-split.ts.
import { DurableObject } from "cloudflare:workers";
import { createHash } from "node:crypto";
export { SpansShards, ShardedClient, sharded } from "./client.js";
/**
 * Split at half of what one object takes: about 800 calls/s on the edge, so 400, sustained for
 * 5 s.
 */
export const DEFAULT_SPLIT_POLICY = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25 };
/** The deepest a shard can be: ownership is decided on 64 bits of the key's hash. */
export const MAX_SPLIT_DEPTH = 64;
/** Checks a split policy, so a bad one fails loudly on the first call instead of quietly never splitting. */
export function checkSplitPolicy(policy) {
    const fail = (what) => {
        throw new Error(`splitPolicy.${what}`);
    };
    if (!(policy.callsPerSecond > 0))
        fail("callsPerSecond must be a positive number");
    if (!Number.isInteger(policy.windowSeconds) || policy.windowSeconds < 1)
        fail("windowSeconds must be a whole number of seconds, at least 1");
    if (!Number.isInteger(policy.maxDepth) || policy.maxDepth < 0 || policy.maxDepth > MAX_SPLIT_DEPTH)
        fail(`maxDepth must be an integer from 0 to ${MAX_SPLIT_DEPTH}`);
    if (policy.keysPerCall !== undefined && !(policy.keysPerCall > 0))
        fail("keysPerCall must be a positive number");
    const initial = policy.initialDepth ?? 0;
    if (!Number.isInteger(initial) || initial < 0 || initial > policy.maxDepth)
        fail("initialDepth must be an integer from 0 to maxDepth");
    if (policy.copy !== undefined && !["auto", "native", "logical"].includes(policy.copy))
        fail('copy must be "auto", "native" or "logical"');
    if (policy.splitTimeoutMs !== undefined && !(policy.splitTimeoutMs > 0))
        fail("splitTimeoutMs must be a positive number");
    if (policy.holdWaitMs !== undefined && !(policy.holdWaitMs >= 0))
        fail("holdWaitMs must be zero or more");
    return policy;
}
/** Levels one presplit round adopts at once: at most 63 new shards, so subrequests stay bounded. */
const PRESPLIT_LEVELS = 6;
/** How many split attempts a shard remembers. */
const SPLIT_HISTORY = 8;
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
/** The hashes a shard owns, inclusive, as 16-hex-digit strings (they sort like the numbers). */
function hashRange(bits) {
    const lo = BigInt(`0b0${bits}${"0".repeat(64 - bits.length)}`);
    const hi = BigInt(`0b0${bits}${"1".repeat(64 - bits.length)}`);
    return [lo.toString(16).padStart(16, "0"), hi.toString(16).padStart(16, "0")];
}
/** An SQL identifier, quoted (a subclass may name its tables anything). */
function quoteIdentifier(name) {
    return `"${name.replace(/"/g, '""')}"`;
}
/**
 * Tables that are SQLite's (sqlite_), the runtime's (_cf_) or local development's (__miniflare_),
 * never a subclass's. By prefix, not LIKE: in LIKE, "_" matches any character, and "_cf_%" also
 * matched a subclass's table such as "scf_notes", which a split then failed to copy.
 */
function isInternalTable(name) {
    return name.startsWith("sqlite_") || name.startsWith("_cf_") || name.startsWith("__miniflare_");
}
function unquoteIdentifier(quoted) {
    return quoted.slice(1, -1).replace(/""/g, '"');
}
export function nodeName(name, bits) {
    return `${name}#${bits}`;
}
/** The bits of the object hosting a shard: its bits without trailing zeros. */
export function hostBits(bits) {
    return bits.replace(/0+$/, "");
}
/** The name of the Durable Object hosting the shard `bits` of `name`. */
export function shardObjectName(name, bits) {
    return nodeName(name, hostBits(bits));
}
/** Thrown inside a method when its keys left this shard; the dispatcher answers "moved". */
class Moved extends Error {
}
/**
 * A write waited on a split for `holdWaitMs`: nothing was applied, the client retries later, until
 * the split is done or gives up at `until`.
 */
class NotReady extends Error {
    until;
    constructor(until) {
        super("not ready");
        this.until = until;
    }
}
const HOLD_WAIT_MS = 2_000;
const SCHEMA = `
CREATE TABLE IF NOT EXISTS sharded_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS sharded_rows (key TEXT PRIMARY KEY, hash TEXT NOT NULL, value TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS sharded_rows_by_hash ON sharded_rows (hash, key);
`;
/** Bytes of rows per logical-copy call, well under the RPC limit; a call always carries one row. */
const COPY_BATCH_BYTES = 4 * 1024 * 1024;
/** The reason a sibling restarts with after scheduling a restore (its caller may see it). */
const RESTORE_ABORT = "sharded: restart on the snapshot";
/** Rows read per page of the logical copy. */
const COPY_PAGE_ROWS = 1000;
/**
 * Clearing what a split gave away is background work: those rows are already invisible. A delete
 * blocks the object while it runs, for longer the more rows it deletes, so it goes in small
 * batches with a pause between them, leaving the object to its calls.
 */
const DELETE_BATCH_ROWS = 250;
const DELETE_PAUSE_MS = 25;
/** How soon the alarm tries again after activating a sibling or cleaning up failed. */
const ALARM_RETRY_MS = 1000;
/** How often a shard with a split's work pending checks that its alarm is set. */
const ALARM_CHECK_MS = 5_000;
/**
 * Extend like DurableObject. Methods called through the client take the keys they act on first
 * (all owned by this shard), may take payloads aligned with the keys second, and read and write
 * only with `read` and one `update`.
 *
 * Subclasses never define `alarm` (the constructor refuses): they override `onAlarm` and schedule
 * it with `scheduleAlarm`.
 */
export class ShardedObject extends DurableObject {
    splitPolicy = DEFAULT_SPLIT_POLICY;
    /** `splitPolicy`, checked once (a subclass sets it after the constructor here runs). */
    get policy() {
        if (this.checkedPolicy !== this.splitPolicy)
            this.checkedPolicy = checkSplitPolicy(this.splitPolicy);
        return this.checkedPolicy;
    }
    checkedPolicy = null;
    sql;
    meta;
    calls = new Map();
    quietUntil = Date.now();
    /** The split in progress here: writes to `right` wait for `released`. */
    hold = null;
    /** A split is starting its new object, before it holds anything. */
    warming = false;
    counters = { since: Date.now(), calls: 0, ok: 0, moved: 0, notReady: 0, stale: 0, held: 0, heldMsMax: 0 };
    /** The split attempt whose logical copy this object takes (in memory: a restart refuses the rest). */
    receiving = null;
    /** An empty shard splitting to the initial depth; calls wait for it, then are moved. */
    presplitting = null;
    /** This shard had rows when it could have presplit: it splits by load only. */
    presplitDeclined = false;
    constructor(ctx, env) {
        super(ctx, env);
        // The alarm is the base class's: it runs splits' work, and a subclass's code after it would
        // also run in a split's copy, on work that is the original's. Subclasses use onAlarm.
        if (this.alarm !== ShardedObject.prototype.alarm) {
            throw new Error(`${new.target.name} overrides alarm(): a sharded object's alarm belongs to the base class. Override onAlarm() and schedule it with scheduleAlarm() instead.`);
        }
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
            const row = this.sql.exec("SELECT value FROM sharded_rows WHERE key = ?", key).toArray()[0];
            if (row)
                out.set(key, JSON.parse(row.value));
        }
        return out;
    }
    /**
     * Reads these keys and writes what `decide` returns, in one transaction, if this shard still owns
     * them. `decide` must be synchronous. While the right half is being copied, an update touching
     * it waits; once the copy serves, it answers "moved".
     */
    async update(keys, decide) {
        await this.waitForHold(keys);
        // No await from here on: the check and the transaction see the same split state.
        this.assertOwner(keys);
        return this.ctx.storage.transactionSync(() => {
            const current = new Map();
            for (const key of keys) {
                const row = this.sql.exec("SELECT value FROM sharded_rows WHERE key = ?", key).toArray()[0];
                if (row)
                    current.set(key, JSON.parse(row.value));
            }
            const { writes, result } = decide(current);
            for (const [key, value] of writes) {
                if (value === null)
                    this.sql.exec("DELETE FROM sharded_rows WHERE key = ?", key);
                else
                    this.sql.exec("INSERT INTO sharded_rows (key, hash, value) VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, keyHash(key), JSON.stringify(value));
            }
            return result;
        });
    }
    /**
     * Runs `fn` in one transaction, fenced like `update`: it waits while any of `keys` is being
     * copied by a split, and runs only if this shard still owns them all. Write the subclass's own
     * tables here, never outside: a write that skips the fence can land after the copy and be lost.
     * List every key whose rows `fn` touches. `fn` must be synchronous.
     */
    transact(keys, fn) {
        return this.update(keys, () => ({ writes: new Map(), result: fn() }));
    }
    /**
     * Up to `limit` of this shard's rows (of `read`/`update`) with keys after `afterKey`, in key
     * order: bounded paging over one shard. Rows of keys it no longer owns are skipped, so a page
     * is right while cleanup is still running.
     */
    ownedRowsPage(afterKey, limit) {
        this.assertOwner([]);
        if (!Number.isInteger(limit) || limit < 1)
            throw new Error("ownedRowsPage limit must be a positive integer");
        const [lo, hi] = hashRange(this.meta.bits);
        const out = [];
        for (const r of this.sql.exec("SELECT key, value FROM sharded_rows WHERE (? IS NULL OR key > ?) AND hash >= ? AND hash <= ? ORDER BY key LIMIT ?", afterKey, afterKey, lo, hi, limit))
            out.push([r.key, JSON.parse(r.value)]);
        return out;
    }
    /**
     * Runs before this shard copies its storage for a split, while writes to the half that moves
     * wait: a subclass with work queued under rows it is giving away (an outbox) finishes it here,
     * so the new shard continues after it. It may run more than once.
     */
    async beforeHandOut() { }
    /**
     * Runs in the new object of a split when it claims its copy, inside that transaction. A split
     * copies every table, the subclass's too, and the original keeps its subclass tables and the
     * work queued in them (an outbox it goes on delivering), so the copy drops its share of that
     * work here. `owns(key)` says whether a key is the new shard's. Synchronous. The rows of
     * `read`/`update` are handled by the base class.
     */
    afterClone(_owns) { }
    /**
     * Deletes up to `limit` rows of the subclass's own tables that belong to keys this shard no
     * longer owns, and returns how many it deleted. After a split, the alarm calls it on both
     * shards, in paced batches, until it returns 0, as it does for the rows of `read`/`update`.
     * Until then a subclass that queries its tables in bulk (listings, counts) filters with
     * `owns`. The default deletes nothing: an original that keeps delivering an outbox for keys
     * that moved keeps those rows.
     */
    cleanupForeign(_owns, _limit) {
        return 0;
    }
    /**
     * Throws (and the call answers "moved") unless this shard serves and owns every key. For reads of
     * the subclass's own tables; writes go through `update` or `transact`, which also wait on splits.
     */
    assertOwns(keys) {
        this.assertOwner(keys);
    }
    /** The routing hashes this shard owns, inclusive, as 16-hex-digit strings that sort like numbers. */
    ownedHashRange() {
        if (!this.meta)
            throw new Error("node has no name yet");
        return hashRange(this.meta.bits);
    }
    /** Whether `key` is this shard's now. Keys of a split's moving half stay this shard's until it commits. */
    owns(key) {
        if (!this.meta)
            throw new Error("node has no name yet");
        return ownsHash(this.meta.bits, keyHash(key));
    }
    /** This shard's name (`${name}#${bits}`), once it has one. */
    nodeLabel() {
        if (!this.meta)
            throw new Error("node has no name yet");
        return nodeName(this.meta.name, this.meta.bits);
    }
    /** The shard this one split from and when; null for the root. */
    parentNode() {
        if (!this.meta || this.meta.parent === null)
            return null;
        return { label: nodeName(this.meta.name, this.meta.parent), adoptedAt: this.meta.splitAt };
    }
    /** The name all shards of this object share (what the client was created with). */
    objectName() {
        if (!this.meta)
            throw new Error("node has no name yet");
        return this.meta.name;
    }
    assertOwner(keys) {
        const meta = this.meta;
        if (meta?.state !== "shard")
            throw new Moved();
        for (const key of keys)
            if (!ownsHash(meta.bits, keyHash(key)))
                throw new Moved();
    }
    /** Waits while a split copies any of these keys, up to `holdWaitMs`; then "not ready". */
    async waitForHold(keys) {
        if (!this.holding(keys))
            return;
        const start = Date.now();
        const deadline = start + (this.policy.holdWaitMs ?? HOLD_WAIT_MS);
        const counted = new Set();
        // Counted on every split this write waits on (one, unless a split follows another).
        const waited = (gaveUp) => {
            const ms = Date.now() - start;
            this.counters.heldMsMax = Math.max(this.counters.heldMsMax, ms);
            for (const split of counted) {
                split.heldMsMax = Math.max(split.heldMsMax, ms);
                if (gaveUp && split === this.hold)
                    split.notReady++;
            }
        };
        this.counters.held++;
        for (let hold = this.holding(keys); hold; hold = this.holding(keys)) {
            if (this.hold && !counted.has(this.hold)) {
                counted.add(this.hold);
                this.hold.held++;
            }
            const left = deadline - Date.now();
            if (left <= 0) {
                waited(true);
                throw new NotReady(this.hold?.deadline ?? Date.now());
            }
            let timer;
            const released = await Promise.race([hold.then(() => true), new Promise((resolve) => (timer = setTimeout(() => resolve(false), left)))]);
            clearTimeout(timer);
            if (!released) {
                waited(true);
                throw new NotReady(this.hold?.deadline ?? Date.now());
            }
        }
        waited(false);
    }
    /** Counts an answer other than "ok". */
    answered(answer) {
        if ("moved" in answer)
            this.counters.moved++;
        else if ("notReady" in answer)
            this.counters.notReady++;
        else if ("stale" in answer)
            this.counters.stale++;
        return answer;
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
    /** A call routed by the client: `method(keys, payloads, ...args)` if this shard owns the keys. */
    async shardedCall(name, bits, method, keys, payloads, args) {
        const answer = await this.arriveSettled(name, bits);
        if (answer)
            return this.answered(answer);
        this.counters.calls++;
        for (const key of keys)
            if (!ownsHash(bits, keyHash(key)))
                throw new Error(`key ${key} is not owned by ${nodeName(name, bits)}`);
        this.countCall(keys.length);
        const fn = this.routedMethod(method);
        try {
            const ok = await fn.call(this, keys, payloads, ...args);
            this.counters.ok++;
            return { ok };
        }
        catch (error) {
            if (error instanceof Moved)
                return this.answered(this.moved(bits));
            if (error instanceof NotReady)
                return this.answered({ notReady: bits, until: error.until });
            throw error;
        }
    }
    /** A call to every shard: `method(...args)`, with no keys. */
    async shardedScan(name, bits, method, args) {
        const answer = await this.arriveSettled(name, bits);
        if (answer)
            return this.answered(answer);
        this.counters.calls++;
        const fn = this.routedMethod(method);
        try {
            const ok = await fn.call(this, ...args);
            this.counters.ok++;
            return { ok };
        }
        catch (error) {
            if (error instanceof Moved)
                return this.answered(this.moved(bits));
            throw error;
        }
    }
    /**
     * The subclass method a routed call or scan names. Never one of this base class's own (its
     * reads and writes, the split protocol, the alarm) nor a runtime handler: a call must not reach
     * them with arguments it shaped for something else.
     */
    routedMethod(method) {
        const fn = this[method];
        if (typeof fn !== "function" || method.startsWith("sharded") || RESERVED_METHODS.has(method))
            throw new Error(`no method ${method}`);
        return fn;
    }
    /** Every row this shard owns, for scans (a subclass's scan method may use it). */
    allRows() {
        this.assertOwner([]);
        const [lo, hi] = hashRange(this.meta.bits);
        const out = new Map();
        for (const r of this.sql.exec("SELECT key, value FROM sharded_rows WHERE hash >= ? AND hash <= ?", lo, hi))
            out.set(r.key, JSON.parse(r.value));
        return out;
    }
    /** "moved" for a call addressed to `bits`: this shard went deeper, so `bits` and more split. */
    moved(bits) {
        const now = this.meta.bits;
        return { moved: now.length > bits.length ? now.slice(0, -1) : bits };
    }
    /**
     * Null if this object serves `bits` now; otherwise what to answer. Splits to the initial depth
     * first if this shard is above it and empty: every call waits for that.
     */
    async arriveSettled(name, bits) {
        const answer = this.arrive(name, bits);
        if (answer)
            return answer;
        const meta = this.meta;
        if (this.presplitDeclined || meta.bits.length >= (this.policy.initialDepth ?? 0))
            return null;
        this.presplitting ??= this.presplit().finally(() => (this.presplitting = null));
        await this.presplitting;
        return this.arrive(name, bits);
    }
    /** Whether this object serves `bits`, initialising the root on its first call. */
    arrive(name, bits) {
        if (!this.meta) {
            // The root starts with the first call and splits only after it: a caller that knows splits
            // of an unused root learned them from storage that is gone. Another object may be a
            // sibling whose copy has not arrived.
            if (bits !== "")
                return hostBits(bits) === "" ? { stale: bits } : { notReady: bits };
            this.saveMeta({ name, bits: "", state: "shard", epoch: 0, splitting: null, activate: null, cleanup: [], parent: null, splitAt: 0 });
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
                void this.armAlarm(Date.now());
        }
        this.checkAlarm();
        if (meta.bits === bits)
            return null;
        if (meta.bits.startsWith(bits))
            return this.moved(bits);
        // Splits are final: a caller that knows this shard split, when it has not, learned that from
        // storage that is gone.
        return { stale: bits };
    }
    async presplit() {
        const meta = this.meta;
        const rows = this.sql.exec("SELECT COUNT(*) AS n FROM sharded_rows").one().n;
        if (rows > 0 || meta.state !== "shard" || this.hold) {
            this.presplitDeclined = true; // not empty: splits by load only
            return;
        }
        // Empty: the shards up to PRESPLIT_LEVELS deeper (all of them, for an initial depth up to 6)
        // start empty and serving, adopted in one round at their bits (an object is named by its bits
        // without trailing zeros); this object keeps the all-zeros one. Each goes deeper on its own
        // first call. Adopting is idempotent, so a round that fails is retried by the next call.
        const depth = this.policy.initialDepth ?? 0;
        const extra = Math.min(depth - meta.bits.length, PRESPLIT_LEVELS);
        const shards = Array.from({ length: 2 ** extra - 1 }, (_, i) => meta.bits + (i + 1).toString(2).padStart(extra, "0"));
        await Promise.all(shards.map((bits) => this.node(bits).shardedAdoptEmpty(meta.name, bits, bits.slice(0, -1))));
        this.saveMeta({ ...this.meta, bits: meta.bits + "0".repeat(extra), parent: meta.bits, splitAt: Date.now() });
    }
    loadMeta() {
        const row = this.sql.exec("SELECT value FROM sharded_meta WHERE key = 'meta'").toArray()[0];
        return row ? JSON.parse(row.value) : null;
    }
    saveMeta(meta) {
        this.meta = meta;
        this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('meta', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", JSON.stringify(meta));
    }
    // ---- Load and splitting ----
    countCall(keys) {
        const second = Math.floor(Date.now() / 1000);
        this.calls.set(second, (this.calls.get(second) ?? 0) + 1 + keys / (this.policy.keysPerCall ?? 25));
        if (this.calls.size > this.policy.windowSeconds + 2) {
            for (const s of this.calls.keys())
                if (s < second - this.policy.windowSeconds - 1)
                    this.calls.delete(s);
        }
        if (Date.now() - this.quietUntil > this.policy.windowSeconds * 1000 && this.rate() > this.policy.callsPerSecond)
            void this.split();
    }
    /** Average calls/s over the last full `windowSeconds`. */
    rate() {
        const now = Math.floor(Date.now() / 1000);
        let sum = 0;
        for (let s = now - this.policy.windowSeconds; s < now; s++)
            sum += this.calls.get(s) ?? 0;
        return sum / this.policy.windowSeconds;
    }
    /** Splits now if this object hosts the shard `bits` (also an operator's lever). */
    async shardedSplit(name, bits) {
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
        const before = this.meta;
        // A presplit is splitting this shard already, by other means.
        if (!before || this.hold || this.warming || this.presplitting || before.state !== "shard" || before.bits.length >= this.policy.maxDepth)
            return false;
        const right = `${before.bits}1`;
        // Start the new object before holding writes: a first call to an object can take seconds.
        this.warming = true;
        const warmStarted = Date.now();
        try {
            await this.node(right).shardedWarm();
        }
        catch {
            // The split's own calls retry, or give up.
        }
        finally {
            this.warming = false;
        }
        const warm = Date.now() - warmStarted;
        // Meanwhile the meta may have been saved (cleanup progress): split from the current one.
        const meta = this.meta;
        if (!meta || meta.bits !== before.bits || meta.state !== "shard" || meta.epoch !== before.epoch || this.hold || this.presplitting)
            return false;
        const epoch = meta.epoch + 1;
        let release;
        const started = Date.now();
        const timeout = this.policy.splitTimeoutMs ?? 60_000;
        const hold = { epoch, right, released: new Promise((resolve) => (release = resolve)), deadline: started + timeout, held: 0, notReady: 0, heldMsMax: 0 };
        this.hold = hold;
        const [lo, hi] = hashRange(meta.bits);
        const [rightLo] = hashRange(right);
        const counted = this.sql.exec("SELECT COUNT(*) AS rows, COALESCE(SUM(hash >= ?), 0) AS rightRows FROM sharded_rows WHERE hash >= ? AND hash <= ?", rightLo, lo, hi).one();
        let copy = null;
        const steps = { warm, handOut: 0, snapshot: 0, restore: 0, restoreInside: 0, copyRows: 0, claim: 0, claimAttempts: 0, claimError: null };
        const record = (outcome, error) => [
            ...(this.meta?.splits ?? []),
            { at: started, ms: Date.now() - started, outcome, copy, rows: counted.rows, rightRows: counted.rightRows, held: hold.held, notReady: hold.notReady, heldMsMax: hold.heldMsMax, steps: { ...steps }, error },
        ].slice(-SPLIT_HISTORY);
        let timer;
        try {
            this.saveMeta({ ...meta, epoch, splitting: epoch, from: this.ctx.id.toString() });
            const live = () => this.hold === hold && Date.now() - started < timeout;
            const copied = this.copyAndClaim(right, epoch, live, steps, (method) => (copy = method));
            copied.catch(() => { }); // after a timeout nobody waits for it
            const expired = new Promise((_, reject) => (timer = setTimeout(() => reject(new Error(`split did not finish in ${timeout} ms`)), timeout)));
            const method = await Promise.race([copied, expired]);
            // The copy is claimed: from this transaction on, the right half is the sibling's.
            const now = this.meta;
            this.saveMeta({ ...now, bits: `${meta.bits}0`, splitting: null, activate: { bits: right, epoch }, cleanup: [...now.cleanup, hashRange(right)], foreign: true, parent: meta.bits, splitAt: Date.now(), lastSplit: { copy: method, ms: Date.now() - started }, splits: record("split", null) });
            this.calls.clear();
            this.quietUntil = Date.now();
            console.log(JSON.stringify({ event: "split", node: nodeName(meta.name, meta.bits), copy: method, ms: Date.now() - started }));
            this.hold = null;
            release();
            await this.armAlarm(Date.now());
            await this.activateSibling().catch(() => { }); // the alarm retries
            return true;
        }
        catch (error) {
            const now = this.meta;
            if (now)
                this.saveMeta({ ...now, splitting: now.splitting === epoch ? null : now.splitting, lastSplitError: String(error).slice(0, 500), splits: record("abandoned", String(error).slice(0, 500)) });
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
    async copyAndClaim(right, epoch, live, steps, copied) {
        let t = Date.now();
        await this.beforeHandOut();
        steps.handOut = Date.now() - t;
        const method = await this.copyTo(right, epoch, live, steps);
        copied(method);
        const meta = this.meta;
        let lastError = null;
        t = Date.now();
        for (let wait = 10; live(); wait = Math.min(wait * 2, 500)) {
            steps.claimAttempts++;
            try {
                // A fresh stub each time: the sibling restarts to apply the restore, which breaks the old one.
                const claimed = await this.node(right).shardedClaim(meta.name, right, meta.bits, epoch);
                steps.claim = Date.now() - t;
                if (claimed)
                    return method;
            }
            catch (error) {
                // The sibling restarting (or still starting on the copy). Claiming is idempotent.
                lastError = error;
                steps.claim = Date.now() - t;
                steps.claimError = String(error).slice(0, 200);
            }
            await new Promise((resolve) => setTimeout(resolve, wait));
        }
        throw new Error(`split timed out claiming its copy${lastError ? `: ${String(lastError)}` : ""}`);
    }
    async copyTo(right, epoch, live, steps) {
        const mode = this.policy.copy ?? "auto";
        if (mode !== "logical" && typeof this.ctx.storage.snapshot === "function") {
            let snapshot = null;
            let t = Date.now();
            try {
                snapshot = await this.ctx.storage.snapshot();
            }
            catch (error) {
                if (mode === "native")
                    throw error; // auto: no snapshots here (local development)
            }
            steps.snapshot = Date.now() - t;
            if (snapshot) {
                t = Date.now();
                try {
                    steps.restoreInside = (await this.node(right).shardedRestore(snapshot)).scheduleMs;
                }
                catch (error) {
                    // The sibling restarts right after replying; under load the restart can overtake the
                    // reply. Claiming tells whether the restore took.
                    if (!String(error).includes(RESTORE_ABORT))
                        throw error;
                }
                finally {
                    steps.restore = Date.now() - t;
                }
                return "native";
            }
        }
        else if (mode === "native") {
            throw new Error("storage snapshots need the durable_object_snapshots compatibility flag");
        }
        const t = Date.now();
        try {
            await this.copyRows(right, epoch, live);
        }
        finally {
            steps.copyRows = Date.now() - t;
        }
        return "logical";
    }
    /**
     * The logical copy, where snapshots are unavailable: every table, as a snapshot would carry
     * them, in byte-bounded batches. Writes to the half that moves are held meanwhile; rows of the
     * other half may change under the copy, which is fine: the copy drops them.
     *
     * Each table is paged by a key that a row keeps while it lives (its rowid, or its primary key
     * in a WITHOUT ROWID table), never by offset: the half that stays, and the cleanup of earlier
     * splits, delete rows during the copy, and an offset would then skip rows of the half that moves.
     */
    async copyRows(right, epoch, live) {
        const meta = this.meta;
        const target = this.node(right);
        // Tables and indexes first, as a snapshot would carry them: the copy may not have created a
        // subclass's yet (a sharded table creates its own on first use).
        const schema = this.sql
            .exec("SELECT name, sql FROM sqlite_master WHERE type IN ('table', 'index') ORDER BY type DESC, name")
            .toArray()
            .filter((o) => o.sql !== null && !isInternalTable(o.name));
        await target.shardedReceiveSchema(meta.name, right, epoch, schema);
        let first = true;
        for (const table of this.tables()) {
            const order = this.pageOrder(table, schema.find((o) => o.name === table)?.sql ?? "");
            const columns = order.columns.join(", ");
            const select = `SELECT ${order.rowid ? `${columns}, ` : ""}* FROM ${quoteIdentifier(table)}`;
            let after = null;
            for (;;) {
                if (!live())
                    throw new Error("split timed out copying rows");
                const where = after ? ` WHERE (${columns}) > (${after.map(() => "?").join(", ")})` : "";
                const cursor = this.sql.exec(`${select}${where} ORDER BY ${columns} LIMIT ?`, ...(after ?? []), COPY_PAGE_ROWS);
                const names = cursor.columnNames;
                const keyAt = order.rowid ? [0] : order.columns.map((c) => names.indexOf(unquoteIdentifier(c)));
                const rows = [];
                let bytes = 0;
                for (const row of cursor.raw()) {
                    after = keyAt.map((i) => row[i]);
                    rows.push(order.rowid ? row.slice(1) : row);
                    for (const value of row)
                        bytes += value instanceof ArrayBuffer ? value.byteLength : String(value).length;
                    if (bytes >= COPY_BATCH_BYTES)
                        break;
                }
                if (rows.length === 0 && !first)
                    break;
                await target.shardedReceive(meta.name, right, epoch, first, table, order.rowid ? names.slice(1) : names, rows);
                first = false;
                if (rows.length === 0 || (rows.length < COPY_PAGE_ROWS && bytes < COPY_BATCH_BYTES))
                    break;
            }
        }
        if (!(await target.shardedReceiveDone(meta.name, right, epoch)))
            throw new Error("the copy did not take this split's meta");
    }
    /**
     * The columns that page a table in a stable order: its rowid (under a name no column shadows),
     * or the primary key of a WITHOUT ROWID table. Returned quoted, ready for SQL.
     */
    pageOrder(table, sql) {
        const info = this.sql.exec(`PRAGMA table_info(${quoteIdentifier(table)})`).toArray();
        if (/\bWITHOUT\s+ROWID\b/i.test(sql)) {
            const key = info.filter((c) => c.pk > 0).sort((a, b) => a.pk - b.pk);
            if (key.length === 0)
                throw new Error(`table ${table} has no primary key to copy it by`);
            return { columns: key.map((c) => quoteIdentifier(c.name)), rowid: false };
        }
        const names = new Set(info.map((c) => c.name.toLowerCase()));
        const alias = ["rowid", "_rowid_", "oid"].find((a) => !names.has(a));
        if (!alias)
            throw new Error(`table ${table} shadows every name of its rowid: it cannot be copied in order`);
        return { columns: [alias], rowid: true };
    }
    /** This object's tables, the subclass's too (not SQLite's or the runtime's). */
    tables() {
        return this.sql
            .exec("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
            .toArray()
            .map((t) => t.name)
            .filter((name) => !isInternalTable(name));
    }
    node(bits) {
        const nodes = this.nodes();
        return nodes.get(nodes.idFromName(shardObjectName(this.meta.name, bits)));
    }
    async activateSibling() {
        const pending = this.meta?.activate;
        if (!pending)
            return;
        if (!(await this.node(pending.bits).shardedActivate(this.meta.name, pending.bits, pending.epoch))) {
            console.error(JSON.stringify({ event: "split-activate-refused", node: this.nodeLabel(), sibling: pending.bits }));
            return;
        }
        const now = this.meta;
        if (now.activate?.epoch === pending.epoch && now.activate.bits === pending.bits)
            this.saveMeta({ ...now, activate: null });
    }
    // ---- Alarms ----
    /**
     * Runs on a serving shard once the time given to `scheduleAlarm` has come (it may run a little
     * later, and the shard may have split since: check what is due). Never in a split's copy before
     * it serves. If it throws, it runs again a second later. The default does nothing.
     */
    async onAlarm() { }
    /**
     * Asks for `onAlarm` at `at` (or earlier, if an earlier time is already asked for). Use it, not
     * `ctx.storage.setAlarm`: the alarm is shared with splits, and this never postpones their work
     * nor loses the time asked for. A split's new shard keeps the time too.
     */
    async scheduleAlarm(at) {
        const time = typeof at === "number" ? at : at.getTime();
        const wake = this.loadWake();
        if (wake === null || time < wake)
            this.saveWake(time);
        if (this.meta?.state === "shard" || !this.meta)
            await this.armAlarm(time);
    }
    /**
     * Sets the alarm for `time` unless it is set for earlier already. For a time that has come it
     * sets it at once, without reading the alarm first: no alarm can be earlier, and setting it
     * before any await keeps it in the same commit as the writes just before (a split's meta, a
     * claim), so a crash cannot keep one without the other.
     */
    async armAlarm(time) {
        if (time <= Date.now())
            return this.ctx.storage.setAlarm(time);
        const current = await this.ctx.storage.getAlarm();
        if (current === null || time < current)
            await this.ctx.storage.setAlarm(time);
    }
    /**
     * While a split's work is pending here (a sibling to activate, rows to clear), makes sure an
     * alarm will do it, at most every ALARM_CHECK_MS: an alarm can be lost, and nothing else would
     * activate the sibling. Called on routed calls, which keep reaching a shard that has split.
     */
    checkAlarm() {
        const meta = this.meta;
        if (!meta || !(meta.activate || meta.cleanup.length > 0 || meta.foreign) || Date.now() - this.alarmCheckedAt < ALARM_CHECK_MS)
            return;
        this.alarmCheckedAt = Date.now();
        void this.ctx.storage.getAlarm().then((at) => (at === null ? this.ctx.storage.setAlarm(Date.now()) : undefined));
    }
    alarmCheckedAt = 0;
    /** When the subclass asked for `onAlarm`, if it did. */
    loadWake() {
        const row = this.sql.exec("SELECT value FROM sharded_meta WHERE key = 'wake'").toArray()[0];
        return row ? Number(row.value) : null;
    }
    saveWake(time) {
        if (time === null)
            this.sql.exec("DELETE FROM sharded_meta WHERE key = 'wake'");
        else
            this.sql.exec("INSERT INTO sharded_meta (key, value) VALUES ('wake', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", String(time));
    }
    /**
     * Activates a new sibling, clears what splits gave away, and runs `onAlarm` when it is due. It
     * never throws: the runtime retries a failing alarm only a few times, and the sibling is
     * activated by nothing else, so a failure here reschedules the alarm itself for as long as there
     * is work left. Subclasses cannot override it (the constructor refuses): see `onAlarm`.
     */
    async alarm() {
        const meta = this.meta;
        // A verbatim copy not claimed yet carries the source's alarm: it is not this object's.
        if (!meta || this.isUnclaimedCopy())
            return;
        let failed = false;
        if (meta.activate) {
            try {
                await this.activateSibling();
            }
            catch (error) {
                failed = true;
                console.error(JSON.stringify({ event: "split-activate-failed", node: this.nodeLabel(), sibling: meta.activate.bits, error: String(error) }));
            }
        }
        try {
            this.cleanupBatch();
        }
        catch (error) {
            failed = true;
            console.error(JSON.stringify({ event: "split-cleanup-failed", node: this.nodeLabel(), error: String(error) }));
        }
        // The subclass's turn, on a serving shard only: a copy's is armed when it is activated.
        const serving = this.meta.state === "shard";
        const wake = this.loadWake();
        if (serving && wake !== null && wake <= Date.now()) {
            this.saveWake(null);
            try {
                await this.onAlarm();
            }
            catch (error) {
                console.error(JSON.stringify({ event: "on-alarm-failed", node: this.nodeLabel(), error: String(error) }));
                await this.scheduleAlarm(Date.now() + ALARM_RETRY_MS);
            }
        }
        const now = this.meta;
        const times = [];
        if (now.activate || failed)
            times.push(Date.now() + ALARM_RETRY_MS);
        else if (now.cleanup.length > 0 || now.foreign)
            times.push(Date.now() + DELETE_PAUSE_MS);
        const next = this.loadWake();
        if (serving && next !== null)
            times.push(next);
        if (times.length > 0)
            await this.armAlarm(Math.min(...times));
    }
    /** Deletes one batch of rows this object gave away: the base table's first, then the subclass's. */
    cleanupBatch() {
        const meta = this.meta;
        const range = meta.cleanup[0];
        if (range) {
            const deleted = this.sql.exec("DELETE FROM sharded_rows WHERE key IN (SELECT key FROM sharded_rows WHERE hash >= ? AND hash <= ? LIMIT ?)", range[0], range[1], DELETE_BATCH_ROWS).rowsWritten;
            if (deleted === 0)
                this.saveMeta({ ...this.meta, cleanup: this.meta.cleanup.slice(1) });
        }
        else if (meta.foreign) {
            const bits = meta.bits;
            const deleted = this.ctx.storage.transactionSync(() => this.cleanupForeign((key) => ownsHash(bits, keyHash(key)), DELETE_BATCH_ROWS));
            if (deleted === 0)
                this.saveMeta({ ...this.meta, foreign: false });
        }
    }
    // ---- The sibling's side of a split ----
    /** Schedules this object's storage to be replaced by the snapshot, then restarts to apply it. */
    /** Nothing: a split calls it first to start this object, before it holds writes. */
    async shardedWarm() { }
    async shardedRestore(snapshot) {
        if (this.serving())
            throw new Error("refusing to overwrite a serving shard");
        const started = Date.now();
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
        return { scheduleMs: Date.now() - started };
    }
    /**
     * The logical copy's tables and indexes: replaces this object's with them. What is here is
     * not a serving shard's (an unused object, or the copy of an abandoned attempt, maybe made
     * under an older schema), so it all goes: the copy must have the source's schema exactly.
     * From here on this object takes the batches of attempt `epoch` only.
     */
    async shardedReceiveSchema(_name, _bits, epoch, objects) {
        if (this.serving())
            throw new Error("refusing to change a serving shard");
        this.receiving = epoch;
        this.ctx.storage.transactionSync(() => {
            for (const t of this.tables())
                this.sql.exec(`DROP TABLE IF EXISTS ${quoteIdentifier(t)}`);
            this.meta = null;
            for (const o of objects)
                this.sql.exec(o.sql);
        });
    }
    /**
     * One batch of the logical copy, from attempt `epoch`: rows of one table. The first batch wipes
     * every table. A batch can arrive late, after its attempt was abandoned (and another began, or
     * even finished): it would put back rows as they were then, so only the batches of the attempt
     * whose schema this object took are written, and never on a serving shard.
     */
    async shardedReceive(_name, _bits, epoch, first, table, columns, rows) {
        if (this.serving())
            throw new Error("refusing to overwrite a serving shard");
        if (this.receiving !== epoch)
            throw new Error(`refusing a batch of split attempt ${epoch}: this object takes attempt ${this.receiving ?? "none"}`);
        this.ctx.storage.transactionSync(() => {
            if (first) {
                for (const t of this.tables())
                    this.sql.exec(`DELETE FROM ${quoteIdentifier(t)}`);
                this.meta = null;
            }
            if (rows.length === 0)
                return;
            // shardedReceiveSchema created every table first.
            if (!this.tables().includes(table))
                throw new Error(`table ${table} does not exist in the copy`);
            const sql = `INSERT OR REPLACE INTO ${quoteIdentifier(table)} (${columns.map(quoteIdentifier).join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`;
            for (const row of rows)
                this.sql.exec(sql, ...row);
        });
    }
    /** The logical copy is complete: it holds the source's meta, from this split. */
    async shardedReceiveDone(name, bits, epoch) {
        if (this.receiving !== epoch)
            return false;
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
     * A shard that serves and is not splitting. Anything else may be overwritten by a split's copy:
     * an unused object, a copy (claimed or not) from an abandoned attempt.
     */
    serving() {
        return this.meta?.state === "shard" && !this.isUnclaimedCopy();
    }
    /**
     * Claims the copy for the shard `bits`, if it is the copy made by attempt `epoch` of its parent:
     * becomes that shard, not serving yet, and deletes the rows of the half it does not own.
     */
    async shardedClaim(name, bits, parent, epoch) {
        const meta = this.meta;
        if (!meta || meta.name !== name)
            return false;
        if (meta.bits === bits)
            return meta.epoch === epoch && meta.state === "copy"; // claimed already
        if (!this.isUnclaimedCopy() || meta.bits !== parent || meta.splitting !== epoch)
            return false; // not this copy
        this.receiving = null; // claimed: no batch may change it now
        this.ctx.storage.transactionSync(() => {
            this.afterClone((key) => ownsHash(bits, keyHash(key)));
            this.saveMeta({ name, bits, state: "copy", epoch, splitting: null, activate: null, cleanup: this.outside(bits), foreign: true, parent, splitAt: Date.now() });
        });
        await this.armAlarm(Date.now());
        return true;
    }
    /** The hash ranges outside a shard's. */
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
    async shardedActivate(name, bits, epoch) {
        const meta = this.meta;
        if (!meta || meta.name !== name)
            return false;
        if (meta.bits !== bits)
            return meta.state === "shard" && meta.splitting === null && meta.bits.startsWith(bits); // serving, and split since
        if (meta.epoch !== epoch)
            return false;
        if (meta.state === "copy") {
            this.saveMeta({ ...meta, state: "shard" });
            // The subclass's alarm, copied from the original, was held until now.
            const wake = this.loadWake();
            if (wake !== null)
                await this.armAlarm(wake);
            this.checkAlarm(); // its cleanup's alarm, in case it was lost while it waited
        }
        return true;
    }
    /** The right half of an empty shard's initial split: empty, and serving at once. */
    async shardedAdoptEmpty(name, bits, parent) {
        if (this.meta)
            return;
        this.saveMeta({ name, bits, state: "shard", epoch: 0, splitting: null, activate: null, cleanup: [], parent, splitAt: Date.now() });
    }
    async shardedDescribe() {
        const meta = this.meta;
        const storedRows = this.sql.exec("SELECT COUNT(*) AS n FROM sharded_rows").one().n;
        let rows = storedRows;
        if (meta) {
            const [lo, hi] = hashRange(meta.bits);
            rows = this.sql.exec("SELECT COUNT(*) AS n FROM sharded_rows WHERE hash >= ? AND hash <= ?", lo, hi).one().n;
        }
        const state = !meta ? "new" : this.hold ? "splitting" : meta.state === "copy" || this.isUnclaimedCopy() ? "copy" : "shard";
        return { bits: meta?.bits ?? "", state, rows, storedRows, callsPerSecond: this.rate(), lastSplit: meta?.lastSplit ?? null, lastSplitError: meta?.lastSplitError ?? null, splits: meta?.splits ?? [], counters: { ...this.counters } };
    }
}
/** Names a routed call may not reach: the base class's own methods, and the runtime's handlers. */
const RESERVED_METHODS = new Set([
    ...Object.getOwnPropertyNames(ShardedObject.prototype),
    ...Object.getOwnPropertyNames(Object.prototype),
    "fetch",
    "alarm",
    "webSocketMessage",
    "webSocketClose",
    "webSocketError",
]);
//# sourceMappingURL=index.js.map