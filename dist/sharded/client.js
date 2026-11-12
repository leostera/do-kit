// The client side of sharded objects: routes keys to shards through the splits it
// knows, learns new ones from "moved" answers, and shares what it learned in the isolate and the
// colo (Cache API). Splits are final, so stale knowledge costs hops, never correctness.
import { ATOMIC_HOLD_MS, atomicTxId } from "./atomic.js";
import { hashBit, keyHash } from "./hash.js";
import { hostBits, shardObjectName, nodeName } from "./index.js";
/**
 * Object ids by name, per namespace binding: computing one from a name is not free, and a client
 * asks for the same shards' ids on every call. Bounded; a namespace made per call (one scoped to a
 * jurisdiction) is simply not remembered.
 */
const knownIds = new WeakMap();
const MAX_KNOWN_IDS = 20_000;
/** Stubs a client keeps, one per shard it called. */
const MAX_KEPT_STUBS = 256;
export function objectId(namespace, objectName) {
    let ids = knownIds.get(namespace);
    if (!ids)
        knownIds.set(namespace, (ids = new Map()));
    let known = ids.get(objectName);
    if (!known) {
        if (ids.size >= MAX_KNOWN_IDS)
            ids.clear();
        const id = namespace.idFromName(objectName);
        ids.set(objectName, (known = { id, text: id.toString() }));
    }
    return known;
}
/** Keys that had to go to one shard belong to several (nothing was applied). */
export class SpansShards extends Error {
}
/**
 * The client and the object route keys differently (`routeOf`): nothing was called. Calls would
 * otherwise reach shards that don't hold their keys once the tree splits.
 */
export class RouteMismatch extends Error {
    key;
    clientHash;
    objectHash;
    constructor(key, clientHash, objectHash) {
        super(`key ${JSON.stringify(key)}: the client routes it to hash ${clientHash}, the object to ${objectHash}; declare the same routeOf on both`);
        this.key = key;
        this.clientHash = clientHash;
        this.objectHash = objectHash;
        this.name = "RouteMismatch";
    }
}
/** v2: the splits and the presplit depth (v1 entries, splits only, are left to expire). */
const CACHE_ORIGIN = "https://do-kit.invalid/v2/";
/**
 * Splits known per object (keyed by its root's Durable Object ID, unique per namespace and
 * name), shared by every client in this isolate.
 */
const knownSplits = new Map();
/**
 * Each object's presplit depth, once a shard said so: every node above it is split (it never
 * existed), so calls go straight to that depth. Without it, each isolate would ask the shards
 * above, one hop per level, for every path: the top ones are shared by all keys and overload.
 */
const knownPresplit = new Map();
/** Objects whose root said a client's `presplitHint` was wrong: they learn the depth from the root. */
const hintRejected = new Set();
const loadedFromCache = new Set();
/**
 * Objects whose splits an isolate keeps, the most recently used: a Worker that reaches one object
 * per account would otherwise grow these for the isolate's life. A forgotten object costs a read
 * of the colo cache, or a few hops.
 */
export const MAX_KNOWN_OBJECTS = 10_000;
/** A hop limit: deeper than any maxDepth, so a loop is a bug, not a slow call. */
const MAX_HOPS = 64;
/**
 * How long a call waits for a shard to serve, by default: a new shard is activated just after a
 * split. A write held by a split in progress waits until that split is done or gives up.
 */
const NOT_READY_WAIT_MS = 30_000;
/** Past a split's deadline, how long a held write still waits for the new shard to be activated. */
const ACTIVATION_SLACK_MS = 5_000;
/** The furthest a shard's "until" can push a call's wait, whatever it says. */
const MAX_HELD_WAIT_MS = 10 * 60_000;
/**
 * After waiting this long on shards that are not ready, a call forgets the splits it knows of this
 * object, once, and routes from the root again: an object whose storage was deleted is not ready
 * forever, and only the root can tell. A shard that is only slow to start costs a few hops.
 */
const FORGET_AFTER_MS = 1_000;
/** How long a hop's calls took: a few quantiles, which tell queued calls from one slow one. */
function spread(took) {
    const sorted = [...took].sort((a, b) => a - b);
    const at = (q) => sorted[Math.min(sorted.length - 1, Math.floor(q * sorted.length))] ?? 0;
    return { p10Ms: at(0.1), p50Ms: at(0.5), p90Ms: at(0.9), slowestMs: at(1) };
}
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** Waits a little longer each time a shard is not ready; throws once that has gone on too long. */
class Backoff {
    what;
    limitMs;
    reason;
    wait = 5;
    waited = 0;
    /** A split in progress said it is done or gives up by then: wait at least that long. */
    until = 0;
    constructor(what, limitMs, reason = "a new shard did not start serving") {
        this.what = what;
        this.limitMs = limitMs;
        this.reason = reason;
    }
    /** A shard holding the call for a split that gives up at `until` (its clock). */
    extend(until) {
        if (until !== undefined)
            this.until = Math.max(this.until, Math.min(until, Date.now() + MAX_HELD_WAIT_MS));
    }
    async next() {
        if (this.waited > this.limitMs && Date.now() > this.until + ACTIVATION_SLACK_MS)
            throw new Error(`${this.what}: ${this.reason}`);
        // Jittered: callers held by one split are released together, and must not retry in step.
        const wait = this.wait / 2 + Math.random() * this.wait;
        await sleep(wait);
        this.waited += wait;
        this.wait = Math.min(this.wait * 2, 250);
    }
}
export class ShardedClient {
    namespace;
    name;
    /** What known splits are filed under: two classes may share a name, never a root ID. */
    identity;
    notReadyWaitMs;
    stubOptions;
    logWideHops;
    presplitHint;
    routeOf;
    constructor(namespace, name, options = {}) {
        this.namespace = namespace;
        this.name = name;
        this.notReadyWaitMs = options.notReadyWaitMs ?? NOT_READY_WAIT_MS;
        this.stubOptions = options.stubOptions ?? null;
        this.logWideHops = options.logWideHops ?? 0;
        this.presplitHint = options.presplitHint ?? 0;
        this.routeOf = options.routeOf ?? null;
        this.identity = objectId(namespace, nodeName(name, "")).text;
    }
    get splits() {
        let set = knownSplits.get(this.identity);
        if (!set) {
            knownSplits.set(this.identity, (set = new Set()));
            if (knownSplits.size > MAX_KNOWN_OBJECTS) {
                const oldest = knownSplits.keys().next().value;
                knownSplits.delete(oldest);
                knownPresplit.delete(oldest);
                hintRejected.delete(oldest);
                loadedFromCache.delete(oldest);
            }
        }
        return set;
    }
    /** Marks this object as just used, so it is the last its isolate forgets. */
    touch() {
        const set = knownSplits.get(this.identity);
        if (!set)
            return;
        knownSplits.delete(this.identity);
        knownSplits.set(this.identity, set);
    }
    /** The presplit depth known for this object (0: none known). */
    get presplitTo() {
        return knownPresplit.get(this.identity) ?? (hintRejected.has(this.identity) ? 0 : this.presplitHint);
    }
    /** Whether the node `bits` is known to be split: above the presplit depth, or seen splitting. */
    isSplit(bits) {
        return bits.length < this.presplitTo || this.splits.has(bits);
    }
    /** The hash a key is routed by: of its route, which is the key itself by default. */
    routeHash(key) {
        if (!this.routeOf)
            return keyHash(key);
        const route = this.routeOf(key);
        if (typeof route !== "string")
            throw new Error(`sharded ${this.name}: routeOf(${JSON.stringify(key)}) is not a string`);
        return keyHash(route);
    }
    /** The deepest known node owning a hash. */
    shardFor(hash) {
        let bits = "";
        while (this.isSplit(bits))
            bits += hashBit(hash, bits.length);
        return bits;
    }
    stub(bits) {
        const ns = this.namespace;
        const { id } = objectId(ns, shardObjectName(this.name, bits));
        return this.stubOptions ? ns.get(id, this.stubOptions) : ns.get(id);
    }
    /**
     * Calls a shard through the stub this client keeps for it, making one on first use: a client
     * that calls a shard again does not open it again. A stub whose call throws is dropped, and the
     * next call makes a new one. A client belongs to the request (or the Durable Object) that made
     * it; if another request uses it, its kept stubs are that first request's and cannot be used, so
     * the call is made again on a stub of this request's (nothing was sent on the other).
     */
    async onShard(bits, call) {
        const kept = this.stubs.get(bits);
        const stub = kept ?? this.keep(bits);
        try {
            return await call(stub);
        }
        catch (error) {
            if (this.stubs.get(bits) === stub)
                this.stubs.delete(bits);
            if (kept && /different request/i.test(String(error)))
                return call(this.keep(bits));
            throw error;
        }
    }
    keep(bits) {
        const stub = this.stub(bits);
        if (this.stubs.size >= MAX_KEPT_STUBS)
            this.stubs.delete(this.stubs.keys().next().value);
        this.stubs.set(bits, stub);
        return stub;
    }
    stubs = new Map();
    /**
     * Calls `method(keys, payloads, ...args)` on every shard owning some of the keys, with its share
     * of them (and of `payloads`, aligned with `keys`). One result per shard.
     */
    async call(keys, method, payloads, ...args) {
        return this.route(false, keys, method, payloads, this.method(method, args));
    }
    /**
     * Like `call`, for keys that must be handled together by one shard (an all-or-nothing update):
     * throws SpansShards, before anything is applied, if they belong to more than one.
     */
    async callOneShard(keys, method, payloads, ...args) {
        if (keys.length === 0)
            throw new Error(`sharded ${this.name}: callOneShard needs at least one key to pick its shard`);
        const [only] = await this.route(true, keys, method, payloads, this.method(method, args));
        return only.result;
    }
    /** How `route` calls a shard for a method of the subclass. */
    method(method, args) {
        return (stub, bits, keys, payloads, hashes) => stub.shardedCall(this.name, bits, method, keys, payloads, args, this.stubOptions, this.presplitTo, hashes);
    }
    /** How `route` calls a shard for a step of an atomic call. */
    atomicStep(step, args) {
        return (stub, bits, keys, payloads, hashes) => stub.shardedAtomic(this.name, bits, step, keys, payloads, args, this.stubOptions, this.presplitTo, hashes);
    }
    /**
     * Calls `method` on the shards owning `keys` (RFD0008), all or nothing: every shard's share
     * applies, or none does. Keys in one shard: one call, as `callOneShard`. Keys in several: the
     * shard of the smallest key (the primary) records the outcome once every other shard has
     * prepared its share, holding its keys; then each applies or drops it. `method` names one of the
     * class's `atomic` methods. Refused anywhere: nothing is applied, and the refusals come back.
     * Writes to the keys wait while the call holds them; reads see the rows from before.
     */
    async atomically(keys, method, payloads, ...args) {
        if (keys.length === 0)
            throw new Error(`sharded ${this.name}: atomically needs at least one key`);
        if (payloads && payloads.length !== keys.length)
            throw new Error(`sharded ${this.name}: ${payloads.length} payloads for ${keys.length} keys (payloads line up with keys)`);
        await this.loadCache();
        // The call's age, kept across retries: an older call never gives way to a younger one.
        const startedAt = Date.now();
        // A call that gave way waits out the older one, up to notReadyWaitMs over all its retries.
        const contention = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs, "an atomic call kept giving way to older calls");
        for (let splits = 0;;) {
            if (splits > MAX_HOPS)
                throw new Error(`sharded ${this.name}: atomic call did not settle`);
            if (new Set(keys.map((k) => this.shardFor(this.routeHash(k)))).size === 1) {
                try {
                    const [only] = await this.route(true, keys, method, payloads, this.atomicStep({ step: "solo", method }, args));
                    const answer = only.result;
                    return "refused" in answer ? { committed: false, refusals: [{ keys, result: answer.refused }] } : { committed: true, results: [{ keys, result: answer.applied }] };
                }
                catch (error) {
                    // A split this client had not heard of: the keys span shards now.
                    if (error instanceof SpansShards) {
                        splits++;
                        continue;
                    }
                    throw error;
                }
            }
            const outcome = await this.atomicAcross(keys, method, payloads, args, startedAt);
            if (outcome === "again")
                splits++;
            else if (outcome === "contended")
                await contention.next();
            else
                return outcome;
        }
    }
    /**
     * An atomic call over several shards. Nothing applied, to be retried: "again" if a split moved the
     * primary's keys apart before the commit, "contended" if a shard gave way to an older call.
     */
    async atomicAcross(keys, method, payloads, args, startedAt) {
        const started = Date.now();
        const tx = atomicTxId(startedAt);
        const primaryKey = [...keys].sort()[0];
        const primaryBits = this.shardFor(this.routeHash(primaryKey));
        const mine = keys.map((k) => this.shardFor(this.routeHash(k)) === primaryBits);
        const pick = (xs, primary) => xs.filter((_, i) => mine[i] === primary);
        const deadline = Date.now() + ATOMIC_HOLD_MS;
        const others = pick(keys, false);
        let prepares;
        try {
            prepares = await this.route(false, others, method, payloads && pick(payloads, false), this.atomicStep({ step: "prepare", tx, method, primaryKey, deadline }, args));
        }
        catch (error) {
            // Some shards may hold: the abort lets them drop at their deadline at the latest.
            await this.decideAbort(tx, primaryKey);
            throw error;
        }
        const prepared = prepares.filter((r) => "prepared" in r.result);
        const contended = prepares.some((r) => "contended" in r.result);
        const refusals = prepares.flatMap((r) => ("refused" in r.result ? [{ keys: r.keys, result: r.result.refused }] : []));
        const preparedAt = Date.now();
        const log = (outcome, extra = {}) => console.log(JSON.stringify({ event: "sharded-atomic", object: this.name, shards: prepares.length + 1, keys: keys.length, outcome, prepareMs: preparedAt - started, ms: Date.now() - started, ...extra }));
        if (refusals.length > 0 || contended) {
            await this.decideAbort(tx, primaryKey);
            await this.resolveAll(prepared, tx, "abort");
            // Refused wins: the call would be refused again.
            if (refusals.length > 0) {
                log("refused");
                return { committed: false, refusals };
            }
            log("contended");
            return "contended";
        }
        let answer;
        let primaryKeys;
        try {
            const [commit] = await this.route(true, pick(keys, true), method, payloads && pick(payloads, true), this.atomicStep({ step: "commit", tx, method }, args));
            answer = commit.result;
            primaryKeys = commit.keys;
        }
        catch (error) {
            if (error instanceof SpansShards) {
                await this.decideAbort(tx, primaryKey);
                await this.resolveAll(prepared, tx, "abort");
                log("again");
                return "again";
            }
            // The commit's answer is lost: the outcome decides, as it would for the shards.
            const outcome = await this.atomicOutcome(tx, primaryKey).catch(() => null);
            if (outcome === null)
                throw error;
            const resolved = await this.resolveAll(prepared, tx, outcome);
            log(outcome, { lostCommitAnswer: true });
            if (outcome === "abort")
                throw error;
            return { committed: true, results: resolved.results };
        }
        if ("contended" in answer) {
            // The primary gave way to an older call, and recorded an abort.
            await this.resolveAll(prepared, tx, "abort");
            log("contended at the primary");
            return "contended";
        }
        if ("refused" in answer || ("outcome" in answer && answer.outcome === "abort")) {
            await this.resolveAll(prepared, tx, "abort");
            log("refused at the primary");
            // Refused by the primary's check, or aborted by a shard's recovery before the commit came.
            return { committed: false, refusals: "refused" in answer ? [{ keys: primaryKeys, result: answer.refused }] : [] };
        }
        const results = "applied" in answer ? [{ keys: primaryKeys, result: answer.applied }] : [];
        const resolved = await this.resolveAll(prepared, tx, "commit");
        if (resolved.all)
            await this.atomicTo(primaryKey, { step: "forget", tx }).catch(() => undefined);
        log("commit", { resolveMs: Date.now() - started });
        return { committed: true, results: [...results, ...resolved.results] };
    }
    /** The outcome of atomic call `tx`, from its primary (recording an abort if it has none). */
    async atomicOutcome(tx, primaryKey) {
        const answer = await this.atomicTo(primaryKey, { step: "outcome", tx });
        return answer.outcome;
    }
    /** A step at the primary of `tx`. */
    async atomicTo(primaryKey, step) {
        const [only] = await this.route(true, [primaryKey], step.step, null, this.atomicStep(step, []));
        return only.result;
    }
    /** Records an abort at the primary, if it can: else the holds end at their deadline, aborted. */
    async decideAbort(tx, primaryKey) {
        await this.atomicTo(primaryKey, { step: "abort", tx }).catch((error) => console.error(JSON.stringify({ event: "sharded-atomic-abort-failed", object: this.name, tx, error: String(error) })));
    }
    /** Tells every shard that prepared the outcome; each that applied answers its result. A lost one recovers at its deadline. */
    async resolveAll(prepared, tx, outcome) {
        const results = [];
        let all = true;
        await Promise.all(prepared.map(async (share) => {
            try {
                const [only] = await this.route(true, share.keys, "resolve", null, this.atomicStep({ step: "resolve", tx, outcome }, []));
                const answer = only.result;
                if ("applied" in answer)
                    results.push({ keys: share.keys, result: answer.applied });
            }
            catch (error) {
                all = false;
                console.error(JSON.stringify({ event: "sharded-atomic-resolve-failed", object: this.name, tx, outcome, error: String(error) }));
            }
        }));
        return { all, results };
    }
    async route(oneShard, keys, method, payloads, invoke) {
        if (payloads && payloads.length !== keys.length)
            throw new Error(`sharded ${this.name}: ${payloads.length} payloads for ${keys.length} keys (payloads line up with keys)`);
        await this.loadCache();
        const hashes = keys.map((key) => this.routeHash(key));
        const out = [];
        let pending = keys.map((_, i) => i);
        const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
        let forgot = false;
        for (let hop = 0; pending.length > 0; hop++) {
            if (hop > MAX_HOPS)
                throw new Error(`sharded ${this.name}: routing did not settle`);
            let waiting = false;
            let stale = false;
            const groups = new Map();
            for (const i of pending) {
                const bits = this.shardFor(hashes[i]);
                let group = groups.get(bits);
                if (!group)
                    groups.set(bits, (group = []));
                group.push(i);
            }
            if (oneShard && groups.size > 1)
                throw new SpansShards(`keys of ${this.name} belong to ${groups.size} shards`);
            const hopStarted = Date.now();
            const took = [];
            const retry = [];
            await Promise.all([...groups].map(async ([bits, positions]) => {
                const groupKeys = positions.map((i) => keys[i]);
                const groupPayloads = payloads ? positions.map((i) => payloads[i]) : null;
                const called = Date.now();
                const groupHashes = positions.map((i) => hashes[i]);
                const answer = (await this.onShard(bits, (stub) => invoke(stub, bits, groupKeys, groupPayloads, groupHashes)));
                took.push(Date.now() - called);
                if ("ok" in answer)
                    out.push({ keys: groupKeys, positions, result: answer.ok });
                else if ("routeMismatch" in answer)
                    throw new RouteMismatch(answer.routeMismatch.key, answer.routeMismatch.client, answer.routeMismatch.object);
                else if ("moved" in answer) {
                    this.learn(answer.moved, answer.presplit);
                    retry.push(...positions);
                }
                else if ("stale" in answer) {
                    stale = true;
                    retry.push(...positions);
                }
                else {
                    waiting = true; // not serving yet: the same shard, a little later
                    backoff.extend(answer.until);
                    retry.push(...positions);
                }
            }));
            // A routed call that fans out wide: how long its hop took, and its slowest shard.
            if (this.logWideHops > 0 && groups.size >= this.logWideHops)
                console.log(JSON.stringify({ event: "sharded-wide-hop", object: this.name, method: String(method), hop, shards: groups.size, keys: pending.length, ms: Date.now() - hopStarted, ...spread(took), retries: retry.length }));
            if (stale || (waiting && !forgot && backoff.waited >= FORGET_AFTER_MS)) {
                forgot = true;
                this.forget();
            }
            else if (waiting) {
                await backoff.next();
                hop--;
            }
            pending = retry;
        }
        return out;
    }
    /** Calls `method(...args)` on every shard. */
    async scan(method, ...args) {
        await this.loadCache();
        for (let attempt = 0;; attempt++) {
            const out = [];
            const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
            let stale = false;
            const visit = async (bits, depth) => {
                if (depth > MAX_HOPS)
                    throw new Error(`sharded ${this.name}: scan did not settle`);
                if (stale)
                    return;
                if (this.isSplit(bits)) {
                    await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
                    return;
                }
                const answer = (await this.onShard(bits, (stub) => stub.shardedScan(this.name, bits, method, args, this.stubOptions, this.presplitTo)));
                if ("ok" in answer)
                    out.push(answer.ok);
                else if ("moved" in answer) {
                    this.learn(answer.moved, answer.presplit);
                    await visit(bits, depth + 1);
                }
                else if ("stale" in answer) {
                    // A scan always reaches the root (its leftmost shard), which says so when the map is
                    // stale: start over from it, once the other visits stop.
                    stale = true;
                }
                else {
                    await backoff.next();
                    await visit(bits, depth);
                }
            };
            await visit("", 0);
            if (!stale)
                return out;
            if (attempt >= 3)
                throw new Error(`sharded ${this.name}: scan did not settle`);
            this.forget();
        }
    }
    /**
     * Deletes every shard's storage and alarm: the whole sharded object, for one that is done with.
     * Waits out splits in progress (both halves are then wiped). Nothing may write to the object
     * meanwhile: a write after its shard is wiped would make that shard again. The root's object
     * goes last, since it is how the others are found: a wipe that fails partway can run again.
     * Returns how many shards held something.
     */
    async wipe() {
        await this.loadCache();
        for (let attempt = 0;; attempt++) {
            let wiped = 0;
            let stale = false;
            let root = null;
            const backoff = new Backoff(`sharded ${this.name}`, this.notReadyWaitMs);
            const visit = async (bits, depth, last = false) => {
                if (depth > MAX_HOPS)
                    throw new Error(`sharded ${this.name}: wipe did not settle`);
                if (stale)
                    return;
                if (this.isSplit(bits)) {
                    await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
                    return;
                }
                // The root's object, at the leftmost shard of a tree that has split: after the rest.
                if (bits !== "" && hostBits(bits) === "" && !last) {
                    root = bits;
                    return;
                }
                const answer = (await this.stub(bits).shardedWipe(this.name, bits));
                if ("ok" in answer)
                    wiped += answer.ok.wiped ? 1 : 0;
                else if ("moved" in answer) {
                    this.learn(answer.moved, answer.presplit);
                    await visit(bits, depth + 1);
                }
                else if ("stale" in answer)
                    stale = true;
                else {
                    await backoff.next();
                    await visit(bits, depth);
                }
            };
            await visit("", 0);
            // A deeper split of the root's object than this client knew defers its leftmost shard again.
            while (!stale && root !== null) {
                const last = root;
                root = null;
                await visit(last, 0, true);
            }
            if (!stale) {
                // What this client learned of the object's splits is of no more use.
                this.forget();
                return wiped;
            }
            if (attempt >= 3)
                throw new Error(`sharded ${this.name}: wipe did not settle`);
            this.forget();
        }
    }
    /** How many shards this client knows of: one more than its known splits. There may be more. */
    async knownShardCount() {
        await this.loadCache();
        let splits = 0;
        const count = (bits) => {
            if (!this.isSplit(bits))
                return;
            splits++;
            count(`${bits}0`);
            count(`${bits}1`);
        };
        count("");
        return splits + 1;
    }
    /** The known splits seen in answers (for tests and operators); the presplit levels are not listed. */
    knownSplits() {
        return [...this.splits].sort();
    }
    /** The presplit depth this client knows of the object (0: none known). */
    knownPresplit() {
        return this.presplitTo;
    }
    /** Drops what this client knows of the object's splits, here and in the colo: it was stale. */
    forget() {
        this.splits.clear();
        knownPresplit.delete(this.identity);
        if (this.presplitHint > 0)
            hintRejected.add(this.identity);
        void this.saveCache();
    }
    learn(bits, presplit) {
        const newDepth = presplit !== undefined && presplit !== this.presplitTo;
        if (newDepth)
            knownPresplit.set(this.identity, presplit);
        if (this.isSplit(bits) && !newDepth)
            return;
        // A node that split had split ancestors: all of them are known to be split. Those above the
        // presplit depth are known without listing them.
        for (let i = this.presplitTo; i <= bits.length; i++)
            this.splits.add(bits.slice(0, i));
        void this.saveCache();
    }
    /**
     * Every shard of this tree, as its objects describe themselves (for operators): each shard at
     * the presplit depth (this client's, else the one the root reports), then down every split. A
     * shard never called is described as "new", and the walk goes on past it: in a presplit tree,
     * shard 10 may never have been called while 11 has, and a walk down from the root stops at 10.
     * Asks the objects, not this client's map of splits, so it sees splits no client here has met.
     */
    async describeTree() {
        const describe = async (bits) => {
            const id = this.namespace.idFromName(shardObjectName(this.name, bits));
            const stub = this.namespace.get(id);
            const node = await stub.shardedDescribe();
            return { ...node, bits: node.state === "new" ? bits : node.bits, object: id.toString() };
        };
        const out = [];
        // The object hosting `bits` hosts a deeper shard once `bits` has split: its left half stayed,
        // and each right half along the way went to an object of its own.
        const visit = async (bits) => {
            const node = await describe(bits);
            out.push(node);
            if (node.state === "new")
                return;
            const rights = [];
            for (let i = bits.length; i < node.bits.length; i++)
                rights.push(`${node.bits.slice(0, i)}1`);
            await Promise.all(rights.map(visit));
        };
        let depth = this.presplitTo;
        if (depth === 0)
            depth = (await describe("")).presplit;
        const leaves = Array.from({ length: 2 ** depth }, (_, i) => (depth === 0 ? "" : i.toString(2).padStart(depth, "0")));
        await Promise.all(leaves.map(visit));
        return out.sort((a, b) => (a.bits < b.bits ? -1 : a.bits > b.bits ? 1 : 0));
    }
    async loadCache() {
        this.touch();
        if (loadedFromCache.has(this.identity) || typeof caches === "undefined")
            return;
        loadedFromCache.add(this.identity);
        try {
            const hit = await caches.default.match(CACHE_ORIGIN + this.identity);
            if (!hit)
                return;
            const cached = (await hit.json());
            for (const bits of cached.splits)
                this.splits.add(bits);
            if (cached.presplit > 0 && !knownPresplit.has(this.identity))
                knownPresplit.set(this.identity, cached.presplit);
        }
        catch {
            // the cache is an optimisation only
        }
    }
    async saveCache() {
        if (typeof caches === "undefined")
            return;
        try {
            const body = JSON.stringify({ splits: [...this.splits], presplit: this.presplitTo });
            await caches.default.put(CACHE_ORIGIN + this.identity, new Response(body, { headers: { "cache-control": "max-age=86400", "content-type": "application/json" } }));
        }
        catch {
            // the cache is an optimisation only
        }
    }
}
export function sharded(namespace, name, options = {}) {
    return new ShardedClient(namespace, name, options);
}
//# sourceMappingURL=client.js.map