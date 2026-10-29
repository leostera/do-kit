// The client side of sharded objects: routes keys to shards through the splits it
// knows, learns new ones from "moved" answers, and shares what it learned in the isolate and the
// colo (Cache API). Splits are final, so stale knowledge costs hops, never correctness.
import { hashBit, keyHash, shardObjectName, nodeName } from "./index.js";
/** Keys that had to go to one shard belong to several (nothing was applied). */
export class SpansShards extends Error {
}
const CACHE_ORIGIN = "https://do-kit.invalid/";
/**
 * Splits known per object (keyed by its root's Durable Object ID, unique per namespace and
 * name), shared by every client in this isolate.
 */
const knownSplits = new Map();
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
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** Waits a little longer each time a shard is not ready; throws once that has gone on too long. */
class Backoff {
    what;
    limitMs;
    wait = 5;
    waited = 0;
    /** A split in progress said it is done or gives up by then: wait at least that long. */
    until = 0;
    constructor(what, limitMs) {
        this.what = what;
        this.limitMs = limitMs;
    }
    /** A shard holding the call for a split that gives up at `until` (its clock). */
    extend(until) {
        if (until !== undefined)
            this.until = Math.max(this.until, Math.min(until, Date.now() + MAX_HELD_WAIT_MS));
    }
    async next() {
        if (this.waited > this.limitMs && Date.now() > this.until + ACTIVATION_SLACK_MS)
            throw new Error(`${this.what}: a new shard did not start serving`);
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
    constructor(namespace, name, options = {}) {
        this.namespace = namespace;
        this.name = name;
        this.notReadyWaitMs = options.notReadyWaitMs ?? NOT_READY_WAIT_MS;
        this.identity = namespace.idFromName(nodeName(name, "")).toString();
    }
    get splits() {
        let set = knownSplits.get(this.identity);
        if (!set) {
            knownSplits.set(this.identity, (set = new Set()));
            if (knownSplits.size > MAX_KNOWN_OBJECTS) {
                const oldest = knownSplits.keys().next().value;
                knownSplits.delete(oldest);
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
    /** The deepest known node owning a hash. */
    shardFor(hash) {
        let bits = "";
        while (this.splits.has(bits))
            bits += hashBit(hash, bits.length);
        return bits;
    }
    stub(bits) {
        const ns = this.namespace;
        return ns.get(ns.idFromName(shardObjectName(this.name, bits)));
    }
    /**
     * Calls `method(keys, payloads, ...args)` on every shard owning some of the keys, with its share
     * of them (and of `payloads`, aligned with `keys`). One result per shard.
     */
    async call(keys, method, payloads, ...args) {
        return this.route(false, keys, method, payloads, args);
    }
    /**
     * Like `call`, for keys that must be handled together by one shard (an all-or-nothing update):
     * throws SpansShards, before anything is applied, if they belong to more than one.
     */
    async callOneShard(keys, method, payloads, ...args) {
        if (keys.length === 0)
            throw new Error(`sharded ${this.name}: callOneShard needs at least one key to pick its shard`);
        const [only] = await this.route(true, keys, method, payloads, args);
        return only.result;
    }
    async route(oneShard, keys, method, payloads, args) {
        if (payloads && payloads.length !== keys.length)
            throw new Error(`sharded ${this.name}: ${payloads.length} payloads for ${keys.length} keys (payloads line up with keys)`);
        await this.loadCache();
        const hashes = keys.map(keyHash);
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
            const retry = [];
            await Promise.all([...groups].map(async ([bits, positions]) => {
                const groupKeys = positions.map((i) => keys[i]);
                const groupPayloads = payloads ? positions.map((i) => payloads[i]) : null;
                const answer = (await this.stub(bits).shardedCall(this.name, bits, method, groupKeys, groupPayloads, args));
                if ("ok" in answer)
                    out.push({ keys: groupKeys, positions, result: answer.ok });
                else if ("moved" in answer) {
                    this.learn(answer.moved);
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
                if (this.splits.has(bits)) {
                    await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
                    return;
                }
                const answer = (await this.stub(bits).shardedScan(this.name, bits, method, args));
                if ("ok" in answer)
                    out.push(answer.ok);
                else if ("moved" in answer) {
                    this.learn(answer.moved);
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
    /** How many shards this client knows of: one more than its known splits. There may be more. */
    async knownShardCount() {
        await this.loadCache();
        return this.splits.size + 1;
    }
    /** The known splits (for tests and operators). */
    knownSplits() {
        return [...this.splits].sort();
    }
    /** Drops what this client knows of the object's splits, here and in the colo: it was stale. */
    forget() {
        this.splits.clear();
        void this.saveCache();
    }
    learn(bits) {
        if (this.splits.has(bits))
            return;
        // A node that split had split ancestors: all of them are known to be split.
        for (let i = 0; i <= bits.length; i++)
            this.splits.add(bits.slice(0, i));
        void this.saveCache();
    }
    async loadCache() {
        this.touch();
        if (loadedFromCache.has(this.identity) || typeof caches === "undefined")
            return;
        loadedFromCache.add(this.identity);
        try {
            const hit = await caches.default.match(CACHE_ORIGIN + this.identity);
            if (hit)
                for (const bits of (await hit.json()))
                    this.splits.add(bits);
        }
        catch {
            // the cache is an optimisation only
        }
    }
    async saveCache() {
        if (typeof caches === "undefined")
            return;
        try {
            const body = JSON.stringify([...this.splits]);
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