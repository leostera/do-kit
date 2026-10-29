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
/** A hop limit: deeper than any maxDepth, so a loop is a bug, not a slow call. */
const MAX_HOPS = 64;
/** How long a call waits for a new shard to start serving (it is activated just after a split). */
const NOT_READY_WAIT_MS = 30_000;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** Waits a little longer each time a shard is not ready; throws once that has gone on too long. */
class Backoff {
    what;
    wait = 5;
    waited = 0;
    constructor(what) {
        this.what = what;
    }
    async next() {
        if (this.waited > NOT_READY_WAIT_MS)
            throw new Error(`${this.what}: a new shard did not start serving`);
        await sleep(this.wait);
        this.waited += this.wait;
        this.wait = Math.min(this.wait * 2, 250);
    }
}
export class ShardedClient {
    namespace;
    name;
    /** What known splits are filed under: two classes may share a name, never a root ID. */
    identity;
    constructor(namespace, name) {
        this.namespace = namespace;
        this.name = name;
        this.identity = namespace.idFromName(nodeName(name, "")).toString();
    }
    get splits() {
        let set = knownSplits.get(this.identity);
        if (!set)
            knownSplits.set(this.identity, (set = new Set()));
        return set;
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
        const backoff = new Backoff(`sharded ${this.name}`);
        for (let hop = 0; pending.length > 0; hop++) {
            if (hop > MAX_HOPS)
                throw new Error(`sharded ${this.name}: routing did not settle`);
            let waiting = false;
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
                else {
                    waiting = true; // not serving yet: the same shard, a little later
                    retry.push(...positions);
                }
            }));
            if (waiting) {
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
        const out = [];
        const backoff = new Backoff(`sharded ${this.name}`);
        const visit = async (bits, depth) => {
            if (depth > MAX_HOPS)
                throw new Error(`sharded ${this.name}: scan did not settle`);
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
            else {
                await backoff.next();
                await visit(bits, depth);
            }
        };
        await visit("", 0);
        return out;
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
    learn(bits) {
        if (this.splits.has(bits))
            return;
        // A node that split had split ancestors: all of them are known to be split.
        for (let i = 0; i <= bits.length; i++)
            this.splits.add(bits.slice(0, i));
        void this.saveCache();
    }
    async loadCache() {
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
export function sharded(namespace, name) {
    return new ShardedClient(namespace, name);
}
//# sourceMappingURL=client.js.map