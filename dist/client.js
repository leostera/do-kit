// The client side of splittable objects: routes keys to leaves through the splits it
// knows, learns new ones from "moved" answers, and shares what it learned in the isolate and the
// colo (Cache API). Splits are final, so stale knowledge costs hops, never correctness.
import { hashBit, keyHash, leafObjectName, nodeName } from "./index.js";
/** Keys that had to go to one leaf belong to several (nothing was applied). */
export class SpansLeaves extends Error {
}
const CACHE_ORIGIN = "https://splittable-object.invalid/";
/**
 * Splits known per object (keyed by its root's Durable Object ID, unique per namespace and
 * name), shared by every client in this isolate.
 */
const knownSplits = new Map();
const loadedFromCache = new Set();
/** A hop limit: deeper than any maxDepth, so a loop is a bug, not a slow call. */
const MAX_HOPS = 64;
/** How long a call waits for a new leaf to start serving (it is activated just after a split). */
const NOT_READY_WAIT_MS = 30_000;
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
/** Waits a little longer each time a leaf is not ready; throws once that has gone on too long. */
class Backoff {
    what;
    wait = 5;
    waited = 0;
    constructor(what) {
        this.what = what;
    }
    async next() {
        if (this.waited > NOT_READY_WAIT_MS)
            throw new Error(`${this.what}: a new leaf did not start serving`);
        await sleep(this.wait);
        this.waited += this.wait;
        this.wait = Math.min(this.wait * 2, 250);
    }
}
export class SplittableClient {
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
    leafFor(hash) {
        let bits = "";
        while (this.splits.has(bits))
            bits += hashBit(hash, bits.length);
        return bits;
    }
    stub(bits) {
        const ns = this.namespace;
        return ns.get(ns.idFromName(leafObjectName(this.name, bits)));
    }
    /**
     * Calls `method(keys, payloads, ...args)` on every leaf owning some of the keys, with its share
     * of them (and of `payloads`, aligned with `keys`). One result per leaf.
     */
    async call(keys, method, payloads, ...args) {
        return this.route(false, keys, method, payloads, args);
    }
    /**
     * Like `call`, for keys that must be handled together by one leaf (an all-or-nothing update):
     * throws SpansLeaves, before anything is applied, if they belong to more than one.
     */
    async callOneLeaf(keys, method, payloads, ...args) {
        const [only] = await this.route(true, keys, method, payloads, args);
        return only.result;
    }
    async route(oneLeaf, keys, method, payloads, args) {
        await this.loadCache();
        const hashes = keys.map(keyHash);
        const out = [];
        let pending = keys.map((_, i) => i);
        const backoff = new Backoff(`splittable ${this.name}`);
        for (let hop = 0; pending.length > 0; hop++) {
            if (hop > MAX_HOPS)
                throw new Error(`splittable ${this.name}: routing did not settle`);
            let waiting = false;
            const groups = new Map();
            for (const i of pending) {
                const bits = this.leafFor(hashes[i]);
                let group = groups.get(bits);
                if (!group)
                    groups.set(bits, (group = []));
                group.push(i);
            }
            if (oneLeaf && groups.size > 1)
                throw new SpansLeaves(`keys of ${this.name} belong to ${groups.size} leaves`);
            const retry = [];
            await Promise.all([...groups].map(async ([bits, positions]) => {
                const groupKeys = positions.map((i) => keys[i]);
                const groupPayloads = payloads ? positions.map((i) => payloads[i]) : null;
                const answer = (await this.stub(bits).splittableCall(this.name, bits, method, groupKeys, groupPayloads, args));
                if ("ok" in answer)
                    out.push({ keys: groupKeys, positions, result: answer.ok });
                else if ("moved" in answer) {
                    this.learn(answer.moved);
                    retry.push(...positions);
                }
                else {
                    waiting = true; // not serving yet: the same leaf, a little later
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
    /** Calls `method(...args)` on every leaf. */
    async scan(method, ...args) {
        await this.loadCache();
        const out = [];
        const backoff = new Backoff(`splittable ${this.name}`);
        const visit = async (bits, depth) => {
            if (depth > MAX_HOPS)
                throw new Error(`splittable ${this.name}: scan did not settle`);
            if (this.splits.has(bits)) {
                await Promise.all([visit(`${bits}0`, depth + 1), visit(`${bits}1`, depth + 1)]);
                return;
            }
            const answer = (await this.stub(bits).splittableScan(this.name, bits, method, args));
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
export function splittable(namespace, name) {
    return new SplittableClient(namespace, name);
}
//# sourceMappingURL=client.js.map