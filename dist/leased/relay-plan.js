// How many relays a hot key has. The source counts direct reads of each key; once one is read
// HOT_READS times a second it is relayed, with K sized by the rate those reads came at, and every
// answer about it names its relay count K.
// Each relay reports the asks it had in the last second with its reads, and K follows their sum:
// RELAY_READS asks a second per relay, so each stays well under one Durable Object's ~300 rps
// budget. K rises at once, falls only after SHRINK_MS, and the key stops being relayed when its
// relays together see fewer than a quarter of HOT_READS.
//
// Relays nest: the first RELAY_FANOUT read the source, and every other one reads its parent
// relay, so the source answers at most RELAY_FANOUT relays however many a key has. A relay's
// reads of its parent are an isolate's, and its report adds its children's.
/** Direct reads a second of one key above which the source relays it (by default). */
export const HOT_READS = 100;
/** Asks a second one relay is sized for. */
export const RELAY_READS = 250;
/** How many relays read the source, and how many children each relay has. */
export const RELAY_FANOUT = 8;
/** Three levels of relays: past ~146,000 asks a second, each relay sees more than RELAY_READS. */
export const MAX_RELAYS = RELAY_FANOUT + RELAY_FANOUT ** 2 + RELAY_FANOUT ** 3;
/** A relay count is lowered, or relaying stopped, only after it has been too high this long. */
const SHRINK_MS = 10_000;
/** A relay's report counts this long. */
const REPORT_MS = 2000;
const WINDOW_MS = 1000;
/** The shortest stretch a rate is measured over (a burst at a window's start reads as at most 100× its size). */
const MIN_ELAPSED_MS = 10;
export class RelayPlan {
    hotReads;
    relayReads;
    relayed = new Map();
    reads = new Map();
    lastReads = new Map();
    windowStart = Date.now();
    /** `relayReads`: asks a second one relay is sized for (RELAY_READS; tests lower it to make trees of relays). */
    constructor(hotReads, relayReads = RELAY_READS) {
        this.hotReads = hotReads;
        this.relayReads = relayReads;
    }
    /**
     * `key`'s relay count, relaying it first with `k` relays if it is not (a key hot with writes is
     * read as often). The relays' reports size it from then on.
     */
    relayAtLeast(key, k) {
        let r = this.relayed.get(key);
        if (!r)
            this.relayed.set(key, (r = { k: clamp(k), reports: new Map(), lowSince: null }));
        return r.k;
    }
    /** Whether any key is relayed. */
    anyRelayed() {
        return this.relayed.size > 0;
    }
    /** Counts direct reads of `keys`; each one's relay count (0 if it is not relayed). */
    direct(keys) {
        const now = Date.now();
        this.roll(now);
        return keys.map((key) => {
            const known = this.relayed.get(key);
            if (known)
                return known.k;
            const reads = (this.reads.get(key) ?? 0) + 1;
            this.reads.set(key, reads);
            const last = this.lastReads.get(key) ?? 0;
            if (Math.max(reads, last) < this.hotReads)
                return 0;
            // Sized by the rate, not the count: HOT_READS reads in the first 14 ms of a window are
            // 7,000 a second, which one relay would queue until it fails.
            const rate = Math.max(last, (reads * WINDOW_MS) / Math.max(MIN_ELAPSED_MS, now - this.windowStart));
            const k = clamp(Math.ceil(rate / this.relayReads));
            this.relayed.set(key, { k, reports: new Map(), lowSince: null });
            return k;
        });
    }
    /** A relay's read of `keys`, with its asks in the last second: each one's relay count (0 to stop relaying). */
    reported(keys, relay, asks) {
        const now = Date.now();
        return keys.map((key) => {
            // A source that restarted knows of no relays: the reports rebuild the count.
            let r = this.relayed.get(key);
            if (!r)
                this.relayed.set(key, (r = { k: clamp(relay + 1), reports: new Map(), lowSince: null }));
            r.reports.set(relay, { asks, at: now });
            let total = 0;
            for (const [i, report] of r.reports) {
                if (now - report.at > REPORT_MS)
                    r.reports.delete(i);
                else
                    total += report.asks;
            }
            const wanted = clamp(Math.ceil(total / this.relayReads));
            const cold = total < this.hotReads / 4;
            if (wanted > r.k) {
                r.k = wanted;
                r.lowSince = null;
            }
            else if (wanted < r.k || cold) {
                r.lowSince ??= now;
                if (now - r.lowSince >= SHRINK_MS) {
                    if (cold) {
                        this.relayed.delete(key);
                        return 0;
                    }
                    r.k = wanted;
                    r.lowSince = null;
                }
            }
            else
                r.lowSince = null;
            return r.k;
        });
    }
    roll(now) {
        if (now - this.windowStart < WINDOW_MS)
            return;
        this.lastReads = now - this.windowStart >= 2 * WINDOW_MS ? new Map() : this.reads;
        this.reads = new Map();
        this.windowStart = now;
    }
}
function clamp(k) {
    return Math.min(MAX_RELAYS, Math.max(1, k));
}
/** The relay relay `index` reads the key from, or null for the source. Always below `index`. */
export function relayParent(index) {
    return index < RELAY_FANOUT ? null : Math.floor(index / RELAY_FANOUT) - 1;
}
/** The relay an isolate reads a key through: `draw` is the isolate's own random number in [0, 1). */
export function relayIndex(draw, k) {
    return Math.min(k - 1, Math.floor(draw * k));
}
//# sourceMappingURL=relay-plan.js.map