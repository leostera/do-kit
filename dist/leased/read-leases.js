// Read leases (the source's side). One Durable Object holds the copy of some keys everyone reads
// (the source); it leases them to relays (relay-reads.ts), which answer readers from the lease and
// give them sub-leases inside it. A write to a leased key commits at once, but is shown (to reads)
// and acknowledged only once every lease on the key is over, so a read served from a relay or an
// isolate never misses an acknowledged write, and reads never go back.
//
// Time: the holder times its lease from before it asked, less READ_LEASE_MARGIN_MS; the source
// from after it granted, plus the margin. So only clock rates need to agree. Before granting, the
// source stores how far its leases may reach (`horizon`, in steps); a restarted source holds every
// write until then, plus RESTART_SKEW_MS for another machine's clock.
//
// The row reads see while a write is held is stored too (`HeldRows`), in the transaction that
// commits the write. Holders with leases from before a restart still answer with that row; a
// source that forgot it would show the write at once, and a read through a holder after one from
// the source would go back. So a restarted source loads the holds it had, and each ends when it
// would have: by then every lease on its key is over.
//
// A named holder's lease (a relay's, by its index) can be asked back: a held write asks each
// holder (`toRevoke`), and a holder that gives it back (`returned`) ends it early, so the write is
// shown once the holders' sub-leases are over, not when their leases would have ended. A named
// holder gets no lease while a write to the key is held: the hold ends with the returns, and a
// lease granted meanwhile would have to be asked back too.
/** How long a read lease lasts. */
export const READ_LEASE_MS = 1000;
/** Clock drift allowed over one lease: the isolate ends it this much early, the source this much late. */
export const READ_LEASE_MARGIN_MS = 150;
/** The holder of leases that can't be asked back (ReadLeases.grant without one). */
const UNNAMED = -1;
/** A lease shorter than this is not worth giving: the holder would ask again at once. */
export const MIN_LEASE_MS = 200;
/** How long before its lease ends a relay renews it (relay-reads.ts). */
export const RENEW_MS = 600;
/** The horizon is stored this far ahead, so a write is needed only every few seconds. */
const HORIZON_STEP_MS = 5000;
/**
 * A key written this recently is not leased to relays: it is being written, and each lease out
 * would hold the next write until it was asked back. Relays read it through instead.
 */
export const WRITTEN_MS = 10_000;
/** How far apart two machines' clocks may be: a restarted source holds this past its horizon. */
const RESTART_SKEW_MS = 1000;
export class ReadLeases {
    /** Until then every write is held: leases granted before a restart may still run. */
    holdAllUntil;
    horizon;
    storeHorizon;
    heldRows;
    /**
     * The leases out on each key, by holder (a relay's index; UNNAMED for holders that can't be
     * asked back): when each ends by this source's clock (margin included), and whether it was asked back.
     */
    leases = new Map();
    /** Keys with a held write: the row reads see until `until`. Each entry is in `heldRows` too. */
    held = new Map();
    /** When each key last changed (holdUntil sees every change): not leased to relays for WRITTEN_MS after. */
    changedAt = new Map();
    /**
     * `horizon`: how far leases granted before this run may reach (stored by `storeHorizon`).
     * `heldRows`: the holds this source had when it last ran, and where it keeps them from now on.
     */
    constructor(horizon, storeHorizon, heldRows) {
        this.horizon = horizon;
        this.storeHorizon = storeHorizon;
        this.heldRows = heldRows;
        this.holdAllUntil = horizon > 0 ? horizon + RESTART_SKEW_MS : 0;
        const now = Date.now();
        for (const h of heldRows.load()) {
            if (h.until > now)
                this.held.set(h.key, { row: h.row, until: h.until });
            else
                heldRows.delete(h.key);
        }
    }
    /**
     * A lease on `keys` (ms; 0 if none). A held write waits for the leases out now, so a lease
     * granted while one is held ends by then (with the margin), and none is given if that leaves
     * less than MIN_LEASE_MS: the hold is never made longer.
     */
    grant(keys, holder = UNNAMED) {
        const now = Date.now();
        let ms = READ_LEASE_MS;
        for (const key of keys) {
            if (holder !== UNNAMED && now - (this.changedAt.get(key) ?? Number.NEGATIVE_INFINITY) < WRITTEN_MS)
                return 0;
            const held = this.held.get(key);
            if (held && held.until > now) {
                if (holder !== UNNAMED)
                    return 0;
                ms = Math.min(ms, held.until - now - READ_LEASE_MARGIN_MS);
            }
        }
        if (ms < MIN_LEASE_MS)
            return 0;
        const end = now + ms + READ_LEASE_MARGIN_MS;
        // Stored before the answer leaves (the output gate holds it until this is durable).
        if (end > this.horizon) {
            this.horizon = now + HORIZON_STEP_MS;
            this.storeHorizon(this.horizon);
        }
        for (const key of keys) {
            let holders = this.leases.get(key);
            if (!holders)
                this.leases.set(key, (holders = new Map()));
            for (const [h, l] of holders)
                if (l.end <= now)
                    holders.delete(h);
            holders.set(holder, { end: Math.max(holders.get(holder)?.end ?? 0, end), revoked: false });
        }
        this.sweep(now);
        return ms;
    }
    /**
     * Forgets the keys whose leases are all over, at most once a lease's length: a key leased once
     * and never again would otherwise stay here for as long as the source runs.
     */
    sweep(now) {
        if (now - this.sweptAt < READ_LEASE_MS)
            return;
        this.sweptAt = now;
        for (const [key, holders] of this.leases) {
            for (const [h, l] of holders)
                if (l.end <= now)
                    holders.delete(h);
            if (holders.size === 0)
                this.leases.delete(key);
        }
        for (const [key, held] of this.held)
            if (held.until <= now)
                this.release(key);
    }
    /** A key's hold is over: forgotten here and in storage. */
    release(key) {
        this.held.delete(key);
        this.heldRows.delete(key);
    }
    /** Sets a key's hold, here and in storage. */
    hold(key, row, until) {
        this.held.set(key, { row, until });
        this.heldRows.put({ key, row, until });
    }
    sweptAt = 0;
    /** How many keys have a lease on record (for tests and operators). */
    get leasedKeys() {
        return this.leases.size;
    }
    /** What a held write on `key` waits for (ms from now): the latest named and unnamed lease, and the restart hold. For logs. */
    waitsOn(key) {
        const now = Date.now();
        let named = 0;
        let namedOut = 0;
        let unnamed = 0;
        for (const [holder, l] of this.leases.get(key) ?? []) {
            if (l.end <= now)
                continue;
            if (holder === UNNAMED)
                unnamed = Math.max(unnamed, l.end - now);
            else {
                named = Math.max(named, l.end - now);
                namedOut++;
            }
        }
        return { named, namedOut, unnamed, restart: Math.max(0, this.holdAllUntil - now) };
    }
    /** When the last lease out on `key` ends (0 if none). */
    leaseEnd(key) {
        let end = 0;
        for (const l of this.leases.get(key)?.values() ?? [])
            end = Math.max(end, l.end);
        return end;
    }
    /** The named holders of leases on `key` a held write waits for and hasn't asked back yet; each is marked asked. */
    toRevoke(key) {
        const now = Date.now();
        const held = this.held.get(key);
        if (!held || held.until <= now)
            return [];
        const out = [];
        for (const [holder, l] of this.leases.get(key) ?? []) {
            if (holder === UNNAMED || l.revoked || l.end <= now)
                continue;
            l.revoked = true;
            out.push(holder);
        }
        return out;
    }
    /**
     * `holder` gave its lease on `key` back: it ends `inMs` from now (when the sub-leases it gave
     * are over, by its clock: plus the margin), or now if none are out, if that is sooner. Returns when a write held on `key` is shown now.
     */
    returned(key, holder, inMs) {
        const now = Date.now();
        const l = this.leases.get(key)?.get(holder);
        // None out: the relay dropped the lease before it answered, so no clock is involved.
        if (l)
            l.end = Math.min(l.end, inMs > 0 ? now + inMs + READ_LEASE_MARGIN_MS : now);
        const held = this.held.get(key);
        if (!held)
            return now;
        const until = Math.min(held.until, Math.max(now, this.leaseEnd(key), this.holdAllUntil));
        // Stored before the answer leaves: the write is acknowledged at `until`, and a restart after
        // that must not show the row from before it again.
        if (until !== held.until)
            this.hold(key, held.row, until);
        return until;
    }
    /**
     * Before a write to these keys is shown: when every lease on them is over. If that is later than
     * now, reads keep seeing `before` (each key's row before the write) until then.
     */
    holdUntil(changes) {
        const now = Date.now();
        for (const c of changes)
            this.changedAt.set(c.key, now);
        for (const [key, at] of this.changedAt)
            if (now - at >= WRITTEN_MS)
                this.changedAt.delete(key);
        let until = now;
        for (const c of changes)
            until = Math.max(until, this.leaseEnd(c.key), this.holdAllUntil);
        if (until <= now)
            return now;
        for (const c of changes) {
            const held = this.held.get(c.key);
            // A second write while one is held: reads keep the row from before the first.
            this.hold(c.key, held && held.until > now ? held.row : c.before, Math.max(until, held?.until ?? 0));
        }
        return until;
    }
    /** The row reads see: the one before a held write until it is shown, else the committed one. */
    visible(key, committed) {
        const held = this.held.get(key);
        if (!held)
            return committed;
        if (held.until > Date.now())
            return held.row;
        this.release(key);
        return committed;
    }
    /** When a write held on `key` is shown (now, if none is). */
    shownAt(key) {
        const held = this.held.get(key);
        return held && held.until > Date.now() ? held.until : Date.now();
    }
    /** When the last held write is shown (now or earlier if none is held). */
    heldUntil() {
        let until = 0;
        for (const held of this.held.values())
            until = Math.max(until, held.until);
        return until;
    }
    /** Whether some write is held: reads (and the digest) see a row from before it. */
    holdsAny() {
        const now = Date.now();
        for (const held of this.held.values())
            if (held.until > now)
                return true;
        return false;
    }
}
//# sourceMappingURL=read-leases.js.map