// A relay's reads of its source. While it holds a lease it answers from it, with a sub-lease
// that ends before its own; without one, the asks waiting share one source read, but an ask joins
// only a read that started after it arrived. A read started earlier may have been answered before
// a write was acknowledged (without a lease: none is given while a write is held), and the ask may
// have come after that acknowledgement. The lease is renewed ahead, RENEW_MS before it ends, so
// asks rarely wait.
//
// A fresh read is what a writer checking the current value needs: a value at least as new as
// every write acknowledged before the ask arrived, kept by nobody afterwards. It comes from the
// lease while there is one, else from a source read that grants none, started after the ask
// arrived and shared like leased asks are. So writers alone never make the source hold an
// accepted write for a lease: only reads do.
//
// A lease can be asked back (`revoke`): the relay stops answering from it, never takes up a lease
// a read already in flight brings back, and says how long until the sub-leases it gave are over.
// For QUIET_MS after a revoke it gives no sub-leases and takes no lease: the key is being written,
// so it is read through. Its asks share source reads that grant none, so the next write finds no
// lease to ask back and is acknowledged as soon as it commits. (Asking many relays back holds each
// write for the slowest of them.) While quiet, a read is a fresh read: one stream of source reads
// per relay, not two. A leased read the source answers without a lease (it leases no relay a key
// written within WRITTEN_MS, read-leases.ts) starts the quiet too. A key that is read but not
// written keeps its sub-leases, and its reads are answered from the isolates' memory.
//
// `readFresh` is where reads through go: the source, or for a relay deep in its key's tree, its
// parent relay's fresh read (relays nest, relay-plan.ts). Leases come only from the source
// (`readSource`).
import { MIN_LEASE_MS, READ_LEASE_MARGIN_MS, RENEW_MS } from "./read-leases.js";
export { RENEW_MS };
/** How long after a revoke a relay gives no sub-leases: the key is being written. */
export const QUIET_MS = 10_000;
export class RelayReads {
    readSource;
    readFresh;
    /** The unleased source read in flight, and the newest ask it answers. */
    currentFresh = null;
    lease = null;
    relays = 1;
    arrivals = 0;
    /** The source read in flight, and the newest ask it answers. */
    current = null;
    /** Until when sub-leases are not given: QUIET_MS after the last revoke. */
    writtenUntil = 0;
    /** When the last sub-lease given ends, by this relay's clock. */
    subsUntil = 0;
    /** Bumped by each revoke: a source read started before one brings back a lease that was asked back. */
    epoch = 0;
    /** `readSource` reads the source for a lease; `readFresh` for a value without one (by default, as `readSource`: a relay never asked fresh). */
    constructor(readSource, readFresh = readSource) {
        this.readSource = readSource;
        this.readFresh = readFresh;
    }
    /** The source asks for the lease back: none is answered from from now on. Returns how long until the sub-leases given are over (ms). */
    revoke() {
        const now = Date.now();
        this.lease = null;
        this.epoch++;
        this.writtenUntil = now + QUIET_MS;
        return Math.max(0, this.subsUntil - now);
    }
    /** A value at least as new as every write acknowledged before this ask, without a sub-lease. */
    async fresh() {
        const seq = ++this.arrivals;
        // While the lease lasts, no write is acknowledged without the lease's value going first.
        if (this.lease && this.lease.until > Date.now())
            return { value: this.lease.value, leaseMs: 0, relays: this.relays };
        for (;;) {
            const c = this.currentFresh;
            if (c && c.covers >= seq) {
                const a = await c.answer;
                return { value: a.value, leaseMs: 0, relays: a.relays };
            }
            if (!c) {
                const answer = this.readFresh().then((a) => {
                    this.relays = a.relays;
                    return a;
                });
                const entry = { covers: this.arrivals, answer };
                this.currentFresh = entry;
                const clear = () => {
                    if (this.currentFresh === entry)
                        this.currentFresh = null;
                };
                answer.then(clear, clear);
                continue;
            }
            // Started before this ask arrived: wait for it, then join or start the next.
            await c.answer.catch(() => undefined);
        }
    }
    /** How many asks have arrived (for the relay's load report). */
    get asks() {
        return this.arrivals;
    }
    async read() {
        // Being written: read through, sharing the fresh reads (no lease is taken or given meanwhile).
        if (Date.now() < this.writtenUntil)
            return await this.fresh();
        const seq = ++this.arrivals;
        const leased = this.fromLease();
        if (leased) {
            // Renewed ahead, in the background: this ask is answered from the lease.
            if (this.lease.until - Date.now() < RENEW_MS && !this.current)
                void this.start().catch(() => undefined);
            return leased;
        }
        const answer = await this.readAfter(seq);
        // From the lease if that read gave one, else its value without a sub-lease.
        return this.fromLease() ?? { value: answer.value, leaseMs: 0, relays: answer.relays };
    }
    /** From the lease, if enough of it is left to give a sub-lease. */
    fromLease() {
        if (!this.lease)
            return null;
        const now = Date.now();
        const left = this.lease.until - now;
        if (left < MIN_LEASE_MS)
            return null;
        const sub = now < this.writtenUntil ? 0 : left;
        this.subsUntil = Math.max(this.subsUntil, now + sub);
        return { value: this.lease.value, leaseMs: sub, relays: this.relays };
    }
    /** The answer of a source read that started after ask `seq` arrived (joined, or started). */
    async readAfter(seq) {
        for (;;) {
            const c = this.current;
            if (!c)
                return await this.start();
            if (c.covers >= seq)
                return await c.answer;
            // Started before this ask arrived: wait for it, then join or start the next.
            await c.answer.catch(() => undefined);
        }
    }
    start() {
        const asked = Date.now();
        const epoch = this.epoch;
        const answer = this.readSource().then((a) => {
            this.relays = a.relays;
            // Timed from before it asked, less the margin: it ends here before it ends at the source. A
            // lease asked back while this read was on its way is never taken up.
            if (epoch === this.epoch)
                this.lease = a.leaseMs > 0 ? { value: a.value, until: asked + a.leaseMs - READ_LEASE_MARGIN_MS } : null;
            // No lease: the key is being written, so it is read through until the quiet ends.
            if (a.leaseMs === 0)
                this.writtenUntil = Math.max(this.writtenUntil, Date.now() + QUIET_MS);
            return a;
        });
        const entry = { covers: this.arrivals, answer };
        this.current = entry;
        const clear = () => {
            if (this.current === entry)
                this.current = null;
        };
        answer.then(clear, clear);
        return answer;
    }
}
//# sourceMappingURL=relay-reads.js.map