import { RENEW_MS } from "./read-leases.js";
export { RENEW_MS };
/** How long after a revoke a relay gives no sub-leases: the key is being written. */
export declare const QUIET_MS = 10000;
/** The source's answer: the value, a lease (ms; 0 if none), and the key's relay count. */
export interface SourceAnswer<V> {
    readonly value: V;
    readonly leaseMs: number;
    readonly relays: number;
}
/** A relay's answer to an isolate: the value, a sub-lease (ms; 0 if none), and the key's relay count. */
export type RelayAnswer<V> = SourceAnswer<V>;
export declare class RelayReads<V> {
    private readonly readSource;
    private readonly readFresh;
    /** The unleased source read in flight, and the newest ask it answers. */
    private currentFresh;
    private lease;
    private relays;
    private arrivals;
    /** The source read in flight, and the newest ask it answers. */
    private current;
    /** Until when sub-leases are not given: QUIET_MS after the last revoke. */
    private writtenUntil;
    /** When the last sub-lease given ends, by this relay's clock. */
    private subsUntil;
    /** Bumped by each revoke: a source read started before one brings back a lease that was asked back. */
    private epoch;
    /** `readSource` reads the source for a lease; `readFresh` for a value without one (by default, as `readSource`: a relay never asked fresh). */
    constructor(readSource: () => Promise<SourceAnswer<V>>, readFresh?: () => Promise<SourceAnswer<V>>);
    /** The source asks for the lease back: none is answered from from now on. Returns how long until the sub-leases given are over (ms). */
    revoke(): number;
    /** A value at least as new as every write acknowledged before this ask, without a sub-lease. */
    fresh(): Promise<RelayAnswer<V>>;
    /** How many asks have arrived (for the relay's load report). */
    get asks(): number;
    read(): Promise<RelayAnswer<V>>;
    /** From the lease, if enough of it is left to give a sub-lease. */
    private fromLease;
    /** The answer of a source read that started after ask `seq` arrived (joined, or started). */
    private readAfter;
    private start;
}
//# sourceMappingURL=relay-reads.d.ts.map