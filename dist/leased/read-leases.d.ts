/** How long a read lease lasts. */
export declare const READ_LEASE_MS = 1000;
/** Clock drift allowed over one lease: the isolate ends it this much early, the source this much late. */
export declare const READ_LEASE_MARGIN_MS = 150;
/** A lease shorter than this is not worth giving: the holder would ask again at once. */
export declare const MIN_LEASE_MS = 200;
/** How long before its lease ends a relay renews it (relay-reads.ts). */
export declare const RENEW_MS = 600;
/**
 * A key written this recently is not leased to relays: it is being written, and each lease out
 * would hold the next write until it was asked back. Relays read it through instead.
 */
export declare const WRITTEN_MS = 10000;
/** A key a write changes, and its row before the write. */
export interface Change<Row> {
    readonly key: string;
    readonly before: Row | undefined;
}
export declare class ReadLeases<Row> {
    /** Until then every write is held: leases granted before a restart may still run. */
    private readonly holdAllUntil;
    private horizon;
    private readonly storeHorizon;
    /**
     * The leases out on each key, by holder (a relay's index; UNNAMED for holders that can't be
     * asked back): when each ends by this source's clock (margin included), and whether it was asked back.
     */
    private readonly leases;
    /** Keys with a held write: the row reads see until `until`. */
    private readonly held;
    /** When each key last changed (holdUntil sees every change): not leased to relays for WRITTEN_MS after. */
    private readonly changedAt;
    /** `horizon`: how far leases granted before this run may reach (stored by `storeHorizon`). */
    constructor(horizon: number, storeHorizon: (horizon: number) => void);
    /**
     * A lease on `keys` (ms; 0 if none). A held write waits for the leases out now, so a lease
     * granted while one is held ends by then (with the margin), and none is given if that leaves
     * less than MIN_LEASE_MS: the hold is never made longer.
     */
    grant(keys: readonly string[], holder?: number): number;
    /**
     * Forgets the keys whose leases are all over, at most once a lease's length: a key leased once
     * and never again would otherwise stay here for as long as the source runs.
     */
    private sweep;
    private sweptAt;
    /** How many keys have a lease on record (for tests and operators). */
    get leasedKeys(): number;
    /** What a held write on `key` waits for (ms from now): the latest named and unnamed lease, and the restart hold. For logs. */
    waitsOn(key: string): {
        named: number;
        namedOut: number;
        unnamed: number;
        restart: number;
    };
    /** When the last lease out on `key` ends (0 if none). */
    private leaseEnd;
    /** The named holders of leases on `key` a held write waits for and hasn't asked back yet; each is marked asked. */
    toRevoke(key: string): number[];
    /**
     * `holder` gave its lease on `key` back: it ends `inMs` from now (when the sub-leases it gave
     * are over, by its clock: plus the margin), or now if none are out, if that is sooner. Returns when a write held on `key` is shown now.
     */
    returned(key: string, holder: number, inMs: number): number;
    /**
     * Before a write to these keys is shown: when every lease on them is over. If that is later than
     * now, reads keep seeing `before` (each key's row before the write) until then.
     */
    holdUntil(changes: readonly Change<Row>[]): number;
    /** The row reads see: the one before a held write until it is shown, else the committed one. */
    visible(key: string, committed: Row | undefined): Row | undefined;
    /** When a write held on `key` is shown (now, if none is). */
    shownAt(key: string): number;
    /** When the last held write is shown (now or earlier if none is held). */
    heldUntil(): number;
    /** Whether some write is held: reads (and the digest) see a row from before it. */
    holdsAny(): boolean;
}
//# sourceMappingURL=read-leases.d.ts.map