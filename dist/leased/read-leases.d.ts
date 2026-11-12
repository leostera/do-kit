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
/** A key with a held write, and when the hold ends by the clock of the source that set it. */
export interface HeldKey {
    readonly key: string;
    readonly until: number;
}
/**
 * Where a source keeps its held keys, so they outlive it: its own durable storage, written
 * synchronously. `put` runs inside `holdUntil`, so call `holdUntil` in the transaction that
 * commits the write: the write and its key are then stored together or not at all.
 */
export interface HeldKeys {
    /** Every key stored: read once, when the source starts. */
    load(): readonly HeldKey[];
    /** A key's hold began, or its end changed: replaces the key's entry. */
    put(held: HeldKey): void;
    /** A key's hold is over. */
    delete(key: string): void;
}
/** A lease a held write asks back: who holds it, and which grant it is. */
export interface Revoked {
    readonly holder: number;
    readonly lease: number;
}
export declare class ReadLeases<Row> {
    /** Until then every write is held: leases granted before a restart may still run. */
    private readonly holdAllUntil;
    private horizon;
    private readonly storeHorizon;
    private readonly heldKeys;
    /**
     * Keys that had a held write when this source last stopped: when their reads may be answered
     * (the stored hold's end plus RESTART_SKEW_MS).
     */
    private readonly unsettled;
    /**
     * The leases out on each key, by holder (a relay's index; UNNAMED for holders that can't be
     * asked back): when each ends by this source's clock (margin included), and whether it was asked back.
     */
    private readonly leases;
    /** Numbers each grant, so a lease given back is told from a later one to the same holder. */
    private grants;
    /** Keys with a held write: the row reads see until `until`. Each key is in `heldKeys` too. */
    private readonly held;
    /** When each key last changed (holdUntil sees every change): not leased to relays for WRITTEN_MS after. */
    private readonly changedAt;
    /**
     * `horizon`: how far leases granted before this run may reach (stored by `storeHorizon`).
     * `heldKeys`: the keys this source held writes on when it last ran, and where it keeps them
     * from now on.
     */
    constructor(horizon: number, storeHorizon: (horizon: number) => void, heldKeys: HeldKeys);
    /**
     * When reads of `keys` (of every key, without them) may be answered: now, or, after a restart,
     * once the holds the source had on them when it stopped are over on any clock. Wait until then
     * before `visible` and `grant`: this source no longer has the rows from before those writes.
     */
    readableAt(keys?: readonly string[]): number;
    /**
     * A lease on `keys` (ms; 0 if none). A held write waits for the leases out now, so a lease
     * granted while one is held ends by then (with the margin), and none is given if that leaves
     * less than MIN_LEASE_MS: the hold is never made longer.
     */
    grant(keys: readonly string[], holder?: number): number;
    /** Whether `key` changed within WRITTEN_MS: it gets no relay leases, so relaying it is no use (RelayPlan). */
    written(key: string, now?: number): boolean;
    /**
     * Forgets the keys whose leases are all over, at most once a lease's length: a key leased once
     * and never again would otherwise stay here for as long as the source runs.
     */
    private sweep;
    /**
     * A key's hold is over: forgotten here, and in storage unless reads of the key still wait out a
     * restart (a later restart must wait as long).
     */
    private release;
    /**
     * Sets a key's hold, here and in storage. A hold set after a restart ends no earlier than the
     * restart rule's (`holdAllUntil`), which is past every hold stored before it: replacing a key's
     * stored end never shortens what a later restart waits for.
     */
    private hold;
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
    /**
     * The named holders of leases on `key` a held write waits for and hasn't asked back yet, each
     * with the lease it is asked for (pass it to `returned`); each is marked asked.
     */
    toRevoke(key: string): Revoked[];
    /**
     * The holder asked by `toRevoke` gave that lease on `key` back: it ends `inMs` from now (when
     * the sub-leases it gave are over, by its clock: plus the margin), or now if none are out, if
     * that is sooner. Returns when a write held on `key` is shown now.
     *
     * A later lease to the same holder is left alone: an answer can come back after the hold it was
     * for is over, the holder has taken a new lease, and the next write waits for that one.
     */
    returned(key: string, revoked: Revoked, inMs: number): number;
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