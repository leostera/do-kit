/** Direct reads a second of one key above which the source relays it (by default). */
export declare const HOT_READS = 100;
/** Asks a second one relay is sized for. */
export declare const RELAY_READS = 250;
/** How many relays read the source, and how many children each relay has. */
export declare const RELAY_FANOUT = 8;
/** Three levels of relays: past ~146,000 asks a second, each relay sees more than RELAY_READS. */
export declare const MAX_RELAYS: number;
export declare class RelayPlan {
    private readonly hotReads;
    private readonly written;
    private readonly relayReads;
    private readonly relayed;
    private reads;
    private lastReads;
    private windowStart;
    /**
     * `written`: whether a key is being written (the source's ReadLeases.written), so not relayed.
     * `relayReads`: asks a second one relay is sized for (RELAY_READS; tests lower it to make trees of relays).
     */
    constructor(hotReads: number, written: (key: string) => boolean, relayReads?: number);
    /** Whether any key is relayed. */
    anyRelayed(): boolean;
    /** Counts direct reads of `keys`; each one's relay count (0 if it is not relayed). */
    direct(keys: readonly string[]): number[];
    /** A relay's read of `keys`, with its asks in the last second: each one's relay count (0 to stop relaying). */
    reported(keys: readonly string[], relay: number, asks: number): number[];
    private roll;
}
/** The relay relay `index` reads the key from, or null for the source. Always below `index`. */
export declare function relayParent(index: number): number | null;
/** The relay an isolate reads a key through: `draw` is the isolate's own random number in [0, 1). */
export declare function relayIndex(draw: number, k: number): number;
//# sourceMappingURL=relay-plan.d.ts.map