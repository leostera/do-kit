// Atomic calls across shards (RFD0008): the shapes a subclass gives and a caller gets, and the
// protocol between the client and the shards. The protocol is modelled in
// specs/atomic-across-shards.ts.
/**
 * A call's id, which orders calls by age: its first attempt's start (fixed width, so ids sort as
 * the times do) and a random part. Retries keep the start, so a call that keeps meeting holds
 * becomes the oldest, and the oldest call never gives way (wait-die).
 */
export function atomicTxId(startedAt) {
    return `${String(startedAt).padStart(15, "0")}-${crypto.randomUUID()}`;
}
/** Whether call `a` is older than call `b`, by their ids: only an older call waits on another's hold. */
export function olderTx(a, b) {
    return a < b;
}
/**
 * How long a share holds its keys before it asks the primary for the outcome. The whole call
 * takes a few round trips: a hold reaches this only if its caller is gone or the primary is slow.
 */
export const ATOMIC_HOLD_MS = 5_000;
/** How long the primary keeps an outcome nobody said it may forget: far past any hold's life. */
export const ATOMIC_OUTCOME_TTL_MS = 24 * 60 * 60_000;
//# sourceMappingURL=atomic.js.map