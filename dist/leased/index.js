// Leased reads: one Durable Object's hot keys, read through relays without ever being stale. The
// source leases what it holds to a few relays (ReadLeases), which answer readers from the lease
// and give isolates sub-leases inside it (RelayReads); RelayPlan says when a key is hot, how many
// relays it gets, and which one an isolate asks. A write to a leased key commits at once but is
// shown and acknowledged only once every lease on it is over or given back, so a read never misses
// an acknowledged write and never goes back. These are the pieces a source and its relays embed;
// the objects, their storage and their calls are the app's.
export { MIN_LEASE_MS, READ_LEASE_MARGIN_MS, READ_LEASE_MS, RENEW_MS, ReadLeases, WRITTEN_MS } from "./read-leases.js";
export { HOT_READS, MAX_RELAYS, RELAY_FANOUT, RELAY_READS, RelayPlan, relayIndex, relayParent } from "./relay-plan.js";
export { QUIET_MS, RelayReads } from "./relay-reads.js";
//# sourceMappingURL=index.js.map