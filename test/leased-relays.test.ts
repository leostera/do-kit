import { afterEach, describe, expect, setSystemTime, test } from "bun:test";
import { READ_LEASE_MARGIN_MS, READ_LEASE_MS } from "../src/leased/read-leases.ts";
import { HOT_READS, MAX_RELAYS, RELAY_FANOUT, RELAY_READS, RelayPlan, relayIndex, relayParent } from "../src/leased/relay-plan.ts";
import { QUIET_MS, RelayReads, RENEW_MS, type SourceAnswer } from "../src/leased/relay-reads.ts";

const T0 = new Date("2026-01-01T00:00:00Z").getTime();
const at = (ms: number) => setSystemTime(new Date(T0 + ms));
afterEach(() => setSystemTime());

/** A leaf whose reads the test answers one by one. */
function leaf() {
  const calls: { resolve: (a: SourceAnswer<string>) => void; reject: (e: Error) => void }[] = [];
  const read = () => new Promise<SourceAnswer<string>>((resolve, reject) => calls.push({ resolve, reject }));
  return { calls, read };
}
const tick = () => new Promise((r) => setTimeout(r, 0));

describe("relay leases asked back", () => {
  test("a revoked lease answers no more asks, and says how long its sub-leases have left", async () => {
    at(0);
    const l = leaf();
    const relay = new RelayReads(l.read);
    const first = relay.read();
    l.calls[0]!.resolve({ value: "v", leaseMs: READ_LEASE_MS, relays: 1 });
    const sub = (await first).leaseMs;
    expect(sub).toBe(READ_LEASE_MS - READ_LEASE_MARGIN_MS);
    at(100);
    expect(relay.revoke()).toBe(sub - 100);
    // Not from the lease: a leaf read.
    const after = relay.read();
    expect(l.calls.length).toBe(2);
    l.calls[1]!.resolve({ value: "v2", leaseMs: 0, relays: 1 });
    expect(await after).toEqual({ value: "v2", leaseMs: 0, relays: 1 });
  });

  test("a lease a read brings back after it was asked back is never taken up", async () => {
    at(0);
    const l = leaf();
    const relay = new RelayReads(l.read);
    const first = relay.read();
    expect(relay.revoke()).toBe(0);
    l.calls[0]!.resolve({ value: "old", leaseMs: READ_LEASE_MS, relays: 1 });
    // The ask arrived before the revoke: the old value, with no sub-lease.
    expect(await first).toEqual({ value: "old", leaseMs: 0, relays: 1 });
    const next = relay.read();
    expect(l.calls.length).toBe(2);
    l.calls[1]!.resolve({ value: "new", leaseMs: READ_LEASE_MS, relays: 1 });
    expect((await next).value).toBe("new");
  });

  test("for QUIET_MS after a revoke a relay reads through: no lease, no sub-leases, so the next push has nothing to ask back; then it leases again", async () => {
    at(0);
    const leased = leaf();
    const through = leaf();
    const relay = new RelayReads(leased.read, through.read);
    relay.revoke();
    // Read through: the unleased read, and a lease it brings back anyway is not taken up.
    const first = relay.read();
    expect(leased.calls.length).toBe(0);
    through.calls[0]!.resolve({ value: "v", leaseMs: READ_LEASE_MS, relays: 1 });
    expect(await first).toEqual({ value: "v", leaseMs: 0, relays: 1 });
    // So the next ask reads the leaf again, after it arrived: it sees a push acknowledged since.
    const second = relay.read();
    await tick();
    expect(through.calls.length).toBe(2);
    through.calls[1]!.resolve({ value: "v2", leaseMs: 0, relays: 1 });
    expect(await second).toEqual({ value: "v2", leaseMs: 0, relays: 1 });
    expect(relay.revoke()).toBe(0);
    at(QUIET_MS + 1);
    const later = relay.read();
    leased.calls[0]!.resolve({ value: "v2", leaseMs: READ_LEASE_MS, relays: 1 });
    expect((await later).leaseMs).toBeGreaterThan(0);
  });
});

describe("fresh relay reads", () => {
  test("a fresh ask takes no lease, and joins only a leaf read that started after it arrived", async () => {
    at(0);
    const leased = leaf();
    const fresh = leaf();
    const relay = new RelayReads(leased.read, fresh.read);
    const first = relay.fresh();
    expect(fresh.calls.length).toBe(1);
    // Arrived while the first read was in flight: it may miss a push acknowledged meanwhile.
    const second = relay.fresh();
    const third = relay.fresh();
    fresh.calls[0]!.resolve({ value: "old", leaseMs: 0, relays: 3 });
    expect(await first).toEqual({ value: "old", leaseMs: 0, relays: 3 });
    await tick();
    expect(fresh.calls.length).toBe(2);
    fresh.calls[1]!.resolve({ value: "new", leaseMs: 0, relays: 3 });
    expect(await second).toEqual({ value: "new", leaseMs: 0, relays: 3 });
    expect((await third).value).toBe("new");
    expect(leased.calls.length).toBe(0);
  });

  test("while the relay holds a lease, a fresh ask is its value, without a sub-lease", async () => {
    at(0);
    const leased = leaf();
    const fresh = leaf();
    const relay = new RelayReads(leased.read, fresh.read);
    const read = relay.read();
    leased.calls[0]!.resolve({ value: "v", leaseMs: READ_LEASE_MS, relays: 2 });
    await read;
    expect(await relay.fresh()).toEqual({ value: "v", leaseMs: 0, relays: 2 });
    expect(fresh.calls.length).toBe(0);
  });

  test("a failed fresh read fails its asks; the next ask reads again", async () => {
    at(0);
    const fresh = leaf();
    const relay = new RelayReads(leaf().read, fresh.read);
    const first = relay.fresh();
    fresh.calls[0]!.reject(new Error("leaf reset"));
    await expect(first).rejects.toThrow("leaf reset");
    await tick();
    const second = relay.fresh();
    expect(fresh.calls.length).toBe(2);
    fresh.calls[1]!.resolve({ value: "v", leaseMs: 0, relays: 1 });
    expect((await second).value).toBe("v");
  });
});

describe("relay reads", () => {
  test("asks that arrive while a read is in flight wait for the next read, not that one", async () => {
    at(0);
    const l = leaf();
    const relay = new RelayReads(l.read);
    const first = relay.read();
    expect(l.calls.length).toBe(1);
    const second = relay.read();
    const third = relay.read();
    // No lease (a push is held): the first ask gets the old value...
    l.calls[0]!.resolve({ value: "old", leaseMs: 0, relays: 1 });
    expect(await first).toEqual({ value: "old", leaseMs: 0, relays: 1 });
    await tick();
    // ...and the two later asks share one new read.
    expect(l.calls.length).toBe(2);
    l.calls[1]!.resolve({ value: "new", leaseMs: 0, relays: 1 });
    expect((await second).value).toBe("new");
    expect((await third).value).toBe("new");
  });

  test("under a lease, asks are answered from memory with a sub-lease that ends before the relay's", async () => {
    at(0);
    const l = leaf();
    const relay = new RelayReads(l.read);
    const first = relay.read();
    l.calls[0]!.resolve({ value: "v", leaseMs: READ_LEASE_MS, relays: 3 });
    const until = T0 + READ_LEASE_MS - READ_LEASE_MARGIN_MS;
    expect(await first).toEqual({ value: "v", leaseMs: until - T0, relays: 3 });
    at(100);
    expect(await relay.read()).toEqual({ value: "v", leaseMs: until - T0 - 100, relays: 3 });
    expect(l.calls.length).toBe(1);
  });

  test("the lease is renewed ahead, while asks go on being answered from it", async () => {
    at(0);
    const l = leaf();
    const relay = new RelayReads(l.read);
    const first = relay.read();
    l.calls[0]!.resolve({ value: "v1", leaseMs: READ_LEASE_MS, relays: 1 });
    await first;
    at(READ_LEASE_MS - READ_LEASE_MARGIN_MS - RENEW_MS + 1);
    expect((await relay.read()).value).toBe("v1");
    expect(l.calls.length).toBe(2);
    expect((await relay.read()).value).toBe("v1");
    expect(l.calls.length).toBe(2);
    l.calls[1]!.resolve({ value: "v2", leaseMs: READ_LEASE_MS, relays: 1 });
    await tick();
    expect((await relay.read()).value).toBe("v2");
  });

  test("a failed read fails the asks it covers; later asks read again", async () => {
    at(0);
    const l = leaf();
    const relay = new RelayReads(l.read);
    const first = relay.read();
    const second = relay.read();
    l.calls[0]!.reject(new Error("overloaded"));
    await expect(first).rejects.toThrow("overloaded");
    await tick();
    l.calls[1]!.resolve({ value: "v", leaseMs: 0, relays: 1 });
    expect((await second).value).toBe("v");
  });
});

describe("relay plan", () => {
  test("a ref is relayed once read HOT_READS times a second", () => {
    at(0);
    const plan = new RelayPlan(HOT_READS);
    // HOT_READS reads spread over a second: hot, at a rate one relay takes.
    for (let i = 1; i < HOT_READS; i++) {
      at(i * 10);
      expect(plan.direct(["main"])).toEqual([0]);
    }
    at(999);
    expect(plan.direct(["main"])).toEqual([1]);
    expect(plan.direct(["main", "other"])).toEqual([1, 0]);
  });

  test("a ref that turns hot fast starts with as many relays as its rate needs", () => {
    // 7,000 reads a second: HOT_READS of them in the window's first 14 ms. One relay would
    // take them all until its reports raised the count, and queue until it failed.
    at(0);
    const plan = new RelayPlan(HOT_READS);
    for (let i = 1; i < HOT_READS; i++) plan.direct(["main"]);
    at(14);
    expect(plan.direct(["main"])).toEqual([Math.ceil((HOT_READS * 1000) / 14 / RELAY_READS)]);
    // A burst all at once counts as if over 10 ms, and the count never passes MAX_RELAYS.
    at(2000);
    const burst = new RelayPlan(HOT_READS);
    for (let i = 1; i < HOT_READS; i++) burst.direct(["main"]);
    expect(burst.direct(["main"])).toEqual([Math.min(MAX_RELAYS, Math.ceil((HOT_READS * 100) / RELAY_READS))]);
  });

  test("the relay count follows the relays' reported asks, up at once and down after a while", () => {
    at(0);
    const plan = new RelayPlan(4);
    for (let i = 0; i < 4; i++) plan.direct(["main"]);
    expect(plan.reported(["main"], 0, 4 * RELAY_READS)).toEqual([4]);
    at(500);
    for (const i of [1, 2, 3]) plan.reported(["main"], i, 1);
    // Reports of 4×RELAY_READS + 3: one more relay.
    expect(plan.reported(["main"], 0, 4 * RELAY_READS)).toEqual([5]);
    // Load falls: the count stays for a while, then follows.
    at(3000);
    expect(plan.reported(["main"], 0, RELAY_READS)).toEqual([5]);
    at(13_000);
    expect(plan.reported(["main"], 0, RELAY_READS)).toEqual([1]);
    // Cold: relaying stops after a while.
    at(14_000);
    plan.reported(["main"], 0, 0);
    at(24_000);
    expect(plan.reported(["main"], 0, 0)).toEqual([0]);
    expect(plan.direct(["main"])).toEqual([0]);
  });

  test("a restarted leaf learns its relays from their reads", () => {
    at(0);
    const plan = new RelayPlan(HOT_READS);
    expect(plan.reported(["main"], 6, RELAY_READS)).toEqual([7]);
    expect(plan.direct(["main"])).toEqual([7]);
  });

  test("an isolate's draw picks a relay below the count", () => {
    expect(relayIndex(0, 1)).toBe(0);
    expect(relayIndex(0.999999, 8)).toBe(7);
    expect(relayIndex(0.5, 8)).toBe(4);
    expect(relayIndex(0.5, MAX_RELAYS)).toBe(Math.floor(MAX_RELAYS / 2));
  });
});

describe("relays nest", () => {
  test("the first RELAY_FANOUT read the leaf; every other relay reads one below it, which has at most RELAY_FANOUT children, three levels deep", () => {
    const children = new Map<number, number>();
    let deepest = 0;
    for (let i = 0; i < MAX_RELAYS; i++) {
      const p = relayParent(i);
      if (i < RELAY_FANOUT) expect(p).toBeNull();
      else {
        expect(p).not.toBeNull();
        expect(p!).toBeLessThan(i);
        children.set(p!, (children.get(p!) ?? 0) + 1);
      }
      let depth = 1;
      for (let q = relayParent(i); q !== null; q = relayParent(q)) depth++;
      deepest = Math.max(deepest, depth);
    }
    expect(Math.max(...children.values())).toBe(RELAY_FANOUT);
    expect(deepest).toBe(3);
    expect(relayParent(RELAY_FANOUT)).toBe(0);
    expect(relayParent(2 * RELAY_FANOUT - 1)).toBe(0);
    expect(relayParent(2 * RELAY_FANOUT)).toBe(1);
  });

  test("while quiet, isolates' reads and pushes' fresh reads are one stream of reads through: one in flight, each ask answered by one started after it", async () => {
    at(0);
    const leased = leaf();
    const through = leaf();
    const relay = new RelayReads(leased.read, through.read);
    relay.revoke();
    const read = relay.read();
    const fresh = relay.fresh();
    const read2 = relay.read();
    await tick();
    expect(leased.calls.length).toBe(0);
    expect(through.calls.length).toBe(1);
    through.calls[0]!.resolve({ value: "v", leaseMs: 0, relays: 9 });
    expect(await read).toEqual({ value: "v", leaseMs: 0, relays: 9 });
    await tick();
    // The two that came after the first read started share the next one.
    expect(through.calls.length).toBe(2);
    through.calls[1]!.resolve({ value: "v2", leaseMs: 0, relays: 9 });
    expect((await fresh).value).toBe("v2");
    expect((await read2).value).toBe("v2");
    expect(through.calls.length).toBe(2);
  });

  test("a leased read the leaf answers without a lease starts the quiet: later reads go through", async () => {
    at(0);
    const leased = leaf();
    const through = leaf();
    const relay = new RelayReads(leased.read, through.read);
    const first = relay.read();
    leased.calls[0]!.resolve({ value: "v", leaseMs: 0, relays: 9 });
    expect(await first).toEqual({ value: "v", leaseMs: 0, relays: 9 });
    const second = relay.read();
    await tick();
    expect(leased.calls.length).toBe(1);
    expect(through.calls.length).toBe(1);
    through.calls[0]!.resolve({ value: "v2", leaseMs: 0, relays: 9 });
    expect((await second).value).toBe("v2");
    // Leased again once the quiet is over.
    at(QUIET_MS + 1);
    void relay.read();
    expect(leased.calls.length).toBe(2);
  });
});
