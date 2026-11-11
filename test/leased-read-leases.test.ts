import { afterEach, describe, expect, setSystemTime, test } from "bun:test";
import { MIN_LEASE_MS, WRITTEN_MS, READ_LEASE_MARGIN_MS, READ_LEASE_MS, ReadLeases, RENEW_MS, type HeldRow, type HeldRows } from "../src/leased/read-leases.ts";

const T0 = new Date("2026-01-01T00:00:00Z").getTime();
const at = (ms: number) => setSystemTime(new Date(T0 + ms));
afterEach(() => setSystemTime());

/** A source's storage for held rows, as a Durable Object's SQLite would keep them: it outlives the ReadLeases. */
function storage<Row = string>(): HeldRows<Row> {
  const rows = new Map<string, HeldRow<Row>>();
  return { load: () => [...rows.values()], put: (held) => void rows.set(held.key, held), delete: (key) => void rows.delete(key) };
}

/** A fresh leaf (no lease horizon stored). */
function leaf(horizons: number[] = [], held: HeldRows<string> = storage()) {
  return new ReadLeases<string>(0, (h) => horizons.push(h), held);
}
function makeHot(leases: ReadLeases<string>, name: string): number {
  return leases.grant([name]);
}

describe("read leases", () => {
  test("a relay gets a whole lease when no push is held", () => {
    at(0);
    expect(leaf().grant(["main"])).toBe(READ_LEASE_MS);
  });

  test("a push to a leased ref is shown only once the lease is over; reads see the old row until then", () => {
    at(0);
    const leases = leaf();
    makeHot(leases, "main");
    at(100);
    const shownAt = leases.holdUntil([{ key: "main", before: "old" }]);
    expect(shownAt).toBe(T0 + READ_LEASE_MS + READ_LEASE_MARGIN_MS);
    expect(leases.visible("main", "new")).toBe("old");
    at(READ_LEASE_MS + READ_LEASE_MARGIN_MS);
    expect(leases.visible("main", "new")).toBe("new");
  });

  test("a lease granted while a push is held ends by the hold's end, so a held push never waits for more", () => {
    at(0);
    const leases = leaf();
    makeHot(leases, "main");
    const shownAt = leases.holdUntil([{ key: "main", before: "old" }]);
    at(300);
    const ms = leases.grant(["main"]);
    expect(ms).toBe(shownAt - (T0 + 300) - READ_LEASE_MARGIN_MS);
    // Another push now waits no longer than the first.
    expect(leases.holdUntil([{ key: "main", before: "old" }])).toBe(shownAt);
    // Too little left: no lease.
    at(shownAt - T0 - READ_LEASE_MARGIN_MS - MIN_LEASE_MS + 1);
    expect(leases.grant(["main"])).toBe(0);
  });

  test("a push to an unleased ref is shown at once", () => {
    at(0);
    const leases = leaf();
    makeHot(leases, "main");
    expect(leases.holdUntil([{ key: "other", before: undefined }])).toBe(T0);
    expect(leases.visible("other", "new")).toBe("new");
  });

  test("a second push while one is held keeps reads on the row from before the first", () => {
    at(0);
    const leases = leaf();
    makeHot(leases, "main");
    leases.holdUntil([{ key: "main", before: "v1" }]);
    leases.holdUntil([{ key: "main", before: "v2" }]);
    expect(leases.visible("main", "v3")).toBe("v1");
  });

  test("the horizon is stored before a lease reaching past it is granted", () => {
    at(0);
    const horizons: number[] = [];
    const leases = leaf(horizons);
    makeHot(leases, "main");
    expect(horizons.length).toBe(1);
    expect(horizons[0]!).toBeGreaterThanOrEqual(T0 + READ_LEASE_MS + READ_LEASE_MARGIN_MS);
  });

  test("a restarted leaf holds every push until its stored horizon has passed", () => {
    at(0);
    const horizon = T0 + 3000;
    const leases = new ReadLeases<string>(horizon, () => {}, storage());
    const shownAt = leases.holdUntil([{ key: "main", before: "old" }]);
    expect(shownAt).toBeGreaterThan(horizon);
    expect(leases.visible("main", "new")).toBe("old");
  });
});

test("holdsAny is true while a push waits for a lease, and false once the lease is over", () => {
  at(0);
  const leases = new ReadLeases<number>(0, () => {}, storage<number>());
  expect(leases.grant(["main"])).toBe(READ_LEASE_MS);
  expect(leases.holdsAny()).toBe(false);
  expect(leases.holdUntil([{ key: "main", before: 1 }])).toBe(T0 + READ_LEASE_MS + READ_LEASE_MARGIN_MS);
  expect(leases.holdsAny()).toBe(true);
  at(READ_LEASE_MS + READ_LEASE_MARGIN_MS);
  expect(leases.holdsAny()).toBe(false);
});

describe("leases asked back", () => {
  test("a held push asks each relay holding a lease once; a relay that gives it back ends the hold early", () => {
    at(0);
    const leases = leaf();
    expect(leases.grant(["main"], 0)).toBe(READ_LEASE_MS);
    expect(leases.grant(["main"], 3)).toBe(READ_LEASE_MS);
    at(100);
    const shownAt = leases.holdUntil([{ key: "main", before: "old" }]);
    expect(shownAt).toBe(T0 + READ_LEASE_MS + READ_LEASE_MARGIN_MS);
    expect(leases.toRevoke("main").sort()).toEqual([0, 3]);
    // Asked already: a second push held meanwhile doesn't ask again.
    expect(leases.toRevoke("main")).toEqual([]);
    // One gave it back with no sub-lease out: the other still holds the push.
    expect(leases.returned("main", 0, 0)).toBe(shownAt);
    expect(leases.visible("main", "new")).toBe("old");
    // Both gave it back with no sub-lease out: shown now, not when the leases would have ended.
    expect(leases.returned("main", 3, 0)).toBe(T0 + 100);
    expect(leases.visible("main", "new")).toBe("new");
  });

  test("a relay gets no lease while a push is held, so the hold ends with the leases given back", () => {
    at(0);
    const leases = leaf();
    leases.grant(["main"], 0);
    leases.holdUntil([{ key: "main", before: "old" }]);
    at(50);
    expect(leases.grant(["main"], 1)).toBe(0);
    // A holder that can't be asked back gets one inside the hold, as before.
    expect(leases.grant(["main"])).toBeGreaterThan(0);
  });

  test("a lease given back with sub-leases out ends when they do; nothing is asked back with no push held", () => {
    at(0);
    const leases = leaf();
    leases.grant(["main"], 0);
    expect(leases.toRevoke("main")).toEqual([]);
    leases.holdUntil([{ key: "main", before: "old" }]);
    leases.toRevoke("main");
    expect(leases.returned("main", 0, 200)).toBe(T0 + 200 + READ_LEASE_MARGIN_MS);
  });
});

describe("a ref being pushed is read through", () => {
  test("for WRITTEN_MS after a change relays get no lease, so the next push has none to wait for; other refs and unnamed holders still do", () => {
    at(0);
    const leases = leaf();
    expect(leases.holdUntil([{ key: "main", before: "old" }])).toBe(T0);
    at(100);
    expect(leases.grant(["main"], 3)).toBe(0);
    expect(leases.grant(["dev"], 3)).toBe(READ_LEASE_MS);
    expect(leases.grant(["main"])).toBe(READ_LEASE_MS);
    // A relay that held no lease when the push came is not leased either: the next push isn't held by it.
    at(WRITTEN_MS - 1);
    expect(leases.grant(["main"], 4)).toBe(0);
    at(WRITTEN_MS);
    expect(leases.grant(["main"], 4)).toBe(READ_LEASE_MS);
  });
});

test("keys whose leases are over are forgotten, not kept for as long as the source runs", () => {
  at(0);
  const leases = leaf();
  for (let i = 0; i < 1_000; i++) leases.grant([`ref-${i}`]);
  expect(leases.leasedKeys).toBe(1_000);
  at(READ_LEASE_MS + READ_LEASE_MARGIN_MS + 1);
  leases.grant(["main"]);
  expect(leases.leasedKeys).toBe(1);
  // A lease still running is kept.
  at(READ_LEASE_MS + READ_LEASE_MARGIN_MS + 500);
  leases.grant(["other"]);
  at(2 * (READ_LEASE_MS + READ_LEASE_MARGIN_MS) + 2);
  leases.grant(["third"]);
  expect(leases.leasedKeys).toBe(2);
});

// A write commits at once; until the leases on its key are over, reads see the row from before
// it. That row must outlive the source: relays holding leases from before a restart still answer
// with it, so a source that forgot it would show the write first, and a later read through a
// relay would go back.
describe("a source restarted while a write is held", () => {
  /** A source with a lease out on main and a write held on it; its storage and stored horizon. */
  function stopped() {
    at(0);
    const held = storage();
    const horizons: number[] = [];
    const leases = leaf(horizons, held);
    makeHot(leases, "main");
    const shownAt = leases.holdUntil([{ key: "main", before: "old" }]);
    return { held, horizon: horizons.at(-1)!, shownAt };
  }
  const restarted = (s: { held: HeldRows<string>; horizon: number }) => new ReadLeases<string>(s.horizon, () => {}, s.held);

  test("keeps showing the row from before it, until the hold it had ends", () => {
    const s = stopped();
    at(100);
    const leases = restarted(s);
    expect(leases.visible("main", "new")).toBe("old");
    expect(leases.holdsAny()).toBe(true);
    expect(leases.shownAt("main")).toBe(s.shownAt);
    setSystemTime(new Date(s.shownAt));
    expect(leases.visible("main", "new")).toBe("new");
  });

  test("a row that did not exist before the write is kept as such", () => {
    at(0);
    const held = storage();
    const leases = leaf([], held);
    makeHot(leases, "main");
    leases.holdUntil([{ key: "main", before: undefined }]);
    expect(new ReadLeases<string>(T0 + 5000, () => {}, held).visible("main", "new")).toBeUndefined();
  });

  test("a write after the restart keeps reads on the row from before the first", () => {
    const s = stopped();
    at(100);
    const leases = restarted(s);
    const shownAt = leases.holdUntil([{ key: "main", before: "new" }]);
    expect(leases.visible("main", "newer")).toBe("old");
    // Held by the restart rule, and stored again: a second restart shows the same.
    expect(shownAt).toBeGreaterThan(s.horizon);
    expect(restarted(s).visible("main", "newer")).toBe("old");
    expect(restarted(s).shownAt("main")).toBe(shownAt);
  });

  test("a lease given back before the restart ended the hold early, and the restart keeps that end", () => {
    at(0);
    const held = storage();
    const horizons: number[] = [];
    const leases = leaf(horizons, held);
    leases.grant(["main"], 0);
    leases.holdUntil([{ key: "main", before: "old" }]);
    at(50);
    const shownAt = leases.returned("main", 0, 0);
    expect(shownAt).toBe(T0 + 50);
    // Shown and acknowledged at 50: a restart must not go back to the row from before.
    at(60);
    expect(new ReadLeases<string>(horizons.at(-1)!, () => {}, held).visible("main", "new")).toBe("new");
  });

  test("a hold that is over is forgotten in storage too: when read, and by the sweep", () => {
    const s = stopped();
    expect(s.held.load().length).toBe(1);
    setSystemTime(new Date(s.shownAt));
    const leases = restarted(s);
    expect(s.held.load().length).toBe(0);
    // Held again, never read again: the sweep (on a later grant) drops it.
    const again = leases.holdUntil([{ key: "side", before: "a" }]);
    expect(s.held.load().length).toBe(1);
    setSystemTime(new Date(again + READ_LEASE_MS));
    leases.grant(["other"]);
    expect(s.held.load().length).toBe(0);
  });

  test("a write to a key with no lease out is shown at once and stores nothing", () => {
    at(0);
    const held = storage();
    const leases = leaf([], held);
    leases.holdUntil([{ key: "cold", before: "old" }]);
    expect(held.load().length).toBe(0);
  });
});
