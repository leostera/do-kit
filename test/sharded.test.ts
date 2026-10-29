// Sharded objects under cf dev (workerd with local Durable Objects), driven through the test
// Worker: concurrent writes and compare-and-swap across splits lose nothing and stay linearisable
// per key; load splits a node by itself; the client finds shards its map does not know yet.
import { expect, test } from "bun:test";
import fc from "fast-check";
import { createHash } from "node:crypto";
import { call, describe, post, shards, useWorker, workerUrl } from "./harness.ts";

useWorker();

let run = 0;
const KEYS = Array.from({ length: 64 }, (_, i) => `key-${i}`);

test("concurrent adds across splits: every acknowledged add is counted once", async () => {
  await fc.assert(
    fc.asyncProperty(
      fc.array(fc.array(fc.tuple(fc.constantFrom(...KEYS), fc.integer({ min: 1, max: 9 })), { minLength: 1, maxLength: 12 }), { minLength: 10, maxLength: 40 }),
      fc.array(fc.integer({ min: 0, max: 39 }), { maxLength: 5 }),
      async (batches, splitAt) => {
        const name = `adds-${run++}-${Date.now()}`;
        const expected = new Map<string, number>();
        const work = batches.map(async (batch, i) => {
          if (splitAt.includes(i)) {
            // Split a random current shard, behind the client's back.
            const all = await shards("m", name);
            await post("/split", { cls: "m", name, bits: all[i % all.length] });
          }
          await call("m", name, "add", batch.map(([k]) => k), batch.map(([, n]) => n));
          for (const [k, n] of batch) expected.set(k, (expected.get(k) ?? 0) + n);
        });
        await Promise.all(work);
        const got = await call<(number | null)[]>("m", name, "get", KEYS);
        KEYS.forEach((k, i) => expect(got[i] ?? 0).toBe(expected.get(k) ?? 0));
        const scanned = new Map((await post<[string, number][][]>("/scan", { cls: "m", name, method: "entries" })).flat());
        expect(scanned).toEqual(new Map([...expected].filter(([, v]) => v > 0)));
      },
    ),
    { numRuns: 8 },
  );
}, 180_000);

test("compare-and-swap across splits: one winner per value, final value counts the wins", async () => {
  await fc.assert(
    fc.asyncProperty(fc.integer({ min: 2, max: 6 }), fc.integer({ min: 4, max: 10 }), fc.array(fc.integer({ min: 0, max: 30 }), { maxLength: 4 }), async (keyCount, workers, splitAt) => {
      const name = `cas-${run++}-${Date.now()}`;
      const keys = KEYS.slice(0, keyCount);
      const wins = new Map<string, number[]>(keys.map((k) => [k, []]));
      let step = 0;
      await Promise.all(
        Array.from({ length: workers }, async (_, w) => {
          for (let round = 0; round < 6; round++) {
            const key = keys[(w + round) % keys.length]!;
            if (splitAt.includes(step++)) {
              const all = await shards("m", name);
              await post("/split", { cls: "m", name, bits: all[step % all.length] });
            }
            const [current] = await call<(number | null)[]>("m", name, "get", [key]);
            const old = current ?? null;
            const [won] = await call<boolean[]>("m", name, "cas", [key], [{ old, new: (old ?? 0) + 1 }]);
            if (won) wins.get(key)!.push(old ?? 0);
          }
        }),
      );
      const final = await call<(number | null)[]>("m", name, "get", keys);
      keys.forEach((k, i) => {
        const w = wins.get(k)!;
        expect(new Set(w).size).toBe(w.length); // no two winners from the same value
        expect(final[i] ?? 0).toBe(w.length);
      });
    }),
    { numRuns: 6 },
  );
}, 180_000);

test("load splits a node by itself, and nothing is lost", async () => {
  const name = `auto-${Date.now()}`;
  const expected = new Map<string, number>();
  const until = Date.now() + 6000;
  await Promise.all(
    Array.from({ length: 8 }, async (_, w) => {
      for (let i = 0; Date.now() < until; i++) {
        const key = KEYS[(w * 7 + i) % KEYS.length]!;
        await call("auto", name, "add", [key], [1]);
        expected.set(key, (expected.get(key) ?? 0) + 1);
      }
    }),
  );
  const found = await shards("auto", name);
  expect(found.length).toBeGreaterThan(1);
  const got = await call<(number | null)[]>("auto", name, "get", KEYS);
  KEYS.forEach((k, i) => expect(got[i] ?? 0).toBe(expected.get(k) ?? 0));
}, 60_000);

test("a new object splits to its initial depth before its first call, and concurrent first calls lose nothing", async () => {
  const name = `presplit-${Date.now()}`;
  const expected = new Map<string, number>();
  // Many first calls at once: they must wait for the presplit, then land on the shards.
  await Promise.all(
    Array.from({ length: 16 }, async (_, w) => {
      for (let i = 0; i < 10; i++) {
        const key = KEYS[(w * 5 + i) % KEYS.length]!;
        await call("presplit", name, "add", [key], [1]);
        expected.set(key, (expected.get(key) ?? 0) + 1);
      }
    }),
  );
  const found = await shards("presplit", name);
  expect(found.sort()).toEqual(["00", "01", "10", "11"]);
  for (const bits of found) expect((await describe("presplit", name, bits)).state).toBe("shard");
  const got = await call<(number | null)[]>("presplit", name, "get", KEYS);
  KEYS.forEach((k, i) => expect(got[i] ?? 0).toBe(expected.get(k) ?? 0));
}, 60_000);

test("a split copies the right half, and each object deletes the half it gave away", async () => {
  const name = `copy-${Date.now()}`;
  const many = Array.from({ length: 2500 }, (_, i) => `row-${i}`);
  for (let i = 0; i < many.length; i += 500) await call("m", name, "add", many.slice(i, i + 500), many.slice(i, i + 500).map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  for (let i = 0; i < 100; i++) {
    const [left, right] = await Promise.all([describe("m", name, "0"), describe("m", name, "1")]);
    if (left.bits === "0" && right.state === "shard" && left.storedRows === left.rows && right.storedRows === right.rows) {
      expect(left.rows + right.rows).toBe(many.length);
      expect(left.rows).toBeGreaterThan(0);
      expect(right.rows).toBeGreaterThan(0);
      const got = await call<(number | null)[]>("m", name, "get", many);
      expect(got.every((v) => v === 1)).toBe(true);
      return;
    }
    await Bun.sleep(100);
  }
  throw new Error("the split did not settle");
}, 60_000);

test("a split copies rows larger than one copy batch", async () => {
  const name = `large-${Date.now()}`;
  // 12 rows of 1 MiB: several batches of the logical copy (4 MiB each).
  const keys = Array.from({ length: 12 }, (_, i) => `big-${i}`);
  const blob = (i: number) => String(i % 10).repeat(1024 * 1024);
  for (const [i, key] of keys.entries()) await call("m", name, "put", [key], [blob(i)]);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  const got = await call<string[]>("m", name, "get", keys);
  got.forEach((v, i) => expect(v === blob(i)).toBe(true));
  expect((await describe("m", name, "1")).state).toBe("shard");
}, 60_000);

test("a split copies subclass tables, and the new shard drops what it does not own", async () => {
  const name = `tables-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  const [left, right] = await post<{ key: string; owned: boolean }[][]>("/scan", { cls: "m", name, method: "history" }).then((shards) => [...shards].sort((a, b) => b.length - a.length));
  // The original keeps its whole table (its queued work stays with it); the copy kept only its keys.
  expect(left!.length).toBe(KEYS.length);
  expect(right!.length).toBeGreaterThan(0);
  expect(right!.every((h) => h.owned)).toBe(true);
  expect(right!.length + left!.filter((h) => h.owned).length).toBe(KEYS.length);
}, 60_000);

test("after a split, cleanupForeign drops the rows each shard no longer owns, on both shards", async () => {
  const name = `foreign-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  for (let i = 0; i < 100; i++) {
    const shards = await post<{ key: string; owned: boolean }[][]>("/scan", { cls: "m", name, method: "listing" });
    if (shards.every((rows) => rows.every((r) => r.owned))) {
      // Each key is listed exactly once, by the shard that owns it.
      expect(shards.flat().map((r) => r.key).sort()).toEqual([...KEYS].sort());
      expect(shards.every((rows) => rows.length > 0)).toBe(true);
      return;
    }
    await Bun.sleep(100);
  }
  throw new Error("the old shard kept rows it no longer owns");
}, 60_000);

test("ownedRowsPage pages each shard in key order, skipping keys it gave away", async () => {
  const name = `paged-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  // Straight after the split, before cleanup: each key once, on the shard that owns it.
  const shards = await post<string[][]>("/scan", { cls: "m", name, method: "pagedKeys", args: [3] });
  expect(shards.length).toBe(2);
  for (const keys of shards) expect(keys).toEqual([...keys].sort());
  expect(shards.flat().sort()).toEqual([...KEYS].sort());
}, 60_000);


/** Bit 0 of a key's routing hash: which half of the root it belongs to. */
const firstBit = (key: string) => (Number.parseInt(createHash("sha256").update(key).digest("hex")[0]!, 16) >> 3) & 1;

test("a logical copy loses no row of the moving half while the half that stays deletes rows", async () => {
  const name = `copy-under-deletes-${Date.now()}`;
  // Enough rows, and large enough, for the copy to take several pages.
  const keys = Array.from({ length: 4000 }, (_, i) => `k-${String(i).padStart(5, "0")}`);
  const value = "x".repeat(2048);
  for (let i = 0; i < keys.length; i += 250) await call("m", name, "put", keys.slice(i, i + 250), keys.slice(i, i + 250).map(() => value));
  const stays = keys.filter((k) => firstBit(k) === 0).sort();
  const moves = keys.filter((k) => firstBit(k) === 1);
  // Delete rows of the half that stays, lowest keys first, while the copy runs: every delete
  // before the copy's position shifts the rows after it.
  let splitting = true;
  const split = post<boolean>("/split", { cls: "m", name, bits: "" }).finally(() => (splitting = false));
  const deleted: string[] = [];
  for (let i = 0; splitting && i < stays.length; i += 20) {
    const batch = stays.slice(i, i + 20);
    await call("m", name, "remove", batch);
    deleted.push(...batch);
  }
  expect(await split).toBe(true);
  expect(deleted.length).toBeGreaterThan(0);
  const got = await call<(string | null)[]>("m", name, "get", moves);
  const lost = moves.filter((_, i) => got[i] !== value);
  expect(lost).toEqual([]);
}, 120_000);

test("a logical copy loses no row while the background cleanup of an earlier split deletes rows", async () => {
  const name = `copy-under-cleanup-${Date.now()}`;
  const keys = Array.from({ length: 6000 }, (_, i) => `k-${String(i).padStart(5, "0")}`);
  const value = "y".repeat(2048);
  for (let i = 0; i < keys.length; i += 250) await call("m", name, "put", keys.slice(i, i + 250), keys.slice(i, i + 250).map(() => value));
  // The first split leaves the root deleting the half it gave away, in paced batches; the second
  // split copies the root's storage while those deletes go on.
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "0" })).toBe(true);
  const got = await call<(string | null)[]>("m", name, "get", keys);
  const lost = keys.filter((_, i) => got[i] !== value);
  expect(lost).toEqual([]);
}, 120_000);

test("ownedRowsPage includes the empty key", async () => {
  const name = `paged-empty-${Date.now()}`;
  await call("m", name, "add", ["", "a", "b"], [1, 1, 1]);
  const shards = await post<string[][]>("/scan", { cls: "m", name, method: "pagedKeys", args: [2] });
  expect(shards.flat().sort()).toEqual(["", "a", "b"]);
}, 60_000);

test("callOneShard with no keys says so", async () => {
  const response = await fetch(`${workerUrl()}/callOne`, { method: "POST", body: JSON.stringify({ cls: "m", name: `one-empty-${Date.now()}`, method: "get", keys: [], payloads: null }) });
  expect(response.status).toBe(500);
  expect(await response.text()).toContain("at least one key");
}, 60_000);

test("a call whose payloads do not line up with its keys is refused before reaching a shard", async () => {
  const name = `misaligned-${Date.now()}`;
  await expect(call("m", name, "add", ["a", "b"], [1])).rejects.toThrow("payloads");
  expect(await call<(number | null)[]>("m", name, "get", ["a", "b"])).toEqual([null, null]);
}, 60_000);

test("a routed call or scan cannot reach the base class's own methods", async () => {
  const name = `reserved-${Date.now()}`;
  await call("m", name, "add", ["a"], [1]);
  for (const method of ["saveMeta", "update", "alarm", "split", "constructor", "toString"]) {
    await expect(call("m", name, method, ["a"])).rejects.toThrow(`no method ${method}`);
    await expect(post("/scan", { cls: "m", name, method })).rejects.toThrow(`no method ${method}`);
  }
  expect(await call<number[]>("m", name, "get", ["a"])).toEqual([1]);
}, 60_000);

test("a split policy that cannot work is refused, saying why", async () => {
  const ok = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12 };
  expect(await post("/checkPolicy", { policy: ok })).toBeNull();
  const bad: [object, string][] = [
    [{ callsPerSecond: 0 }, "callsPerSecond"],
    [{ windowSeconds: 0 }, "windowSeconds"],
    [{ windowSeconds: 1.5 }, "windowSeconds"],
    [{ maxDepth: 65 }, "maxDepth"],
    [{ maxDepth: -1 }, "maxDepth"],
    [{ keysPerCall: 0 }, "keysPerCall"],
    [{ initialDepth: 13 }, "initialDepth"],
    [{ copy: "fast" }, "copy"],
    [{ splitTimeoutMs: 0 }, "splitTimeoutMs"],
    [{ holdWaitMs: -1 }, "holdWaitMs"],
  ];
  for (const [change, field] of bad) expect(await post<string>("/checkPolicy", { policy: { ...ok, ...change } })).toContain(`splitPolicy.${field}`);
}, 60_000);

test("a client recovers when an object's storage was deleted and its name is used again", async () => {
  const name = `wiped-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "0" })).toBe(true);
  // The client learns the splits.
  expect(await call<number[]>("m", name, "get", KEYS)).toEqual(KEYS.map(() => 1));
  expect((await post<string[]>("/splits", { cls: "m", name })).length).toBeGreaterThan(1);
  // Every shard's storage is deleted (an account removed), then the name is used again.
  for (const bits of ["", "1", "01"]) await post("/wipe", { cls: "m", name, bits });
  await call("m", name, "add", ["a", "b"], [5, 7]);
  expect(await call<(number | null)[]>("m", name, "get", ["a", "b", KEYS[0]!])).toEqual([5, 7, null]);
  // A scan starts over from the root too, straight after the next deletion.
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  expect((await post<[string, number][][]>("/scan", { cls: "m", name, method: "entries" })).length).toBe(2);
  for (const bits of ["", "1"]) await post("/wipe", { cls: "m", name, bits });
  expect(await post<[string, number][][]>("/scan", { cls: "m", name, method: "entries" })).toEqual([[]]);
}, 120_000);

test("work a shard schedules for later is done when due, though a split comes in between", async () => {
  const name = `alarm-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  const key = KEYS.find((k) => firstBit(k) === 0)!; // stays on the original
  await call("m", name, "scheduleWork", [key], [1500]);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  await Bun.sleep(3000);
  expect((await post<string[][]>("/scan", { cls: "m", name, method: "workFired" })).flat()).toEqual([key]);
}, 60_000);

test("work a shard schedules much later does not hold back a split's cleanup", async () => {
  const name = `alarm-late-${Date.now()}`;
  const many = Array.from({ length: 1500 }, (_, i) => `row-${i}`);
  for (let i = 0; i < many.length; i += 500) await call("m", name, "add", many.slice(i, i + 500), many.slice(i, i + 500).map(() => 1));
  // Recurring work: every onAlarm asks for the next turn a minute later.
  await call("m", name, "scheduleWork", ["__recurring"], [0]);
  await Bun.sleep(200);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  // And calls keep scheduling work a minute out, on both shards, while they clean up.
  const left = many.find((k) => firstBit(k) === 0)!;
  const right = many.find((k) => firstBit(k) === 1)!;
  for (let i = 0; i < 50; i++) {
    await Promise.all([call("m", name, "scheduleWork", [left], [60_000]), call("m", name, "scheduleWork", [right], [60_000])]);
    const [l, r] = await Promise.all([describe("m", name, "0"), describe("m", name, "1")]);
    if (l.storedRows === l.rows && r.storedRows === r.rows) return;
    await Bun.sleep(100);
  }
  throw new Error("cleanup waited for the subclass's alarm");
}, 60_000);

test("a sharded object that overrides alarm() is refused, pointing at onAlarm", async () => {
  await expect(call("oldAlarm", `old-${Date.now()}`, "get", ["a"])).rejects.toThrow("onAlarm");
}, 60_000);

test("an isolate keeps the splits of the objects it used last, up to a bound", async () => {
  const name = `bounded-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  await call("m", name, "get", KEYS);
  expect(await post<string[]>("/splits", { cls: "m", name })).toEqual([""]);
  await post("/useManyObjects", { cls: "m", name });
  expect(await post<string[]>("/splits", { cls: "m", name })).toEqual([]);
  // Forgetting costs hops, never answers.
  expect(await call<number[]>("m", name, "get", KEYS)).toEqual(KEYS.map(() => 1));
}, 60_000);

test("a serving shard refuses a late batch of a split's copy", async () => {
  const name = `late-batch-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  const key = KEYS.find((k) => firstBit(k) === 1)!; // now on the new shard "1"
  await call("m", name, "add", [key], [1]); // serving, at 2
  // A batch of the copy, from an attempt that timed out, arrives now with the value it copied then.
  await expect(post("/lateBatch", { cls: "m", name, bits: "1", keys: [key], payloads: [1], epoch: 1 })).rejects.toThrow();
  expect(await call<number[]>("m", name, "get", [key])).toEqual([2]);
}, 60_000);

test("a write held by a slow split waits for it, however short the client's own wait", async () => {
  const name = `slow-split-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  const key = KEYS.find((k) => firstBit(k) === 1)!; // in the half that moves
  await post("/slowHandOut", { cls: "m", name, bits: "", ms: 1500 });
  const split = post<boolean>("/split", { cls: "m", name, bits: "" });
  await Bun.sleep(100); // the split holds writes to the moving half now
  // The client would give up after 300 ms of "not ready"; the split takes longer, and says so.
  const added = await post<number[]>("/call", { cls: "m", name, method: "add", keys: [key], payloads: [1], notReadyWaitMs: 300 });
  expect(added).toEqual([2]);
  expect(await split).toBe(true);
  expect(await call<number[]>("m", name, "get", [key])).toEqual([2]);
}, 60_000);

test("a shard keeps a record of its splits, and counts what it answered", async () => {
  const name = `records-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  const key = KEYS.find((k) => firstBit(k) === 1)!; // in the half that moves
  await post("/slowHandOut", { cls: "m", name, bits: "", ms: 500 });
  const split = post<boolean>("/split", { cls: "m", name, bits: "" });
  await Bun.sleep(100);
  // Held, and with holdWaitMs 0 answered "not ready" at once; the client retries until the split is done.
  await post<number[]>("/call", { cls: "m", name, method: "add", keys: [key], payloads: [1] });
  expect(await split).toBe(true);

  const root = await describe("m", name, "");
  expect(root.splits).toHaveLength(1);
  const [record] = root.splits;
  expect(record).toMatchObject({ outcome: "split", copy: "logical", rows: KEYS.length, rightRows: KEYS.filter((k) => firstBit(k) === 1).length, error: null });
  expect(record!.ms).toBeGreaterThanOrEqual(500);
  // The slow hand-out is where the time went; locally the copy is logical.
  expect(record!.steps.handOut).toBeGreaterThanOrEqual(500);
  expect(record!.steps).toMatchObject({ snapshot: 0, restore: 0, restoreInside: 0 });
  expect(record!.steps.warm).toBeGreaterThanOrEqual(0); // before the hold, so not in ms
  expect(record!.steps.claimAttempts).toBeGreaterThanOrEqual(1);
  expect(record!.ms).toBeGreaterThanOrEqual(record!.steps.handOut + record!.steps.copyRows + record!.steps.claim);
  expect(record!.held).toBeGreaterThanOrEqual(1);
  expect(record!.notReady).toBe(record!.held); // holdWaitMs 0: every held write is answered "not ready"
  expect(root.counters.notReady).toBeGreaterThanOrEqual(record!.notReady);
  expect(root.counters.held).toBe(record!.held);
  expect(root.counters.calls).toBeGreaterThanOrEqual(2);
  expect(root.counters.ok).toBeGreaterThanOrEqual(1); // the first add
  expect(root.counters.ok).toBeLessThan(root.counters.calls); // held writes answered "not ready" from inside

  const sibling = await describe("m", name, "1");
  expect(sibling.splits).toEqual([]); // a new shard starts its own record
}, 60_000);


test("a split copies every subclass table, whatever its name", async () => {
  const name = `names-${Date.now()}`;
  await call("m", name, "add", KEYS, KEYS.map(() => 1));
  await call("m", name, "note", KEYS, KEYS.map((k) => `note of ${k}`));
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  const notes = (await post<[string, string][][]>("/scan", { cls: "m", name, method: "notes" })).flat().sort();
  expect(notes).toEqual(KEYS.map((k): [string, string] => [k, `note of ${k}`]).sort());
}, 60_000);
