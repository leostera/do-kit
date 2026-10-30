// SPIKE: durability's outbox on a sharded object (test/worker MirrorCounters → Sink). Every write
// stages the key's new value in the same fenced transaction; the sink keeps the highest version.
// Checks that, through splits and a sink outage, the sink converges to every key's value, and
// that a pending entry lives only on the shard that owns its key.
import { expect, test } from "bun:test";
import { call, post, shards, useWorker } from "./harness.ts";

useWorker();

type SinkState = { latest: Record<string, { version: number; value: number }>; deliveries: number; duplicates: number; senders: Record<string, string[]> };
type Stats = { statements: number; rowsWritten: number; stageRowsWritten: number; virtualSets: number; scheduleAlarmCalls: number; schedulerRuns: number };
type ShardState = { label: string; rows: Record<string, number>; pending: { key: string; version: number; attempt: number; owned: boolean }[]; stats: Stats; alarm: number | null; rowsWritten: number };

const KEYS = Array.from({ length: 200 }, (_, i) => `k-${i}`);
const sink = (name: string, fail?: boolean) => post<SinkState>("/sink", fail === undefined ? { name } : { name, fail });
const states = (name: string) => post<ShardState[]>("/scan", { cls: "mirror", name, method: "outboxState" });

/** Waits until the sink holds every key's current value and no shard has anything pending. */
async function converged(name: string, keys: string[], ms = 30_000): Promise<{ sink: SinkState; shards: ShardState[]; waitedMs: number }> {
  const started = Date.now();
  for (;;) {
    const [s, all] = await Promise.all([sink(name), states(name)]);
    const rows = Object.assign({}, ...all.map((a) => a.rows)) as Record<string, number>;
    const done = keys.every((k) => s.latest[k]?.value === rows[k]) && all.every((a) => a.pending.length === 0);
    if (done) return { sink: s, shards: all, waitedMs: Date.now() - started };
    if (Date.now() - started > ms) {
      const behind = keys.filter((k) => s.latest[k]?.value !== rows[k]);
      throw new Error(`did not converge: ${behind.length} keys behind (${behind.slice(0, 5).join(", ")}), pending ${JSON.stringify(all.map((a) => [a.label, a.pending.length]))}`);
    }
    await Bun.sleep(100);
  }
}

test("each write reaches the sink, staged with the write", async () => {
  const name = `mirror-basic-${Date.now()}`;
  const started = Date.now();
  await call("mirror", name, "set", KEYS.slice(0, 50), KEYS.slice(0, 50).map((_, i) => i));
  const { sink: s, waitedMs } = await converged(name, KEYS.slice(0, 50));
  console.log(JSON.stringify({ test: "basic", deliveries: s.deliveries, duplicates: s.duplicates, convergedMs: waitedMs, totalMs: Date.now() - started }));
  expect(s.duplicates).toBe(0);
}, 60_000);

test("through splits under writes, the sink converges and entries follow their keys", async () => {
  const name = `mirror-split-${Date.now()}`;
  const values = new Map<string, number>();
  let stop = false;
  const writers = Array.from({ length: 8 }, async (_, w) => {
    for (let i = 0; !stop; i++) {
      const batch = KEYS.filter((_, j) => j % 8 === w).slice(i % 5, (i % 5) + 5);
      const value = i * 10 + w;
      await call("mirror", name, "set", batch, batch.map(() => value));
      for (const k of batch) values.set(k, value);
    }
  });
  // Split each shard twice over, while the writers write.
  for (let round = 0; round < 3; round++) {
    await Bun.sleep(400);
    for (const bits of await shards("mirror", name)) await post("/split", { cls: "mirror", name, bits });
  }
  await Bun.sleep(400);
  stop = true;
  await Promise.all(writers);
  const found = await shards("mirror", name);
  const { sink: s, shards: all, waitedMs } = await converged(name, [...values.keys()]);
  // Every key ends at the value its last write set.
  for (const [k, v] of values) expect(s.latest[k]?.value).toBe(v);
  const multiSender = Object.values(s.senders).filter((f) => f.length > 1).length;
  console.log(JSON.stringify({ test: "splits", shards: found.length, keys: values.size, deliveries: s.deliveries, duplicates: s.duplicates, keysSentByMoreThanOneShard: multiSender, convergedMs: waitedMs }));
  expect(all.length).toBe(found.length);
}, 120_000);

test("a backlog built while the sink is down splits with its keys, and drains once it is back", async () => {
  const name = `mirror-outage-${Date.now()}`;
  await sink(name, true);
  for (let i = 0; i < KEYS.length; i += 50) await call("mirror", name, "set", KEYS.slice(i, i + 50), KEYS.slice(i, i + 50).map(() => 1));
  await call("mirror", name, "set", KEYS.slice(0, 20), KEYS.slice(0, 20).map(() => 2)); // supersede some before they publish
  const before = await states(name);
  expect(before.flatMap((s) => s.pending).length).toBe(KEYS.length); // one entry per key: superseded versions coalesce

  expect(await post<boolean>("/split", { cls: "mirror", name, bits: "" })).toBe(true);
  // Wait for both sides to clean up what they gave away.
  let after: ShardState[] = [];
  for (let i = 0; i < 100; i++) {
    after = await states(name);
    if (after.length === 2 && after.every((s) => s.pending.every((p) => p.owned))) break;
    await Bun.sleep(100);
  }
  const pending = after.flatMap((s) => s.pending);
  console.log(JSON.stringify({ test: "outage", shards: after.map((s) => [s.label, s.pending.length]), pending: pending.length }));
  expect(after.length).toBe(2);
  expect(pending.every((p) => p.owned)).toBe(true); // each entry on its key's shard only
  expect(new Set(pending.map((p) => p.key)).size).toBe(pending.length); // never on both
  expect(pending.length).toBe(KEYS.length);

  await sink(name, false);
  const { sink: s, waitedMs } = await converged(name, KEYS);
  for (const k of KEYS) expect(s.latest[k]?.value).toBe(KEYS.indexOf(k) < 20 ? 2 : 1);
  console.log(JSON.stringify({ test: "outage-drain", deliveries: s.deliveries, duplicates: s.duplicates, convergedMs: waitedMs }));
}, 120_000);

test("after a crash, a backlog drains from the alarm alone (no in-memory timers)", async () => {
  const name = `mirror-crash-${Date.now()}`;
  await sink(name, true);
  await call("mirror", name, "set", KEYS.slice(0, 30), KEYS.slice(0, 30).map(() => 7));
  await Bun.sleep(300); // a failed attempt or two, with retries 200 ms apart
  await post("/crash", { cls: "mirror", name, bits: "" });
  await sink(name, false);
  const [state] = await states(name); // starts the object again: nothing in memory, the alarm is all there is
  console.log(JSON.stringify({ test: "crash", pending: state!.pending.length, alarmInMs: state!.alarm === null ? null : state!.alarm - Date.now() }));
  expect(state!.alarm).not.toBeNull();
  const { sink: s, waitedMs } = await converged(name, KEYS.slice(0, 30));
  console.log(JSON.stringify({ test: "crash-drain", deliveries: s.deliveries, duplicates: s.duplicates, convergedMs: waitedMs }));
}, 60_000);

test("cost: rows written and latency per write, with and without the outbox", async () => {
  const name = `mirror-cost-${Date.now()}`;
  const n = 200;
  // Warm both objects first: their schema and meta are written once.
  await call("m", `plain-${name}`, "put", ["warm"], [0]);
  await call("mirror", name, "set", ["warm"], [0]);
  await converged(name, ["warm"]);
  const [plainBefore] = await post<number[]>("/scan", { cls: "m", name: `plain-${name}`, method: "writtenRows" });
  const [mirrorBefore] = await states(name);
  let started = Date.now();
  for (let i = 0; i < n; i++) await call("m", `plain-${name}`, "put", [`c-${i % 50}`], [i]);
  const plainMs = (Date.now() - started) / n;
  const [plainAfter] = await post<number[]>("/scan", { cls: "m", name: `plain-${name}`, method: "writtenRows" });
  started = Date.now();
  for (let i = 0; i < n; i++) await call("mirror", name, "set", [`c-${i % 50}`], [i]);
  const mirrorMs = (Date.now() - started) / n;
  await converged(name, Array.from({ length: 50 }, (_, i) => `c-${i}`));
  const [state] = await states(name);
  const s = await sink(name);
  const st = state!.stats;
  console.log(JSON.stringify({ test: "cost", plainRowsPerWrite: (plainAfter! - plainBefore!) / n, mirrorRowsPerWrite: (state!.rowsWritten - mirrorBefore!.rowsWritten) / n, writes: n, published: s.deliveries - 1, plainMsPerWrite: plainMs, mirrorMsPerWrite: mirrorMs, stageRowsPerWrite: st.stageRowsWritten / n, durabilityRowsPerWrite: st.rowsWritten / n, durabilityStatementsPerWrite: st.statements / n, virtualSets: st.virtualSets, schedulerRunsFromAlarm: st.schedulerRuns }));
}, 120_000);
