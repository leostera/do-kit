// Per-key version histories, not end states. Clients write and read a few hot keys and some cold
// ones while the table splits twice, and every call is recorded with its start, end and outcome.
// Then, per key: acknowledged writes have distinct versions that rise in real time, and no read is
// stale (it sees every write acknowledged before it started) or invented (it sees only a version
// some write returned, or could have: an unknown outcome). End states can't see a stale read; this
// can.
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
type Settled = { ok?: unknown; threw?: string };
type Kind = "put" | "patch" | "delete" | "apply" | "get" | "staleGet";
/** One call: `version` is what it returned (0 for an absent row); null if its outcome is unknown. */
type Event = { key: string; kind: Kind; start: number; end: number; acked: boolean; version: number | null };

const seed = Number(process.env.FC_SEED ?? 42);
function random(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 2 ** 32;
  };
}

const doc = (name: string, n: number): Doc => ({ name, id: name, createdAt: n, lastEditAt: n, deleted: false, meta: null });

/**
 * A get from an isolate whose tree is from before the splits: sent to the root object itself. A
 * root that split answers moved (or not ready), and the get goes on through the client; one that
 * answers serves it.
 */
async function staleGet(name: string, key: string): Promise<Event> {
  const start = performance.now();
  const answer = await post<{ ok?: ({ version: number } | null)[] }>("/object", { cls: "table", name, bits: "", method: "shardedCall", args: [name, "", "tableGet", [key], null, []] }).catch(() => ({}) as { ok?: undefined });
  if (answer.ok === undefined) return { ...(await perform(name, key, "get", 0)), kind: "staleGet", start };
  return { key, kind: "staleGet", start, end: performance.now(), acked: true, version: answer.ok[0]?.version ?? 0 };
}

async function perform(name: string, key: string, kind: Kind, n: number): Promise<Event> {
  if (kind === "staleGet") return staleGet(name, key);
  const args = kind === "put" ? [doc(key, n)] : kind === "patch" ? [key, { lastEditAt: n }] : kind === "apply" ? [key, "bump", { by: 1 }] : [key];
  const start = performance.now();
  const answer = await post<Settled>("/table", { cls: "table", name, op: `try:${kind}`, args }).catch((e: Error) => ({ threw: e.message }) as Settled);
  const end = performance.now();
  if (answer.threw !== undefined) return { key, kind, start, end, acked: false, version: null };
  const ok = answer.ok as { ok?: boolean; version?: number } | null;
  if (kind === "get") return { key, kind, start, end, acked: true, version: ok === null ? 0 : (ok.version ?? 0) };
  // A refused conditional write (or a patch of an absent row) returns the row's current version.
  return { key, kind, start, end, acked: ok?.ok !== false, version: ok?.version ?? 0 };
}

/** Every way a key's history breaks: [] if it holds. */
export function check(events: readonly Event[]): string[] {
  const problems: string[] = [];
  const byKey = new Map<string, Event[]>();
  for (const e of events) byKey.set(e.key, [...(byKey.get(e.key) ?? []), e]);
  for (const [key, history] of byKey) {
    const writes = history.filter((e) => e.kind !== "get" && e.kind !== "staleGet");
    // Writes that left the row at a version: a delete returns no version of its own to order by.
    const changed = writes.filter((e) => e.kind !== "delete" && e.acked && e.version !== null && e.version > 0);
    const versions = new Set(writes.filter((e) => e.version !== null).map((e) => e.version!));
    const unknown = writes.some((e) => e.version === null);
    // Distinct versions, rising in real time.
    const written = new Map<number, Event>();
    for (const w of changed) {
      const other = written.get(w.version!);
      if (other && other !== w && other.kind !== "patch" && w.kind !== "patch") problems.push(`${key}: two writes acknowledged version ${w.version}`);
      written.set(w.version!, w);
    }
    for (const a of changed) for (const b of changed) if (a.end < b.start && a.version! >= b.version!) problems.push(`${key}: ${a.kind} v${a.version} ended before ${b.kind} v${b.version} started`);
    for (const g of history.filter((e) => (e.kind === "get" || e.kind === "staleGet") && e.acked)) {
      // Not invented: a version some call returned, or an unknown write's.
      if (g.version! > 0 && !versions.has(g.version!) && !unknown) problems.push(`${key}: get returned v${g.version}, which no write returned`);
      // Not stale: at least every write acknowledged before it started.
      for (const w of changed.filter((w) => w.end < g.start)) {
        if (g.version! >= w.version!) continue;
        // Absent: a delete may have come after w (it started after w did) and no write is known after it.
        const deletedSince = (d: Event) => d.kind === "delete" && d.start < g.end && d.end > w.start && !changed.some((x) => x.start > d.end && x.end < g.start);
        if (g.version === 0 && writes.some(deletedSince)) continue;
        problems.push(`${key}: stale get: v${g.version} after ${w.kind} v${w.version} was acknowledged`);
      }
    }
  }
  return problems;
}

test("versions of each key rise in real time and no read is stale, through splits", async () => {
  const name = `history-${Date.now().toString(36)}`;
  const rand = random(seed);
  const hot = Array.from({ length: 8 }, (_, i) => `hot-${i}`);
  const cold = Array.from({ length: 48 }, (_, i) => `cold-${i}`);
  const kinds: Kind[] = ["put", "put", "patch", "delete", "apply", "get", "get", "staleGet"];
  const plans = Array.from({ length: 8 }, () =>
    Array.from({ length: 50 }, () => ({ key: rand() < 0.7 ? hot[Math.floor(rand() * hot.length)]! : cold[Math.floor(rand() * cold.length)]!, kind: kinds[Math.floor(rand() * kinds.length)]!, n: Math.floor(rand() * 1e6) })),
  );
  const events: Event[] = [];
  const clients = plans.map(async (plan) => {
    for (const step of plan) events.push(await perform(name, step.key, step.kind, step.n));
  });
  const splits = (async () => {
    await Bun.sleep(300);
    await post("/split", { cls: "table", name, bits: "" });
    await Bun.sleep(300);
    await post("/split", { cls: "table", name, bits: "0" });
  })();
  await Promise.all([...clients, splits]);

  const counters = (await post<{ counters: Record<string, number> }>("/describe", { cls: "table", name, bits: "" })).counters;
  const sure = events.filter((e) => e.version !== null).length;
  console.log(`seed ${seed}: ${events.length} calls, ${sure} sure, ${events.length - sure} unknown; root moved ${counters.moved}, notReady ${counters.notReady}`);
  expect(check(events)).toEqual([]);
  // The race happened: calls met the splits.
  expect((counters.moved ?? 0) + (counters.notReady ?? 0) + (counters.stale ?? 0)).toBeGreaterThan(0);
}, 180_000);

test("the checker finds a stale read and a version that goes back", () => {
  const e = (kind: Kind, start: number, end: number, version: number | null): Event => ({ key: "k", kind, start, end, acked: version !== null, version });
  expect(check([e("put", 0, 1, 1), e("put", 2, 3, 2), e("get", 4, 5, 1)])).toEqual(["k: stale get: v1 after put v2 was acknowledged"]);
  expect(check([e("put", 0, 1, 2), e("put", 2, 3, 1)])).toEqual(["k: put v2 ended before put v1 started"]);
  expect(check([e("put", 0, 1, 1), e("get", 2, 3, 7)])).toEqual(["k: get returned v7, which no write returned"]);
  expect(check([e("put", 0, 1, 1), e("delete", 2, 5, null), e("get", 3, 4, 0)])).toEqual([]);
});
