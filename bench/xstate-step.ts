// What deciding one event costs, DurableStateMachine's handlers against XState's transition(), on
// the bench Worker's workloads, with no storage and no RPC (Bun):
//   bun bench/xstate-step.ts            # BENCH_RESULTS_NAME=xstate-step-<what> to keep it
//
// Each case is the work a behaviour step does before it writes: the decision, and the state's
// JSON when the event changes it (a step that keeps the state writes nothing). Cases alternate
// within a round, so drift touches them all alike.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";
import { assign, initialTransition, setup, transition, type AnyMachineSnapshot } from "xstate";

Bun.plugin({
  name: "cloudflare-workers-stand-in",
  setup(build) {
    build.module("cloudflare:workers", () => ({ loader: "object", exports: { DurableObject: class {} } }));
  },
});
const { keep, next, stateTimeout } = await import("../src/behaviours/index.ts");
const { durableActions, reply } = await import("../src/xstate/index.ts");

type Seat = { readonly holder: string | null };
type SeatEvent = { readonly type: "grab"; readonly who: string } | { readonly type: "release" };
const VALUE = "x".repeat(1024);

// ---- DurableStateMachine: the bench's handlers ----

const seatStates = {
  available: (event: SeatEvent, data: Seat) => (event.type === "grab" ? next("held", { holder: event.who }, { reply: true, actions: [stateTimeout(60_000, { type: "expire" })] }) : keep(data, { reply: false })),
  held: (event: SeatEvent, data: Seat) => (event.type === "release" ? next("available", { holder: null }, { reply: true }) : keep(data, { reply: false })),
};
const kvState = (event: { op: string }, data: { n: number }) => (event.op === "inc" ? keep({ n: data.n + 1 }, { reply: data.n + 1 }) : keep(data, { reply: 1 }));

// ---- XState: the same machines ----

const seatMachine = setup({ types: { context: {} as Seat, events: {} as SeatEvent }, actions: durableActions }).createMachine({
  context: { holder: null },
  initial: "available",
  states: {
    available: { on: { grab: { target: "held", actions: [assign({ holder: ({ event }) => event.who }), reply(() => true)] }, release: { actions: reply(() => false) } } },
    held: { after: { 60_000: { target: "available", actions: assign({ holder: null }) } }, on: { release: { target: "available", actions: [assign({ holder: null }), reply(() => true)] }, grab: { actions: reply(() => false) } } },
  },
});
type KvEvent = { type: "put"; keys: string[]; values: string[] } | { type: "inc" };
const kvMachine = setup({ types: { context: {} as { n: number }, events: {} as KvEvent }, actions: durableActions }).createMachine({
  context: { n: 0 },
  initial: "serving",
  states: { serving: { on: { put: { actions: reply(() => 1) }, inc: { actions: [assign({ n: ({ context }) => context.n + 1 }), reply(({ context }) => context.n)] } } } },
});

let sink = 0;
const machineCase = (step: (i: number) => void) => step;

const CASES: Record<string, () => (i: number) => void> = {
  "kv put, DurableStateMachine": () => {
    const data = { n: 0 };
    return machineCase(() => {
      const t = kvState({ op: "put" }, data) as { data: { n: number } };
      sink += t.data === data ? 0 : JSON.stringify({ state: "serving", data: t.data }).length;
    });
  },
  "kv put, XState": () => {
    let s = initialTransition(kvMachine)[0] as AnyMachineSnapshot;
    return (i) => {
      const [n] = transition(kvMachine, s, { type: "put", keys: [`key-${i % 1000}`], values: [VALUE] });
      sink += n === s ? 0 : JSON.stringify(kvMachine.getPersistedSnapshot(n)).length;
      s = n;
    };
  },
  "kv inc, DurableStateMachine": () => {
    let data = { n: 0 };
    return () => {
      const t = kvState({ op: "inc" }, data) as { data: { n: number } };
      sink += JSON.stringify({ state: "serving", data: t.data }).length;
      data = t.data;
    };
  },
  "kv inc, XState": () => {
    let s = initialTransition(kvMachine)[0] as AnyMachineSnapshot;
    return () => {
      const [n] = transition(kvMachine, s, { type: "inc" });
      sink += JSON.stringify(kvMachine.getPersistedSnapshot(n)).length;
      s = n;
    };
  },
  "seat grab/release, DurableStateMachine": () => {
    let state: "available" | "held" = "available";
    let data: Seat = { holder: null };
    return (i) => {
      const t = seatStates[state](i % 2 === 0 ? { type: "grab", who: "p" } : { type: "release" }, data) as { next?: "available" | "held"; data: Seat };
      state = t.next ?? state;
      data = t.data;
      sink += JSON.stringify({ state, data }).length;
    };
  },
  "seat grab/release, XState": () => {
    let s = initialTransition(seatMachine)[0] as AnyMachineSnapshot;
    return (i) => {
      const [n] = transition(seatMachine, s, i % 2 === 0 ? { type: "grab", who: "p" } : { type: "release" });
      sink += JSON.stringify(seatMachine.getPersistedSnapshot(n)).length;
      s = n;
    };
  },
  "seat restore + grab/release, XState": () => {
    // As after a restart: the snapshot comes back from storage before the event.
    let stored: unknown = seatMachine.getPersistedSnapshot(initialTransition(seatMachine)[0]);
    return (i) => {
      const s = seatMachine.resolveState(stored as never);
      const [n] = transition(seatMachine, s, i % 2 === 0 ? { type: "grab", who: "p" } : { type: "release" });
      stored = JSON.parse(JSON.stringify(seatMachine.getPersistedSnapshot(n)));
    };
  },
};

const CALLS = Number(process.env.BENCH_CALLS ?? 100_000);
const ROUNDS = Number(process.env.BENCH_ROUNDS ?? 9);
const nanosPerEvent: Record<string, number[]> = Object.fromEntries(Object.keys(CASES).map((name) => [name, []]));
for (const [, make] of Object.entries(CASES)) {
  const step = make();
  for (let i = 0; i < 10_000; i++) step(i); // warm
}
for (let round = 0; round < ROUNDS; round++) {
  for (const [name, make] of Object.entries(CASES)) {
    const step = make();
    const before = Bun.nanoseconds();
    for (let i = 0; i < CALLS; i++) step(i);
    nanosPerEvent[name]!.push((Bun.nanoseconds() - before) / CALLS);
  }
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
for (const [name, xs] of Object.entries(nanosPerEvent)) console.log(`${name.padEnd(42)} ${Math.round(median(xs)).toString().padStart(7)} ns/event (min ${Math.round(Math.min(...xs))}, max ${Math.round(Math.max(...xs))})`);
if (sink < 0) console.log(sink);

const results = {
  schemaVersion: 1,
  kind: "xstate-step",
  generatedAt: new Date().toISOString(),
  provenance: {
    commit: (await $`git rev-parse HEAD`.quiet().text()).trim(),
    dirty: (await $`git status --porcelain -- src bench ${":!bench/results"}`.quiet().text()).trim().length > 0,
    target: `Bun ${Bun.version}, ${process.platform} ${process.arch}`,
    xstate: (await import("xstate/package.json", { with: { type: "json" } })).default.version,
  },
  config: { calls: CALLS, rounds: ROUNDS },
  notes: ["Each case: the decision for one event, plus the state's JSON when the event changes it. No storage, no RPC. Medians over rounds."],
  nanosPerEvent,
};
if (process.env.BENCH_RESULTS_NAME) {
  const dir = join(import.meta.dir, "results");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `${process.env.BENCH_RESULTS_NAME}.json`), JSON.stringify(results, null, 2) + "\n");
  console.log(`Saved bench/results/${process.env.BENCH_RESULTS_NAME}.json`);
}
