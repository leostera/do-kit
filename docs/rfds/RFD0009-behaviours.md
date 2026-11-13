# RFD0009 - Behaviours, and a Client for any object

- Feature Name: `behaviours`
- Status: Draft (built)
- Author: `leostera`
- Start Date: `2026-11-12`
- Updated: `2026-11-13`
- Discussion PR: none
- Tracking Issue: none

## Summary

Two layers that sit beside the sharding layers rather than on them:

- **`Client`** (`@leostera/do-kit/client`) is the caller's side of a call to any Durable Object:
  a deadline, retries with backoff on transient failures only, cancellation, metadata,
  interceptors around every attempt, and errors with a status code. Options are set on a client
  and narrowed per call with `.with({ ... })`.
- **`DurableStateMachine`** (`@leostera/do-kit/behaviours`) runs an [XState](https://stately.ai/docs/xstate)
  machine as a Durable Object. XState decides each transition with its pure `transition()`, which
  returns the next snapshot and the actions to run without running them; the base class runs one
  event at a time and commits the snapshot, the machine's SQL, its timers (XState's delayed
  transitions), its messages to other objects and its answer in one transaction. The class is also its
  own client: `Class.client(ns, name, options)` runs the class's public methods in the caller, and
  sends only the messages they make.

## Motivation

A Durable Object is one thread with storage, and the obvious way to write one is a class whose
async methods read, await, and write. That way has the same four failures in every app:

1. **Interleaving.** An `await` on anything but storage lets other calls run in the middle of a
   method. Code that read a value, awaited a `fetch`, and wrote back has lost updates.
2. **Half-done failures.** A method that throws after some writes leaves them. The object restarts
   into that state, every time: a restart doesn't clear storage the way restarting a process
   clears memory.
3. **Work lost between a write and its consequence.** A write followed by a call to another
   object, or a timer, loses the second half if the object resets between them.
4. **Timeouts and time-based logic.** An object has one alarm. "Expire this hold in 15 minutes",
   "retry this webhook", "close the session if idle" each want their own, so every app writes an
   alarm multiplexer, and a status column with an if-ladder in every method.

And the caller side has its own: a stub call waits as long as the object takes, fails with
whatever the runtime threw, and leaves deadlines, retries, logging and tracing to each call site.
A retry of a call that may have run is unsafe unless the object can recognize it.

None of this needs sharding, so it belongs beside the sharding layers, usable by any object.

## Goals

- **One event at a time** per object, with no lock and no `blockConcurrencyWhile`.
- **All or nothing per event:** a transition's snapshot, its own SQL writes, its timers, its
  outbox messages and its answer commit together, or none of them do.
- **Durable timers and outbox**, multiplexed over the one alarm.
- **The class is the client:** typed client methods, written once, run in the caller.
- **Calls with a deadline, retries that are safe, typed errors**, from any caller to any object.
- **Overhead a caller can't see:** microseconds inside the object against calls of milliseconds;
  nothing is paid for an option that isn't used.

## Non-goals

- **Supervision across objects** (restart a child, escalate to a parent). The runtime already
  restarts every object on its next event; what would remain is health checking, which needs
  failure notifications between objects that the platform doesn't give. Future work.
- **Async actions, and XState's in-memory actors.** Actions run synchronously inside the
  transaction; one that awaited would interleave. Invoked actors, spawned children, `sendTo` and
  `emit` live in XState's in-memory system, which an evicted object loses, so they are refused at
  the transition that asks for them. I/O is a message out and an event back.
- **Answers that survive the object resetting.** A deferred answer reaches a caller still waiting
  in the same instance of the object; a caller of an instance that reset gets an error.
- **An event manager** (one object fanning events out to handlers added at run time). Handlers in
  one object share its isolate and its transaction, so they isolate exceptions and nothing else; a
  state machine that keeps a list is the same thing, and fan-out across objects is `send` effects
  to each. Prototyped, then dropped.
- **A server of requests** (calls, casts and timer messages on a state, with no states to move
  between). Prototyped as `StatefulServer`, then dropped: over a plain Durable Object it adds a
  message protocol and loses native RPC's types, while what it gave (one event at a time, rollback,
  durable timers, an outbox, idempotency) a one-state `DurableStateMachine` gives too. An object
  with nothing to time out or postpone is a plain Durable Object called through `Client`.
- **Cancelling work on the object** when a caller gives up. The deadline travels with the call, and
  the object drops calls that arrive after it; one already running finishes.

## Guide-level explanation

### Client

```ts
import { Client } from "@leostera/do-kit/client";

const counter = new Client(env.COUNTERS, "visits", { timeoutMs: 500 });
await counter.invoke("increment", 1);

// Narrow per call: a shorter deadline, retries, an interceptor.
await counter.with({ timeoutMs: 50, retry: { maxAttempts: 3 } }).invoke("read");

// Method by method:
const remote = Client.of(env.COUNTERS, "visits");
await remote.with({ timeoutMs: 50 }).increment(1);
```

A failed call throws a `CallError` with a `code`:

| Code | Meaning | Retried? |
|---|---|---|
| `DEADLINE_EXCEEDED` | no answer by the deadline; the work may still have happened | no |
| `CANCELLED` | the caller's signal aborted | no |
| `UNAVAILABLE` | the object restarted or moved, or the connection dropped | yes, if `retry` is set |
| `RESOURCE_EXHAUSTED` | the object is overloaded | never: retrying adds load |
| `INVALID_ARGUMENT`, `FAILED_PRECONDITION`, `NOT_FOUND` | the object's code refused the call | no |
| `ABORTED`, `INTERNAL`, `UNKNOWN` | the object reset, has a bug, or threw something unlabelled | no |

### DurableStateMachine

```ts
import { assign, setup } from "xstate";
import { DurableStateMachine, durableActions, reply, sendToObject } from "@leostera/do-kit/behaviours";

const door = setup({
  types: { context: {} as { code: string; entered: string }, events: {} as DoorEvent },
  actions: { ...durableActions },
  guards: { opens: ({ context, event }) => (context.entered + event.digit).slice(-4) === context.code },
}).createMachine({
  context: { code: "1234", entered: "" },
  initial: "locked",
  states: {
    locked: {
      on: {
        press: [
          { guard: "opens", target: "open", actions: [assign({ entered: "" }), sendToObject("AUDIT", "doors", () => ({ type: "opened" }))] },
          { actions: assign({ entered: ({ context, event }) => (context.entered + event.digit).slice(-4) }) },
        ],
        status: { actions: reply(() => "locked") },
      },
    },
    // Open for 10 s: a timer on the alarm, cancelled if the state changes first.
    open: { after: { 10_000: "locked" }, on: { lock: "locked", status: { actions: reply(() => "open") } } },
  },
});

export class Door extends DurableStateMachine<Env, typeof door> {
  protected readonly machine = door;
  // Client API: public, and run in the caller.
  press(digit: string) { return this.cast({ type: "press", digit }); }
  status() { return this.call({ type: "status" }) as Promise<"locked" | "open">; }
}

const front = Door.client(env.DOORS, "front", { timeoutMs: 200 });
await front.press("1");   // `press` runs here; one RPC carries { type: "press", digit: "1" }
```

The machine is plain XState, designed and tested as XState is. What the object adds:

- `reply(fn)` answers the event's caller; `replyTo(to, fn)` answers an earlier one, kept from
  `callOf(event).from`, which also carries the caller's metadata.
- `sendToObject(binding, name, fn)` sends an event to another object through the outbox.
- `after` transitions are timers on the alarm; leaving the state cancels them.
- An event the current state takes no transition for waits, and runs again after the next state
  change, oldest first (`postponeUnhandled`, on by default); `postpone()` does the same from a
  guarded transition. A postponed call keeps its caller waiting, and is answered when the state
  lets it run.

## Reference-level explanation

### Architecture and boundaries

- `src/client`: `Client`, `CallError`, options and interceptors. No runtime imports: it works in
  any Worker or object, against any class.
- `src/behaviours/behaviour.ts`: the shared base (`Behaviour`): the `receive` RPC method, the event
  loop, the transaction, timers, outbox, kept answers, postponed events, and `Class.client`.
- `durable-state-machine.ts`: runs XState's `transition()` as the base's one internal
  `behaviourStep(event, state) → Step`, and turns its actions into the base's effects:
  `xstate.raise` with a `delay` becomes a timer named by its id, `xstate.cancel` cancels it, and
  the durable actions become answers, outbox rows and postponed events. Other actions run inside
  the transaction. The base is kept separate so the event loop doesn't depend on XState.

### Data model and interfaces

Every client call is one RPC, `receive(envelope)`, where an envelope is
`{ kind: "call" | "cast", message, deadline?, key?, metadata? }`, and the answer is an `Outcome`,
`{ ok: true, value } | { ok: false, code, message }`. Failures are values so their codes survive
RPC.

Tables, created on first use: `behaviour_state` (one row: version and XState's persisted snapshot
as JSON),
`behaviour_timers` (name, due, message), `behaviour_outbox`, `behaviour_replies` (answers kept by
idempotency key, pruned after `idempotencyTtlMs`, 10 minutes by default) and
`behaviour_postponed`.

The snapshot is written only when XState returns a new one: a transition that only runs actions
(answers, sends, SQL) keeps it. Actions may write their own tables through `this.sql`, given to a
machine built per object (`machine = build(() => this.sql)`): they run inside the event's
transaction. A new object's initial state is committed with the timers entering it sets.

### Lifecycle and failure semantics

One event:

1. `receive` refuses an envelope whose deadline has passed, unhandled (`DEADLINE_EXCEEDED`).
2. With a key: a kept answer is returned as is; a call with the same key still running is joined.
3. In one `transactionSync`: XState's transition and the actions that run here, then the
   snapshot and the effects (timers, outbox rows, kept answers) are written. If anything throws,
   the transaction rolls back, the in-memory snapshot is reloaded from storage, and the caller gets
   the error's code (`INTERNAL` for anything not a `CallError`).
4. After the commit: deferred answers are delivered, the alarm is set for the earliest timer, and
   the outbox drains in the background (`waitUntil`). Output gates hold the answer until the commit
   is durable.
5. If a state machine changed state, postponed events run again, each in its own transaction: one
   that throws is dropped and fails its caller, without undoing the others.

Timers fire from the alarm as the delayed event XState asked for. A timer whose transition throws
is dropped, not retried forever. The outbox sends each message as a cast with the key
`<object id>/<row id>`, so a message sent twice (the object reset after sending, before deleting
the row) is handled once by its receiver. A failed send is retried from the alarm after a second.

A deferred answer (a call no action answered, or a postponed call) is a resolver held in memory: if the object
resets first, the caller's RPC fails. A `From` kept in state across a restart names an earlier
instance and matches no new caller.

The client: a call without options is the stub call plus an error mapping. With a deadline, the
wait is bounded; with `retry`, an `UNAVAILABLE` failure is retried with full-jitter backoff
(50 ms doubling, capped at 2 s) while the deadline allows, on a new stub. A behaviour client with
retries puts an idempotency key in the envelope, the same for every attempt, so a retry of a call
that ran gets its first answer.

### Invariants

- At most one transition runs at a time per object, and it runs inside a transaction.
- A transition's snapshot, SQL writes, timers, outbox rows and kept answer commit together or not
  at all.
- An answer leaves the object only after its event committed (output gates).
- A message in the outbox is delivered at least once and handled at most once per receiver key.
- A call with an idempotency key runs at most once within `idempotencyTtlMs`.
- A raw stub can't run a callback: `migrate`, on the prototype, is wrapped to refuse unless an
  event is running. (RPC reaches every prototype method, `protected` or not; instance fields, such
  as the `machine`, are never exposed.)

### Compatibility and migration

New layers; nothing else changes. A behaviour's stored state carries `version`: bump it and
implement `migrate(fromVersion, stored)` when the state's shape changes. The base class owns
`alarm()`, as `ShardedObject` does; a subclass that defines it fails at construction.

### Security, privacy, and observability

Metadata travels in the envelope and reaches actions through `callOf(event)`; it is not
authenticated by the layer.
Interceptors are the hook for tracing and logs. Outbox failures and dropped timers are logged with
`console.error`.

### Rollout and validation

- `test/behaviours.test.ts` runs XState machines on objects under cf dev (or deployed): calls,
  casts, client methods running in the caller, metadata, restarts, rollback (rows and context),
  deferred answers, deadlines on both sides, idempotency (repeated and concurrent), a caller of an
  object that reset, raw-stub access to callbacks, delayed transitions across restarts and their
  cancellation, postponed events and calls, and an outbox message from one machine reaching
  another.
- `test/seats.test.ts` runs a seat hold: a delayed transition fired from the alarm after a
  restart, its cancellation, an event waiting for a state that takes it, an action that throws,
  and an outbox message.
- `test/client.test.ts` covers the Client's retries, overload, stub reuse, deadlines, interceptors and
  option merging against a scripted namespace.

### Overhead, measured

On the Cloudflare edge, a `DurableStateMachine` costs what plain methods doing the same SQL on the
same object cost, at every load measured: the behaviour adds about 1 µs of work per event, and its
client under 1 µs per call, against calls of 5 to 40 ms. An idempotency key is the exception: it
writes one more row, so a keyed read waits for a durable commit. The tables below are printed by
`bun bench/behaviours-tables.ts`.

Inside one object, 20,000 one-key 1 KiB puts or counter increments per call, 11 rounds
(`bench/results/behaviours-probe-edge.json`, Worker version `4b985fbf`):

| Inside one object, per event | best | median |
|---|---|---|
| put, plain transaction | 22.1 µs | 23.3 µs |
| put, behaviour event | 22.4 µs | 23.8 µs |
| put, envelope with a deadline | 22.4 µs | 24.0 µs |
| put, envelope with an idempotency key (kept) | 35.5 µs | 42.6 µs |
| counter in SQL, plain | 10.4 µs | 11.7 µs |
| counter in the behaviour's JSON state | 14.3 µs | 14.8 µs |

The same Worker version, 200 sequential puts from the Worker per round, 11 rounds, targets
alternating within a round:

| One put end to end, one at a time | best | median |
|---|---|---|
| plain object, raw stub | 16.05 ms | 17.26 ms |
| plain object, Client | 16.39 ms | 17.00 ms |
| plain object, Client with a deadline, retries, an interceptor | 16.38 ms | 16.86 ms |
| state machine's object, raw stub, plain write | 16.25 ms | 16.77 ms |
| state machine's object, raw stub, envelope | 16.49 ms | 16.73 ms |
| DurableStateMachine client | 16.21 ms | 16.75 ms |
| DurableStateMachine client with retries (idempotency key) | 16.41 ms | 16.87 ms |

Under load, one object called by up to 256 calls in flight, through plain methods and as a state
machine, on that same object, so placement is the same and only the behaviour differs
(`bench/results/behaviours-load-edge.json`, the same Worker version; calls/s, p50 / p99):

Writes:

| Target | 1 in flight | 16 in flight | 64 in flight | 256 in flight |
|---|---|---|---|---|
| state-machine-plain | 25/s, 40 / 58 ms | 150/s, 95 / 285 ms | 511/s, 73 / 430 ms | 1,204/s, 102 / 976 ms |
| state-machine | 25/s, 41 / 55 ms | 158/s, 80 / 212 ms | 594/s, 96 / 220 ms | 1,351/s, 126 / 525 ms |
| state-machine-keyed | 25/s, 41 / 50 ms | 154/s, 95 / 214 ms | 551/s, 99 / 302 ms | 1,105/s, 177 / 776 ms |

Reads:

| Target | 1 in flight | 16 in flight | 64 in flight | 256 in flight |
|---|---|---|---|---|
| state-machine-plain | 206/s, 6 / 8 ms | 768/s, 6 / 161 ms | 1,844/s, 10 / 334 ms | 2,165/s, 45 / 806 ms |
| state-machine | 243/s, 5 / 9 ms | 756/s, 6 / 156 ms | 1,929/s, 11 / 195 ms | 2,324/s, 50 / 538 ms |
| state-machine-keyed | 44/s, 24 / 31 ms | 239/s, 45 / 216 ms | 710/s, 61 / 275 ms | 905/s, 227 / 820 ms |

In the caller, with no network, Bun, 200,000 calls per round, 9 rounds
(`bench/results/behaviours-client-bun.json`):

| In the caller, per call, no network | median |
|---|---|
| raw stub | 37 ns |
| Client | 196 ns |
| Client, timeout + retries + interceptor | 641 ns |
| DurableStateMachine client | 239 ns |
| DurableStateMachine client, timeout | 880 ns |
| DurableStateMachine client, retries (idempotency key) | 457 ns |

Three things to know when reading these numbers:

- **A write is a durable write only if it changes bytes.** A put of the value a key already holds
  changes no page, commits nothing, and costs what a read does (7 ms against 15 ms, measured). So
  every benchmark write here stores a value no write stored before (`BENCH_UNIQUE_VALUES`).
- **An idempotency key turns any call into a write.** Its answer is kept in `behaviour_replies` in
  the call's transaction. A keyed write costs nothing extra (the write commits anyway), but a keyed
  read waits for a durable commit: 24 ms against 5 ms, and under half the throughput under load.
  Callers should send keys on calls that change state, and retry reads without one.
- **The context is JSON**, written whole after every event that changes it, and grows with it;
  actions can keep big data in tables.

## Drawbacks

- **Synchronous actions are a constraint.** Anything that needs I/O mid-transition becomes two
  steps: a message out, an event back.
- **XState costs CPU.** Deciding one event takes it 6 to 9 µs where a hand-written decision takes
  0.05 to 0.2 µs (see below). A caller doesn't see it; one object sustaining over about a thousand
  events a second, or handling tens of thousands in one call, does.
- **XState is a dependency**, and the layer leans on its pure `transition()` and on the shape of
  its actions (`xstate.raise` with `delay` and `id`, `xstate.cancel` with `sendId`).
- **JSON state is written whole** after every event that changes it. Fine for small state; big
  state belongs in tables.
- **Deferred answers don't survive resets.** A long wait between objects should be a message back,
  not a held call.
- **Every prototype method is RPC-reachable.** The guard covers `migrate`; a subclass's own helper
  methods are as exposed as on any Durable Object.

## Rationale and alternatives

### Proposed design

Synchronous transitions plus effects is the smallest design that gets all four motivations at
once: serial events without a lock, rollback by the transaction, effects that can't be lost, and
timers as data. XState's pure `transition()` is exactly that shape, so the transitions are
XState's and the durability is this layer's. Putting the client on the same class keeps the
protocol in one place: client methods and the machine change together.

### Why XState, and not our own handlers

The first version had its own handler API: one function per state returning `next(state, data)`,
`keep(data)` or `postpone()`, with `stateTimeout` and named timeouts. It was dropped for XState:

- XState is the richer language (nested and parallel states, guards, history, entry and exit
  actions), with an editor, a visualizer and model-based tests, and people already know it.
- The cost is CPU only. Deciding one event, with no storage or RPC, measured in Bun
  (`bench/results/xstate-step-bun.json`, at commit `b522906`, where the handlers still existed):

  | Workload | handlers | XState |
  |---|---|---|
  | key/value put, state unchanged | 51 ns | 5,940 ns |
  | counter in the state | 150 ns | 6,933 ns |
  | seat grabbed and released (state change and timer) | 169 ns | 8,802 ns |

  On the edge, inside one object, XState added 1 to 106 µs per event over the handlers, in three
  runs whose own baselines varied twofold (`bench/results/behaviours-xstate-probe-edge-{1,2,3}.json`,
  commit `8254e89`); a put end to end cost the same through both, 15 to 22 ms.
- So it matters at one object near its limit: about 1,000 events a second sustained on one object
  (where a 0.1 ms decision on a ~0.8 ms request lowers its ceiling by up to about a tenth), or
  tens of thousands of events in one call. Spreading over objects removes it.

XState's own runtime inside a Durable Object doesn't work: delayed transitions are `setTimeout`,
and invoked promises live in memory, so an evicted object forgets both; its snapshot, SQL and
messages commit separately unless every app wires them into one transaction; and it has no
answers to callers, idempotency or deadlines.

### Simpler or narrower approach

Helpers (`withTransaction`, an alarm multiplexer, a retrying fetch) fix each problem separately,
and leave the composition (a timer set in the same transaction as the state change that needs it,
an outbox row written with the write it reports) to every app. That composition is the part that
goes wrong.

### Other alternatives considered

- **Async callbacks under `blockConcurrencyWhile`.** Serial, but every await holds the object,
  and a callback can still half-write before it throws.
- **Agents SDK `Lifecycle` capabilities.** It already multiplexes the alarm and orders startup; a
  behaviour could be a capability. It would tie these layers to the `agents` package, and the
  contract here (synchronous transitions, effects after commit) is the part it leaves open.
- **Workflows** for long-running steps: durable, but per run, not per entity with state.

### Do nothing

Each app keeps writing its own status columns, alarm multiplexers, retry loops and outboxes.

## Prior art

- XState and Stately: the machines, the pure `transition()`, delayed transitions.
- Erlang/OTP's behaviours: the split between generic code and callbacks, calls, casts, deferred
  replies, state machines with state timeouts and postponed events. The failure
  model is different: a restarted process loses its state, a reset object keeps it, so here the
  transaction does what a restart does there.
- gRPC: deadlines that travel with the call, status codes, interceptors, retry policies.
- splittable-object and `ShardedObject.update`: a synchronous decide inside a transaction.

## Unresolved questions

### Before acceptance

- Should `cast` return before handling (enqueue and answer), rather than after the commit?
- Should `postponeUnhandled` stay on by default? On, a misspelled event waits forever.

### During implementation

- Bounding the in-memory postponed retry rounds, now 64, by configuration.
- Invoked promises made durable: run after the commit, with a row so they run again after a
  reset, and their `done`/`error` events fed back.

### Out of scope

- Supervision trees across objects.
- Behaviours on sharded objects.

## Future possibilities

A `Supervisor` that probes the work its children hold and resets them on repeated failure; typed
request/answer maps for `call`; model-checking the transaction-plus-outbox protocol with Feynman,
as the split protocol is.
