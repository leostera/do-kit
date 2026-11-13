# RFD0009 - Behaviours, and a Client for any object

- Feature Name: `behaviours`
- Status: Draft (built)
- Author: `leostera`
- Start Date: `2026-11-12`
- Updated: `2026-11-12`
- Discussion PR: none
- Tracking Issue: none

## Summary

Two layers that sit beside the sharding layers rather than on them:

- **`Client`** (`@leostera/do-kit/client`) is the caller's side of a call to any Durable Object:
  a deadline, retries with backoff on transient failures only, cancellation, metadata,
  interceptors around every attempt, and errors with a status code. Options are set on a client
  and narrowed per call with `.with({ ... })`.
- **Behaviours** (`@leostera/do-kit/behaviours`) are base classes for the shapes most objects take:
  `GenServer` (an object serving requests on its state), `GenStatem` (a state machine with durable
  timeouts and postponed events) and `GenEvent` (one stream of events fanned out to handlers added
  at run time). A subclass writes synchronous callbacks that turn an event and the state into a
  new state and a list of effects; the base class runs one event at a time and commits state,
  timers, outbox and kept answers in one transaction. The class is also its own client:
  `Class.client(ns, name, options)` runs the class's public methods in the caller, and sends only
  the messages they make.

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
- **All or nothing per event:** a callback's state change, its own SQL writes, its timers, its
  outbox messages and its answer commit together, or none of them do.
- **Durable timers and outbox**, multiplexed over the one alarm.
- **The class is the client:** typed client methods, written once, run in the caller.
- **Calls with a deadline, retries that are safe, typed errors**, from any caller to any object.
- **Minimal overhead:** a behaviour event costs about a microsecond inside the object, and a client
  call well under one; nothing is paid for an option that isn't used.

## Non-goals

- **Supervision across objects** (restart a child, escalate to a parent). The runtime already
  restarts every object on its next event; what would remain is health checking, which needs
  failure notifications between objects that the platform doesn't give. Future work.
- **Async callbacks.** A callback that awaits would interleave. I/O is an effect, and its result
  comes back as an event.
- **Answers that survive the object resetting.** A deferred answer reaches a caller still waiting
  in the same instance of the object; a caller of an instance that reset gets an error.
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

### GenServer

```ts
export class Channels extends GenServer<Env, Pool, PoolCall, number | null, PoolCast> {
  // Client API: public, and run in the caller.
  alloc() { return this.call({ op: "alloc" }); }
  free(channel: number) {
    if (!Number.isInteger(channel)) return Promise.reject(new CallError("INVALID_ARGUMENT", "not a channel"));
    return this.cast({ op: "free", channel });
  }

  // Callbacks: synchronous transitions.
  protected init() { return { free: [0, 1, 2], waiting: [] }; }
  protected handleCall(request: PoolCall, state: Pool, from: From) {
    const [channel, ...free] = state.free;
    if (channel !== undefined) return { reply: channel, state: { ...state, free } };
    return { noreply: true, state: { ...state, waiting: [...state.waiting, from] } }; // answer later
  }
  protected override handleCast(message: PoolCast, state: Pool) {
    const [waiter, ...waiting] = state.waiting;
    if (waiter) return { state: { ...state, waiting }, effects: [replyTo(waiter, message.channel)] };
    return { state: { ...state, free: [...state.free, message.channel] } };
  }
}

const pool = Channels.client(env.CHANNELS, "pool", { timeoutMs: 200 });
await pool.alloc();       // `alloc` runs here; one RPC carries { op: "alloc" }
await pool.free(-1);      // refused here: no RPC at all
```

### GenStatem

```ts
export class Door extends GenStatem<Env, "locked" | "open", Data, DoorEvent> {
  press(digit: string) { return this.cast({ type: "press", digit }); }

  protected init() { return { state: "locked", data: { code: "1234", entered: "" } } as const; }
  protected readonly states = {
    locked: (event, data) => {
      if (event.type !== "press") return keep(data);
      const entered = (data.entered + event.digit).slice(-4);
      if (entered !== data.code) return keep({ ...data, entered });
      // Open for 10 s; the timeout is cancelled if the state changes first.
      return next("open", { ...data, entered: "" }, { actions: [stateTimeout(10_000, { type: "lock" })] });
    },
    open: (event, data) => (event.type === "lock" ? next("locked", data) : postpone()),
  };
}
```

`postpone()` keeps the event and runs it again after the next state change, oldest first: presses
while the door is open wait until it locks. A postponed call keeps its caller waiting, and is
answered when the state lets it run.

### GenEvent

```ts
export class Audit extends GenEvent<Env, AuditEvent> {
  protected readonly handlerTypes = { counter, recent };
}
const audit = Audit.client(env.AUDIT, "billing");
await audit.addHandler("totals", "counter");
await audit.notify({ type: "charge", cents: 500 });
await audit.callHandler("totals", "count");
```

Another object sends to it with an effect: `send("AUDIT", "billing", GenEvent.notification(event))`.

## Reference-level explanation

### Architecture and boundaries

- `src/client`: `Client`, `CallError`, options and interceptors. No runtime imports: it works in
  any Worker or object, against any class.
- `src/behaviours/behaviour.ts`: the shared base (`Behaviour`): the `receive` RPC method, the event
  loop, the transaction, timers, outbox, kept answers, postponed events, and `Class.client`.
- `gen-server.ts`, `gen-statem.ts`, `gen-event.ts`: each maps its callbacks onto one internal
  `behaviourStep(event, state) → Step`.

### Data model and interfaces

Every client call is one RPC, `receive(envelope)`, where an envelope is
`{ kind: "call" | "cast", message, deadline?, key?, metadata? }`, and the answer is an `Outcome`,
`{ ok: true, value } | { ok: false, code, message }`. Failures are values so their codes survive
RPC.

Tables, created on first use: `behaviour_state` (one row: version and state as JSON),
`behaviour_timers` (name, due, message), `behaviour_outbox`, `behaviour_replies` (answers kept by
idempotency key, pruned after `idempotencyTtlMs`, 10 minutes by default) and
`behaviour_postponed`.

State is written only when a callback returns a new state object. Callbacks may also write their
own tables through `this.sql`: they run inside the event's transaction.

### Lifecycle and failure semantics

One event:

1. `receive` refuses an envelope whose deadline has passed, unhandled (`DEADLINE_EXCEEDED`).
2. With a key: a kept answer is returned as is; a call with the same key still running is joined.
3. In one `transactionSync`: the callback runs, then its state, effects (timers, outbox rows, kept
   answers) are written. If the callback throws, the transaction rolls back, the in-memory state is
   reloaded from storage (the callback may have changed it in place), and the caller gets the
   error's code (`INTERNAL` for anything not a `CallError`).
4. After the commit: deferred answers are delivered, the alarm is set for the earliest timer, and
   the outbox drains in the background (`waitUntil`). Output gates hold the answer until the commit
   is durable.
5. If a state machine changed state, postponed events run again, each in its own transaction: one
   that throws is dropped and fails its caller, without undoing the others.

Timers fire from the alarm as `info` events (timeouts, for a state machine). A timer whose callback
throws is dropped, not retried forever. The outbox sends each message as a cast with the key
`<object id>/<row id>`, so a message sent twice (the object reset after sending, before deleting
the row) is handled once by its receiver. A failed send is retried from the alarm after a second.

A deferred answer (`noreply`, or a postponed call) is a resolver held in memory: if the object
resets first, the caller's RPC fails. A `From` kept in state across a restart names an earlier
instance and matches no new caller.

The client: a call without options is the stub call plus an error mapping. With a deadline, the
wait is bounded; with `retry`, an `UNAVAILABLE` failure is retried with full-jitter backoff
(50 ms doubling, capped at 2 s) while the deadline allows, on a new stub. A behaviour client with
retries puts an idempotency key in the envelope, the same for every attempt, so a retry of a call
that ran gets its first answer.

### Invariants

- At most one callback runs at a time per object, and it runs inside a transaction.
- A callback's state, SQL writes, timers, outbox rows and kept answer commit together or not at all.
- An answer leaves the object only after its event committed (output gates).
- A message in the outbox is delivered at least once and handled at most once per receiver key.
- A call with an idempotency key runs at most once within `idempotencyTtlMs`.
- A raw stub can't run a callback: callbacks on the prototype are wrapped to refuse unless an event
  is running. (RPC reaches every prototype method, `protected` or not; instance fields, such as a
  state machine's `states`, are never exposed.)

### Compatibility and migration

New layers; nothing else changes. A behaviour's stored state carries `version`: bump it and
implement `migrate(fromVersion, stored)` when the state's shape changes. The base class owns
`alarm()`, as `ShardedObject` does; a subclass that defines it fails at construction.

### Security, privacy, and observability

Metadata travels in the envelope and reaches callbacks; it is not authenticated by the layer.
Interceptors are the hook for tracing and logs. Outbox failures and dropped timers are logged with
`console.error`.

### Rollout and validation

- `test/behaviours.test.ts` runs every behaviour on objects under cf dev (or deployed): calls,
  casts, client methods running in the caller, metadata, restarts, rollback (rows, state, a state
  changed in place), deferred answers, deadlines on both sides, idempotency (repeated and
  concurrent), a caller of an object that reset, raw-stub access to callbacks, state timeouts
  across restarts and their cancellation, postponed events and calls, handler removal, and an
  outbox message from a state machine reaching an event manager.
- `test/client.test.ts` covers the Client's retries, overload, stub reuse, deadlines, interceptors and
  option merging against a scripted namespace.

### Overhead, measured

On the Cloudflare edge, a GenServer costs what a plain Durable Object doing the same SQL costs:
the behaviour adds about 1 µs of work per event, and its client well under 1 µs per call, against
calls of 10 to 40 ms. An idempotency key is the exception: it writes one more row, so a keyed
read pays for a durable write. The tables below come from `bun bench/behaviours-tables.ts`.

Inside one object, 20,000 one-key 1 KiB puts or counter increments per call, 11 rounds
(`bench/results/behaviours-probe-edge.json`, Worker version `b97352f0`):

| Inside one object, per event | best | median |
|---|---|---|
| put, plain transaction | 19.5 µs | 21.5 µs |
| put, behaviour event | 19.8 µs | 22.1 µs |
| put, envelope with a deadline | 20.8 µs | 24.2 µs |
| put, envelope with an idempotency key (kept) | 38.0 µs | 40.3 µs |
| counter in SQL, plain | 9.7 µs | 10.7 µs |
| counter in the behaviour's JSON state | 11.4 µs | 13.0 µs |

The same Worker version, 200 sequential puts from the Worker per round, 11 rounds, targets
alternating within a round:

| One put end to end, one at a time | best | median |
|---|---|---|
| plain object, raw stub | 14.05 ms | 14.34 ms |
| plain object, Client | 13.71 ms | 14.21 ms |
| plain object, Client with a deadline, retries, an interceptor | 13.90 ms | 14.11 ms |
| GenServer's object, raw stub, plain write | 16.50 ms | 16.89 ms |
| GenServer's object, raw stub, envelope | 16.41 ms | 16.64 ms |
| GenServer client | 16.38 ms | 16.80 ms |
| GenServer client with retries (idempotency key) | 16.50 ms | 16.95 ms |
| GenStatem client | 13.96 ms | 14.12 ms |

The GenServer's object is 2.5 ms slower than the plain one whether it is called as a GenServer or
through plain methods: that is where the object was placed, not the behaviour. Under load, one
object called by up to 256 calls in flight, plain methods and the GenServer on that same object
(`bench/results/behaviours-load-edge-shared.json`, Worker version `df25b5e3`; calls/s, p50 / p99):

| Writes | 1 in flight | 16 in flight | 64 in flight | 256 in flight |
|---|---|---|---|---|
| plain methods | 34/s, 29 / 51 ms | 233/s, 36 / 194 ms | 669/s, 48 / 393 ms | 903/s, 168 / 932 ms |
| GenServer | 41/s, 24 / 43 ms | 231/s, 44 / 194 ms | 787/s, 55 / 206 ms | 924/s, 239 / 590 ms |
| GenServer, idempotency key | 41/s, 25 / 40 ms | 238/s, 34 / 190 ms | 615/s, 93 / 234 ms | 774/s, 245 / 857 ms |

| Reads | 1 in flight | 16 in flight | 64 in flight | 256 in flight |
|---|---|---|---|---|
| plain methods | 104/s, 10 / 20 ms | 278/s, 40 / 194 ms | 655/s, 59 / 405 ms | 869/s, 195 / 925 ms |
| GenServer | 100/s, 10 / 19 ms | 289/s, 39 / 186 ms | 722/s, 70 / 231 ms | 958/s, 236 / 648 ms |
| GenServer, idempotency key | 44/s, 23 / 47 ms | 173/s, 75 / 255 ms | 333/s, 157 / 1238 ms | 355/s, 755 / 1366 ms |

`bench/results/behaviours-load-edge.json` ramps every target, each on its own object, to 1,024
calls in flight. Every target levels off at 600 to 1,400 writes/s, and from 1,024 calls in flight
some calls fail on all of them, plain objects included. The differences between targets there
follow where each object was placed, so it shows one object's ceiling, not the behaviours' cost.

In the caller, with no network, Bun, 200,000 calls per round, 9 rounds
(`bench/results/behaviours-client-bun.json`):

| In the caller, per call | median |
|---|---|
| raw stub | 26 ns |
| Client | 158 ns |
| Client, timeout + retries + interceptor | 493 ns |
| GenServer client | 222 ns |
| GenServer client, timeout | 544 ns |
| GenServer client, retries (idempotency key) | 322 ns |

Three things to know when reading these numbers:

- **A write is a durable write only if it changes bytes.** A put of the value a key already holds
  changes no page, commits nothing, and costs what a read does (7 ms against 15 ms, measured). So
  every benchmark write here stores a value no write stored before (`BENCH_UNIQUE_VALUES`).
- **An idempotency key turns any call into a write.** Its answer is kept in `behaviour_replies` in
  the call's transaction. A keyed write costs nothing extra (the write commits anyway), but a keyed
  read waits for a durable commit: 23 ms against 10 ms, and a third of the throughput under load.
  Callers should send keys on calls that change state, and retry reads without one.
- **JSON state** costs about 2 µs over a SQL counter for a small state, and grows with the state;
  callbacks can keep big state in tables.

## Drawbacks

- **Synchronous callbacks are a constraint.** Anything that needs I/O mid-transition becomes two
  steps: an effect out, an event back.
- **JSON state is written whole** after every event that changes it. Fine for small state; big
  state belongs in tables.
- **Deferred answers don't survive resets.** A long wait between objects should be a message back,
  not a held call.
- **Every prototype method is RPC-reachable.** The callback guard covers the base classes'
  callbacks; a subclass's own helper methods are as exposed as on any Durable Object.

## Rationale and alternatives

### Proposed design

Synchronous transitions plus effects is the smallest design that gets all four motivations at
once: serial events without a lock, rollback by the transaction, effects that can't be lost, and
timers as data. Putting the client on the same class keeps the protocol in one place: client
methods and callbacks change together, and the types check them together.

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

- Erlang/OTP's behaviours: the split between generic code and callbacks, calls, casts, deferred
  replies, state machines with state timeouts and postponed events, event managers. The failure
  model is different: a restarted process loses its state, a reset object keeps it, so here the
  transaction does what a restart does there.
- gRPC: deadlines that travel with the call, status codes, interceptors, retry policies.
- splittable-object and `ShardedObject.update`: a synchronous decide inside a transaction.

## Unresolved questions

### Before acceptance

- Run the edge benchmark, and decide whether any measured overhead needs work.
- Should `cast` return before handling (enqueue and answer), rather than after the commit?

### During implementation

- An `onEnter` callback for state machines.
- Bounding the in-memory postponed retry rounds, now 64, by configuration.

### Out of scope

- Supervision trees across objects.
- Behaviours on sharded objects.

## Future possibilities

A `Supervisor` that probes the work its children hold and resets them on repeated failure; typed
request/answer maps for `call`; model-checking the transaction-plus-outbox protocol with Feynman,
as the split protocol is.
