# RFD0013 - Overload refusal: fail fast, typed, at the caller

- Feature Name: `overload-refusal`
- Status: Draft (the base class and the answer built)
- Author: `leostera`
- Start Date: `2026-11-15`
- Updated: `2026-11-15`
- Discussion PR: none
- Tracking Issue: none

## Summary

An overloaded object refuses calls at once, before it runs them, and its caller gets an
`Overloaded` error that says what is overloaded and when trying again may help. `Overloaded` is a
base class: every do-kit layer has its subclasses (`ShardOverloaded`, `KeyOverloaded`,
`RouteOverloaded`, `IndexPartitionOverloaded`, `CoordinatorOverloaded`), and an app adds its own
by subclassing it, throwing it in an object and registering it on its client. A refused call was
not applied. Nothing in do-kit queues or retries a refused call: the caller decides. The client
remembers a refusal until its retry-after, and refuses calls it would cover without sending them.
The model is [specs/overload-refusal.ts](../../specs/overload-refusal.ts).

## Motivation

Today a shard takes every call until the runtime sheds them, and the caller learns of the overload
late and vaguely. Measured on Cloudflare's network (docs/performance.md):

| Load | What the caller saw |
|---|---|
| Table writes, 1 shard, 1,024 in flight | 1,604/s with a median of 515 ms, and 9,089 failed |
| One object, 2,048 in flight | a median of 860 ms for writes, 895 ms for reads, before any refusal |
| Ten hot config objects, 2,048 in flight | 41,713 failed in 10 s on one object |
| Indexed table, 1 + 1 shards, 1,024 in flight | 5,130 updates failed |

Three things are wrong with that:

- **Slow failure.** A call the runtime sheds has waited in its queue first, hundreds of ms. The
  caller pays the wait and gets the error anyway, and every call queued behind it waits too.
- **Untyped failure.** The error is the runtime's message ("Durable Object is overloaded"), which
  the client classifies, but cannot say what to do: wait for a split, or stop sending one key.
- **No help for the hot spots that splits cannot fix.** One key, one route (RFD0006), or one
  partition of a global index (RFD0003) is one object however far the tree splits. The split
  policy does not know this, and splits a shard that is hot because of one key: in the hot-config
  benchmark it went to 32 shards for 10 keys and did worse than presplitting.

The layers above make it worse. An `IndexedTable` write makes three calls in a row (reserve, write
the row, deliver the entries), so one hot index partition fails or slows every write that touches
it, and a work graph's single coordinator serves every lease of its run.

## Goals

- An overloaded object answers in about a millisecond, without touching storage, rather than
  after its queue.
- The caller gets a typed error: what is overloaded (a shard, a key, a route, an index
  partition, a coordinator, or something the app names), and a retry-after.
- A refused call was not applied: `Overloaded` is a definite "not applied", like `DeadlineExpired`.
- One hot key does not take down the other keys of its shard.
- One error type across every layer, that apps extend with their own kinds.
- Splits see refusals as load, and do not split for load a split cannot move.

## Non-goals

- Queueing calls for the caller, or retrying them. A caller that wants to wait can; do-kit does
  not decide for it.
- Making a hot key faster. Reads of one key are spread by leased reads (RFD0004); hot writes of
  one row are merged by `apply` (RFD0010). This RFD makes their overload clear, not cheaper.
- Rate limits or quotas per tenant. An app can build one on the base class (a kind of its own).
- Fairness between callers.

## Guide-level explanation

```ts
import { KeyOverloaded, Overloaded, ShardOverloaded } from "@leostera/do-kit";

try {
  await users.put(row);
} catch (e) {
  if (e instanceof KeyOverloaded) return busy(`${e.key} is too hot to write`, e.retryAfterMs); // spread it, or apply()
  if (e instanceof ShardOverloaded) return busy("try again", e.retryAfterMs); // a split is under way
  if (e instanceof Overloaded) return busy(e.message, e.retryAfterMs); // any other kind
  throw e;
}
```

An app names its own overloads the same way. The object throws it, before it writes anything, and
the client turns the answer back into the class:

```ts
class TenantOverQuota extends Overloaded {
  static override readonly kind = "tenantQuota";
  get tenant(): string { return String(this.detail.tenant); }
}

// In the object: before any write.
if (this.spentToday(tenant) > QUOTA) throw new TenantOverQuota(`tenant ${tenant} is over quota`, { retryAfterMs: msToMidnight(), scope: { key }, detail: { tenant } });

// In the Worker: the client rebuilds it from the shard's answer.
const t = table(env.USERS, "users", users, { overloads: [TenantOverQuota] });
```

`scope` says what the client may refuse itself until the retry-after: this key, this route, or the
whole shard. Without one, the client remembers nothing.

### Diagram

```
caller ──call──▶ client ── remembers a refusal covering it? ──yes──▶ throws (source: "client")
                   │ no
                   ▼
                 shard ── overloaded, and covers this call? ──yes──▶ { overloaded: … } ──▶ throws (source: "object")
                   │ no                     (before the method runs)
                   ▼
                 method runs ──▶ { ok }
```

## Reference-level explanation

### Architecture and boundaries

Refusal is in `ShardedObject`: every routed call (`shardedCall`), scan page (`shardedScan`), and
atomic `prepare` (`shardedAtomic`) goes through one admission check after routing (`moved`,
ownership) and before the method. Atomic `commit`, `abort`, `resolve` and `forget` are never
refused: they finish work already admitted, and refusing them would keep holds longer. A held
write (a split's hold, an atomic hold) was admitted; its wait is not load.

The layers above get it for free and add their kinds: `IndexedTable` maps a `RouteOverloaded` from
its index shards to `IndexPartitionOverloaded`, and the work-graph coordinator, which is not a
sharded object, uses the same check for its leases.

### Data model and interfaces

```ts
/** What an object answers for an Overloaded it threw or decided on: plain data, over RPC. */
interface OverloadAnswer {
  readonly kind: string;
  readonly message: string;
  readonly retryAfterMs: number;
  readonly scope: { readonly shard: string } | { readonly key: string } | { readonly route: string } | null;
  readonly detail: Readonly<Record<string, string | number | boolean | null>>;
}

class Overloaded extends Error {
  static readonly kind: string = "overloaded";
  readonly kind: string;
  readonly retryAfterMs: number;
  readonly scope: OverloadAnswer["scope"];
  readonly detail: OverloadAnswer["detail"];
  /** "object": an object refused it; "client": this client did, remembering a refusal; "runtime": the runtime shed it. */
  readonly source: "object" | "client" | "runtime";
  constructor(message: string, options: { retryAfterMs: number; scope?: OverloadAnswer["scope"]; detail?: OverloadAnswer["detail"] });
  toAnswer(): OverloadAnswer;
}

class ShardOverloaded extends Overloaded { static kind = "shard"; readonly bits: string }
class KeyOverloaded extends Overloaded { static kind = "key"; readonly key: string }
class RouteOverloaded extends Overloaded { static kind = "route"; readonly route: string }
class IndexPartitionOverloaded extends Overloaded { static kind = "indexPartition"; readonly index: string; readonly partition: readonly unknown[] }
class CoordinatorOverloaded extends Overloaded { static kind = "coordinator"; readonly run: string }
```

- Workers RPC keeps an error's message, not its class or fields, so an object never throws
  `Overloaded` across RPC: `shardedCall` catches it and answers `{ overloaded: OverloadAnswer }`.
- The client's options take `overloads: (typeof Overloaded)[]`. Each answer becomes the class
  whose `kind` matches, do-kit's own first, then the app's; an unknown kind is a plain
  `Overloaded` with that `kind`. Kinds are names in one flat space: an app's kind may not be one of
  do-kit's (the client refuses the registration).
- The shard's split policy gets an `overload` part:
  `{ maxWaitMs: 250, intervalMs: 100, minInFlight: 8, hotKeyShare: 0.5 }` (defaults to tune on
  the network).

### Lifecycle and failure semantics

**When a shard is overloaded.** A shard knows each call's wait from the client's send to the
method's start (`sentAt`, which `describe` already reports as `waitedMs`), and how many calls are
in its handlers at once. It is overloaded, CoDel-style, when the *smallest* wait over the last
`intervalMs` was above `maxWaitMs` and at least `minInFlight` calls are in flight: a queue that
stays long, not a burst. It stops being overloaded after an interval whose smallest wait is below
`maxWaitMs`. While overloaded, it refuses calls that waited more than `maxWaitMs`; calls that did
not wait are served, so the queue drains to its target rather than to nothing.

**What it refuses.** The shard keeps the few keys (and routes, with a `routeOf`) that took most of
its calls in the window, a top-8 by count. While overloaded:

- if one key took at least `hotKeyShare` of the window's calls, it refuses only calls that include
  that key: `KeyOverloaded` (`RouteOverloaded` when the key's route is the hot one), scope that
  key or route, retry-after `windowSeconds`. Other keys are served.
- otherwise it refuses any call: `ShardOverloaded`, scope the shard, retry-after the time a split
  takes (from its last split, at least 1 s) if one is coming, else `windowSeconds`.

**The client.** It throws `Overloaded` on the first refusal, and does not send the call again: a
refusal is not a lost answer, so the one retry of an unknown outcome does not apply. It remembers
the refusal's scope until the retry-after, in the isolate, and throws for calls the scope covers
without sending them (`source: "client"`). A shard scope is of the shard's bits: once the client
learns the shard split, its halves are not covered. The runtime's own shedding ("Durable Object is
overloaded", "Too many requests queued") becomes `ShardOverloaded` with `source: "runtime"` and a
retry-after of 1 s, remembered as an object's refusal is.

**What was applied.** A refused call was not applied on that shard: the check comes before the
method, and an app's `Overloaded` must be thrown before its method writes (or inside `transact`,
which rolls back), as a method's own error is today. Over several shards (`putMany`, `getMany`) a
refusal fails only that shard's part; the error is the first shard's, and the partial outcome
(RFD0002) lists the refused keys as `refused`, a definite "not applied", next to `settled` and
`unknown`.

**IndexedTable.** A write refused at a reservation is not written; reservations it made on other
indexes are resolved as a writer's that never wrote is today (RFD0003). A write refused at its row
shard is not written. A write whose row is written but whose index shard refuses the delivery is
`IndexPending`, as today: the row's alarm delivers the entries later. So `Overloaded` from an
`IndexedTable` always means "not written".

**Splits.** Refused calls count as load, as they do now (`countCall` runs before admission). A key
overload's calls do not count towards a split: splitting does not move one key's load, and the
other keys are served anyway.

### Invariants

1. **Refused means not applied.** A call the caller was told is `Overloaded`, by the object, the
   client or the runtime, was not applied (`refusedNotApplied`; rule `refuseBeforeWork`).
2. **An ok was applied** (`okApplied`).
3. **One key does not take down its shard.** While only one key is overloaded, no call to another
   key is refused, by the shard or the client (`othersServed`; rule `keyScoped`).
4. **Refusals end.** Once the object has recovered, the client stops refusing calls itself
   (`recovers`, a liveness property; rule `clientForgets`).
5. **A lost answer is unknown, never overloaded.** With answers lost, the caller is told
   `OutcomeUnknown`, not `Overloaded`, and the invariants above hold with one and two faults.
6. Every routed call ends with an answer, an `Overloaded`, or `OutcomeUnknown`: no refusal is
   swallowed or retried inside do-kit.

The spec checks 1–5 with each rule shown needed: without `refuseBeforeWork` a refused call has
been applied (4 events), without `keyScoped` a cold key is refused (4 events), without
`clientForgets` the client refuses forever (1 event).

### Compatibility and migration

A breaking change to errors: callers that caught the runtime's overload message, or relied on the
client sending a call again, get `ShardOverloaded` instead. Nothing stored changes. A client
without this code talking to a shard with it gets an unexpected answer (`UnknownAnswer`); deploy
both together, as every do-kit change.

### Security, privacy, and observability

- `detail` crosses to the caller: an app must not put in it what its callers may not see.
- `describe` reports, per shard, whether it is overloaded, its refusals by kind, and its hot keys
  (hashed, as the route sample is).
- With `logShardErrors`, the client logs each refusal with its kind and scope, not only the
  runtime's message.
- The bench records refusals by kind and source per point.

### Rollout and validation

1. **The spec** (done): `specs/overload-refusal.ts`, with one and two lost answers.
2. **The base class and the answer** (built): `Overloaded`, the registry, `{ overloaded }` in
   every routed and scan answer, the runtime's errors mapped, the partial outcome's `refused`. Tests under `cf dev`: a method that throws an app's
   kind reaches the caller as that class, with its detail; an unknown kind is a plain `Overloaded`;
   a refused call is not sent again (counted shard calls).
3. **Admission in the shard**: the wait and in-flight signal, the hot-key top-8, key and shard
   refusals, the client's memory. Tests: a shard with calls held back so their wait passes
   `maxWaitMs` refuses, writing nothing; a hot key is refused while a cold one on the same shard is
   served; the client refuses without sending until the retry-after, then sends. A mutant per
   rule of the spec.
4. **The layers**: `IndexPartitionOverloaded`, `CoordinatorOverloaded`, and splits that skip a
   hot key's load.
5. **Deployed**: the same ramps to 2,048 in flight on 1, 16 and 64 shards, and the hot-config
   benchmark. Success is fewer slow failures (refusals in milliseconds, not after the queue), a
   lower p99 for the calls served, the same or higher throughput, and no splits for one hot key.

## Drawbacks

- **The wait crosses clocks.** `sentAt` is the caller's clock and the start is the shard's; a skew
  of a few ms is noise against a 250 ms target, but a machine far off would make a shard look
  overloaded. `minInFlight` keeps an idle shard from refusing, and the deployed run must show the
  waits at one in flight are near zero before the defaults are kept.
- **A refusal still costs an invocation.** It is cheap (no storage), but a flood of refusals is
  still a flood; the client's memory is what stops one isolate from sending them.
- **Callers must handle a new error.** That is the point, but code that treated every error as
  retryable now retries overloads itself, harder than the runtime would let it.
- **A key refused for 5 s** may be refused after its load has gone. The retry-after errs long,
  because a hot key does not get better by itself.

## Rationale and alternatives

### Proposed design

Refuse early, in the object, typed, with the caller deciding. It turns the runtime's slow,
anonymous shedding into an answer in a millisecond that says what to do, and scopes it to the key
when one key is the cause.

### Simpler or narrower approach

- **Only map the runtime's error to `ShardOverloaded`.** Typed, but still slow (after the queue),
  still whole-shard, and still without a retry-after. It is step 2 of the rollout, and worth
  having alone.
- **A fixed in-flight limit.** Simpler than the wait, but a shard's capacity depends on its calls
  (a page and a get differ by orders of magnitude): any one number is wrong for some table.

### Other alternatives considered

- **Queue in the object, with a cap.** It keeps latency low only until the cap, and then refuses
  anyway; meanwhile the caller does not know, and the caller is the one that knows what to do:
  show an error, shed its own work, or try another key.
- **Retry budgets in the client** (a token bucket per shard, refilled by successes). They stop
  retry storms, but with no automatic retry there is no storm: the caller's retries are the
  caller's.
- **One error class with a `kind` string.** Simpler, but every layer and app would switch on
  strings; subclasses give `instanceof` and typed fields, and the string is still there.

### Do nothing

Callers wait half a second to be refused, cannot tell a hot key from a busy shard, and splits go
on splitting for one key.

## Prior art

- CoDel (Nichols and Jacobson, 2012): drop on the smallest queue delay over an interval, not its
  length.
- Load shedding by request age at the server, with an explicit "busy, retry after" answer, as
  HTTP's 503 with `Retry-After` and gRPC's `RESOURCE_EXHAUSTED` do.
- RFD0002's partial outcomes and RFD0008's definite "not committed": an error that says what was
  applied.

## Unresolved questions

### Before acceptance

- The defaults: `maxWaitMs` 250, `intervalMs` 100, `minInFlight` 8, `hotKeyShare` 0.5. To set from
  the deployed waits at 1 to 2,048 in flight.
- "too many concurrent storage operations" is thrown inside a method, possibly after it wrote
  outside a transaction: is it `Overloaded` (not applied) or `OutcomeUnknown`? Today it is
  classified as overloaded.

### During implementation

- Whether the client's memory should be shared through the colo cache, as splits are, or stay per
  isolate.
- How a work-graph coordinator measures its own wait: its leases come from runners, not clients.

### Out of scope

- Rate limits and quotas: an app's own kind.
- Spreading hot keys (leased reads in `ShardedObject`, RFD0004) and hot index partitions (ordered
  global indexes, RFD0012).

## Future possibilities

- Refusing by priority: a caller marks calls that may be shed first.
- Splitting on overload, not only on calls per second, once refusals are measured.
