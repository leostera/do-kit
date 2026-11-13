# RFD0010 - Table commands: conditional updates inside the shard

- Feature Name: `table-commands`
- Status: Implemented
- Author: `leostera`
- Start Date: `2026-11-13`
- Updated: `2026-11-13`
- Discussion PR: none
- Tracking Issue: none

## Summary

A `ShardedTable` can register **commands**: pure, synchronous functions from a row (or null) and
arguments to a decision, `{ result, write? }`. `client.apply(key, command, args)` runs one inside
the row's shard, in the row's transaction: it reads the row, decides, writes, and answers the
result. A conditional update is then one call, with no version retries, however many callers
update the row at once. The client infers each command's argument and result types.

## Motivation

The common write of a control plane is a read-modify-write: read a row, run the domain's rules
(a lifecycle, an "ensure", a counter), and write the result only if the row did not change
meanwhile. On a `ShardedTable` today that is `get`, then `put(row, { ifVersion })`, then a fresh
read and a retry on a conflict:
- at least two calls per write, each a routed hop;
- under contention on one row, conflicts and retries pile up, and the tail grows with them (see
  Measured).

Inside the shard there is nothing to retry: a Durable Object runs one transaction at a time, so a
read and a write in one synchronous transaction cannot race. Only the code has to be there.

## Goals

- One call per conditional update, with the decision in the shard's transaction.
- Typed per command: the client infers a command's arguments and result from the class's
  commands, as it does for `callOneShard`.
- A clear answer when a write would break a unique constraint (route-scoped constraints are planned),
  apart from the command's own result.
- Safe to route again: an answer of "moved" or "not ready" means the command did not run.

## Non-goals

- Commands that span rows or shards: `transact` (one shard) and `atomically` ([RFD0008](RFD0008-atomic-calls-across-shards.md)) do that.
- Asynchronous commands, or commands that read anything but their row and arguments.
- `apply` on an `IndexedTable`: its writes reserve their global index entries before the write
  ([RFD0003](RFD0003-indexed-table.md)), which a synchronous command can't. It throws.

## Guide-level explanation

```ts
import { defineCommands, defineTable, ShardedTable, table, type Decision } from "@leostera/do-kit/table";

type Account = { id: string; status: "active" | "closed"; plan: string; updatedAt: number };
export const accounts = defineTable<Account>({ primaryKey: "id", columns: { id: "text", status: "text", plan: "text", updatedAt: "integer" } });

export const accountCommands = defineCommands<Account>()({
  // Create if absent; a no-op when it exists.
  ensure: (current, args: { id: string; plan: string; now: number }): Decision<Account, "created" | "exists"> =>
    current ? { result: "exists" } : { result: "created", write: { id: args.id, status: "active", plan: args.plan, updatedAt: args.now } },
  close: (current, args: { now: number }): Decision<Account, "closed" | "absent" | "already"> => {
    if (!current) return { result: "absent" };
    if (current.row.status === "closed") return { result: "already" };
    return { result: "closed", write: { ...current.row, status: "closed", updatedAt: args.now } };
  },
});

export class Accounts extends ShardedTable<Env, Account> {
  protected table = accounts;
  protected override commands = accountCommands;
  protected nodes() { return this.env.ACCOUNTS; }
}

const t = table(env.ACCOUNTS, "accounts", accounts, { commands: accountCommands });
const outcome = await t.apply("acct-1", "ensure", { id: "acct-1", plan: "pro", now: Date.now() });
if (outcome.ok) outcome.result; // "created" | "exists"
```

A command's `write` is a whole row, `"delete"`, or nothing (absent or null). Its `current` is the
row with its version, or null when absent.

`get` and `put(row, { ifVersion })` remain the fallback, for decisions that need something the
shard doesn't have (another service, a clock read the caller wants to own).

## Reference-level explanation

### Data model and interfaces

```ts
type Decision<Row, R> = { result: R; write?: Row | "delete" | null };
type Command<Row, A, R> = (current: Versioned<Row> | null, args: A) => Decision<Row, R>;
type ApplyOutcome<R> =
  | { ok: true; result: R; version: number }                                   // committed; the row's version now (0: absent)
  | { ok: false; conflict: "unique"; constraint: string; version: number };    // nothing written
```

- The class registers `protected commands`; the client takes the same object as its `commands`
  option, for the types and to refuse an unknown name before the call. Functions don't cross an
  RPC: the call carries the command's name and its arguments, which must be serializable.
- `tableApply(keys, [{ command, args }])` is the shard's method. `apply` sends one key.

### Lifecycle and failure semantics

One `apply` is one `transact` over its key:
1. It waits while the key's half is held by a split, and checks this shard owns the key. Either
   can end the call as "not ready" or "moved", before anything runs. The client routes the call
   again. So a command runs once per call that commits, and a call answered "moved" or "not ready"
   ran nowhere.
2. In one synchronous transaction: read the row, run the command, write its decision with the
   row's version as the condition, and answer.
3. A command that throws, or returns a promise, fails the call, and the transaction writes nothing.
4. A write refused by a unique constraint answers `{ ok: false, conflict: "unique", constraint }`
   and writes nothing; the command's result is dropped.

A call whose answer is lost (the connection drops after the commit) may have committed. Callers
that need exactly-once carry their own operation IDs in the row, as with any write.

### Invariants

- **No lost update.** The row a command sees is the row its write replaces: nothing runs between
  the read and the write.
- **At most once on refusal.** "Moved" and "not ready" come before the transaction.
- **Pure commands.** A command reads only its row and its arguments: no clock, storage, network
  or random values. Pass the time in the arguments. This keeps it deterministic and testable
  outside the shard, and is documented, not enforced (only a returned promise is refused).

### Rollout and validation

- Tests (`test/table-apply.test.ts`): create, update, delete, and a no-op answer the command's
  result and the row's version; 40 concurrent applies to one row all commit, each seeing the one
  before; a command that throws, answers late, or is unknown writes nothing; applies racing two
  splits run exactly once each; an apply held by a split lands once in the shard that owns it;
  an `IndexedTable` refuses. Removing the promise check or the rollback on a throw fails a test.
  Removing the split fence does not: the router refuses calls to a shard that split, and writes
  made while a split waits to copy are copied, so no test here reaches a write between the copy
  and the commit. The fence is the same one every table write goes through.
- The client's types are checked at compile time (`applyTypes` in the test Worker).
- On the edge: see Measured.

### Measured

On the edge, on 2026-11-13 (`bench/`, operations `apply` and `rmw`, data in
`bench/results/v050-table-apply.json`). Both make the same decision: a row's size goes up by one,
or the row is created. `rmw` is a `get` and a `put(row, { ifVersion })`, made again from a fresh
read on a conflict, up to 8 times; a call that conflicts 8 times counts as failed. 10 s per point.

| One hot row, in flight | apply, 1 shard | get + put, 1 shard | apply, 16 shards | get + put, 16 shards |
|---|---|---|---|---|
| 1 | 34/s, p99 52 ms | 19/s, p99 123 ms | 19/s, p99 67 ms | 24/s, p99 62 ms |
| 16 | 177/s, p99 284 ms | 17/s, p99 2,935 ms, 72 failed | 120/s, p99 321 ms | 22/s, p99 2,617 ms, 117 failed |
| 64 | 432/s, p99 378 ms | 12/s, p99 3,401 ms, 344 failed | 471/s, p99 304 ms | 15/s, p99 3,000 ms, 391 failed |
| 128 | 562/s, p99 395 ms | 9/s, p99 4,237 ms, 492 failed | 698/s, p99 447 ms | 10/s, p99 3,644 ms, 611 failed |
| 256 | 596/s, p99 597 ms, 540 failed | 6/s, p99 5,231 ms, 713 failed | 879/s, p99 565 ms | 6/s, p99 5,353 ms, 827 failed |

| 10,000 rows, uniform, in flight | apply, 1 shard | get + put, 1 shard | apply, 16 shards | get + put, 16 shards |
|---|---|---|---|---|
| 1 | 23/s, p99 93 ms | 25/s, p99 86 ms | 24/s, p99 120 ms | 21/s, p99 144 ms |
| 16 | 173/s, p99 202 ms | 78/s, p99 994 ms | 146/s, p99 366 ms | 81/s, p99 1,003 ms |
| 64 | 395/s, p99 364 ms | 200/s, p99 2,056 ms, 6 failed | 616/s, p99 272 ms | 305/s, p99 1,278 ms |
| 128 | 505/s, p99 483 ms | 221/s, p99 3,354 ms, 6 failed | 1,180/s, p99 341 ms | 516/s, p99 1,618 ms |
| 256 | 590/s, p99 606 ms | 188/s, p99 5,347 ms, 25 failed | 2,064/s, p99 584 ms | 1,061/s, p99 1,539 ms, 2 failed |

- **On one hot row, apply commits 50–90 times as many updates.** Every `rmw` caller reads the
  same version and all but one put fails, so from 16 in flight most attempts are wasted and many
  calls run out of attempts: at 128 in flight, 9–10 commits/s, p99 3.6–4.2 s, and 492–611 calls
  failed. `apply` commits 562/s on one shard and 698/s on 16 (the row's shard is one object
  either way), with p99 under 450 ms and no failures. At 256 in flight one object's queue fills,
  and calls are refused as overloaded.
- **On uniform keys, apply is about twice as fast at a fifth of the tail.** At 128 in flight on 16
  shards: 1,180/s with p99 341 ms, against 516/s with p99 1,618 ms. The difference is the second
  call, and the conflicts that keys shared by concurrent callers still have.
- At one in flight, both are one or two hops from the caller, and the location of the caller and
  the shard decides the latency.

## Drawbacks

- The shard class and the client must register the same commands. A mismatch is caught at the
  call (`no command`), not at compile time.
- Commands run in the shard's thread: an expensive command slows every call to that shard.

## Rationale and alternatives

### Simpler or narrower approach

- **A custom method per update**, with `getRow`/`putRow` in `transact`: possible today, and what
  `apply` packages. Each adopter would write the same routing, typing and conflict mapping.
- **`patch` with conditions** (a declarative `where` on the write): covers counters and status
  flips, not lifecycles with branches.

### Other alternatives considered

- **Closures over RPC**: they don't cross it. A command is named.
- **Asynchronous commands**: an await inside the transaction would let other calls in between,
  which is the race `apply` exists to remove.

### Do nothing

Contended rows keep paying two calls per attempt and a retry storm under load.

## Prior art

- Durable Objects' own single-threaded transactions, which make the read-decide-write safe.
- Stored procedures and conditional updates in databases (`UPDATE … WHERE version = ?`, DynamoDB
  update expressions), and the reducers of event-sourced systems.

## Unresolved questions

### Out of scope

- `applyMany` over several keys of one shard in one transaction: `transact` covers it.
- Exported stats for `apply` (conflicts, holds): with the per-shard stats work.
