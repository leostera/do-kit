# RFD0001 - Sharded objects

- Feature Name: `sharded-object`
- Status: Implemented (snapshot)
- Author: `leostera`
- Start Date: `2026-10-29`
- Updated: `2026-10-30`
- Discussion PR: none (developed on `main`)
- Tracking Issue: none

This is a snapshot RFD: it records the contract of `ShardedObject` as built in
[`src/sharded/index.ts`](../../src/sharded/index.ts) and
[`src/sharded/client.ts`](../../src/sharded/client.ts). It is descriptive, not a redesign.

## Summary

A `ShardedObject` is a Durable Object class that splits itself when it gets busy. Keys are
routed by the SHA-256 of the key. Each shard owns a hash prefix, and lives in an object named by
that prefix. A busy shard hands the right half of its prefix to a new object, using a storage
snapshot. It holds only the writes to that half while the copy is made. Clients find shards
themselves and follow splits lazily. A stale view costs an extra hop, never a wrong answer.

## Motivation

One Durable Object is one thread with 10 GB of storage, and serves roughly 800 calls/s on the edge.
Past that, the runtime sheds requests as overloaded. A logical object with many independent keys
(an account's repositories, a namespace's configuration, a room's messages) hits that limit, and
the usual answer is to shard it by hand. That means choosing a shard count up front, routing, and
migrating when the count turns out wrong.

The measured cost of not sharding (1 KiB values, uniform keys, 2,048 in flight): one object
completes 1,435 writes/s and refuses 26,816 calls in 10 s. The same object in 16 shards completes
9,960 writes/s and refuses none. See [docs/performance.md](../performance.md).

## Goals

- Write the class as if it were one object: methods that read and update keys.
- Split by load, without a shard count chosen up front, and optionally presplit an empty object.
- Keep serving during a split: reads everywhere, and writes to the half that stays.
- Never lose an acknowledged write, apply one twice, or let two objects serve one key.
- Route with no directory: the object for a shard is computed from its prefix.

## Non-goals

- Transactions across shards. `update` and `transact` are transactional within one shard.
  `callOneShard` refuses, before applying anything, keys that span shards.
- Splitting one hot key. A key lives on one shard.
- Merging shards back.

## Guide-level explanation

```ts
export class Counter extends ShardedObject<Env, number> {
  protected nodes() {
    return this.env.COUNTERS; // the binding every shard of this class lives in
  }

  async add(keys: readonly string[], amounts: readonly number[]) {
    return this.update(keys, (current): UpdateResult<number, number[]> => {
      const writes = new Map(keys.map((k, i) => [k, (current.get(k) ?? 0) + amounts[i]!]));
      return { writes, result: keys.map((k) => writes.get(k)!) };
    });
  }
}

// In a Worker
const counters = sharded(env.COUNTERS, "account-42");
await counters.call(["a", "b"], "add", [1, 2]); // one call per shard owning some of the keys
```

A routed method takes `(keys, payloads, ...args)`: the keys it acts on, payloads aligned with
them, and extra arguments. The client splits a batch by shard and calls each shard with its share.
`scan(method, ...args)` calls every shard once.

Inside a method, `read(keys)` and `update(keys, decide)` use the built-in rows. Subclasses may
also keep their own tables, written only inside `update` or `transact(keys, fn)`, which fence
the write against splits (see Invariants).

## Reference-level explanation

### Routing and naming

- `keyHash(key)` is the SHA-256 of the key, as hex. A shard is a bit string `bits`: it owns
  the hashes that start with those bits. The root is `""`.
- The shard `bits` lives in the object named `shardObjectName(name, bits)`, that is
  `name` plus `hostBits(bits)`, which is `bits` without trailing zeros. When `01` splits, `010`
  stays in the same object, and only `011` goes to a new one. No object is left behind as a
  router.
- The client keeps the set of prefixes it knows have split, per object and isolate, for at most
  10,000 objects (`MAX_KNOWN_OBJECTS`). It shares them in the colo through the Cache API. To route
  a key, it walks down its known splits to the deepest known shard.
- A shard answers:
  - `ok`: it served the call;
  - `moved`: it split since the caller last knew, and the answer names the deepest split it
    knows of;
  - `notReady`: a new shard not activated yet, or a write held too long by a split;
  - `stale`: the caller knows splits this storage never had, because the object was deleted
    and its name reused.

  The client learns from `moved` and retries. It backs off on `notReady` with jitter. After 1 s of
  `notReady`, it forgets the object's splits once and routes from the root.
- Shards make stubs of other shards (to split, presplit and activate) in their own
  jurisdiction (`ctx.id.jurisdiction`): an object reached through `namespace.jurisdiction("eu")`
  keeps every shard there. The same name in the plain namespace is another object.
- The client's `stubOptions` (a version cohort, a location hint) go into every `get` it makes,
  and along with each call. A shard makes its own stubs with the options of the last call it
  served (`stubOptions()`, which a subclass may override), so a split's new shard runs the
  version its callers asked for. These options are kept in memory only: work an alarm resumes
  after a restart uses none until the next call.

### Splitting

A shard splits when its call rate stays above `callsPerSecond` (400 by default) for
`windowSeconds` (5), and it is shallower than `maxDepth` (12). A call weighs 1 plus
`keys / keysPerCall` (25). The default threshold is half of what one object serves, which leaves
headroom for the split itself.

1. **Warm.** It calls the new object once (`shardedWarm`), before holding anything. A first call
   to an object can take seconds.
2. **Hold.** Writes (`update`, `transact`) to keys of the right half wait. Reads go on, as do
   writes to the left half. A held write waits at most `holdWaitMs` (2 s) in the object, then
   answers `notReady` with the split's deadline. The client keeps retrying until that deadline,
   plus 5 s for activation (capped at 10 minutes).
3. **Copy.** It copies its storage to the new object.
   - **Native:** `ctx.storage.snapshot()`, then `shardedRestore` on the new object, which
     schedules the restore and restarts on it.
   - **Logical:** the fallback where snapshots are unavailable. It copies every table and index,
     in batches of at most 4 MiB, paged by a stable key. Batches are tagged with the attempt's
     epoch, so a late batch cannot overwrite newer rows.
4. **Claim.** It calls `shardedClaim` on the new object until the copy answers that it is this
   attempt's copy.
5. **Commit.** In one transaction, it becomes `bits + "0"`, records the right half for cleanup,
   and marks the sibling to activate. Then it releases the held writes. Those now answer `moved`,
   and the client sends them to the new owner.
6. **Activate.** It calls `shardedActivate` on the sibling, which starts serving. The alarm
   retries this until it succeeds.
7. **Clean up.** Both shards delete what they gave away, in batches of 250 rows from the alarm.
   The core rows go first, then the subclass's rows through `cleanupForeign(owns, limit)`. Rows a
   shard gave away are invisible from the commit onward: every read filters by ownership.

A split that has not finished after `splitTimeoutMs` (60 s) is abandoned. The held writes then go
ahead on the original, and the copy is never activated. Each attempt has an epoch, and only the
attempt that made a copy can claim or activate it.

`initialDepth` presplits an empty object on its first call. The root adopts every shard up to 6
levels below it in one round of at most 63 calls. Each adopted shard goes deeper on its own first
call. A non-empty object splits by load only.

### Hooks and alarms

- `beforeHandOut()` runs before the copy, while writes to the half that moves wait.
- `afterClone(owns)` runs in the new object as it claims the copy, in that transaction.
- `cleanupForeign(owns, limit)` runs on both shards after a split, paced, until it returns 0.
- `owns(key)`, `ownedHashRange()`, `assertOwns(keys)` and `ownedRowsPage(afterKey, limit)`
  help a subclass filter its own tables until cleanup is done.
- The alarm belongs to the base class. A subclass that defines `alarm` is refused at
  construction. Subclasses override `onAlarm()` and ask for it with `scheduleAlarm(at)`, which
  keeps the earliest time asked for. `onAlarm` runs only on a serving shard.

### Observability

`shardedDescribe()` returns:
- the shard's bits, state, owned and stored rows, and call rate;
- its last 8 split attempts, each with step timings (warm, hand-out, snapshot, restore, copy,
  claim), held writes, `notReady` answers and the longest hold;
- counters since the object started: calls, `ok`, `moved`, `notReady` and `stale` answers, and
  held writes.

### Invariants

- At any moment one object serves a key. The original stops owning the right half in the commit
  transaction, and the new shard serves only once activated after it.
- An acknowledged write is never lost or applied twice. Writes to the half that moves wait for
  the split, and are checked for ownership after the wait, in the same synchronous turn as their
  transaction. A write that skips `update` or `transact` can land after the copy and be lost.
- Splits are final, so knowledge of a split is never wrong, only incomplete.

These are model-checked:
- [`specs/snapshot-split.ts`](../../specs/snapshot-split.ts) covers the split with faults:
  no lost or double write, one server per key, no stale read.
- [`specs/scan-visibility.ts`](../../specs/scan-visibility.ts) covers scans during cleanup.
- [`specs/call-one-shard.ts`](../../specs/call-one-shard.ts) covers batches that turn out to
  span a split.

### Compatibility and migration

The class and RPC names changed once, from `SplittableObject` (v0.1) to `ShardedObject`. Since
then, changes have kept storage compatible. Native splits need the `durable_object_snapshots`
compatibility flag; without it, `copy: "auto"` falls back to the logical copy.

### Rollout and validation

- Integration tests run under `cf dev`, and against a deployed Worker where splits use
  snapshots (`test/sharded.test.ts`).
- Benchmarks on the edge are in [docs/performance.md](../performance.md).

## Drawbacks

- A hot key cannot be split, and the split policy keeps splitting a shard that is hot because
  of one key.
- Each split costs a cold object and a restart on the snapshot, which blocks the moving half's
  writes for about 0.6–4 s (the restart is the platform's).
- Splitting by load during a sudden burst ramps through several rounds (about 20–25 s from 1 to
  16 shards). Presplitting and capping avoid it.

## Rationale and alternatives

- **Copy both halves into two new objects** (freeze, snapshot into two new objects). It is
  simpler, but every write blocks for the whole copy, and the original becomes a router. At
  128 MiB it took 24.3 s with every write blocked, against 5.7 s here.
- **Stream rows to two new objects** (v0.1). It took 235 s at 128 MiB and did not finish 1 GiB
  in 2 hours.
- **Range partitioning by key.** It keeps keys sorted for scans, but needs split points chosen
  from the data, and suffers hot ranges for sequential keys. Hashing spreads load without
  coordination. Range partitioning is proposed for indexes in
  [RFD0003](RFD0003-indexed-table.md).
- **Do nothing.** Users shard by hand with a fixed count and migrate when it is wrong.

## Prior art

- **Extendible hashing** and **consistent-hash rings** route by hash prefix or ring position.
  The hash-prefix tree here is extendible hashing with the directory replaced by object naming.
- **Bigtable and Spanner** split by key range at data-chosen points, with a directory (the
  metadata tablets).

## Unresolved questions

### Out of scope

- Splitting several levels at once for a shard far over the threshold, with `maxDepth` as a
  hard cap.
- Telling a shard that is hot because of one key apart from one that is hot because of many.
