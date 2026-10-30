# Spike: durability's outbox inside a sharded object

Branch `spike/durability`. Question: can do-kit build its next layers (cached reads, indexes,
change feeds) on [durability](https://github.com/avenceslau/durability)'s primitives instead of
writing its own, and what does it cost?

What was built:

- `src/durability/index.ts`: `ShardedDurability`, a helper a `ShardedObject` subclass holds. It
  owns a `DurabilityScheduler` and any number of `DurabilityOutbox` channels (durability 5.1.0).
- `test/worker`: `MirrorCounters` (every `set` stages the key's value for a sink, in the same
  fenced transaction) and `Sink` (keeps the highest version per key, logs every delivery, can fail).
- `test/durability.test.ts`: five tests under `cf dev` (logical copies; local workerd has no
  snapshots).

## Result: it works, through three seams

All five tests pass: the sink converges to every key's last value through 3 rounds of splits
(8 shards) under 8 writers, through a sink outage with a split in the middle, and after a crash
that loses every in-memory timer. No duplicate `(key, version)` delivery was seen in any run.

The three seams, each a place where durability assumes it owns the object:

### 1. The alarm: durability wants to own it, and so does do-kit

`Engine.reconcile` sets, moves later, and deletes the physical alarm on every transaction;
`armFallbackAlarm` sets it directly. Inside a sharded object that would postpone or delete the
alarm a split's activation and cleanup run on.

Fix in the spike: the scheduler gets a `Proxy` of `ctx.storage` (and of the transaction object)
whose `getAlarm`/`setAlarm`/`deleteAlarm` act on a row (`dokit_durability_alarm`). Setting it also
calls the shard's `scheduleAlarm` (earlier wins), and `onAlarm` runs `scheduler.alarm()` when the
row is due. Moving it later or deleting it only touches the row: an early wake-up finds nothing due.

This works and was never in the way of splits, but it relies on durability only reaching the alarm
through `storage` and the transaction argument, which is not a contract.

**Upstream ask:** an `alarm` port on the scheduler (`{ get, set, delete }`), defaulting to
`ctx.storage`. Then no proxy is needed.

### 2. Staging: `stage()` is async, do-kit's fence is a synchronous transaction

`update`/`transact` wait out a split's hold, check ownership, then run one `transactionSync`. Only
a write inside that is safe from being lost to a split's copy. `DurabilityOutbox.stage()` opens its
own async `storage.transaction`, so it cannot be called there, and calling it after would leave a
window where the row is written and the entry is not (or the entry lands behind the fence).

Fix in the spike: `ShardedDurability.stage()` writes the outbox row itself, mirroring the private
`#stageIn` (same id format, serialization, version counter and generation fencing). Then `kick()`
runs a scheduler pass in `waitUntil` so publication starts at once.

Durability already has exactly this function, package-internal (`stageWithin`, used by topology).

**Upstream asks:** a public `stageSync(key, value)` (synchronous, for callers already inside a
transaction), and a public `refresh()` (the internal `scheduleNextAlarm`) so a kick does not need
a whole alarm pass.

### 3. Splits copy durability's tables too

Every table goes to the new shard, `durability_outbox` included. Its rows carry `entry_key`, and
the spike always stages with the routing key as `entry_key`, so pending entries follow their
keys: `afterClone` drops the entries of keys the new shard does not own, and `cleanupForeign`
drops the ones the original gave away. Tested: after a split with a 200-entry backlog, the entries
were 86/114 across the two shards, each on its key's shard, none on both.

Versions stay monotonic per key: the copy inherits the counter, and only a key's owner stages it.
An in-flight publication on the original for a moved key can still land after the claim; that is
one duplicate of the same `(key, version)`, which the sink contract already absorbs (none
observed).

This reads durability's table directly (`durability_outbox.entry_key`).

**Upstream ask:** `outbox.discardWhere(predicate, limit)` (or a listing of pending keys), so a
host can partition entries without knowing the schema.

Helpers with no key (`Durability` operations, `DurabilityAlarms`) were not tried. They have no
routing key, so after a split both shards hold them and both would run them. They would need a
rule (they stay with the original; the copy drops them in `afterClone`).

## The cost: about 10x the rows written per write

Measured under `cf dev`, 200 single-key writes over 50 keys, after warm-up (rows counted from
every statement's `rowsWritten`):

| | rows written per write | ms per write (local, two runs) |
|---|---|---|
| `ShardedObject` `put` | 1.25 | 17.0, 7.9 |
| Same write, mirrored through the outbox | 13.25 | 19.6, 11.5 |

Row counts were identical across runs; local latency is noisy (15–45% slower here), so don't read
much into it.

Where the 12 extra rows go:

- **6 to stage**: the counter update, and an `INSERT OR REPLACE` of the outbox row, which has a
  primary key and **three secondary indexes** (`due`, `pending`, `created`).
- **4 to publish**: the claim (`UPDATE attempt, next_attempt_at`, which moves the row in two
  indexes) and the acknowledgement (`DELETE`, from all four b-trees).
- **2 for the alarm**: each publication arms the fallback alarm while it is in flight and deletes
  it after (here, the virtual alarm's row; on a plain object, a billed `setAlarm`).

At $1.00 per million rows written that is about $12 more per million mirrored writes, against
$1.25 for the write itself. That cost is durability's design (a general retry engine with
indexes for every way it queries), not the spike's glue.

## What we learned

1. **The primitives compose.** The outbox's contract (newest value per key, version, sink keeps
   the highest, retry forever) is the right one for cached reads and index maintenance, and it
   survives splits once entries are keyed by routing key. No change to the split protocol was
   needed: `afterClone`, `cleanupForeign`, `onAlarm` and `scheduleAlarm` were enough.
2. **Every seam is somewhere durability assumes it owns the object:** the alarm, the transaction,
   the tables. Each has a small upstream fix (an alarm port, `stageSync` + `refresh`,
   `discardWhere`). With those, the glue is about 60 lines and touches no internals.
3. **The outbox is too expensive per row for do-kit's hot path.** For "publish every write" at
   sharded-object volumes, 10x the rows written is a lot. A do-kit-native outbox can keep the
   pending mark on the row itself (a `pending_version` column with a partial index): staging adds
   about one index row to a write that already happens, and publishing adds about one write. That
   is about 2–3 rows, not 12.
4. **So use it by volume:** durability for low-volume, retry-heavy work on a shard (named alarms,
   operations, a fanout of rare events). A native, row-attached outbox for publishing every write
   (cached reads, secondary indexes, change feeds), with durability's contract copied, not its
   engine.

## Not covered

- Native snapshots (local workerd has none): the copy path differs, the hooks do not.
- `Durability` operations and `DurabilityAlarms` across splits (no routing key, see above).
- Production latency and cost; the numbers above are local and row counts only.
