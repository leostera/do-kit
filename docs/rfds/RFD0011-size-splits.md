# RFD0011 - Size splits: shards that split before they get big, and a cap they never pass

- Feature Name: `size-splits`
- Status: Implemented
- Author: `leostera`
- Start Date: `2026-11-13`
- Updated: `2026-11-14`
- Discussion PR: none
- Tracking Issue: none

## Summary

A shard splits when its database reaches `splitAtBytes` (1 GiB by default), whatever its load.
It refuses writes that would grow it at `refuseAtBytes` (1.1 × `splitAtBytes`), and still serves the writes it
needs to drain. Rows lie on disk in the order of their key's hash, and a table's index keys are
copied into dense tables after a split, so the space a split gives away is freed. A split that
cannot shrink a shard is not started, and one given up leaves nothing behind.

## Motivation

Before this, a shard split only by its call rate. A table that took 100 writes a second of 1 KiB
rows never split, and grew towards the platform's 10 GB, where every write fails, deletes too.
Three findings shaped the design, all measured here on local workerd:

- **A split freed almost nothing.** Rows were stored in the order they were written, so the half a
  split gives away was spread over every page. Deleting it left each page half full, and SQLite
  does not give back a page that still holds a row. A table of 5,000 rows with three indexes was
  2.77 MB before a split and 2.52 MB after, on the side that kept its half (0.91). VACUUM cannot
  run in a Durable Object (every statement runs in a transaction). A size trigger on such a shard
  would split it again and again, down to `maxDepth`, each split doubling the bytes stored.
- **Large rows cost four times their size.** In a WITHOUT ROWID table, SQLite keeps less than
  about a kilobyte of a row in its page; the rest takes an overflow page of its own. A 1 KB value
  took 4,696 bytes; in a rowid table, 1,423.
- **Index keys are most of a table.** The three key tables of that 5,000-row table took 1.77 MB of
  its 2.77 MB.

## Goals

- A shard splits at a size, checked after every write and by its alarm, level-triggered.
- At a cap below the platform's, a write that grows the shard is refused with `ShardFull`, and
  one that does not (a delete, a change in place, an atomic outcome already decided) is served.
- Both halves of a split end near half the parent's size.
- A split that would move none of the rows, or all of them, is not started (the floor).
- A split given up deletes its copy, and automatic splits back off after failures.
- Starting a split reads no more than a few rows.

## Non-goals

- Merging shards. Splits stay final.
- Recovering space inside a page that no split touched (deletes in place by the app).

## Guide-level explanation

```ts
protected override splitPolicy: SplitPolicy = {
  ...DEFAULT_SPLIT_POLICY,
  splitAtBytes: 1024 ** 3,      // split here, whatever the load
  refuseAtBytes: 1.1 * 1024 ** 3, // refuse growth here (default 1.1 × splitAtBytes): ShardFull
};
```

A client that gets `ShardFull` knows nothing was applied. It is final: retrying does not help,
deleting does. A shard reaches its cap only when it cannot split: its rows share one route, or
it is at `maxDepth`.

## Reference-level explanation

### The trigger

`writeTransaction` (under `update`, `transact` and the atomic steps) checks
`ctx.storage.sql.databaseSize` after each commit, and the alarm checks it too. A shard at
`splitAtBytes` starts a split (trigger `"size"` in its records), unless:
- it still deletes what an earlier split gave away (its size counts those rows);
- an earlier split of this instance ended less than `windowSeconds` ago;
- automatic splits are backing off after failures (below);
- it is at `maxDepth`.

### The floor

Before a split starts, `splitHalves` says whether each half holds a row: two seeks on the hash
index for a `ShardedObject`, the per-bucket counts for a `ShardedTable`. An automatic split with
an empty half is not started, and the shard waits a window before asking again. An operator's
split goes ahead. A split record's `rows` and `rightRows` are null where the layer keeps no count
(a `ShardedObject`'s own rows); counting them would read every row.

### The cap

Inside the write's transaction, `databaseSize` moves a page at a time. A write is refused when the
database is at `refuseAtBytes` and the write added a row (`markGrowth`) or grew the database. Once
a write is refused, the shard counts as full until deletes take it 5% under the size it was
refused at: the refused write rolled back, and the next must not take the room it left. The
outcome of an atomic call already decided (`atomicResolve`) is never refused; holding its keys
forever would be worse. A refusal answers `{ full }`, and the client throws `ShardFull`. A refused
write runs the split decision as a committed one does: only after writes that apply, a shard at
its cap would refuse every write and never split.

The default cap is 1.1 × `splitAtBytes`: the split starts at 1.0, and the margin is room for the
writes that land while it runs. A shard that cannot split stops just past where it would have:
rows of one route are capped near 1.1 GiB unless the table raises `refuseAtBytes`. Keeping objects
near a gigabyte keeps their wakes, splits and migrations in seconds.

### Space

- **Clustered rows.** `sharded_rows` and `sharded_table` are rowid tables whose rowid comes from
  the route hash: its first 32 bits, then 21 bits of the key's own hash (2^53 in all, exact as a
  number). A rowid already taken (two keys of one route sharing the 21 bits) takes the next free
  one of its route's slot. The rows a split gives away lie together, and deleting them frees whole
  pages. Rows land in hash order rather than at the end, so pages fill less: a 1 KB value took
  1,606 bytes with its indexes, against 1,423 appended.
- **Compacted key tables.** A key table is ordered by its index key, which no layout can make
  follow the hash. After a split, at the first cleanup batch, each built index's key table is
  copied in key order, only the keys the shard owns, into a new table, in steps the size of a
  build step. The old table serves listings until the copy is done. A write meanwhile changes both
  tables where the copy has passed its key, and only the old one ahead of the copy. Then the copy
  takes the table's name (a rename), and the old table is emptied and dropped in steps, freeing
  all its pages.
- **The logical copy sends one half.** A table whose rows carry their hash (`copiedHash`) is sent
  for the right half only, in batches of at most 4 MiB counted as the heap holds them (two bytes a
  character of text).

The 5,000-row table above was 2.92 MB before a split; after it, the half that stayed took 1.57 MB,
and its copy 1.50 MB. A `ShardedObject` of 2,000 1 KB values was 3.26 MB, and its halves 1.65 MB
and 1.50 MB.

### Splits given up

- The parent records the copy to discard and tells the sibling (`shardedDiscard`), its alarm
  retrying until the sibling confirms. The sibling deletes the copy of that attempt or an older
  one, claimed or not, and keeps a newer attempt's copy or a serving shard.
- A split is given up only before its claim commits. The sibling deletes a claimed copy, so a
  claimed copy must belong to a split the parent gave up. Once the parent commits, the right half
  is the copy's. A step after that fails (a storage error arming the alarm, or activating the
  sibling) is logged and retried by the alarm or the next call, never taken for a split given up.
  Taken for one, it discarded the shard that owned the right half, with the writes it had
  acknowledged.
- After n automatic attempts given up in a row, the next starts no sooner than `2^n` split
  timeouts after the last one started, capped at 30 minutes. A size split given up asks the alarm
  for that time, so it is tried again with no write to start it.
- The copy refuses a first batch sent again after later ones; it would wipe them.

### Atomic holds

A shard splits while an atomic call holds keys of the half that stays: those holds stay with the
parent, and the copy drops its copies of them. A hold on the half that moves still keeps the
shard whole until the call ends.

### Compatibility and migration

- `sharded_rows` was a WITHOUT ROWID table. A shard of that layout renames it to
  `sharded_rows_legacy` when it wakes, a rename that moves no row, and makes the new table. Reads
  look in the new table, then the old. A write moves its key. The alarm moves 250 rows a turn in
  clustered order, and drops the old table once empty. Listings and cleanup read both until then.
- Table rows written before this keep their place until rewritten; only their split frees less.

### Invariants

- A key is in exactly one of `sharded_rows` and `sharded_rows_legacy`.
- During a compaction, every key at or before the copy's cursor is in the copy exactly when it is
  in the serving table. At the swap, the copy holds every key of the serving table the shard owns.
- A write refused at the cap applied nothing.

### Rollout and validation

`test/size.test.ts` covers the trigger, the cap, the floor, the backoff, the discard, the per-row
cost, both halves' size, the copy's batches and the old layout's migration.
`test/table-compaction.test.ts` writes during a compaction spread over many steps, and checks every
index against a model. Mutations of the mirrored writes, the old-layout reads and moves, and the
partial-bucket rule each fail them.

## Drawbacks

- Rows inserted in hash order fill pages less than rows appended: about 13% more disk for 1 KB
  values.
- A compaction rewrites every key table once per split: I/O the size of the indexes.
- The cap counts a write as growth if it adds a row, even one that fits a page already there.

## Rationale and alternatives

- **A size estimate instead of freeing space.** The trigger could subtract what a split is expected
  to have freed. It would hide the bytes still billed, and the guess drifts as holes fill.
- **Clustering key tables by hash.** A listing walks a key table in key order; ordering it by hash
  would make every page of a listing a sort.
- **VACUUM after a split.** Not allowed in a Durable Object, and it would rewrite the whole
  database in one statement.

## Unresolved questions

- The default `splitAtBytes` comes from split times measured at 128 MiB and 1 GiB of logical data.
  A curve against `databaseSize` on the edge would set it better.
- Group counts are not compacted; their rows are small.
