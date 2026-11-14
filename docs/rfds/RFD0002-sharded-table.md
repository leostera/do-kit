# RFD0002 - Sharded tables

- Feature Name: `sharded-table`
- Status: Implemented (snapshot)
- Author: `leostera`
- Start Date: `2026-10-29`
- Updated: `2026-10-30`
- Discussion PR: none (developed on `main`)
- Tracking Issue: none

This is a snapshot RFD: it records the contract of `ShardedTable` as built in
[`src/table/`](../../src/table/). It replaces the earlier design note, `docs/design/sharded-table.md`.

## Summary

A `ShardedTable` is a table of typed rows on a sharded object ([RFD0001](RFD0001-sharded-object.md)).
The primary key routes a row to its shard. Each shard keeps the table's indexes for its own rows,
and a listing merges one page from every shard into a sorted page with a cursor that survives
splits. Writes are conditional on a version per row. Counts, total and per value of a column,
are kept with every write.

## Motivation

Most users of a sharded object build the same thing by hand: a directory, a control plane, records
listed several ways. They need typed rows, indexes, create-if-absent, compare-and-set, listings with
a cursor, and totals. Built on raw keys and values, every one of them has to get pagination across
splits right. With no indexes, a sharded table is a key-value store.

## Goals

- Get, put, patch and delete rows by primary key, one or many, with a version per row and
  compare-and-set on it.
- Indexes declared in the schema, kept by every shard for its own rows, in the same transaction
  as the write.
- `list({ index, direction, where, limit, cursor })` across every shard: sorted, paged, with an
  opaque cursor that survives splits. Across pages, no row is skipped or returned twice while shards
  split and clean up.
- Bounded work per page: a shard scans at most a budget of rows per page.
- Exact counts without scanning rows: the total, and rows per value of declared columns.
- Subclassable: custom transactional methods use the same row helpers inside `transact`.

## Non-goals

- Unique constraints on columns other than the primary key, and lookups by them across shards:
  [RFD0003](RFD0003-indexed-table.md).
- A snapshot across pages. Each page reflects its own moment, as any keyset pagination does.
- Counts of arbitrary conditions (`count({ where })`). Count the values you filter on with a
  count group instead.
- Joins, and transactions across rows on different shards.

## Guide-level explanation

```ts
type Order = { id: string; customer: string; status: string; createdAt: number };
const orders = defineTable<Order>({
  primaryKey: "id",
  columns: { id: "text", customer: "text", status: "text", createdAt: "integer" },
  indexes: { byCreated: ["createdAt"] },
  counts: { byStatus: "status" },
});

export class Orders extends ShardedTable<Env, Order> {
  protected table = orders;
  protected nodes() { return this.env.ORDERS; }
}

// A Worker
const t = table(env.ORDERS, "shop-42", orders);
await t.put({ id: "o_1001", customer: "ada", status: "pending", createdAt: Date.now() }, { ifVersion: 0 }); // only if new
const page = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "status", op: "!=", value: "cancelled" }], limit: 50, withCount: "byStatus" });
const next = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "status", op: "!=", value: "cancelled" }], limit: 50, cursor: page.cursor });
const perStatus = await t.countBy("byStatus"); // [{ value: "shipped", count: 812 }, ...]
```

`where` conditions are `=`, `!=`, `<`, `<=`, `>`, `>=`, `isNull`, `notNull`, `prefix`, and
`contains` (substring, ignoring ASCII case). They are ANDed.

## Reference-level explanation

### Schema

- Column types: `text`, `integer`, `real`, `boolean` (stored 0/1) and `json` (stored as text,
  not indexable or countable). Every column but the primary key may be null.
- The primary key is a `text` column, and is what `keyHash` routes on.
- An index is a list of columns. The primary key is appended to every index, so the order is
  total and a cursor names one position.
- Rows are stored in real SQLite columns, in table `sharded_table`, with `_hash` (the routing
  hash of the key) and `_version`.
- A value comes back as it went in, or the write is refused, naming the column: NaN, an integer
  past 2^53, text that is not well-formed UTF-16 (SQLite would store U+FFFD, and a key read back
  would route elsewhere), and in `json` columns anything `JSON.stringify` changes (non-finite
  numbers, `undefined`, functions, bigints, a `Date` or other non-plain object, at any depth).
- **Limits.** A table has at most 100 columns with its hidden ones (`_hash`, `_version`, one per
  index): SQLite's limit in a Durable Object, checked by `defineTable`. A listing takes at most 32
  conditions. A row is at most `maxRowBytes` (default and maximum 2 MB, SQLite's row limit), text
  counted as UTF-8: a larger one throws `RowTooLarge` and writes nothing.

**Schema changes.** Each shard brings its storage up to the schema the first time it runs code
of that version, with cheap statements only: it adds missing columns, records the indexes and
count groups it must build, drops the SQL indexes no longer declared, and records dropped count
groups. A dropped index's key column stays (dropping a column rewrites every row), and is cleared
to null in steps, which frees its space. Removing or retyping a
column is not supported: add a new one. A cursor is refused once its index is redefined.

What is proportional to the rows is done in build steps, each one transaction of at most
`indexBuildRows` (10,000) rows: deleting a dropped group's counts, filling in a new index's keys
in key order, and counting a new group in key order. Each call takes a step (the call that
migrates takes none), and so does the alarm, so a shard that gets few calls still finishes. A
step that fails is rolled back and taken again; it never fails the call. Meanwhile the shard
serves gets, writes and other listings: only a listing by an index being built, or a count of a
group being counted, answers "not ready". A write during a count counts its change only for a
row the count has passed: the count reaches the others with their value then. On a shard with
no rows, what is new is built at once.

A new index's SQL index is made at migration, while its column is empty, and holds only the
keys that are set: SQLite reads each row once to make it, but sorts and writes nothing, and each
step adds its page's keys. It is the one statement of a schema change that reads every row, and
SQLite has no cheaper way to make an index over a table with rows. An index declared again over
a column a drop left is made once the column is cleared, so it too starts empty.

A shard that has nothing left to build records the schema it migrated to. Woken with the same
schema, it reads that record, one statement, instead of checking every table and index.

**Versions and rollbacks.** `version` in the schema is a whole number, raised with every change.
A shard records the highest it ran. Code with a lower one (a rollback, or a deploy halfway)
leaves the indexes it doesn't know alone, and marks every row it writes. The newer code then
refreshes the marked rows a step at a time (their index keys, and an IndexedTable's global
entries), with listings by an index waiting until done, instead of rebuilding. Count groups the
older code doesn't know are dropped, since its writes would not keep them, and counted again by
the newer code. Without versions every schema is 0, and each deploy drops what it no longer
declares.

### Writes

- `put(row, { ifVersion })` inserts or replaces a whole row. `patch(key, changes, { ifVersion })`
  changes some columns. `delete(key, { ifVersion })` deletes.
- Each returns `{ ok, version }`: the row's new version, or its current version when `ifVersion`
  did not match. `ifVersion: 0` means "only if absent".
- Every write runs in the core's `transact`. It waits while a split copies the key, checks the
  shard still owns it, and is one SQLite transaction with the index keys and the counts.
- A write may carry an `opId`. The shard keeps the opId with the write's result, in the
  write's transaction, for `OP_ID_TTL_MS` (a day). The same request sent again under that opId
  (its answer was lost) answers the kept result and writes nothing. Another request under it
  throws `OperationMismatch`. The opId is kept with the row's hash, so a split leaves it with the
  shard that owns the key. A write refused before writing (an index entry not reserved) keeps
  nothing.
- A write of rows over several shards, one of which fails, throws that shard's error with
  `results`: each row's `WriteResult` where its shard answered, `{ unknown: true }` where its
  shard failed (it may have applied). `get` and `getMany` write nothing, and are sent again once
  after an error of unknown outcome.
- A row written where none is starts above every version a deleted row of its shard had
  (`sharded_table_versions`). So a key never has the same version twice, and a write conditional
  on a version read before a delete fails on the row written since.

### Listing

- A page in query order asks every shard each round. The client refuses a tree wider than
  `maxShardsPerPage` (64) with `TooManyShards`: list such a tree in shard order, which asks a
  few shards per page.

The cursor is the sort position of the last row returned, plus a fingerprint of the query (the
index and its columns, the direction and the conditions), so it cannot continue another query. It
holds no state per shard, so it does not care how many shards there are or whether they split.

A page:

1. The client asks every shard (the core's `scan`) for its share of the page after the cursor:
   twice `limit / shards`, plus two. Each shard returns matching rows in index order, scanning at
   most `budget` rows.
2. Each shard answers its rows and a **frontier**: the position up to which it has reported every
   match. That is its last row if it returned its limit, the last row it scanned if it ran out of
   budget, or the end.
3. The client merges. Every shard has reported everything up to the smallest frontier, so the rows
   up to it are complete. It returns those, sorted, up to `limit`. If the page comes up short, it
   asks again from the frontier, up to three rounds.

Why no row is skipped or repeated across a split:

- A row is owned by one shard at any moment, and a shard returns only rows it owns (`_hash` in
  its range), even while cleanup has not deleted the rest.
- The new shard answers `notReady` until it is activated, and the original answers `moved` for
  the half it gave away, so a page covers each part of the key space once.
- A cursor from before a split is a valid position in both halves.
- The client drops a key it sees twice in one page.

The guarantee is for rows whose sort columns do not change during the listing. A row whose sort
value changes between pages moves in the order, and may be seen twice or not at all, as with any
keyset pagination.

**Positions and index keys.** Each index keeps one hidden column per row: its index key, which is
the index's values and the primary key in an order-preserving encoding, as hex.
- Null is `00`.
- A number is `01` and an order-preserving float64.
- Text is `02`, its UTF-8 with `00` escaped as `00 FF`, then `00`.

The SQL index is on that column alone, and a page is one range seek
(`INDEXED BY`, `key > cursor`). Comparing the index columns one by one with null-aware `OR`s could
not seek, and got slower as the table grew. A property test checks the encoding against SQLite's
order on random tuples.

### Counting

- **The total.** Each shard keeps rows per 16-bit hash prefix (`sharded_table_counts`), updated
  in the write's transaction. A shard's count sums the prefixes in its range, which is exact at
  every depth up to 16 (the table's `maxDepth` limit). After a split both sides store all
  prefixes but sum only their own, so nothing is recomputed.
- **Count groups.** `counts: { byStatus: "status" }` counts rows per value of a column the same
  way, in `sharded_table_group_counts`. `countBy(group)` sums a shard's range per value. Count
  groups are for columns of few values: past `maxCountValues` (1,000) values, `countBy` answers
  `{ counts, truncated: true }` with the first that many, in order of their JSON text: each shard
  reads its first ones in its key's order, with nothing sorted, and sends one more.
- **With a listing.** `withCount: true`, or a group's name, returns `total`, and the group's
  `counts`, from the shards' first-round pages: one call per shard for a page and its total.
- **During a split.** The object's count is the sum over shards. It may count a row twice, or not
  at all, at the instant a split commits.

### Invariants

- A listing never returns a row twice, returns rows in order, and, once finished, has returned
  every row. [`specs/table-listing.ts`](../../specs/table-listing.ts) model-checks this while the
  shard splits, the copy waits for activation, and cleanup runs, including with a shard asked for
  less than a page.
- Index keys, versions and counts change in the same transaction as the row.

### Compatibility and migration

Schema changes are applied in place on first use. Storage written by earlier versions is migrated
the same way.

### Rollout and validation

- Tests under `cf dev` (`test/table.test.ts`) cover:
  - conditional writes and versions;
  - listings every way against a reference sort, across splits and with cleanup held back;
  - sparse filters with a small budget;
  - counts and count groups through every kind of write, across splits;
  - schema changes on a live object.
- `test/index-key.test.ts` checks the encoding on 20,000 random tuples.
- Benchmarks are in [docs/performance.md](../performance.md#sharded-tables).

## Drawbacks

**Listings do not scale with shards.** Every page asks every shard, so listing throughput stays
near one object's (about 370–400 pages/s at 1, 4 and 16 shards), and a page waits for its slowest
shard. At 64 in flight a page's median is 805 ms on 64 shards against 231 ms on one. Keep
list-heavy tables to 16 shards or fewer, or use the global indexes of
[RFD0003](RFD0003-indexed-table.md).

## Rationale and alternatives

- **A cursor with a position per shard.** It would let each shard resume exactly, but it grows
  with the shards, and breaks when they split.
- **Offset pagination.** It skips and repeats rows as rows are added, deleted, and moved by splits.
- **Counting with a scan.** It is exact but reads every row, which is what the per-prefix counts
  avoid.

## Prior art

Keyset pagination with a total order (index columns plus the primary key) is the standard way to
page without offsets. The k-way merge with a frontier is the scatter-gather of sharded query
routers (Vitess, Citus), made safe across splits by ownership filtering.

## Unresolved questions

### Out of scope

- Indexes partitioned by sort order, unique constraints and lookups across shards:
  [RFD0003](RFD0003-indexed-table.md).
