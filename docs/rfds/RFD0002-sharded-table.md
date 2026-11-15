# RFD0002 - Sharded tables

- Feature Name: `sharded-table`
- Status: Implemented (snapshot)
- Author: `leostera`
- Start Date: `2026-10-29`
- Updated: `2026-11-15`
- Discussion PR: none (developed on `main`)
- Tracking Issue: none

This is a snapshot RFD: it records the contract of `ShardedTable` as built in
[`src/table/`](../../src/table/). It replaces the earlier design note, `docs/design/sharded-table.md`.
[RFD0012](RFD0012-shard-local-tables.md) removed what asked every shard: the listing in query order
(a merge of one page from every shard), `withCount`, `count()`, `countBy()` and count groups.
Listings walk the shards ([RFD0009](RFD0009-walks-in-shard-order.md)).

## Summary

A `ShardedTable` is a table of typed rows on a sharded object ([RFD0001](RFD0001-sharded-object.md)).
The primary key routes a row to its shard. Each shard keeps the table's indexes for its own rows,
and a listing walks the shards, each shard's rows in the query's order, one shard a page, with a
cursor that survives splits. Writes are conditional on a version per row. Every call reaches the
shards its keys are on, or one shard a page.

## Motivation

Most users of a sharded object build the same thing by hand: a directory, a control plane, records
listed several ways. They need typed rows, indexes, create-if-absent, compare-and-set, and listings
with a cursor. Built on raw keys and values, every one of them has to get pagination across
splits right. With no indexes, a sharded table is a key-value store.

## Goals

- Get, put, patch and delete rows by primary key, one or many, with a version per row and
  compare-and-set on it.
- Indexes declared in the schema, kept by every shard for its own rows, in the same transaction
  as the write.
- `list({ index, direction, where, limit, cursor })` over every shard in turn: paged, each
  shard's rows in order, with an opaque cursor that survives splits. Across pages, no row is skipped
  or returned twice while shards split and clean up.
- Bounded work per page: one shard a page (up to three, when one runs out partway), and a shard
  scans at most a budget of rows per page.
- No call whose number of shard calls grows with the number of shards, but the operators'
  `verifyCounts`.
- Subclassable: custom transactional methods use the same row helpers inside `transact`.

## Non-goals

- Unique constraints on columns other than the primary key, and lookups by them across shards:
  [RFD0003](RFD0003-indexed-table.md).
- A snapshot across pages. Each page reflects its own moment, as any keyset pagination does.
- An order or a count across the whole table: [RFD0012](RFD0012-shard-local-tables.md) gives them
  to `IndexedTable`'s global indexes.
- Joins, and transactions across rows on different shards.

## Guide-level explanation

```ts
type Order = { id: string; customer: string; status: string; createdAt: number };
const orders = defineTable<Order>({
  primaryKey: "id",
  columns: { id: "text", customer: "text", status: "text", createdAt: "integer" },
  indexes: { byCreated: ["createdAt"] },
});

export class Orders extends ShardedTable<Env, Order> {
  protected table = orders;
  protected nodes() { return this.env.ORDERS; }
}

// A Worker
const t = table(env.ORDERS, "shop-42", orders);
await t.put({ id: "o_1001", customer: "ada", status: "pending", createdAt: Date.now() }, { ifVersion: 0 }); // only if new
// Newest first within each shard, one shard a page.
const page = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "status", op: "!=", value: "cancelled" }], limit: 50 });
const next = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "status", op: "!=", value: "cancelled" }], limit: 50, cursor: page.cursor });
```

`where` conditions are `=`, `!=`, `<`, `<=`, `>`, `>=`, `isNull`, `notNull`, `prefix`, and
`contains` (substring, ignoring ASCII case). They are ANDed.

## Reference-level explanation

### Schema

- Column types: `text`, `integer`, `real`, `boolean` (stored 0/1) and `json` (stored as text,
  not indexable). Every column but the primary key may be null.
- The primary key is a `text` column, and is what `keyHash` routes on.
- An index is a list of columns. The primary key is appended to every index, so the order is
  total and a cursor names one position.
- Rows are stored in real SQLite columns, in table `sharded_table`, with `_hash` (the routing
  hash of the key) and `_version`.
- A value comes back as it went in, or the write is refused, naming the column: NaN, an integer
  past 2^53, text that is not well-formed UTF-16 (SQLite would store U+FFFD, and a key read back
  would route elsewhere), and in `json` columns anything `JSON.stringify` changes (non-finite
  numbers, `undefined`, functions, bigints, a `Date` or other non-plain object, at any depth).
- **Limits.** A table has at most 100 columns with its hidden ones (`_hash`, `_version`):
  SQLite's limit in a Durable Object, checked by `defineTable`. Indexes take no column. A listing takes at most 32
  conditions. A row is at most `maxRowBytes` (default and maximum 2 MB, SQLite's row limit), text
  counted as UTF-8: a larger one throws `RowTooLarge` and writes nothing.

**Schema changes.** Each shard brings its storage up to the schema the first time it runs code
of that version, with cheap statements only: it adds missing columns, makes an empty key table for
each new index (see Positions and index keys) and records the build, renames the key tables of
indexes no longer declared out of the way. Removing or
retyping a column is not supported: add a new one. A cursor is refused once its index is
redefined.

What is proportional to the rows is done in build steps, each one transaction of at most
`indexBuildRows` (10,000) rows: filling in a new index's keys in key order, deleting a dropped
index's keys (and then its empty table). Each call takes a step (the call that
migrates takes none), and so does the alarm, so a shard that gets few calls still finishes. A
step that fails is rolled back and taken again; it never fails the call. Meanwhile the shard
serves gets, writes and other listings: only a listing by an index being built answers "not
ready". On a shard with no rows, what is new is built at once.

No statement of a schema change touches more than a step of rows. That is why an index keeps
its keys in a table of its own rather than a column of the row table: SQLite makes an index over
a table with rows only by reading all of them, while an empty table is filled a page at a time.
An index declared again while its old keys are still being deleted starts from a new empty
table. If the migration's request for the alarm fails, the next call asks again.

Shards written by releases before key tables kept each index as a hidden column of the row table
with an SQL index on it. Their migration drops those SQL indexes, builds the key tables like any
new index (listings by an index answer "not ready" until it is built), and clears the old
columns to null in steps, to free their space. The columns stay: dropping one rewrites every row.

A shard that has nothing left to build records the schema it migrated to. Woken with the same
schema, it reads that record, one statement, instead of checking every table and index.

**Versions and rollbacks.** `version` in the schema is a whole number, raised with every change.
A shard records the highest it ran. Code with a lower one (a rollback, or a deploy halfway)
leaves the indexes it doesn't know alone, and marks every row it writes. The newer code then
refreshes the marked rows a step at a time (their index keys, and an IndexedTable's global
entries), with listings by an index waiting until done, instead of rebuilding. Without versions every schema is 0, and each deploy drops what it no longer
declares.

### Writes

- `put(row, { ifVersion })` inserts or replaces a whole row. `patch(key, changes, { ifVersion })`
  changes some columns. `delete(key, { ifVersion })` deletes.
- Each returns `{ ok, version }`: the row's new version, or its current version when `ifVersion`
  did not match. `ifVersion: 0` means "only if absent".
- Every write runs in the core's `transact`. It waits while a split copies the key, checks the
  shard still owns it, and is one SQLite transaction with the index keys and the row counts. A put
  of a row that exists updates it in place, and touches an index only if its key changed.
- The client sends a batch 1,000 rows a call at most, the calls at once: a shard's part of a
  larger batch is several transactions.
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

A listing walks the shards: from the cursor's shard, a page asks one shard for the rows after the
cursor's position in it, and moves on to the next shard when that one runs out (up to three shards
a page). Rows come in the query's order within each shard, shard after shard. A walk begun without
a cursor starts at a random shard and wraps around, so walks begun together spread over the
shards. [RFD0009](RFD0009-walks-in-shard-order.md) has the cursor (the shard, the position in it,
and a floor for each shard that split under the walk) and why it lists every row that existed
when the walk began once, through splits.

A shard's page:
- Rows in index order after the position, scanning at most `budget` rows, up to the rows asked for,
  or rows holding 4 MiB of text (it says `full`, and the client ends its page there). Its
  **frontier** is the position up to which it reported every match: its last row, the last row
  it scanned if it ran out of budget, or the end.
- Only rows it owns (`_hash` in its range), even while cleanup has not deleted the rest.
- Read in statements each read to its end before the next, seeking from the last row seen: the
  first of the rows the page asks for, each after it twice the one before, up to 64. SQLite refuses
  DROP and ALTER TABLE while any statement on the connection is unfinished, and a cursor left
  part-read stays open until it is collected: a page that stopped early once blocked its shard's
  key-table drops, compactions and schema changes, the alarm failing on them again and again.
- A shard whose listings scanned 200,000 rows this second (`scanRowsPerSecond`, which a class may
  change) answers the next "not ready" until the second is over, and the client backs off: a
  burst of wide listings can't take the shard from its writes.

The cursor carries a fingerprint of the query (the index and its columns, the direction and the
conditions), so it cannot continue another query. A row whose sort value changes during the walk
may be seen twice or not at all, as with any keyset pagination.

Until RFD0012, a listing could also be in the query's order across the whole table: each page
asked every shard for its share and merged them up to the smallest frontier. Every shard served
every page, so it got slower as shards were added, and it was removed.

**Positions and index keys.** Each index keeps a table of keys, one per row (`sharded_table_keys_*`:
`_key`, its primary key, with the row's primary key and routing hash, and an index on the row's
key for refreshes and split cleanup). A row's index key is the index's values and the primary key
in an order-preserving encoding, as hex.
- Null is `00`.
- A number is `01` and an order-preserving float64.
- Text is `02`, its UTF-8 with `00` escaped as `00 FF`, then `00`.

After a split, each key table is copied in key order into a dense one that takes its place, so
the half given away frees its pages (RFD0011). Rows are stored at a rowid from their route hash,
for the same reason.

A page is one range seek in the key table (`_key > cursor`), and a lookup of each row by its
primary key. A write changes an index's keys only if the row's key in it changed. Comparing the index columns one by one with null-aware `OR`s could
not seek, and got slower as the table grew. A property test checks the encoding against SQLite's
order on random tuples.

### Row counts

- Each shard keeps rows per 16-bit hash prefix (`sharded_table_counts`), updated in the write's
  transaction. A shard's rows are the sum of the prefixes in its range, exact at every depth up to
  16 (the table's `maxDepth` limit). After a split both sides store all prefixes but sum only
  their own. Splits read them to decide and to name their halves, with no row read, and a subclass
  reads its shard's with `ownedRowCount()`.
- The client counts nothing across shards (RFD0012).
- **Repairs.** Counts are kept by the library's writes only. A subclass that writes the table
  directly, or a migration gone wrong, can make them drift. `client.verifyCounts(budget)`, for
  operators, recounts each shard's buckets from its rows, one shard at a time, a slice of at most
  `budget` rows a call by the hash's index (a bucket holding more, as a route's can, is counted
  whole), and sets each drifted bucket to what its rows say.

### Invariants

- A listing never returns a row twice, returns each shard's rows in order, and, once finished, has
  returned every row. [`specs/shard-walk.ts`](../../specs/shard-walk.ts) model-checks this while
  shards split under the walk, before and after cleanup.
- Index keys, versions and row counts change in the same transaction as the row.

### Compatibility and migration

Schema changes are applied in place on first use. Storage written by earlier versions is migrated
the same way.

### Rollout and validation

- Tests under `cf dev` (`test/table.test.ts`) cover:
  - conditional writes and versions;
  - listings every way against a reference sort within each shard, across splits and with cleanup
    held back;
  - sparse filters with a small budget;
  - row counts through every kind of write, across splits;
  - schema changes on a live object.
- `test/index-key.test.ts` checks the encoding on 20,000 random tuples.
- Benchmarks are in [docs/performance.md](../performance.md#sharded-tables).

## Drawbacks

**No order across the whole table.** A listing's order holds within each shard only. An order, or
a count, across the table needs an `IndexedTable`'s global index, a second class and another call
per write ([RFD0012](RFD0012-shard-local-tables.md)).

## Rationale and alternatives

- **A cursor with a position per shard.** It would let each shard resume exactly, but it grows
  with the shards, and breaks when they split.
- **Offset pagination.** It skips and repeats rows as rows are added, deleted, and moved by splits.
- **A listing in the query's order across every shard** (built until RFD0012): every shard serves
  every page, so it does not scale with shards.

## Prior art

Keyset pagination with a total order (index columns plus the primary key) is the standard way to
page without offsets. A walk shard by shard is the parallel scan of DynamoDB and Cassandra, made safe
across splits by ownership filtering and the cursor's floors.

## Unresolved questions

### Out of scope

- Indexes partitioned by sort order, unique constraints and lookups across shards:
  [RFD0003](RFD0003-indexed-table.md).
