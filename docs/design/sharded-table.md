# Design: ShardedTable

Status: accepted, being built. Layer 2 of do-kit, on top of `ShardedObject`.

## What it is

A table of typed rows spread over the shards of a sharded object. The primary key is also the
routing key, so a row lives on one shard. Each shard keeps the table's indexes for its own rows,
and a listing merges the shards' pages into one sorted page with a cursor.

It is what most users of a sharded object build by hand: a registry, a control plane, a set of
records with lists sorted several ways. With no indexes, it is a key-value store.

## Goals

- Get, put, patch and delete rows by primary key, one or many, with a version per row and
  compare-and-set on it.
- Indexes declared in the schema, kept by every shard for its own rows, in the same transaction
  as the write.
- `list({ index, direction, where, limit, cursor })` across every shard: sorted, paged, with an
  opaque cursor that survives splits. Across pages, no row is skipped or returned twice, while
  shards split and clean up.
- Bounded work per page: a shard scans at most a budget of rows per page, so a sparse filter
  returns a short page and a cursor rather than scanning a whole shard.
- A row count per object, exact while no split is in flight and cheap: no full scan.
- Subclassable: custom transactional methods use the same row helpers inside `transact`.

## Not goals (yet)

- Unique constraints on columns other than the primary key, and lookups by them across shards:
  the index layer, next.
- A snapshot across pages. Each page reflects its own moment, as any keyset pagination does.
- Counts of filtered rows. Count the rows a listing returns, or keep your own counter.
- Joins, and transactions across rows on different shards.

## Schema

```ts
const repos = defineTable({
  primaryKey: "name",
  columns: { name: "text", id: "text", createdAt: "integer", lastPushAt: "integer", deleted: "boolean", meta: "json" },
  indexes: { byCreated: ["createdAt"], byPush: ["lastPushAt"], byId: ["id"] },
});
```

- Column types: `text`, `integer`, `real`, `boolean` (stored 0/1) and `json` (stored as text,
  not indexable). Every column but the primary key may be null.
- The primary key is a `text` column. It is what `keyHash` routes on.
- An index is a list of columns. The primary key is appended to every index, so the order is
  total and a cursor names one position.
- Rows are stored in real SQLite columns, in table `sharded_table`, with two more: `_hash` (the
  routing hash of the key) and `_version`. Each index is an SQL index `(columns…, key)`.

### Schema changes

Each shard brings its storage up to the schema the first time it runs code of this version:
missing columns are added (`ALTER TABLE ADD COLUMN`), declared indexes created, and indexes no
longer declared dropped. Shards that split later carry the table with them in the snapshot.
Removing or retyping a column is not supported: add a new one.

## Writes

- `put(row, { ifVersion })` inserts or replaces a whole row. `patch(key, changes, { ifVersion })`
  changes some columns. `delete(key, { ifVersion })` deletes. Each returns the row's new version,
  or the current version when `ifVersion` did not match. `ifVersion: 0` means "only if absent".
- Every write runs in the core's `transact`: it waits while a split copies the key, checks the
  shard still owns it, and is one SQLite transaction with the index entries and the count.
- Versions start at 1 and grow by 1 on every write of that row.

## Listing

The cursor is the sort position of the last row returned: the index's column values and the
primary key, plus a fingerprint of the query so it cannot be reused with another one. It holds
no per-shard state, so it does not care how many shards there are, or whether they split since
the last page.

A page:

1. The client asks every shard (the core's `scan`) for up to `limit` rows after the cursor, in
   index order, matching `where`, scanning at most `budget` rows.
2. Each shard answers its rows and a **frontier**: the position up to which it has reported
   every match. That is the last row it returned if it returned `limit`, the last row it scanned
   if it ran out of budget, or "the end" if it ran out of rows.
3. The client merges. Every shard has reported everything up to the smallest frontier, so the
   rows up to it are complete. It returns those, sorted, up to `limit`, and the new cursor is the
   last row returned, or the frontier when it returned fewer. The listing is done when every
   shard reached the end and nothing is left.

Why no row is skipped or duplicated across a split:

- **A row is owned by one shard at any moment**, and a shard returns only rows it owns, even
  while cleanup has not deleted the rows it gave away (`_hash` within its owned range).
- **The new shard does not answer until it is active**, after the original stopped owning the
  half (it answers "not ready" and the client retries), and **the original answers "moved"**
  for the half it gave away, so a page always covers each part of the key space once.
- Both children of a split hold subsets of the same ordered set, so a cursor from before the
  split is a valid position in both.
- The client also drops a key it sees twice in one page, which costs nothing.

The guarantee is for rows whose sort columns do not change during the listing. A row whose sort
value changes between pages moves in the order, and may be seen twice or not at all, as with any
keyset pagination.

Nulls sort first, as in SQLite: first ascending, last descending. The client compares positions
exactly as SQLite does: null, then numbers, then text by code point.

The cost of a page is one call per shard, so listings slow down as an object splits. A smaller
per-shard limit (from what earlier pages returned), or skipping shards with nothing after the
cursor, can come later.

## Counting

Each shard keeps a count of rows per 16-bit hash prefix (`sharded_table_counts`), updated in the
write's transaction. A shard's count is the sum over the prefixes in its range: at most 65,536
small rows, usually far fewer, and exact at every depth up to 16. After a split both sides still
store all prefixes, but each sums only its own range, so there is nothing to recompute. The
object's count is the sum over shards. It may count a row twice or not at all for the instant of
a split commit.

## API

```ts
// The Durable Object
export class Repos extends ShardedTable<Env, Repo> {
  protected table = repos;
  protected nodes() { return this.env.REPOS; }
}

// A Worker
const t = table(env.REPOS, "acct:namespace");
await t.put({ name: "foo", id: "r1", createdAt: Date.now(), lastPushAt: null, deleted: false, meta: {} }, { ifVersion: 0 });
const row = await t.get("foo");
const page = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "deleted", op: "=", value: false }], limit: 50 });
const next = await t.list({ ...sameQuery, cursor: page.cursor });
```

`where` conditions: `=`, `!=`, `<`, `<=`, `>`, `>=`, `isNull`, `notNull`, `prefix`, and
`contains` (case-insensitive substring). They are ANDed.

## How it is checked

- `specs/table-listing.ts`: a pager walks a listing page by page while the shard splits, the copy
  waits to be activated and cleanup runs. Properties: no row is returned twice, and a finished
  listing returned every row, in order.
- Tests under `cf dev`: writes and versions, schema changes, listing every way across a split
  and during cleanup, sparse filters with a small budget, counts.
- Benchmarks on the edge: list, get and put throughput and latency at 1, 4, 16 and 64 shards
  against one Durable Object with the same SQL table and indexes.
