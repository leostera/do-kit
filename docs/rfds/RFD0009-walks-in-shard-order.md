# RFD0009 - Walks in shard order

- Feature Name: `walks-in-shard-order`
- Status: Implemented
- Author: `leostera`
- Start Date: `2026-11-13`
- Updated: `2026-11-13`
- Discussion PR: none
- Tracking Issue: none

## Summary

`list({ order: "shard" })` lists a sharded table shard by shard: rows in the query's order within
each shard, one shard a page. A walk starts at a random shard and wraps around, and lists every
row that existed when it began exactly once, through splits. Walking a table of N shards costs
about 1/N of the calls of a listing in query order, and its throughput grows with shards where
query order's cannot.

## Motivation

A page in query order ([RFD0002](RFD0002-sharded-table.md)) asks every shard for its share, so
every shard serves every page. On the edge, on 10,000 rows of 1 KiB with pages of 50:
- query-order throughput stayed at or below one object's, 400–580 pages/s, on 1 to 64 shards;
- from 1,024 in flight, 16 and 64 shards refused most pages ("Durable Object is overloaded"). The
  client's shard error log (`logShardErrors`) named every shard, not one hot shard;
- a page costs up to 3 × shards subrequests in the caller's invocation.

Many reads of a whole table don't need the global order: exports, backfills, reindexing, scans
that aggregate. They pay for an order they throw away.

## Goals

- A page that asks one shard, and walks whose total throughput grows with shards.
- The listing guarantee of RFD0002 kept: every row that existed when the walk began, and whose
  sort columns don't change, is listed once, across splits and cleanup.
- Walks begun together do not all ask the same shard.

## Non-goals

- A global order. Rows are in the query's order within a shard, not across shards.
- Counts with the walk (`withCount`): they need every shard. Use `count()`.

## Guide-level explanation

```ts
let cursor: string | null = null;
do {
  const page = await t.list({ index: "byCreated", direction: "desc", limit: 500, order: "shard", cursor });
  await exportRows(page.rows);
  cursor = page.cursor;
} while (cursor !== null);
```

`all({ order: "shard" })` does the same as a generator. A cursor of one order is refused by the
other (the query fingerprint includes it).

## Reference-level explanation

### Data model and interfaces

The cursor holds where the walk is, and where it began:
- `leaf`: the shard being listed, as its bits;
- `after`: the position of the last row listed from it, or null for its start;
- `floor`: set when `leaf`'s shard split partway through it, `{ bits, after }`: every shard under
  `bits` is listed from after `after`;
- `start` and `wrapped`: the node the walk began at, and whether it has gone past the last shard
  back to the first.

The sharded client gains `scanShard(bits, method, ...args)`, which calls one shard if it is still a
shard and otherwise answers the shards it split into, and `knownLeaves()`.

### Lifecycle and failure semantics

A page:
1. Asks the cursor's shard for up to `limit` rows after `after` (`tablePage`, as a page in query
   order asks each shard). A shard lists only rows it owns, as in RFD0002.
2. If the shard has split since, its first half takes over, from the same position (`floor`
   remembers it for the other half). A walk that has not listed anything yet instead starts at a
   random one of the halves.
3. If the shard has more, the cursor moves to its frontier. If it has run out, the walk moves to
   the node right after its subtree: the sibling of its nearest ancestor-or-self that is a left
   half. That node exists, since a split makes both halves. Past the last shard the walk wraps to
   the root, whose first shard comes next. Back under `start`, the walk is done.
4. If the page is not full, the next shard fills it, up to three shards a page.

The next node is computed from the cursor's bits alone, not the client's map of splits: a client
in a new isolate, given another's cursor, may know fewer splits than the cursor does, and would
otherwise end the walk early.

### Invariants

- **Every row once.** A row is owned by one shard at a time. Positions are per row, and a split's
  new shard holds its rows at their positions. So a shard that splits after its rows up to `p` were
  listed has, between its halves, exactly the rows after `p` left to list, which the floor lists.
  The walk visits each node of the tree's cyclic order once, from `start` back to it.
- **Order within a shard.** Each shard's rows come in the query's order, so a walk goes back in
  that order at most once per shard.

### Rollout and validation

- Integration tests (`test/table.test.ts`): every query's walk lists every row once, with cleanup
  held back so both halves of a split store every row; the shard being walked splits partway, and
  so does one not reached yet; a walk costs at most one call a page plus one per shard that runs
  out; walks start at different shards. Each test fails without the code it covers: the floor, the
  stop after wrapping around, the random start, and one shard a page.
- Unit tests (`test/table-merge.test.ts`) of the next node, wrapping, and the floor.
- On the edge (`bench/`, operation `walk`, data in `bench/results/v050-table-list.json`):

  | 2,048 in flight | 1 shard | 4 shards | 16 shards | 64 shards |
  |---|---|---|---|---|
  | Query order | 463/s, 2,175 failed | 88/s, 152,277 failed | 77/s, 182,098 failed | 86/s, 94,481 failed |
  | Shard order | 450/s, 4,778 failed | 3,452/s | 10,979/s, p50 158 ms | 8,804/s, p50 149 ms |

  A first version started every walk at the first shard. Walks begun together then moved through
  the shards as one hot spot: 737 pages/s on 16 shards at 1,024 in flight, against 9,294/s with
  random starts.

## Drawbacks

- A second listing mode to know about. The name says what it gives up.
- A walk lists rows written during it if they land in a shard it has not reached, and not if they
  land in one it has passed. That is the same as a query-order listing's rows written behind its
  cursor, but spread over the whole table rather than one end of the order.

## Rationale and alternatives

### Simpler or narrower approach

- `scan` with a per-shard method, which exists: one result per shard, all at once, with no paging.
  It does not bound a page's size.
- Asking each shard for fewer rows per page in query order: a page still asks every shard.

### Other alternatives considered

- **Fan-out from a shard**: the caller asks one shard, which asks the rest. It moves the
  subrequests out of the caller's invocation, but every shard still serves every page.
- **Range-partitioned index shards**, so a page in query order reads one or two shards: the right
  answer for hot listings in order, and a layer of its own. Partitioned global indexes
  ([RFD0003](RFD0003-indexed-table.md)) already give one index shard per page within a partition.

### Do nothing

Whole-table reads stay capped at one object's page rate, and refused under load.

## Unresolved questions

### Out of scope

- Parallel walks of disjoint shard ranges by one caller (`start` as a parameter). A random start
  spreads independent walks already.
