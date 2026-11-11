# do-kit

**Building blocks for Cloudflare Durable Objects.** The smallest set of layers that lets you pick what fits your app: each adds one capability on top of the one below, and stops there. Use the lowest layer that does the job:

Everything is exported from `@leostera/do-kit`. Each layer also has its own path, if you want to import only that layer.

| Layer | Also at | What it gives you |
|---|---|---|
| **Sharded object** (`ShardedObject`) | `@leostera/do-kit/sharded` | One object that splits itself into shards as it gets busy. Transactions within a shard, routing that follows splits. |
| **Sharded table** (`ShardedTable`) | `@leostera/do-kit/table` | Typed rows on a sharded object: indexes in every shard, conditional writes on a version per row, counts, and sorted listings across shards with a cursor that survives splits. With no indexes, a key-value store. |
| **Indexed table** (`IndexedTable`) | `@leostera/do-kit/indexed` ([RFD0003](docs/rfds/RFD0003-indexed-table.md)) | Global indexes on shards of their own: lookups by another column, unique constraints, and listings within a partition that read one index shard per page. A write returns once its index entries are written. Not benchmarked yet. |
| **Work graphs** (`WorkGraphRunner`) | `@leostera/do-kit/workgraph` ([RFD0005](docs/rfds/RFD0005-work-graphs.md)) | Incremental, parallel execution of a graph of pure units that your app defines, too big for one invocation's CPU. Typed edges, chunks sized to the CPU budget, N runners pulling leases, results keyed by a hash of their inputs and reused by later runs. |
| **Leased reads** (`ReadLeases`, `RelayReads`, `RelayPlan`) | `@leostera/do-kit/leased` | Reads of one object's hot keys spread over relays and isolates, never stale: the object leases a key to relays, which answer from the lease and hand isolates sub-leases inside it. A write commits at once and is acknowledged once the leases on its key are over or given back, so no read misses an acknowledged write. For a key too hot for one object to answer, which splitting can't help. |

To try one, run an [example](examples): small apps for each layer, each started with `bun dev` or `npm run dev`, with a page that shows its shards splitting live.

## Sharded objects: Durable Objects that split themselves when they get busy

A Durable Object is a great home for state, but it is one object: one thread, 10 GB of storage. When a hot account, document or room outgrows it, you are left sharding by hand. A `ShardedObject` lets you write the object as if it were one, and splits it for you. When a shard stays busy, it hands half of its keys to a new object, and your Workers follow the split without you changing a line.

A split moves that half with a single storage snapshot, so it takes seconds, not minutes, and the object keeps serving while it happens.

## What it buys you over one Durable Object

![Throughput vs concurrency](docs/bench/throughput.svg)

At 2,048 writes in flight, a single Durable Object is past its limit. It completes 2,068 writes/s and refuses 23,263 calls in 10 seconds. The same object split into 16 shards completes **9,800 writes/s and refuses none**. Reads tell the same story: 1,611 reads/s with 31,785 failed calls, against **14,947 reads/s with none**.

| 1 KiB values, uniform keys | One Durable Object | Sharded object, 16 shards |
|---|---|---|
| **Throughput**, 1,024 in flight | 2,064 writes/s | **5,527 writes/s** |
| **Throughput**, 2,048 in flight | 2,068 writes/s, 23,263 calls refused in 10 s | **9,800 writes/s, none refused** |
| **Reads**, 1,024 in flight | 1,753 reads/s, median 597 ms | **7,445 reads/s, median 79 ms** |
| **Reads**, 2,048 in flight | 1,611 reads/s, 31,785 calls failed in 10 s | **14,947 reads/s, none failed** |
| **Data written**, 2,048 in flight | 2.0 MiB/s | **9.6 MiB/s** |
| **Median latency**, 1,024 in flight | 386 ms | **110 ms** |
| **p99 latency**, 2,048 in flight | 1,357 ms | **705 ms** |
| **Storage ceiling** | 10 GB | **160 GB** (10 GB per shard) |

![Write latency p99 vs concurrency](docs/bench/latency-p99.svg)

![Failed calls vs concurrency](docs/bench/errors.svg)

![Data throughput vs concurrency](docs/bench/bandwidth.svg)

These were measured on Cloudflare's network, with up to 128 load-driver Durable Objects in several regions. Each point is 10 s of one-key reads or writes. The 16-shard object was presplit, so these numbers show the capacity that splitting gives, not the cost of the splits along the way. That cost is next. More latency graphs ([p50](docs/bench/latency-p50.svg), [p95](docs/bench/latency-p95.svg), [max](docs/bench/latency-max.svg)) and the method are in [docs/performance.md](docs/performance.md).

### A control plane: a handful of hot objects

Ten configuration objects, a few far more popular than the rest (Zipf), read 95% of the time.

![Control plane: throughput vs concurrency](docs/bench/control-plane-throughput.svg)

| In flight | One Durable Object | Sharded object, splitting by load | Sharded object, 16 shards |
|---|---|---|---|
| 256 | 1,142/s, p99 939 ms | 1,668/s | 1,904/s, p99 632 ms |
| 1,024 | 1,527/s, p99 1,392 ms | 2,388/s | 3,547/s, p99 881 ms |
| 2,048 | 1,713/s, 27,650 calls failed | 4,656/s, 9,655 calls failed | 4,522/s, 10,516 calls failed |

With only ten keys, splitting still spreads them over several objects. It cannot split a single key, though, and the most popular object takes about a third of all calls, so throughput levels off near 4,700/s: the most one object can serve of that one key. At 2,048 in flight, that object is overloaded and refuses calls even with 16 shards. Splitting by load also kept splitting past the number of keys (to 29 shards for 10 keys), which cannot help. If one object is your bottleneck, look at the [control plane graphs](docs/performance.md#a-control-plane-a-handful-of-hot-objects).

## Splits take seconds

![Time to split vs data size](docs/bench/split-time.svg)

![Slowest write during a split](docs/bench/split-latency.svg)

When load splits an object, it pays for it once. Here an object seeded with 1 MiB to 1 GiB of 1 KiB rows, split once while 8 writers kept writing to random keys. No write failed in any run.

| Data in the object | Sharded object | Freeze, snapshot into two new objects | v0.1: copy rows to two new objects |
|---|---|---|---|
| 1 MiB | 1.8 s | 1.2 s | 1.1 s |
| 16 MiB | **1.7 s** | 2.1 s | 5.1 s |
| 128 MiB | **3.4 s**, slowest write 3.0 s | 13.4 s, every write blocked for it | 267 s, write p95 2.2 s throughout |
| 1 GiB | **16.9 s**, write p99 265 ms | the split failed (earlier version) | did not finish in 2 hours (earlier version) |

The times are until the new object serves with all of its data. The two older designs were not run again at 1 GiB: those two cells are from an earlier version. See [what was measured and how](docs/performance.md).

## What it costs

Splitting does not change what you pay per call: each write is one request to one shard, as it would be to one object.

![Estimated cost per million writes](docs/bench/cost-writes.svg)

| Per million writes (Workers Paid list prices) | One Durable Object | Sharded object |
|---|---|---|
| Update an existing key | $1.15 | $1.15 |
| Write a new key | $1.15 | $2.15 |

The difference is the index on each key's hash, which makes a new key cost one more row written ($1.00 per million rows). Duration is billed per object while it is busy, so 16 busy shards cost more than one object, but at $12.50 per million GB-s that comes to $0.0010 against $0.0025 per million writes.

A split costs the rows it deletes: each side deletes the half it gave away, at two rows written per row (table and index). The snapshot is made by the storage layer and is not billed as rows written. For 1 KiB rows that is about $0.03 to split 16 MiB, $0.26 for 128 MiB and $2.10 for 1 GiB, and an object usually splits long before it holds that much.

![Estimated cost of one split](docs/bench/cost-split.svg)

These are estimates from the [published prices](https://developers.cloudflare.com/durable-objects/platform/pricing/) and the row counts of the code, not from an invoice. Included monthly usage is not subtracted.

## Why it is fast

- **Only half moves.** A shard lives in the object named by its bits without trailing zeros, so when `01` splits, `010` stays where it is and only `011` goes to a new object. That means one copy, not two, and no object is left behind as a router.
- **One snapshot, not a row stream.** The new object restores from `ctx.storage.snapshot()`, a point-in-time copy of the whole database made by the storage layer. It still takes longer for a bigger database, but far less than a row-by-row copy: 5.7 s against 235 s at 128 MiB.
- **Most traffic never notices.** While the copy is made, reads go on, and so do writes to the half that stays. Only writes to the half that moves wait, and then they are sent to the new owner.
- **Cleanup stays out of the way.** Each side deletes the half it gave away in small batches in the background. Those rows are invisible from the moment of the switch.

## What you get

- **One object to write.** You write methods that read and update keys, and the base class runs them on whichever shard owns those keys.
- **Splits under load, by itself.** A shard splits when its call rate stays above a threshold. There are no shard counts to choose up front.
- **Clients that find their way.** Workers learn about splits lazily, and share what they learn in the isolate and the colo cache. A stale map costs one extra hop, never a wrong answer, because splits are final.
- **Correctness you can check.** The split protocol is an executable [Feynman](https://github.com/leostera/feynman) model, [`specs/snapshot-split.ts`](specs/snapshot-split.ts). It checks that no acknowledged write is lost or applied twice, that two objects never serve the same key, and that no read is stale, across lost messages and restarts.

## When it helps, and when it does not

**It helps** when one logical object holds many independent keys and outgrows one Durable Object: too many writes per second, or too much data for 10 GB. Each shard is a full Durable Object with its own thread and storage limit.

**It does not help:**
- **One hot key.** A key always lives on one shard, so the most popular key caps what a shard can serve. The split policy does not yet know this, and keeps splitting a shard that is hot because of one key.
- **Cross-key transactions.** `update` is transactional within one shard, and `callOneShard` refuses (before applying anything) when keys span shards. Nothing spans shards.
- **Small loads.** Below what one object can take, a plain Durable Object is simpler and as fast.

## Sharded tables: rows, indexes and listings

Most things built on a sharded object end up as a table: a directory, a control plane, records listed newest first. `ShardedTable` is that table. Declare the columns and indexes, and every shard keeps them for its own rows.

```ts
import { defineTable, ShardedTable, table } from "@leostera/do-kit";

type Order = { id: string; customer: string; total: number; createdAt: number; shippedAt: number | null; cancelled: boolean };
const orders = defineTable<Order>({
  primaryKey: "id", // also what routes a row to its shard
  columns: { id: "text", customer: "text", total: "integer", createdAt: "integer", shippedAt: "integer", cancelled: "boolean" },
  indexes: { byCreated: ["createdAt"], byShipped: ["shippedAt"] },
  counts: { byCancelled: "cancelled" }, // rows per value of a column, kept with every write
});

export class Orders extends ShardedTable<Env, Order> {
  protected table = orders;
  protected nodes() { return this.env.ORDERS; }
}

// In a Worker
const t = table(env.ORDERS, "shop-42", orders);
await t.put({ id: "o_1001", customer: "ada", total: 4200, createdAt: Date.now(), shippedAt: null, cancelled: false }, { ifVersion: 0 }); // only if new
const page = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "cancelled", op: "=", value: false }], limit: 50 });
const next = await t.list({ index: "byCreated", direction: "desc", where: [{ column: "cancelled", op: "=", value: false }], limit: 50, cursor: page.cursor });
```

- **Writes** (`put`, `patch`, `delete`, and batches) are conditional on a version per row: `ifVersion: 0` means "only if absent". Each is one transaction with its index entries, fenced against splits. A key never has the same version twice: a row written again after a delete starts above the versions of the rows its shard deleted, so a write conditional on a version read before the delete fails. That costs a delete one more row written.
- **Listings** merge one page from every shard. The cursor holds only a sort position, so it survives splits. No row is skipped or returned twice across pages while shards split and clean up; that is model-checked in [`specs/table-listing.ts`](specs/table-listing.ts) and tested against a reference sort. `where` takes `=`, `!=`, `<`, `<=`, `>`, `>=`, `isNull`, `notNull`, `prefix` and `contains` (ignoring case).
- **Counts** are kept per hash prefix with every write, so `count()` is cheap and exact at any depth up to 16. Declare `counts` groups to also count rows per value of a column: `countBy("byCancelled")` returns `[{ value: false, count: 812 }, { value: true, count: 40 }]`, just as cheaply. A listing takes `withCount: true` (or a group's name) and returns `total` (and `counts`) from the same calls to the shards, so a page with its total costs one fan-out, not two. Counts don't take `where`: counting arbitrary conditions would read every row of every shard.
- **Schema changes:** add columns and indexes in the schema; each shard applies them the first time it runs the new code.
- Custom transactional methods use `getRow`, `putRow`, `patchRow` and `deleteRow` inside `transact`.

### What a table buys you, measured

![Table writes vs concurrency](docs/bench/table-write-throughput.svg)

10,000 rows of 1 KiB with two indexes, on 1 shard (one Durable Object running the same SQL) and on 4, 16 and 64 shards from the start:

| 1,024 in flight | 1 shard | 4 shards | 16 shards | 64 shards |
|---|---|---|---|---|
| **Writes** | 1,226/s, median 688 ms | 2,109/s | 5,125/s | **5,174/s, median 124 ms** |
| **Reads by key** | 1,745/s, median 561 ms | 4,333/s | **5,555/s, median 127 ms** | not measured* |
| **List pages** (50 rows) | 578/s | 564/s | 169/s, shed load | 369/s |

\* The 64-shard read runs at 256 and 1,024 in flight never answered: a Durable Object used up its CPU time until the runtime reset it. Other runs on 0.4.0 hit the same thing at times (in the 4,096-shard benchmark too), and the cause is not known yet.

**Listings do not scale with shards.** Every page asks every shard for its share, so each shard serves every page: listing throughput stays near what one object serves (about 560–580 pages/s here), and a page waits for its slowest shard. At 64 in flight, a page's median is 61 ms on 1 shard, 126 ms on 4, 193 ms on 16 and 456 ms on 64. Keep list-heavy tables to 16 shards or fewer. Listings that must scale further need an index stored by sort order rather than by key, which is the next layer.

![Table listings vs concurrency](docs/bench/table-list-throughput.svg)

More in [docs/performance.md](docs/performance.md#sharded-tables) and the design in [RFD0002](docs/rfds/RFD0002-sharded-table.md).

## Indexed tables: global indexes

A sharded table answers "the row with this key" in one call. Anything else, such as "which user has this email?", asks every shard, and nothing stops two rows on different shards from having the same email. `IndexedTable` adds **global indexes**: they live on shards of their own, routed by the indexed values, so a lookup is one call and a unique index is enforced across the whole table.

```ts
import { defineTable, GlobalIndex, IndexedTable, indexedTable } from "@leostera/do-kit";

type User = { id: string; email: string; name: string; team: string; createdAt: number };
const users = defineTable<User>({
  primaryKey: "id",
  columns: { id: "text", email: "text", name: "text", team: "text", createdAt: "integer" },
  globalIndexes: {
    byEmail: { columns: ["email"], unique: true },
    byTeam: { columns: ["team", "createdAt"], partition: 1 }, // one team per index shard, sorted by createdAt
  },
});

export class Users extends IndexedTable<Env, User> {
  protected table = users;
  protected nodes() { return this.env.USERS; }
  protected indexNodes() { return this.env.USER_INDEXES; }
}

export class UserIndexes extends GlobalIndex<Env> {
  protected nodes() { return this.env.USER_INDEXES; }
  protected rowNodes() { return this.env.USERS; }
}

// In a Worker
const t = indexedTable(env.USERS, env.USER_INDEXES, "workspace-42", users);
await t.put({ id: "u_1", email: "ada@example.com", name: "Ada", team: "core", createdAt: Date.now() }, { ifVersion: 0 });
// { ok: true, version: 1 }
await t.lookup("byEmail", ["ada@example.com"]); // { row: { id: "u_1", ... }, version: 1 }
await t.put({ id: "u_2", email: "ada@example.com", name: "Ada L.", team: "web", createdAt: Date.now() });
// { ok: false, version: 0, conflict: "byEmail" }: nothing was written
await t.list({ index: "byTeam", partition: ["core"], direction: "desc", limit: 50 }); // one index shard per page
```

- **A write returns once every index has it.** A write reserves its unique values, writes the row and its index changes (an outbox) in one transaction, and delivers them before it returns. If delivery takes longer than `indexWaitMs` (5 s), the client throws `IndexPending`: the row is written, and the row shard's alarm goes on delivering.
- **Unique values are reserved with fencing tokens.** A reservation left behind by a writer that never wrote is resolved by asking the row's shard, which fences the token so that writer's write cannot land later. The protocol is model-checked in [`specs/indexed-write.ts`](specs/indexed-write.ts): at most one row has a value, a write that returned is found, and the indexes converge to the rows, across lost calls, timeouts and late deliveries.
- **The rows stay the truth.** Index entries carry a copy of the row, so a lookup or page needs no second call. Every write rewrites its entries, so each global index costs one more write per row write, and a write with a unique index makes one more call before it.
- Rows and index shards split by load, independently. Per-shard `indexes` and `counts` work as in `ShardedTable`. Custom methods that write rows use `indexedTransact`.
- A global index lists rows within one partition value, not sorted across the whole table, and is declared with the table: it is not added to a table that already has rows.

## Work graphs: work too big for one invocation

An invocation gets at most 5 minutes of CPU. Some work is a graph of pure pieces with far more than that in total: resolving the deltas of a large git pack, rebuilding a derived index, an import of many objects. You write the graph; do-kit cuts it into **chunks** that each fit one invocation, and **runners** (Durable Objects) pull them from a **coordinator**, one chunk per alarm. Each unit's result is stored before its chunk counts as done, under a hash of its inputs (or of its key), so a retried chunk, a runner evicted partway, and a later run all do the same thing: compute what is missing.

```ts
import { defineTask, ownResultsName, planWorkGraph, workGraphOf, workGraphResults, workGraphRun } from "@leostera/do-kit";

// A task is a pure function: (input, parent's value, results it runs after) → { result, value }.
// This one resolves a slice of a pack's delta forest: whole trees, about 10 s of CPU. It writes its
// rows to the app's own storage, under its own key (so a retry overwrites, not duplicates), and
// returns a small receipt.
const slice = defineTask({
  name: "slice", version: "1",
  async compute(input: { pack: string; trees: Uint8Array }, _parent: null, _after: {}, { env }) {
    const rows = await resolveTrees(env, input.pack, input.trees); // the app's code
    await env.INDEX.put(`${input.pack}/${sliceKey(input.trees)}`, encodeRows(rows));
    return { result: { objects: rows.length, digest: await digestOf(rows) }, value: null };
  },
});

// The graph's types follow its edges: a slice takes no parent, so passing one does not compile.
const g = workGraphOf({ slice });
const refs = slicesOf(pack).map((s, i) => g.add(slice, `pack-1234/slice-${i}`, { pack: "pack-1234", trees: s.trees }, { cost: s.estimatedMs }));

// Every pack is new: identify results by key, and skip hashing inputs.
const plan = planWorkGraph(g, { chunkMs: 30_000, identity: "key" });
const run = workGraphRun(env.GRAPH_COORDINATORS, "pack-1234");
await run.submit(plan);
await run.start({ runners: 32, deadlineMs: 15 * 60_000 });
await run.status(); // later: { state: "done", units: { computed, reused }, busyMs, ... }
await workGraphResults(env.GRAPH_RESULTS, ownResultsName("pack-1234")).get(plan, refs[0]); // { objects, digest } | undefined
await run.delete(); // or wait: an hour after it ends, nothing of the run is left
```

- **Runs leave nothing behind.** Once a run is done, failed, deleted or past its deadline, its runners delete their storage and alarms, and so do the coordinator and the run's own results store (shared ones, given as `results`, are yours to keep). The status is kept for `keepStatusMs`, an hour by default, so you can start a run per request.

- **Make units coarse.** Running a unit costs the layer about 0.11 to 0.12 ms of wall time on the edge (its result read and written, and its share of its chunk's calls), and planning it about 0.03 ms more (measured locally). So a unit should cost at least about 10 ms, for an overhead near 1%: batch fine-grained work, such as one delta of 0.1 ms, into one unit. Hundreds to tens of thousands of units per run is the range it is built for.
- **Keep outputs in your storage**, written by `compute` under the unit's own key, and return a receipt: results and values are capped at 1 MiB, as JSON.
- **Inputs may be bytes.** A `Uint8Array` anywhere in an input travels and is stored as raw bytes, up to 16 MiB per chunk. The whole graph, inputs included, is planned in one invocation (128 MB of memory), so put bulk inputs in your storage, and a pointer to them in the input.
- **Units can take a parent's value** (a tree edge) or other units' results (`after`), typed the same way; a tree too big for one chunk is cut, and the piece below either replays the path above it or loads its stored value. See [RFD0005](docs/rfds/RFD0005-work-graphs.md#guide-level-explanation) for that, and for the runner, coordinator and results classes, a few lines each.

### How close to the bound, measured

A graph's time on N runners is bounded by `T₁/N + T∞`: its work over N, plus its longest chain. Runners take whole chunks, so here `T∞` is the longest chain of chunks the plan has. Measured on Cloudflare's network, with units that burn 5 ms of CPU each, chunks of 1 s, and every run computing from scratch:

![Speedup over one runner](docs/bench/workgraph-speedup.svg)

![Time to finish a graph, 8 to 64 runners](docs/bench/workgraph-time-many.svg)

| Graph | one runner | 64 runners | speedup | `T₁/64 + T∞` |
|---|---|---|---|---|
| 40,000 units, in trees of 5 | 236.5 s | 5.1 s | 47× | 4.7 s |
| One random tree of 40,000 units | 185.0 s | 5.9 s | 32× | 3.9 s |
| One chain of 2,000 units | 9.5 s | 9.3 s | 1× | 10.1 s |

- A forest of small trees spreads across runners almost perfectly. One big tree is cut into pieces that replay the short path above them, so they need no other chunk, and it spreads too, at about 1.5× the bound. A chain is its own span: no runner count helps it.
- On one runner, the same 200 s of planned work took 179 s to 266 s across six runs, depending on the machine the runner landed on. The table uses the median. This is also why the forest beats N× at 8 and 16 runners: its median single-runner run was a slow one.
- **Faults cost what they redo.** On 16 runners, 13.6 s without faults; 21.9 s with 20% of chunks evicted once after computing (each is computed again on the alarm's retry); 16.8 s with 4 of the 16 runners lost for good, holding leases of 10 s that run out and pass on. Every unit is computed either way.
- **Coarse units come within 1.02× of the bound.** 437 slices of about 10 s with binary inputs, and a few big trees cut into pieces, in 176 chunks of 30 s: 981 s on 4 runners, 514 s on 8, 250 s on 16, 147 s on 32, and 76 s on 64, each at or under `T₁/N + T∞` from that run's own measured work ([graph](docs/bench/workgraph-slices.svg)).
- **One coordinator hands out about 450 to 630 chunks a second**, from 64 to 1,024 runners, measured with chunks that cost nothing. With chunks of 30 s that is room for over 10,000 runners on one run, and each run has its own coordinator.

Raw data in [bench/results/workgraph.json](bench/results/workgraph.json), more graphs ([time](docs/bench/workgraph-time.svg), [faults](docs/bench/workgraph-faults.svg), [coordinator](docs/bench/workgraph-ceiling.svg)), and the method in [bench/README.md](bench/README.md#work-graphs).

## Leased reads: one hot key, read through relays

Splitting spreads keys over objects, so it can't help one key that everyone reads: a ref,
a config document, a feature flag. Leased reads let the object that holds the key (the source)
lend it out for a second at a time, without ever serving a stale value.

```
writer ──► source: commit, holdUntil(changes) ──► held: reads see the value from before
                     │  toRevoke ──► relays give their leases back (returned) ──► shown, acknowledged
readers ──► isolate sub-lease ──► relay (lease from the source, renewed RENEW_MS early) ──► source
```

- **`ReadLeases<Row>`**, in the source: grants leases (1 s, ending 150 ms late by its own clock,
  the holder ending 150 ms early by its own, so only clock rates must agree), holds a write to a
  leased key until its leases are over or given back, and asks named holders to give them back.
  Before granting, it stores how far its leases may reach, so a restarted source holds every
  write until then. A key written in the last `WRITTEN_MS` (10 s) isn't leased to relays: they
  read it through, so a lease never holds the next write.
- **`RelayReads<V>`**, in a relay: answers from its lease and renews it early; without one, asks
  share a source read that started after they arrived. `fresh()` is a value at least as new as
  every write acknowledged before the ask, for writers checking the current value.
- **`RelayPlan`**, in the source: when a key is hot (`HOT_READS` direct reads a second), how many
  relays it gets (sized by the relays' reports, `RELAY_READS` each), and the tree they form
  (`relayParent`: fanout 8, at most 584 relays, so the source answers at most 8).

The layer is the logic a source and its relays embed. The objects, their storage of the horizon,
and the calls between them are the app's. It was built for git refs read thousands of times a
second while they are pushed.

## Install

```sh
npm install @leostera/do-kit
```

You need SQLite-backed Durable Objects, `nodejs_compat`, and, for snapshot splits, the `durable_object_snapshots` compatibility flag. Without that flag, or in local development (where workerd has no snapshots), splits copy rows over RPC instead. That copy is slower, but it works the same way.

## Quick start

```ts
import { ShardedObject, sharded, type UpdateResult } from "@leostera/do-kit";

interface Env {
  COUNTERS: DurableObjectNamespace<Counter>;
}

export class Counter extends ShardedObject<Env, number> {
  protected nodes() {
    return this.env.COUNTERS;
  }

  async add(keys: readonly string[], amounts: readonly number[]) {
    return this.update(keys, (current): UpdateResult<number, number[]> => {
      const writes = new Map<string, number>();
      const result = keys.map((key, i) => {
        const value = (current.get(key) ?? 0) + amounts[i]!;
        writes.set(key, value);
        return value;
      });
      return { writes, result };
    });
  }

  async get(keys: readonly string[], _payloads: null) {
    const values = await this.read(keys);
    return keys.map((key) => values.get(key) ?? null);
  }
}

// In a Worker:
const counters = sharded(env.COUNTERS, "account-123");
const shards = await counters.call(["apples", "pears"], "add", [2, 1]);
```

A method called through `client.call` takes its `keys` first and their `payloads` second (`null` when it needs none), then any other arguments. The client groups keys by shard and returns one `ShardResult` per shard, with the `positions` of its keys in your list.

- **All-or-nothing updates:** `callOneShard` runs a method on one shard, or throws `SpansShards` before applying anything.
- **Waiting:** a call waits up to 30 s for a shard that is not serving yet (`sharded(ns, name, { notReadyWaitMs })` to change it). A write held by a split in progress waits until that split finishes or gives up (`splitTimeoutMs`), however long that is.
- **Jurisdictions:** pass `env.X.jurisdiction("eu")` to `sharded` and every shard of the object stays in that jurisdiction, including the ones splits create.
- **Stub options:** `sharded(ns, name, { stubOptions: { version: { cohort } } })` passes options to every stub of the object's shards, including the stubs shards make for each other when they split.
- **Scans:** `client.scan("method")` calls a method on every shard. It can enumerate that shard's rows with `this.allRows()`. There is one result per shard, not a global snapshot.

## Split policy

```ts
protected override splitPolicy: SplitPolicy = {
  callsPerSecond: 400, // a shard splits above this, sustained over windowSeconds
  windowSeconds: 5,
  maxDepth: 12,        // at most 2^12 shards
  keysPerCall: 25,     // a call weighs 1 + keys / keysPerCall
  initialDepth: 0,     // start at 2^initialDepth shards, each made by its first call, for uniformly distributed keys; clients route straight to them
  copy: "auto",        // "native" snapshots, "logical" row copy, or auto-detect
  splitTimeoutMs: 60_000,
  holdWaitMs: 2_000,   // a write to the half being copied waits this long, then the client backs off and retries until the split ends
};
```

The default threshold is about half of what one object handles on the edge, which leaves headroom for real work and for the split itself. Tune it with your own workload.

## Writing a subclass

- `nodes()` returns the namespace that every shard of the class lives in.
- Routed methods take `(keys, payloads, ...args)` and use `read` and `update` for keyed state. The `decide` callback of `update` runs synchronously inside a transaction, so it cannot do I/O.
- A split copies **every table**, yours too. Two hooks decide what happens to your rows for keys a shard no longer owns:
  - `afterClone(owns)` runs in the new object as it claims the copy, inside that transaction. Drop work it must not repeat here, such as its share of an outbox the original keeps delivering.
  - `cleanupForeign(owns, limit)` runs in paced batches from the alarm, on both shards after a split, until it returns 0. Delete up to `limit` rows for keys you no longer own and return how many you deleted. The default deletes nothing, so an original that delivers an outbox keeps its rows.
- **Write your own tables only inside `update` or `transact(keys, fn)`.** Both wait while a split copies any of `keys`, check that the shard still owns them, and run in one transaction. A write that skips them can land after the copy and be lost. List every key whose rows you touch.
- `ownedRowsPage(afterKey, limit)` pages this shard's rows in key order, a bounded number at a time, skipping keys it gave away.
- `owns(key)` says whether a key is this shard's now. Until cleanup finishes, filter bulk queries over your own tables (listings, counts, uniqueness checks) with it.
- `beforeHandOut()` runs before the copy, while writes to the moving half wait. Use it to finish work tied to rows that are about to move.
- **Alarms: override `onAlarm()` and schedule it with `scheduleAlarm(at)`, never `alarm` or `ctx.storage.setAlarm`.** The alarm also activates new shards and runs the background cleanup. `scheduleAlarm` keeps the earliest time asked for, so your work never postpones a split's and a split never loses yours. `onAlarm` runs only on a serving shard, never in a split's copy before it serves. A split's new shard keeps the scheduled time, so check in `onAlarm` which due work is yours with `owns(key)`. Overriding `alarm` throws when the object starts.
- `shardedDescribe()` reports a shard's bits, rows and call rate, plus two things for attributing errors under load:
  - `splits`: its last 8 split attempts, each with when it started, how long it took, the copy method, the rows it owned and how many moved, the writes it held, how many of those were answered "not ready", the longest wait, and why it was abandoned, if it was. A new shard starts with an empty record.
  - `counters`: since the object last started, the writes held by splits and the longest hold, and:
    - `calls`: routed calls and scans that reached a method. `ok` of them returned; the rest answered "moved" or "not ready" from inside the method (a held write released as "moved", or held past `holdWaitMs`) or threw.
    - `moved`, `notReady`, `stale`: every such answer, both from inside a method and on arrival. Arrival answers (the caller's map was out of date) are not in `calls`.
    - Calls the runtime sheds as overloaded never reach the object, so they appear nowhere: compare what you sent with `calls` plus the arrival answers.

  Reach any shard with `env.NS.get(env.NS.idFromName(shardObjectName(name, bits))).shardedDescribe()`.

## Development

```sh
bun install
bun run check        # types
bun run build        # dist/: JavaScript and type declarations, what the package exports
bun test             # integration tests under cf dev: concurrent writes and CAS across splits, load splits, presplit, large rows, subclass tables
bun run specs:check  # model-check the split protocol
```

Design decisions are RFDs in [docs/rfds](docs/rfds/README.md): one numbered document per decision, proposals and snapshots of what is built. There are no other design documents.

Locally, splits copy rows: workerd has no storage snapshots. To run the same tests against snapshots, deploy the test Worker under another name with a token, and point the tests at it:

```sh
cd test/worker
echo "TEST_TOKEN=$(openssl rand -hex 32)" > /tmp/do-kit-test.vars   # not .dev.vars: cf dev would read it
TEST_WORKER_NAME=do-kit-test-snapshots cf deploy --secrets-file /tmp/do-kit-test.vars
cd ../..
set -a; . /tmp/do-kit-test.vars; set +a
TEST_URL=https://do-kit-test-snapshots.<your-subdomain>.workers.dev bun test --timeout 300000
```

Behind Cloudflare Access, also set `TEST_ACCESS_TOKEN` (`cloudflared access token -app=$TEST_URL`).

The benchmark Worker and the scripts that produced the graphs above are in [`bench/`](bench/README.md).

## License

MIT
