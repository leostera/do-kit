# Performance

Everything here was measured on Cloudflare's network, on a Workers paid plan, using the benchmark Worker in [`bench/`](../bench/README.md) (compatibility date 2026-10-29, with `durable_object_snapshots`). Raw results are in [`bench/results/`](../bench/results/), and the graphs are regenerated from them with `bun run bench:plot`.

## How long a split takes

The split benchmark builds one object with N MiB of 1 KiB rows. Then, with 8 writers on random keys running from a second before until a second after, it splits that object once. It compares three ways to split:

- **Sharded object.** Only the right half moves, as one storage snapshot restored into one new object. Reads and left-half writes go on during the copy.
- **Freeze, snapshot into two new objects.** The object stops serving, restores its snapshot into two children, which each delete the half they do not own, and stays behind as a router.
- **v0.1: copy rows to two new objects.** Children serve at once and fetch rows from the parent as calls need them, while a background copy streams the rest in batches of 1,000 rows.

![Time to split vs data size](bench/split-time.svg)

| Data | Sharded object | Freeze, two snapshots | v0.1 row copy |
|---|---|---|---|
| 1 MiB | 1.9 s | 1.5 s | 1.4 s |
| 16 MiB | 2.0 s | 3.0 s | 7.4 s |
| 128 MiB | 5.7 s | 24.3 s | 235 s |
| 1 GiB | 26.7 s | failed | did not finish in 2 h |

"Time to split" is the time until the new owner serves with all of its data:
- **Sharded object:** until the new object has claimed its copy. Deleting the half each side gave away continues in the background: 44 s at 128 MiB, during which those rows are invisible.
- **Freeze:** the whole freeze.
- **Row copy:** until the last row has moved.

At 1 GiB, the sharded object served after 26.7 s. During the run, writes had a p50 of 140 ms, a p95 of 379 ms and a p99 of 500 ms, with no failures. The slowest write, 26.2 s, was one to the moving half that waited for the copy. Deleting the half each side gave away then took 11 minutes in the background. The freeze-into-two design gave up on the split (its single-transaction cleanup of half a million rows per child is the likely cause). The v0.1 row copy was still copying after two hours.

### What callers feel during a split

![Slowest write during a split](bench/split-latency.svg)

![Write latency p95 during a split](bench/split-p95.svg)

Measured from 1 s before the split until 1 s after it settles:

| Data | Sharded object p95 / max | Freeze p95 / max | Row copy p95 / max |
|---|---|---|---|
| 1 MiB | 69 ms / 1.9 s | 42 ms / 1.5 s | 54 ms / 0.2 s |
| 16 MiB | 102 ms / 2.0 s | 116 ms / 3.0 s | 264 ms / 1.6 s |
| 128 MiB | 179 ms / 5.4 s | 42 ms / 24.3 s | 2.4 s / 11.6 s |

- **Snapshots trade a short wait for a short split.** With either snapshot design, the slowest write is one that waited for the copy. With the sharded object, only writes to the half that moves wait. With the freeze, every call waits, reads too.
- **The row copy never blocks outright, but it is slow and loads the busy object.** Its children ask the parent for rows on every new key while the background copy competes with those requests. That is why its p95 is 2.4 s for the four minutes it takes at 128 MiB.
- **Deleting rows has a cost too.** A delete blocks the object while it runs, and takes longer the more rows it deletes. Each object deletes the half it gave away 250 rows at a time, 25 ms apart. Deleting in large batches stalls every write for as long as each batch takes.

No write failed in any split run.

## Throughput against one Durable Object

The throughput benchmark sends one-key writes of 1 KiB values to uniform keys from driver Durable Objects in several regions, at increasing concurrency. Each driver keeps at most 16 calls in flight, so 2,048 in flight means 128 drivers. Each level runs once unmeasured, then once measured for 10 s. The object under test is either one plain Durable Object, or a sharded object presplit into 16 shards, so no split happens while it is measured.

![Throughput vs concurrency](bench/throughput.svg)

| In flight | One Durable Object | Sharded object, 16 shards |
|---|---|---|
| 1 | 43 writes/s, p50 14 ms | 9 writes/s, p50 69 ms |
| 16 | 283 writes/s, p50 37 ms | 173 writes/s, p50 71 ms |
| 64 | 626 writes/s, p50 49 ms | 683 writes/s, p50 67 ms |
| 256 | 1,208 writes/s, p50 87 ms, p99 1,160 ms | 1,438 writes/s, p50 104 ms, p99 817 ms |
| 1,024 | 1,499 writes/s, p50 613 ms, p99 1,334 ms | 5,207 writes/s, p50 103 ms, p99 709 ms |
| 2,048 | 1,435 writes/s, p99 1,822 ms, **26,816 calls refused** | 9,960 writes/s, p50 126 ms, p99 765 ms, none refused |

Reads, in the same way:

| In flight | One Durable Object | Sharded object, 16 shards |
|---|---|---|
| 1 | 66 reads/s, p50 15 ms | 103 reads/s, p50 10 ms |
| 64 | 1,003 reads/s, p50 34 ms | 1,181 reads/s, p50 34 ms |
| 256 | 1,230 reads/s, p50 111 ms | 1,839 reads/s, p50 85 ms |
| 1,024 | 1,653 reads/s, p50 479 ms, p99 1,300 ms | 4,587 reads/s, p50 106 ms, p99 1,088 ms |
| 2,048 | 1,425 reads/s, p99 1,837 ms, **16,901 calls failed** | 8,182 reads/s, p50 118 ms, p99 1,129 ms, none failed |

![Write latency p99 vs concurrency](bench/latency-p99.svg)

![Failed calls vs concurrency](bench/errors.svg)

![Data throughput vs concurrency](bench/bandwidth.svg)

- **One object saturates at about 1,500 writes/s.** Past that, it queues, so its median latency at 1,024 in flight is 613 ms. Then it refuses calls: 26,816 in 10 s at 2,048 in flight. (Its 17 ms median at that level is misleading, because most of those calls failed fast.)
- **16 shards keep their median latency near 100 ms** while writes grow from 1,438/s to 9,960/s, and they had not saturated at the highest load we offered.
- **At low load, one object can be faster.** With one call in flight, a shard answered in 69 ms against 14 ms. A new shard is placed near the object that created it, not near the caller, so some keys live farther away. Splitting pays off when one object is the bottleneck, not before.
- **The load generator matters.** A driver Durable Object completes about 90 calls/s, however many it keeps in flight. An earlier run with 64 calls per driver capped every target near 1,500 writes/s, and made splitting look useless. The graphs above use the fixed harness.
- **Read runs seed their keys from one driver, with retries.** When a hundred drivers seeded at once, they overloaded the single object before it was measured.

More graphs from the same run: [latency p50](bench/latency-p50.svg), [p95](bench/latency-p95.svg), [max](bench/latency-max.svg), and [storage ceiling](bench/capacity.svg) (shards × the 10 GB limit of one object).

## A control plane: a handful of hot objects

Ten objects, `config-0` to `config-9`, with Zipf popularity (s = 1.1): the most popular gets about a third of the calls. 95% of calls read and 5% write 1 KiB.

![Control plane: throughput vs concurrency](bench/control-plane-throughput.svg)

![Control plane: p99 latency vs concurrency](bench/control-plane-p99.svg)

![Control plane: failed calls vs concurrency](bench/control-plane-errors.svg)

| In flight | One Durable Object | Sharded object, splitting by load | Sharded object, 16 shards |
|---|---|---|---|
| 1 | 37/s, p50 26 ms | 5/s (p99 8.7 s: splitting began) | 132/s, p50 8 ms |
| 16 | 206/s, p50 56 ms | 164/s, p50 79 ms | 321/s, p50 37 ms |
| 64 | 558/s, p50 68 ms | 377/s, 2 shards | 1,100/s, p50 36 ms |
| 256 | 633/s, p50 239 ms, p99 1,165 ms | 745/s, 6 shards, p99 6.3 s | 1,849/s, p50 78 ms, p99 977 ms |
| 1,024 | 867/s, p50 1,023 ms, p99 1,999 ms | 2,894/s, 15 shards | 3,388/s, p50 191 ms, p99 963 ms |
| 2,048 | 665/s, **43,951 calls failed** | 3,081/s, 25 shards, 11,175 failed | 3,803/s, p50 400 ms, p99 1,692 ms, none failed |

- **Even ten keys gain from splitting.** They land on different shards, so the object's work is shared.
- **The most popular key sets the ceiling.** A key always lives on one shard, and here one key takes a third of the calls. That shard runs out of capacity near 3,800 calls/s in total.
- **Splitting by load overshoots.** It went to 25 shards for 10 keys. Splits that leave a hot key where it was do not help, but they still pause writes to the half that moves. The split policy should stop splitting a shard whose load comes from one key. That is not done yet.

## Cost

These are estimates from the [published Durable Objects prices](https://developers.cloudflare.com/durable-objects/platform/pricing/) (Workers Paid, beyond included usage) and the row counts of the code.

![Estimated cost per million writes](bench/cost-writes.svg)

- **Requests:** one per write, in both cases ($0.15 per million).
- **Rows written:** one per write. A new key costs one more in a sharded object, for the index on its hash ($1.00 per million rows).
- **Rows read:** a sharded object reads each row before writing it ($0.001 per million rows).
- **Duration:** billed per object while it is busy, at 128 MB. At the measured peaks, 1,499 writes/s for one object and 9,960 writes/s for 16 shards, that is $0.0010 and $0.0025 per million writes.

![Estimated cost of one split](bench/cost-split.svg)

A split is billed as the rows it writes:
- **The sharded object** deletes the half each side gave away, at two rows written per row (table and index). The snapshot restore is done by the storage layer and is not billed as rows written.
- **The freeze into two** also deletes everything from the parent.
- **The v0.1 row copy** inserts every row into a child, tracks each key, and deletes it from the parent.

With 1 KiB rows, a split costs about $0.002 at 1 MiB, $0.03 at 16 MiB, $0.26 at 128 MiB, $2.10 at 1 GiB and $21 at 10 GiB.

The index is the one cost a plain object does not have. It makes deletes and range scans by hash cheap in rows read. Rows read cost a thousandth of rows written, so a version without it is worth measuring.

## Reference: what one Durable Object takes

A separate load test of no-op Durable Object RPC calls topped out at about 1,800 to 2,800 calls/s per object. With real work, about 1,000 events/s per object is a sensible starting budget. The default split threshold, 400 weighted calls/s, sits below that, to leave room for the split itself.

## Running it yourself

See [`bench/README.md`](../bench/README.md).
