# Performance

Everything here was measured on Cloudflare's network, on a Workers paid plan, using the benchmark Worker in [`bench/`](../bench/README.md) (compatibility date 2026-10-29, with `durable_object_snapshots`). Raw results are in [`bench/results/`](../bench/results/), and the graphs are regenerated from them with `bun run bench:plot`.

## How long a split takes

The split benchmark builds one object with N MiB of 1 KiB rows. Then, with 8 writers on random keys running from a second before until a second after, it splits that object once. It compares three ways to split:

- **splittable-object.** Only the right half moves, as one storage snapshot restored into one new object. Reads and left-half writes go on during the copy.
- **Freeze, snapshot into two new objects.** The object stops serving, restores its snapshot into two children, which each delete the half they do not own, and stays behind as a router.
- **v0.1: copy rows to two new objects.** Children serve at once and fetch rows from the parent as calls need them, while a background copy streams the rest in batches of 1,000 rows.

![Time to split vs data size](bench/split-time.svg)

| Data | splittable-object | Freeze, two snapshots | v0.1 row copy |
|---|---|---|---|
| 1 MiB | 1.9 s | 1.5 s | 1.4 s |
| 16 MiB | 2.0 s | 3.0 s | 7.4 s |
| 128 MiB | 5.7 s | 24.3 s | 235 s |
| 1 GiB | 16.6 s (earlier run) | — | did not finish in 2 h |

"Time to split" is the time until the new owner serves with all of its data:
- **splittable-object:** until the new object has claimed its copy. Deleting the half each side gave away continues in the background: 44 s at 128 MiB, during which those rows are invisible.
- **Freeze:** the whole freeze.
- **Row copy:** until the last row has moved.

The 1 GiB point for splittable-object comes from an earlier run of the same protocol, before cleanup was paced. A rerun is pending.

### What callers feel during a split

![Slowest write during a split](bench/split-latency.svg)

![Write latency p95 during a split](bench/split-p95.svg)

Measured from 1 s before the split until 1 s after it settles:

| Data | splittable-object p95 / max | Freeze p95 / max | Row copy p95 / max |
|---|---|---|---|
| 1 MiB | 69 ms / 1.9 s | 42 ms / 1.5 s | 54 ms / 0.2 s |
| 16 MiB | 102 ms / 2.0 s | 116 ms / 3.0 s | 264 ms / 1.6 s |
| 128 MiB | 179 ms / 5.4 s | 42 ms / 24.3 s | 2.4 s / 11.6 s |

- **Snapshots trade a short wait for a short split.** With either snapshot design, the slowest write is one that waited for the copy. With splittable-object, only writes to the half that moves wait. With the freeze, every call waits, reads too.
- **The row copy never blocks outright, but it is slow and loads the busy object.** Its children ask the parent for rows on every new key while the background copy competes with those requests. That is why its p95 is 2.4 s for the four minutes it takes at 128 MiB.
- **Deleting rows has a cost too.** A delete blocks the object while it runs, and takes longer the more rows it deletes. Each object deletes the half it gave away 250 rows at a time, 25 ms apart. Deleting in large batches stalls every write for as long as each batch takes.

No write failed in any split run.

## Throughput against one Durable Object

The throughput benchmark sends one-key writes of 1 KiB values to uniform keys from driver Durable Objects in several regions, at increasing concurrency. Each driver keeps at most 16 calls in flight, so 2,048 in flight means 128 drivers. Each level runs once unmeasured, then once measured for 10 s. The object under test is either one plain Durable Object, or a splittable object presplit into 16 leaves, so no split happens while it is measured.

![Throughput vs concurrency](bench/throughput.svg)

| In flight | One Durable Object | splittable-object, 16 leaves |
|---|---|---|
| 256 | 1,208 writes/s, p50 87 ms, p99 1,160 ms | 1,438 writes/s, p50 104 ms, p99 817 ms |
| 1,024 | 1,499 writes/s, p50 613 ms, p99 1,334 ms | 5,207 writes/s, p50 103 ms, p99 709 ms |
| 2,048 | 1,435 writes/s, p99 1,822 ms, **26,816 calls refused** | 9,960 writes/s, p50 126 ms, p99 765 ms, none refused |

![Write latency p99 vs concurrency](bench/latency-p99.svg)

![Failed calls vs concurrency](bench/errors.svg)

![Data throughput vs concurrency](bench/bandwidth.svg)

- **One object saturates at about 1,500 writes/s.** Past that, it queues, so its median latency at 1,024 in flight is 613 ms. Then it refuses calls: 26,816 in 10 s at 2,048 in flight. (Its 17 ms median at that level is misleading, because most of those calls failed fast.)
- **16 leaves keep their median latency near 100 ms** while writes grow from 1,438/s to 9,960/s, and they had not saturated at the highest load we offered.
- **The load generator matters.** A driver Durable Object completes about 90 calls/s, however many it keeps in flight. An earlier run with 64 calls per driver capped every target near 1,500 writes/s, and made splitting look useless. The graphs above use the fixed harness.
- **Not yet measured with this harness:** reads, an object that splits by load while measured (the run stopped with an error from the controlling Worker at 256 in flight), and a single hot key. A hot key cannot be split: it always lives on one leaf. In an earlier run, a hot key also kept triggering splits that could not help, which is a known gap in the split policy.

More graphs from the same run: [latency p50](bench/latency-p50.svg), [p95](bench/latency-p95.svg), [max](bench/latency-max.svg), and [storage ceiling](bench/capacity.svg) (leaves × the 10 GB limit of one object).

## Reference: what one Durable Object takes

A separate load test of no-op Durable Object RPC calls topped out at about 1,800 to 2,800 calls/s per object. With real work, about 1,000 events/s per object is a sensible starting budget. The default split threshold, 400 weighted calls/s, sits below that, to leave room for the split itself.

## Running it yourself

See [`bench/README.md`](../bench/README.md).
