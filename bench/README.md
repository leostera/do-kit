# Benchmarks

The numbers and graphs in the [README](../README.md) and [docs/performance.md](../docs/performance.md) come from here. You can run them on your own account.

## What is in the Worker

`worker/` is a Worker with SQLite-backed Durable Objects that all serve the same one-key `put`/`get` interface:

| Target | What it is |
|---|---|
| `single` | One plain Durable Object, the baseline. |
| `split` | Sharded object, splitting by load (400 calls/s per shard). |
| `presplit` | Sharded object, starting as 16 shards: steady-state capacity with no split while measuring. |
| `legacy` | v0.1 of this library (then `splittable-object`), which split by streaming rows to two children (`worker/src/legacy/`, for comparison only). |
| `table-1`, `table-4`, `table-16`, `table-64` | A `ShardedTable` (10,000 rows of 1 KiB, two indexes) starting as that many shards and never splitting: `write` (put), `read` (get) and `list` (a page of 50 newest first, walking a cursor). Drivers scale with the calls a page makes. |
| `two-copy` | A design that freezes, snapshots into two children and stays as a router (`worker/src/two-copy/`, for comparison only). |
| `indexed-1`, `indexed-4`, `indexed-16` | An `IndexedTable` on the same rows, with a unique global index on email and one on (team, createdAt), its rows and index entries each on that many shards. Adds `create` (new rows only if absent), and its reads and listings go through the global indexes. |
| `reader-single`, `reader-leased` | One hot key, read by the drivers as an app's isolates would: from one plain Durable Object, or with leased reads (`worker/src/leased.ts`: a source, its relays, and a sub-lease kept in the driver's memory). Take `read` and `mixed`. A point on `reader-leased` also reports what reached the source. |

Operations besides `read` and `write`: `list` and `create` (table targets); `atomic-2` and `atomic-4`, one `atomically` put of that many distinct keys, against `multi-2` and `multi-4`, the same put as one plain call (`presplit` or `split`; with `hot`, keys come from a pool of 8); and `mixed`, reads with 1% writes (`reader-*`).

Driver Durable Objects generate the load from several regions, using `locationHint` (placement is advisory). Each driver keeps at most 16 calls in flight, because one driver completes only about 90 calls/s. With fewer drivers, you measure the drivers, not the target.

## What a run measures

`bun run bench:run` runs two suites. Pick them with `BENCH_SUITES=load,split`.

- **load:** one-key writes and reads of 1 KiB values, on uniform keys and on one hot key, at 1, 16, 64, 256 and 1,024 in-flight calls by default, and up to 2,048 (`BENCH_CONCURRENCY`; also `BENCH_TARGETS`, `BENCH_OPERATIONS`, `BENCH_DISTRIBUTIONS`). Each level runs once unmeasured, so splits settle, then for 10 s measured. It records operations/s, MiB/s, p50/p95/p99/max latency, errors, and how many shards the object had.
- **split:** for each size in `BENCH_SPLIT_MIB` (default 1, 16, 128 and 1,024 MiB), it seeds a fresh object, starts 8 writers on random keys, and splits it once. It records:
  - when the new owner served;
  - when every row lived only at its owner;
  - write latency throughout;
  - errors.

  Each split runs as a background job on the Worker that the script polls, since a large one takes minutes.

Results go to `results/latest.json` (or `results/$BENCH_RESULTS_NAME.json`) after every point, with the commit, whether the tree had changes, and the deployed Worker version (`BENCH_WORKER_VERSION`). `bun bench/collect.ts` builds the files the graphs read (`latest.json`, `table.json`, `indexed.json`, `atomic.json`, `leased.json`, `workgraph.json`) from the named runs that back the published numbers, listed in the script; `bun run bench:plot` then draws the graphs into `docs/bench/`.

A point always answers. Each driver call that has not answered in 30 s counts as failed, with its message in `errorSamples`. Every stage of a point (preparing the drivers, the run, describing the shards, counting rows) is bounded, and one that stalled is named in the point's `stalls`. With `BENCH_ACCESS_APP` set to the Worker's URL, the runner fetches a new Access token from `cloudflared` every 10 minutes, and when Access answers with its sign-in page.

## Running it

You need Bun, the `cf` CLI, and an account you are happy to fill with test objects. Runs create Durable Objects and data that are not cleaned up.

```sh
cd bench/worker
bun install
# A random token that guards the endpoints, kept out of git:
echo "BENCH_TOKEN=$(openssl rand -hex 32)" > .dev.vars
# On a custom domain of yours, behind an Access application: workers.dev is off.
BENCH_HOSTNAME=do-kit-bench.<your-zone> cf deploy --secrets-file .dev.vars
```

Then run it from the repository root:

```sh
set -a; . bench/worker/.dev.vars; set +a
BENCH_URL=https://do-kit-bench.<your-zone> BENCH_ACCESS_TOKEN=$(cloudflared access token -app=https://do-kit-bench.<your-zone>) bun run bench:run
bun run bench:plot
```

`BENCH_ACCESS_TOKEN` is a short-lived Access token. It is sent as a header and never written to results.

Results depend on placement, load, runtime version and the day. Run more than once before drawing conclusions.

## Work graphs

`bench/workgraph/` is a Worker of its own: a coordinator, runners and a results store (presplit to 16 shards), with units that burn SHA-256 rounds ([`src/graphs.ts`](workgraph/src/graphs.ts)). `bench/workgraph/run.ts` drives it, and saves every point to `results/workgraph.json`; `bun bench/workgraph/plot.ts` draws the `workgraph-*` graphs.

- **Calibration.** Rounds per millisecond, timed by the Worker around a call to a runner object, less an empty call. Pin it with `BENCH_ITERS_PER_MS` so runs from separate invocations do the same work: it varies by machine. Each point records the value it used.
- **A run** is a new graph (inputs salted by the run's name), coordinator, runners and results store: nothing is reused, and every object starts cold. Its time is the coordinator's, from `graphStart` to the last chunk done, so it includes starting the runners and every read and write of the results store.
- **Suites** (`BENCH_SUITES`): `scaling`, three shapes on 1 to 64 runners (`BENCH_SHAPES`, `BENCH_RUNNERS`, repeatable: `1,1` runs twice); `faults`, evictions and lost runners on 16 (`BENCH_FAULTS`); `ceiling`, chunks that cost nothing on up to 1,024 runners (`BENCH_CEILING_RUNNERS`); `overhead`, units that cost nothing on one runner; `slices`, coarse units with binary inputs on 4 to 64 runners (`BENCH_SLICE_RUNNERS`); `bodies`, chunk bodies of 10 MiB. `BENCH_WAKE_AFTER_MS` sets the coordinator's `wakeAfterMs` for every run, and `BENCH_RUN_TIMEOUT_MIN` how long a run may take.
- **The slices bound** comes from each run's own work: `T₁` is its `busyMs`, the runners' time on chunks, and `T∞` the plan's chunk span scaled by the same ratio of measured to planned time. A 1-runner run of that shape would take over an hour.
- **The bound** is drawn from the median single-runner time and the plan's chunk span, computed by planning the same shape (its structure is seeded).

```sh
cd bench/workgraph && bun install
echo "BENCH_TOKEN=$(openssl rand -hex 32)" > .dev.vars
cf deploy --secrets-file .dev.vars   # on a custom domain behind Access: workers.dev is off
cd ../.. && set -a; . bench/workgraph/.dev.vars; set +a
BENCH_URL=https://<its domain> BENCH_ACCESS_TOKEN=$(cloudflared access token -app=https://<its domain>) bun bench/workgraph/run.ts
bun bench/workgraph/plot.ts
```

The custom domain in `cloudflare.config.ts` is the author's; use one of yours, behind an Access application.
