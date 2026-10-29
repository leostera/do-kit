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
| `two-copy` | A design that freezes, snapshots into two children and stays as a router (`worker/src/two-copy/`, for comparison only). |

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

Results go to `results/latest.json` (or `results/$BENCH_RESULTS_NAME.json`) after every point, and `bun run bench:plot` draws the graphs into `docs/bench/`.

## Running it

You need Bun, the `cf` CLI, and an account you are happy to fill with test objects. Runs create Durable Objects and data that are not cleaned up.

```sh
cd bench/worker
bun install
# A random token that guards the endpoints, kept out of git:
echo "BENCH_TOKEN=$(openssl rand -hex 32)" > .dev.vars
cf deploy --secrets-file .dev.vars
```

Then run it from the repository root:

```sh
set -a; . bench/worker/.dev.vars; set +a
BENCH_URL=https://<your-worker>.workers.dev bun run bench:run
bun run bench:plot
```

If the Worker is behind Cloudflare Access, set `BENCH_ACCESS_TOKEN` to a short-lived token (`cloudflared access token -app=$BENCH_URL`). It is sent as a header and never written to results.

Results depend on placement, load, runtime version and the day. Run more than once before drawing conclusions.
