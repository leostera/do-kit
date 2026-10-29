# Deployed performance benchmark

This is a separate `cf` CLI Worker project. It deploys three SQLite-backed Durable Object classes: an ordinary single-object key/value baseline, the same API built on `SplittableObject`, and distributed load-driver objects. The driver classes use advisory `locationHint`s across several regions; placement is not guaranteed.

## What a run measures

`bun run bench:run` exercises one-key RPC reads and writes with 1 KiB values at 1, 16, 64, and 128 concurrent in-flight operations. Each workload uses a fresh object name; concurrency steps run in increasing order so adaptive splits can settle. Read workloads seed their keyspace before timing. The run tests both uniform keys and one hot key. A hot key should remain bound to one leaf and demonstrates the limit of key-based splitting.

Each result includes successful operations/s, errors, logical MiB/s, p50/p95/p99 latency, maximum observed per-method latency, and split leaf/row/depth counts. Percentiles use one-millisecond histogram buckets; max is kept separately at hundredth-millisecond precision. Driver-to-object RPC latency is measured, not the time for the external control request.

The driver count is capped at eight and spread with location hints. Results are specific to the deployment, placement, workload, and runtime version. Run the suite more than once before drawing conclusions. Increasing offered concurrency is not the same as increasing throughput; report both.

## Deploy and run

Requires Bun 1.4+, the `cf` CLI, and an authenticated `work` profile with access to the intended account. Never commit `.dev.vars` or a bearer token.

```sh
cd bench/worker
bun install --frozen-lockfile
cf auth activate work
# Set CLOUDFLARE_ACCOUNT_ID to the intended account before deploying.
# Generate a unique BENCH_TOKEN locally and save it in .dev.vars (ignored by Git).
cf deploy --secrets-file .dev.vars
```

Copy the deployed Worker URL to `BENCH_URL`, and load `BENCH_TOKEN` from `.dev.vars`. If the Worker is behind Access, provide a short-lived `BENCH_ACCESS_TOKEN` in the shell; it is sent only as an HTTP header and is never written to result files.

```sh
BENCH_URL=https://<deployed-worker> \
BENCH_TOKEN=<from-.dev.vars> \
BENCH_ACCESS_TOKEN=<short-lived-access-token-if-required> \
bun run bench:run
bun run bench:plot
```

The endpoint requires both the bearer token and any applicable Access authentication. Keep the secret local. Use an account and Worker dedicated to benchmarking; the run creates persistent Durable Objects and writes test data. Current defaults seed 10,000 keys with 1 KiB values for read workloads. Results do not automatically clean up these objects, so use fresh names and explicitly remove test data through an approved cleanup plan rather than reusing production objects.

`bench/results/latest.json` and `.csv` contain raw results; `docs/performance.svg` is generated from the latest run. Results should be reviewed before committing: they can contain account-specific performance details. Do not commit credentials, Access tokens, or private account identifiers.
