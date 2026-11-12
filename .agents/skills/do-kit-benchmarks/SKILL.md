---
name: do-kit-benchmarks
description: Plan, run, publish and tear down do-kit's edge benchmarks (bench/). Use when measuring a layer on Cloudflare's network, refreshing the README and docs/performance.md numbers or graphs, adding a benchmark target or operation, or comparing do-kit against a baseline.
---

# do-kit benchmarks

The benchmarks in `bench/` measure each layer on Cloudflare's network against a named baseline,
and every number in the README and `docs/performance.md` comes from a results file they wrote.
`bench/README.md` has the commands; this is how we use them.

## The rules

1. **Measure a real user operation against a named baseline, in the same run.** One plain
   Durable Object running the same code is the default baseline. What one object takes varies
   with the day and the machine it lands on, so never compare a target with a baseline measured
   in another run.
2. **Don't measure the load generator.** Drivers are Durable Objects with a capped number of
   calls in flight, scaled with the calls an operation makes (a listing of a 64-shard table is
   up to 192 calls). If every target levels off at the same rate, suspect the drivers first.
3. **Every point answers.** Every stage of a point is bounded; a call with no answer in 30 s
   counts as failed, with its message in `errorSamples`; a stage that stalled is named in
   `stalls`; a point with no measurement is retried, then skipped and logged, never written as
   zero or NaN.
4. **Keep raw data, with provenance.** Each run is named (`BENCH_RESULTS_NAME=v050-<what>`) and
   records the commit, whether the tree was dirty, and the deployed Worker version
   (`BENCH_WORKER_VERSION`). `bench/collect.ts` lists the named runs that back the published
   numbers; a later run of the same point replaces the earlier one. Delete timestamped scratch
   results; never hand-edit results.
5. **Tables and graphs come from the data.** Generate markdown tables from `bench/results/*.json`
   with a script, don't type them; then `bun bench/collect.ts`, `bun run bench:plot` and
   `bun bench/workgraph/plot.ts`. A number in prose must be in a results file.
6. **Report what you saw, failures included.** Absolute numbers with p50 or p99 and failed calls.
   If a target got worse, or a run behaved in a way nobody can explain yet, the docs say so.
7. **Always tear down.** When the runs are done, tear down every Worker you deployed, and any
   Access application made only for it (see below). No exceptions, even if you might rerun
   tomorrow: redeploying takes a minute.

## Workflow

1. **Fixes first.** Land the library changes before a rebench, so the numbers describe a commit
   on `main`.
2. **Deploy from a worktree** on an `exp/` branch, on a custom domain behind Cloudflare Access,
   with `workersDev` and `previewUrls` off and a bearer token in `.dev.vars` (never committed).
   Never use workers.dev, and never loosen an account-wide Access policy to get a token: give the
   benchmark hosts their own Access application if needed.
3. **Probe before sweeping.** One point at low concurrency (`/measure` or `/probe-table`) shows
   a broken deploy in seconds instead of an hour.
4. **Run long sweeps in the background** with `nohup`, one log per named run, and follow the
   log (parse the `{"target": …}` lines), never a blind `sleep`. Long runs refresh their Access
   token themselves when `BENCH_ACCESS_APP` is set.
5. **When a point fails, find out why before rerunning.** Read `errorSamples` and `stalls`.
   Platform limits show up as specific messages:
   - "Durable Object is overloaded": the target, a real result.
   - "Too many API requests by single Worker invocation": a driver ran out of subrequests,
     a harness limit (the bench Worker raises `limits.subrequests`).
   - "exceeded its CPU time limit": often a loop that never yields. The clock moves only with
     I/O, so a loop of calls that do no I/O never sees its stop time (`yieldIfNoIO`).
6. **Rerun what a harness bug touched**, on the fixed build, and only that, then collect, plot,
   and update the docs from the data.
7. **Check and commit.** `bun run check`, the tests, results and graphs in one `bench:`/`docs:`
   commit, harness changes in their own.
8. **Tear down** (below), and confirm it.

## Tearing down

```sh
cd bench/worker
BENCH_TEARDOWN=1 BENCH_HOSTNAME=<host> cf deploy   # a stub; every Durable Object class deleted, with its storage
cf workers delete <worker-name> --force
cf workers get <worker-name>                       # expect 404
```

The same for `bench/workgraph`. Then delete the benchmark's own Access application, if one was
made, and leave shared policies alone. A new Durable Object class in a bench Worker must be
added to its config's exports, so teardown deletes it too.

## What never goes in the repository

Tokens, account IDs, personal or company hostnames, and names of private projects or internal
tools: not in code, results, docs or commit messages. Configs read the hostname from
`BENCH_HOSTNAME` and the runner from `BENCH_URL`; docs write `<your-zone>`.
