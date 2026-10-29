# Performance expectations and evidence

## What splitting is expected to improve

A Durable Object processes its events on one thread. Splitting a keyspace across `N` leaves lets independent keys be served concurrently by different objects. Under uniform key distribution, enough available leaves, and no shared bottleneck, throughput can grow roughly with the number of active leaves.

This is a capacity/scaling technique, **not a promise of lower latency for one call**. A call still goes to one leaf. A client with stale routing may pay an extra redirect-and-retry hop, and splitting adds background row-copy work. A single hot key remains on one leaf and does not gain capacity from more leaves. Skew, storage work, network latency, cache state, and the surrounding Worker can all limit scaling.

## Durable Object reference point

A team no-op load test of ordinary Durable Object RPC calls reported roughly **1,800–2,800 top-level calls/s per object** before the next offered-load step failed. That is a runtime ceiling for no-op work, not a result for this library. For production workloads that do real work, a safer initial planning budget is about **1,000 events/s per object**, followed by measurement on the actual workload and deployment.

The no-op test used one target object, 8–16 driver Durable Objects across four regions, 30-second offered-load steps, and no retries. Capacity varied substantially between objects and machines. Treat these figures as a reference point, not a service guarantee; runtime versions, logging, location, payload size, and application work change the result.

The default split threshold here is **400 weighted calls/s per leaf**, sustained over a five-second window. It is intentionally below the practical planning budget to create headroom for variation and split/inheritance work. A weighted call counts as `1 + keys.length / keysPerCall` (default `keysPerCall: 25`), so it is not directly comparable to raw events/s for every workload.

## Illustrative capacity arithmetic — not a package benchmark

Using 1,000 events/s per object only as a planning budget, ideal uniform distribution gives this rough arithmetic:

| Active leaves | Idealized aggregate planning budget |
| ---: | ---: |
| 1 | ~1,000 events/s |
| 2 | ~2,000 events/s |
| 4 | ~4,000 events/s |
| 8 | ~8,000 events/s |

These are **not measured results for `splittable-object`**. They assume each leaf is independently placed and loaded evenly, and omit routing, storage, migration, and caller overhead. The package has not yet been benchmarked against a single-object implementation in a deployed Workers environment, so no package-specific speedup or latency improvement is claimed.

## How to measure this package fairly

A useful comparison should benchmark a plain single-object implementation and an equivalent `SplittableObject` implementation using the same RPC method, state shape, payloads, and data volume.

1. Run against a deployed Workers runtime, not only local `workerd`; generate load from Workers or driver Durable Objects in multiple regions.
2. Compare uniform keys, moderately skewed keys, and a single hot key. The last case establishes the limit where splitting cannot help.
3. Measure both steady state and the split/inheritance period. For steady state, separate cold routing from warmed client routing.
4. Ramp offered load in fixed steps; use enough drivers to saturate the target without saturating the load generator. Do not retry errors in the primary measurement.
5. Record successful operations/s, error rate, p50/p95/p99 latency, split count/depth, and whether the run was still inheriting. Repeat against several fresh objects because capacity varies between machines.
6. Report the last clean offered-load step (for example, under 1% errors) and include the exact compatibility date, region, Worker/runtime versions, split policy, and benchmark revision.

Until those results are recorded, use the reference figures above only for capacity planning and tune `SplitPolicy` with workload-specific load tests. See the [split policy](../README.md#split-policy) and [subclass contract](../README.md#subclass-contract-and-caveats).
