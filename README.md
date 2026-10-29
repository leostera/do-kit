# splittable-object

**Durable Objects that split themselves when they get busy.**

A Durable Object is a great home for state, but it is one object: one thread, 10 GB of storage. When a hot account, repository or room outgrows it, you are left sharding by hand. `splittable-object` lets you write the object as if it were one, and splits it for you. When a leaf stays busy, it hands half of its keys to a new object, and your Workers follow the split without you changing a line.

A split moves that half with a single storage snapshot, so it takes seconds, not minutes, and the object keeps serving while it happens.

## What it buys you over one Durable Object

![Throughput vs concurrency](docs/bench/throughput.svg)

At 2,048 writes in flight, a single Durable Object is past its limit. It completes 1,435 writes/s and refuses 26,816 calls in 10 seconds. The same object split into 16 leaves completes **9,960 writes/s and refuses none**.

| 1 KiB writes, uniform keys | One Durable Object | splittable-object, 16 leaves |
|---|---|---|
| **Throughput**, 1,024 in flight | 1,499 writes/s | **5,207 writes/s** |
| **Throughput**, 2,048 in flight | 1,435 writes/s, 26,816 calls refused in 10 s | **9,960 writes/s, none refused** |
| **Data written**, 2,048 in flight | 1.4 MiB/s | **9.7 MiB/s** |
| **Median latency**, 1,024 in flight | 613 ms | **103 ms** |
| **p99 latency**, 2,048 in flight | 1,822 ms | **765 ms** |
| **Storage ceiling** | 10 GB | **160 GB** (10 GB per leaf) |

![Write latency p99 vs concurrency](docs/bench/latency-p99.svg)

![Failed calls vs concurrency](docs/bench/errors.svg)

![Data throughput vs concurrency](docs/bench/bandwidth.svg)

These were measured on Cloudflare's network, with up to 128 load-driver Durable Objects in several regions. Each point is 10 s of one-key writes. The 16-leaf object was presplit, so these numbers show the capacity that splitting gives, not the cost of the splits along the way. That cost is next. More latency graphs ([p50](docs/bench/latency-p50.svg), [p95](docs/bench/latency-p95.svg), [max](docs/bench/latency-max.svg)) and the method are in [docs/performance.md](docs/performance.md).

## Splits take seconds

![Time to split vs data size](docs/bench/split-time.svg)

![Slowest write during a split](docs/bench/split-latency.svg)

When load splits an object, it pays for it once. Here an object seeded with 1 MiB to 1 GiB of 1 KiB rows, split once while 8 writers kept writing to random keys. No write failed in any run.

| Data in the object | splittable-object | Freeze, snapshot into two new objects | v0.1: copy rows to two new objects |
|---|---|---|---|
| 1 MiB | 1.9 s | 1.5 s | 1.4 s |
| 16 MiB | **2.0 s** | 3.0 s | 7.4 s |
| 128 MiB | **5.7 s**, slowest write 5.4 s | 24.3 s, every write blocked for it | 235 s, write p95 2.4 s throughout |
| 1 GiB | **26.7 s**, write p99 500 ms | the split failed | did not finish in 2 hours |

The times are until the new object serves with all of its data. See [what was measured and how](docs/performance.md).

## What it costs

Splitting does not change what you pay per call: each write is one request to one leaf, as it would be to one object.

![Estimated cost per million writes](docs/bench/cost-writes.svg)

| Per million writes (Workers Paid list prices) | One Durable Object | splittable-object |
|---|---|---|
| Update an existing key | $1.15 | $1.15 |
| Write a new key | $1.15 | $2.15 |

The difference is the index on each key's hash, which makes a new key cost one more row written ($1.00 per million rows). Duration is billed per object while it is busy, so 16 busy leaves cost more than one object, but at $12.50 per million GB-s that comes to $0.0010 against $0.0025 per million writes.

A split costs the rows it deletes: each side deletes the half it gave away, at two rows written per row (table and index). The snapshot is made by the storage layer and is not billed as rows written. For 1 KiB rows that is about $0.03 to split 16 MiB, $0.26 for 128 MiB and $2.10 for 1 GiB, and an object usually splits long before it holds that much.

![Estimated cost of one split](docs/bench/cost-split.svg)

These are estimates from the [published prices](https://developers.cloudflare.com/durable-objects/platform/pricing/) and the row counts of the code, not from an invoice. Included monthly usage is not subtracted.

## Why it is fast

- **Only half moves.** A leaf lives in the object named by its bits without trailing zeros, so when `01` splits, `010` stays where it is and only `011` goes to a new object. That means one copy, not two, and no object is left behind as a router.
- **One snapshot, not a row stream.** The new object restores from `ctx.storage.snapshot()`, a point-in-time copy of the whole database made by the storage layer. It still takes longer for a bigger database, but far less than a row-by-row copy: 5.7 s against 235 s at 128 MiB.
- **Most traffic never notices.** While the copy is made, reads go on, and so do writes to the half that stays. Only writes to the half that moves wait, and then they are sent to the new owner.
- **Cleanup stays out of the way.** Each side deletes the half it gave away in small batches in the background. Those rows are invisible from the moment of the switch.

## What you get

- **One object to write.** You write methods that read and update keys, and the base class runs them on whichever leaf owns those keys.
- **Splits under load, by itself.** A leaf splits when its call rate stays above a threshold. There are no shard counts to choose up front.
- **Clients that find their way.** Workers learn about splits lazily, and share what they learn in the isolate and the colo cache. A stale map costs one extra hop, never a wrong answer, because splits are final.
- **Correctness you can check.** The split protocol is an executable [Feynman](https://github.com/leostera/feynman) model, [`specs/snapshot-split.ts`](specs/snapshot-split.ts). It checks that no acknowledged write is lost or applied twice, that two objects never serve the same key, and that no read is stale, across lost messages and restarts.

## When it helps, and when it does not

**It helps** when one logical object holds many independent keys and outgrows one Durable Object: too many writes per second, or too much data for 10 GB. Each leaf is a full Durable Object with its own thread and storage limit.

**It does not help:**
- **One hot key.** A key always lives on one leaf.
- **Cross-key transactions.** `update` is transactional within one leaf, and `callOneLeaf` refuses (before applying anything) when keys span leaves. Nothing spans leaves.
- **Small loads.** Below what one object can take, a plain Durable Object is simpler and as fast.

## Install

```sh
bun add github:leostera/splittable-object
```

You need SQLite-backed Durable Objects, `nodejs_compat`, and, for snapshot splits, the `durable_object_snapshots` compatibility flag. Without that flag, or in local development (where workerd has no snapshots), splits copy rows over RPC instead. That copy is slower, but it works the same way.

## Quick start

```ts
import { SplittableObject, splittable, type UpdateResult } from "splittable-object";

interface Env {
  COUNTERS: DurableObjectNamespace<Counter>;
}

export class Counter extends SplittableObject<Env, number> {
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
const counters = splittable(env.COUNTERS, "account-123");
const leaves = await counters.call(["apples", "pears"], "add", [2, 1]);
```

A method called through `client.call` takes its `keys` first and their `payloads` second (`null` when it needs none), then any other arguments. The client groups keys by leaf and returns one `LeafResult` per leaf, with the `positions` of its keys in your list.

- **All-or-nothing updates:** `callOneLeaf` runs a method on one leaf, or throws `SpansLeaves` before applying anything.
- **Scans:** `client.scan("method")` calls a method on every leaf. It can enumerate that leaf's rows with `this.allRows()`. There is one result per leaf, not a global snapshot.

## Split policy

```ts
protected override splitPolicy: SplitPolicy = {
  callsPerSecond: 400, // a leaf splits above this, sustained over windowSeconds
  windowSeconds: 5,
  maxDepth: 12,        // at most 2^12 leaves
  keysPerCall: 25,     // a call weighs 1 + keys / keysPerCall
  initialDepth: 0,     // presplit an empty object, for uniformly distributed keys
  copy: "auto",        // "native" snapshots, "logical" row copy, or auto-detect
  splitTimeoutMs: 60_000,
};
```

The default threshold is about half of what one object handles on the edge, which leaves headroom for real work and for the split itself. Tune it with your own workload.

## Writing a subclass

- `nodes()` returns the namespace that every leaf of the class lives in.
- Routed methods take `(keys, payloads, ...args)` and use `read` and `update` for keyed state. The `decide` callback of `update` runs synchronously inside a transaction, so it cannot do I/O.
- A split copies **every table**, yours too. The original keeps its tables and any work queued in them, such as an outbox it is still delivering. The new object drops its share in `afterClone(owns)`.
- `beforeHandOut()` runs before the copy, while writes to the moving half wait. Use it to finish work tied to rows that are about to move.
- If you override `alarm`, call `await super.alarm()`, which activates new leaves and runs the background cleanup.
- `splittableDescribe()` reports a leaf's bits, rows, call rate, last split and, if it gave one up, why.

## Development

```sh
bun install
bun run check        # types
bun test             # integration tests under cf dev: concurrent writes and CAS across splits, load splits, presplit, large rows, subclass tables
bun run specs:check  # model-check the split protocol
```

The benchmark Worker and the scripts that produced the graphs above are in [`bench/`](bench/README.md).

## License

MIT
