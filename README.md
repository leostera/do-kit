# splittable-object

A TypeScript base class and client for **Cloudflare Durable Objects** that split a busy keyspace into independently addressable Durable Object leaves. Keys are assigned by the leading bits of their SHA-256 hash. Splits are permanent; clients learn the tree lazily and retry calls when their route is stale.

> This package is an early-stage library extracted from another project. The API and persistence format may change before `1.0.0`.

## Install

```sh
bun add github:leostera/splittable-object
```

The package imports `cloudflare:workers` and requires a Workers runtime with **SQLite-backed Durable Objects** and `nodejs_compat`. Bind the same Durable Object namespace to every class that extends `SplittableObject`.

## Quick start

```ts
import {
  SplittableObject,
  splittable,
  type UpdateResult,
} from "splittable-object";

interface Env {
  COUNTERS: DurableObjectNamespace<Counter>;
}

class Counter extends SplittableObject<Env, number> {
  protected nodes() {
    return this.env.COUNTERS;
  }

  async add(keys: readonly string[], payloads: readonly number[]) {
    return this.update(keys, (current): UpdateResult<number, number[]> => {
      const writes = new Map<string, number>();
      const result = keys.map((key, i) => {
        const value = (current.get(key) ?? 0) + payloads[i]!;
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

// In a Worker handler:
const counters = splittable(env.COUNTERS, "account-123");
const leaves = await counters.call(["apples", "pears"], "add", [2, 1]);
```

Each method callable with `client.call` takes `keys` first and `payloads` second (`null` when unused); any further arguments follow. Keys and payloads are partitioned together. A `LeafResult` contains the input `keys`, their original `positions`, and that leaf's result. Use those positions to reconstruct the caller's original order.

### Keeping keys together

For an atomic update that requires all keys to be handled by one leaf, use `callOneLeaf`. It throws `SpansLeaves` **before applying the method** if the keys currently belong to multiple leaves. The client does not silently turn a cross-leaf operation into a non-atomic one.

### Scanning all leaves

A scan method takes no keys and can use `this.allRows()` to enumerate the rows owned by that leaf. Invoke it with `client.scan("methodName")`; the result is one value per leaf, not a globally ordered or transactional snapshot. During active splitting, plan for split-level results to reflect the individual calls' timing.

## How splitting works

1. The root Durable Object is named `${name}#`; descendants use `${name}#${bits}`, where `bits` is a hash-prefix such as `01`.
2. A leaf counts calls (and, optionally, key counts) over a rolling window. Once its configured threshold is exceeded, it creates both children and then marks itself split. New work no longer runs on the parent.
3. A child is adopted and begins inheriting only the rows whose hashes fall in its range. Foreground reads/updates for keys not inherited yet fetch those keys from the parent first; a background alarm copies the remaining rows in bounded batches.
4. The parent releases each range only after the corresponding child has finished inheriting. A crash or retry can repeat adoption/copy/release safely; rows are not handed out based on an incomplete child.
5. The client keeps known splits in isolate memory and the Workers Cache API. If a stale route reaches a split node, it learns that prefix and retries. Cached routing is only an optimization: because splits are final, a stale map may add a hop but cannot send a key to the wrong leaf.
6. An empty object may presplit to `initialDepth` before its first user call, useful when SHA-256 keys are expected to be uniformly distributed. A node with rows does not presplit; it waits for the load policy.

The implementation currently hashes keys with the first 64 bits of SHA-256. A key's ownership is determined by the corresponding most-significant-first prefix bits.

## Split policy

Override `protected splitPolicy` in a subclass, or use `DEFAULT_SPLIT_POLICY` as a starting point:

```ts
protected override splitPolicy: SplitPolicy = {
  callsPerSecond: 400,
  windowSeconds: 5,
  maxDepth: 12,
  keysPerCall: 25,
  initialDepth: 0,
};
```

`callsPerSecond` is a per-leaf threshold. Each call weighs `1 + keys.length / keysPerCall`; `keysPerCall` defaults to 25. `maxDepth` caps the tree depth, and `initialDepth` is optional and defaults to zero. This is a request-rate heuristic, not a CPU or storage-capacity measurement. Tune it against your workload and test on the Workers runtime you deploy to.

## Subclass contract and caveats

- `nodes()` must return the namespace used to address **all** nodes of the subclass. The class binding must include SQLite storage.
- Routed methods must accept `(keys, payloads, ...args)` and use `read`/`update` for keyed state. `update` runs its `decide` callback synchronously inside a storage transaction; do not perform I/O there.
- `update` provides per-node transactional updates. `callOneLeaf` can keep an operation on one leaf, but there is no cross-leaf transaction.
- If the subclass overrides `alarm`, it must call `await super.alarm()` so inheritance and parent-release work continues.
- If work external to the stored rows must finish before rows leave a parent (for example an outbox), override `beforeHandOut()` and make it safe to call more than once.
- Deletions are represented by `null` in `UpdateResult.writes`. Do not mutate the input map passed to `decide` as a substitute for returning writes.
- `allRows()` is for a leaf's scan method and is valid only while that object is a leaf.
- Cache API failures are ignored; cache contents are not authoritative. Split depth and method arguments should stay within practical Durable Object RPC limits.

## Development

Requires Bun 1.3+ and a local Workers-compatible runtime. The integration suite starts the test Worker via `cf dev`; it exercises concurrent updates, compare-and-swap, manual and automatic splitting, empty presplitting, and row inheritance.

```sh
bun install
bun run check
bun test
bun run specs:check
```

The executable system explanation lives in [`specs/splitting-and-routing.ts`](specs/splitting-and-routing.ts). It models stale-route retries and the parent/child row-handoff invariant. Run `bunx feynman run splitting-and-routing` to read its scenario traces.

## License

MIT. See [LICENSE](LICENSE).
