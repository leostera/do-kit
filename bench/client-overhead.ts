// What DOClient and behaviour clients cost the caller, without the network (RFD0009): each makes
// the same call on a stub that answers at once, timed in a tight loop. The difference from calling
// the stub directly is the client's own work per call: options, the envelope, promises.
//
//   bun bench/client-overhead.ts            # BENCH_RESULTS_NAME=behaviours-client-<what> to keep it
//
// Runs on Bun (JavaScriptCore), not workerd (V8): inside workerd the clock does not move while code
// runs, so a loop with no I/O can't be timed there. Treat the numbers as an order of magnitude.
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { $ } from "bun";

// The behaviours extend the runtime's DurableObject: a stand-in, for the client half only.
Bun.plugin({
  name: "cloudflare-workers-stand-in",
  setup(build) {
    build.module("cloudflare:workers", () => ({ loader: "object", exports: { DurableObject: class {} } }));
  },
});

const { DOClient } = await import("../src/client/index.ts");
const { GenServer } = await import("../src/behaviours/index.ts");

type Put = (keys: readonly string[], values: readonly string[]) => Promise<unknown>;

/** A namespace whose stubs answer every call at once, as an object would with no network. */
const stub = {
  put: async (keys: readonly string[]) => keys.length,
  receive: async (envelope: { message: { keys: string[] } }) => ({ ok: true, value: envelope.message.keys.length }),
};
const namespace = { getByName: () => stub } as unknown as DurableObjectNamespace<any>;

class Kv extends GenServer<unknown, null, { op: "put"; keys: readonly string[]; values: readonly string[] }, number> {
  put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.call({ op: "put", keys, values });
  }
  protected init(): null {
    return null;
  }
  protected handleCall(): never {
    throw new Error("the client never runs callbacks");
  }
}

const interceptor = (attempt: unknown, next: (a: any) => Promise<unknown>) => next(attempt);
/** Each case makes its client per call, as a Worker makes one per request. */
const CASES: Record<string, () => Put> = {
  "raw stub": () => (keys, values) => namespace.getByName("x").put(keys, values),
  "DOClient": () => (keys, values) => new DOClient(namespace, "x").invoke("put", keys, values),
  "DOClient, timeout + retries + interceptor": () => (keys, values) => new DOClient(namespace, "x", { timeoutMs: 30_000, retry: { maxAttempts: 3 }, interceptors: [interceptor] }).invoke("put", keys, values),
  "GenServer client": () => (keys, values) => Kv.client(namespace, "x").put(keys, values),
  "GenServer client, timeout": () => (keys, values) => Kv.client(namespace, "x", { timeoutMs: 30_000 }).put(keys, values),
  "GenServer client, retries (idempotency key)": () => (keys, values) => Kv.client(namespace, "x", { retry: { maxAttempts: 3 } }).put(keys, values),
};

const CALLS = Number(process.env.BENCH_CALLS ?? 200_000);
const ROUNDS = Number(process.env.BENCH_ROUNDS ?? 9);
const keys = ["key-1"];
const values = ["x".repeat(1024)];

async function time(put: Put): Promise<number> {
  const before = Bun.nanoseconds();
  for (let i = 0; i < CALLS; i++) await put(keys, values);
  return (Bun.nanoseconds() - before) / CALLS;
}

const nanosPerCall: Record<string, number[]> = Object.fromEntries(Object.keys(CASES).map((name) => [name, []]));
for (const make of Object.values(CASES)) await time(make()); // warm the JIT
for (let round = 0; round < ROUNDS; round++) {
  // Cases alternate within a round, rotating, so drift touches them all alike.
  const names = Object.keys(CASES);
  for (let k = 0; k < names.length; k++) {
    const name = names[(k + round) % names.length]!;
    nanosPerCall[name]!.push(await time(CASES[name]!()));
  }
}

const median = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)]!;
for (const [name, xs] of Object.entries(nanosPerCall)) console.log(`${name.padEnd(46)} ${median(xs).toFixed(0).padStart(6)} ns/call (median of ${ROUNDS})`);

const results = {
  schemaVersion: 1,
  kind: "behaviours-client-overhead",
  generatedAt: new Date().toISOString(),
  provenance: {
    commit: (await $`git rev-parse HEAD`.quiet().text()).trim(),
    dirty: (await $`git status --porcelain -- src bench ${":!bench/results"}`.quiet().text()).trim().length > 0,
    engine: `Bun ${Bun.version} (JavaScriptCore)`,
    platform: `${process.platform}-${process.arch}`,
  },
  config: { calls: CALLS, rounds: ROUNDS, payloadBytes: values[0]!.length },
  notes: [
    "Each case makes its client per call and calls a stub that answers at once: the time is the client's own work, no network or object.",
    "Cases alternate within each round, in a rotating order; the medians are over rounds.",
    "Bun (JavaScriptCore), not workerd (V8): an order of magnitude, not a workerd measurement.",
  ],
  nanosPerCall,
};
if (process.env.BENCH_RESULTS_NAME) {
  const dir = join(import.meta.dir, "results");
  mkdirSync(dir, { recursive: true });
  writeFileSync(join(dir, `${process.env.BENCH_RESULTS_NAME}.json`), JSON.stringify(results, null, 2) + "\n");
  console.log(`Saved bench/results/${process.env.BENCH_RESULTS_NAME}.json`);
}
