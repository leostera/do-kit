// What DOClient and behaviours cost (RFD0009), on the same one-object key/value work as the
// `single` target: the same table, the same SQL, the same 1 KiB values.
//
//   single              a plain Durable Object through a raw stub: the baseline
//   single-client       the same object through DOClient, no options
//   single-client-opts  the same, with a deadline, retries and an interceptor
//   genserver           a GenServer doing the same SQL in its callbacks, through its client
//   genserver-keyed     the same, with retries on: every call carries an idempotency key, kept
//   genstatem           a one-state GenStatem doing the same SQL
//
// `/probe-behaviours` times what runs inside the object, without RPC: N events in one call.
import { DOClient, type CallOptions } from "../../../src/client/index.ts";
import { GenServer, GenStatem, keep, type CallResult, type StateHandlers } from "../../../src/behaviours/index.ts";
import type { SingleObject } from "./index.ts";

export type BehaviourTarget = "single-client" | "single-client-opts" | "genserver" | "genserver-keyed" | "genstatem";
export const BEHAVIOUR_TARGETS: readonly BehaviourTarget[] = ["single-client", "single-client-opts", "genserver", "genserver-keyed", "genstatem"];
export const isBehaviour = (target: string): target is BehaviourTarget => (BEHAVIOUR_TARGETS as readonly string[]).includes(target);

export interface BehavioursEnv {
  readonly SINGLE: DurableObjectNamespace<SingleObject>;
  readonly KV_SERVER: DurableObjectNamespace<KvServer>;
  readonly KV_MACHINE: DurableObjectNamespace<KvMachine>;
}

type KvCall = { readonly op: "put"; readonly keys: readonly string[]; readonly values: readonly string[] } | { readonly op: "get"; readonly keys: readonly string[] };

const CREATE = "CREATE TABLE IF NOT EXISTS bench_values (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID";
const UPSERT = "INSERT INTO bench_values (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value";
const SELECT = "SELECT value FROM bench_values WHERE key = ?";

/** The work every target does, as SingleObject does it. Callbacks already run in a transaction. */
function kv(sql: SqlStorage, call: KvCall): number | (string | null)[] {
  if (call.op === "put") {
    for (let i = 0; i < call.keys.length; i++) sql.exec(UPSERT, call.keys[i]!, call.values[i]!);
    return call.keys.length;
  }
  return call.keys.map((key) => sql.exec<{ value: string }>(SELECT, key).toArray()[0]?.value ?? null);
}

type Counter = { readonly n: number };

export class KvServer extends GenServer<BehavioursEnv, Counter, KvCall | { readonly op: "inc" }, unknown> {
  // ---- client API ----
  put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.call({ op: "put", keys, values }) as Promise<number>;
  }
  get(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    return this.call({ op: "get", keys }) as Promise<(string | null)[]>;
  }

  // ---- callbacks ----
  protected init(): Counter {
    this.sql.exec(CREATE);
    return { n: 0 };
  }

  protected handleCall(call: KvCall | { readonly op: "inc" }, state: Counter): CallResult<Counter, unknown> {
    // "inc" keeps its counter in the state, which is stored as JSON after every event.
    if (call.op === "inc") return { reply: state.n + 1, state: { n: state.n + 1 } };
    // put and get keep the state as it is: nothing but their own SQL is written.
    return { reply: kv(this.sql, call), state };
  }

  // ---- probes, through a raw stub: inside the object, no RPC ----

  /** `n` puts through the behaviour: one event each, as a call from a client would run. */
  async probeEvents(n: number, mode: "put" | "inc"): Promise<number> {
    const value = "x".repeat(1024);
    for (let i = 0; i < n; i++) await this.call(mode === "put" ? { op: "put", keys: [`key-${i % 1000}`], values: [value] } : { op: "inc" });
    return n;
  }

  /**
   * `n` puts through `receive`, as envelopes from a client arrive: with a deadline, and with an
   * idempotency key when `keyed` (each kept, for retries of the same call).
   */
  async probeReceive(n: number, keyed: boolean): Promise<number> {
    const value = "x".repeat(1024);
    const run = crypto.randomUUID();
    for (let i = 0; i < n; i++) {
      const message = { op: "put", keys: [`key-${i % 1000}`], values: [value] };
      await this.receive(keyed ? { kind: "call", message, deadline: Date.now() + 30_000, key: `${run}-${i}` } : { kind: "call", message, deadline: Date.now() + 30_000 });
    }
    return n;
  }

  /** The same `n` puts as SingleObject does them: a transaction each, no behaviour. */
  async probePlain(n: number, mode: "put" | "inc"): Promise<number> {
    const value = "x".repeat(1024);
    this.sql.exec(CREATE);
    this.sql.exec("CREATE TABLE IF NOT EXISTS bench_counter (k INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    for (let i = 0; i < n; i++) {
      if (mode === "put") this.ctx.storage.transactionSync(() => this.sql.exec(UPSERT, `key-${i % 1000}`, value));
      else this.ctx.storage.transactionSync(() => this.sql.exec("INSERT INTO bench_counter (k, n) VALUES (0, 1) ON CONFLICT (k) DO UPDATE SET n = n + 1"));
      await Promise.resolve(); // as `await this.call` yields once per event
    }
    return n;
  }
}

export class KvMachine extends GenStatem<BehavioursEnv, "serving", null, KvCall, unknown> {
  put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.call({ op: "put", keys, values }) as Promise<number>;
  }
  get(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    return this.call({ op: "get", keys }) as Promise<(string | null)[]>;
  }

  protected init() {
    this.sql.exec(CREATE);
    return { state: "serving" as const, data: null };
  }

  protected readonly states: StateHandlers<"serving", null, KvCall, unknown> = {
    serving: (call, data) => keep(data, { reply: kv(this.sql, call) }),
  };
}

/** A deadline, retries and an interceptor: every option's path, with none of them firing. */
const OPTIONS: CallOptions = {
  timeoutMs: 30_000,
  retry: { maxAttempts: 3 },
  interceptors: [(attempt, next) => next(attempt)],
};

/** A key/value client of `target`'s object `name`, made per call, as a Worker makes one per request. */
function clientOf(env: BehavioursEnv, target: BehaviourTarget, name: string): Pick<SingleObject, "put" | "get"> {
  switch (target) {
    case "single-client": {
      const client = new DOClient(env.SINGLE, name);
      return { put: (keys, values) => client.invoke("put", keys, values), get: (keys) => client.invoke("get", keys, null) };
    }
    case "single-client-opts": {
      const client = new DOClient(env.SINGLE, name, OPTIONS);
      return { put: (keys, values) => client.invoke("put", keys, values), get: (keys) => client.invoke("get", keys, null) };
    }
    case "genserver":
      return KvServer.client(env.KV_SERVER, name);
    case "genserver-keyed":
      return KvServer.client(env.KV_SERVER, name, { retry: { maxAttempts: 3 } });
    case "genstatem":
      return KvMachine.client(env.KV_MACHINE, name);
  }
}

export async function behaviourWrite(env: BehavioursEnv, target: BehaviourTarget, name: string, keys: readonly string[], values: readonly string[]): Promise<void> {
  await clientOf(env, target, name).put(keys, values);
}

export async function behaviourRead(env: BehavioursEnv, target: BehaviourTarget, name: string, key: string): Promise<string | null> {
  return (await clientOf(env, target, name).get([key], null))[0] ?? null;
}

export interface ProbeBehaviours {
  readonly objectName: string;
  /** Events per call. */
  readonly n: number;
  /** Calls of each kind, alternated, after one of each unmeasured. */
  readonly rounds: number;
}

type Kind = "plain-put" | "events-put" | "receive-put" | "receive-put-keyed" | "plain-inc" | "events-inc";

export interface ProbeResult {
  readonly n: number;
  readonly rounds: number;
  /** Microseconds per event, per round, by what ran. "empty" is a call that runs nothing. */
  readonly microsPerEvent: Record<Kind, number[]>;
  readonly emptyCallMs: number[];
}

/**
 * Times `n` events inside one object, from the caller (the clock in the object does not move
 * while it computes): a call running `n` behaviour events against one doing the same writes
 * plainly, less a call that runs none. Each timed call follows an untimed empty one, so it doesn't
 * pay for flushing the writes of the call before it. Kinds alternate within a round, so drift
 * (other load on the machine, a runtime warming up) touches them all alike.
 */
export async function probeBehaviours(env: BehavioursEnv, probe: ProbeBehaviours): Promise<ProbeResult> {
  const stub = env.KV_SERVER.getByName(probe.objectName);
  const kinds = [
    ["plain-put", () => stub.probePlain(probe.n, "put")],
    ["events-put", () => stub.probeEvents(probe.n, "put")],
    ["receive-put", () => stub.probeReceive(probe.n, false)],
    ["receive-put-keyed", () => stub.probeReceive(probe.n, true)],
    ["plain-inc", () => stub.probePlain(probe.n, "inc")],
    ["events-inc", () => stub.probeEvents(probe.n, "inc")],
  ] as const;
  const empty = () => stub.probePlain(0, "put");
  const timed = async (run: () => Promise<unknown>) => {
    await empty();
    const before = performance.now();
    await run();
    return performance.now() - before;
  };
  for (const [, run] of kinds) await run(); // warm: the object exists, its tables too
  const microsPerEvent = Object.fromEntries(kinds.map(([kind]) => [kind, [] as number[]])) as Record<Kind, number[]>;
  const emptyCallMs: number[] = [];
  for (let round = 0; round < probe.rounds; round++) {
    const emptyMs = await timed(empty);
    emptyCallMs.push(emptyMs);
    for (const [kind, run] of kinds) microsPerEvent[kind].push((((await timed(run)) - emptyMs) * 1000) / probe.n);
  }
  return { n: probe.n, rounds: probe.rounds, microsPerEvent, emptyCallMs };
}

export interface ProbeRpc {
  readonly objectName: string;
  /** Sequential one-key puts per target per round. */
  readonly calls: number;
  readonly rounds: number;
  readonly targets?: readonly ("single" | BehaviourTarget)[];
}

/**
 * Times one-key puts end to end, one at a time, from the caller: what a call through each target
 * costs the caller over a raw stub, RPC included. Targets alternate within a round, and each
 * round's order rotates, so drift touches them all alike.
 */
export async function probeRpc(env: BehavioursEnv, probe: ProbeRpc): Promise<{ calls: number; rounds: number; microsPerCall: Record<string, number[]> }> {
  const targets = probe.targets ?? (["single", ...BEHAVIOUR_TARGETS] as const);
  const value = "x".repeat(1024);
  const put = (target: "single" | BehaviourTarget, i: number) => {
    const keys = [`key-${i % 1000}`];
    if (target === "single") return env.SINGLE.getByName(probe.objectName).put(keys, [value]);
    return behaviourWrite(env, target, probe.objectName, keys, [value]);
  };
  for (const target of targets) for (let i = 0; i < 20; i++) await put(target, i); // warm
  const microsPerCall: Record<string, number[]> = Object.fromEntries(targets.map((t) => [t, []]));
  for (let round = 0; round < probe.rounds; round++) {
    for (let k = 0; k < targets.length; k++) {
      const target = targets[(k + round) % targets.length]!;
      const before = performance.now();
      for (let i = 0; i < probe.calls; i++) await put(target, i);
      microsPerCall[target]!.push(((performance.now() - before) * 1000) / probe.calls);
    }
  }
  return { calls: probe.calls, rounds: probe.rounds, microsPerCall };
}
