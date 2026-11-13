// What Client and behaviours cost (RFD0009), on the same one-object key/value work as the
// `single` target: the same table, the same SQL, the same 1 KiB values.
//
//   single                 a plain Durable Object through a raw stub: the baseline
//   single-client          the same object through Client, no options
//   single-client-opts     the same, with a deadline, retries and an interceptor
//   state-machine          a one-state DurableStateMachine doing the same SQL in its handler, through
//                          its client
//   state-machine-keyed    the same, with retries on: every call carries an idempotency key, kept
//   state-machine-plain    the DurableStateMachine's object through a raw stub, doing the SQL in plain
//                          methods: with BENCH_SHARED_OBJECT, the same object as state-machine, so
//                          placement is the same and only the behaviour differs
//
// `/probe-behaviours` times what runs inside the object, without RPC: N events in one call.
import { Client, type CallOptions } from "../../../src/client/index.ts";
import { DurableStateMachine, keep, type StateHandlers } from "../../../src/behaviours/index.ts";
import type { SingleObject } from "./index.ts";

export type BehaviourTarget = "single-client" | "single-client-opts" | "state-machine" | "state-machine-keyed" | "state-machine-plain";
export const BEHAVIOUR_TARGETS: readonly BehaviourTarget[] = ["single-client", "single-client-opts", "state-machine", "state-machine-keyed", "state-machine-plain"];
export const isBehaviour = (target: string): target is BehaviourTarget => (BEHAVIOUR_TARGETS as readonly string[]).includes(target);

export interface BehavioursEnv {
  readonly SINGLE: DurableObjectNamespace<SingleObject>;
  readonly KV_MACHINE: DurableObjectNamespace<KvMachine>;
}

type KvCall = { readonly op: "put"; readonly keys: readonly string[]; readonly values: readonly string[] } | { readonly op: "get"; readonly keys: readonly string[] };

const CREATE = "CREATE TABLE IF NOT EXISTS bench_values (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID";
const UPSERT = "INSERT INTO bench_values (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value";
const SELECT = "SELECT value FROM bench_values WHERE key = ?";

/**
 * A 1 KiB value no other write stored: a write of the bytes a key already holds changes no page, so
 * the object commits nothing durable and the write costs what a read does.
 */
let written = 0;
const freshValue = (): string => {
  const tag = `${Date.now()}:${++written}:${Math.random()}:`;
  return tag + "x".repeat(1024 - tag.length);
};

/** The work every target does, as SingleObject does it. Callbacks already run in a transaction. */
function kv(sql: SqlStorage, call: KvCall): number | (string | null)[] {
  if (call.op === "put") {
    for (let i = 0; i < call.keys.length; i++) sql.exec(UPSERT, call.keys[i]!, call.values[i]!);
    return call.keys.length;
  }
  return call.keys.map((key) => sql.exec<{ value: string }>(SELECT, key).toArray()[0]?.value ?? null);
}

type Counter = { readonly n: number };
type KvEvent = KvCall | { readonly op: "inc" };

export class KvMachine extends DurableStateMachine<BehavioursEnv, "serving", Counter, KvEvent, unknown> {
  // ---- client API ----
  put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.call({ op: "put", keys, values }) as Promise<number>;
  }
  get(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    return this.call({ op: "get", keys }) as Promise<(string | null)[]>;
  }

  // ---- callbacks ----
  protected init() {
    this.sql.exec(CREATE);
    return { state: "serving" as const, data: { n: 0 } };
  }

  protected readonly states: StateHandlers<"serving", Counter, KvEvent, unknown> = {
    // "inc" keeps its counter in the data, which is stored as JSON after every event that changes
    // it; put and get keep the data as it is, so nothing but their own SQL is written.
    serving: (event, data) => (event.op === "inc" ? keep({ n: data.n + 1 }, { reply: data.n + 1 }) : keep(data, { reply: kv(this.sql, event) })),
  };

  // ---- the same work without the behaviour, through a raw stub ----

  async plainPut(keys: readonly string[], values: readonly string[]): Promise<number> {
    this.sql.exec(CREATE);
    return this.ctx.storage.transactionSync(() => kv(this.sql, { op: "put", keys, values }) as number);
  }
  async plainGet(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    this.sql.exec(CREATE);
    return kv(this.sql, { op: "get", keys }) as (string | null)[];
  }

  // ---- probes, through a raw stub: inside the object, no RPC ----

  /** `n` puts through the behaviour: one event each, as a call from a client would run. */
  async probeEvents(n: number, mode: "put" | "inc"): Promise<number> {
    for (let i = 0; i < n; i++) await this.call(mode === "put" ? { op: "put", keys: [`key-${i % 1000}`], values: [freshValue()] } : { op: "inc" });
    return n;
  }

  /**
   * `n` puts through `receive`, as envelopes from a client arrive: with a deadline, and with an
   * idempotency key when `keyed` (each kept, for retries of the same call).
   */
  async probeReceive(n: number, keyed: boolean): Promise<number> {
    const run = crypto.randomUUID();
    for (let i = 0; i < n; i++) {
      const message = { op: "put", keys: [`key-${i % 1000}`], values: [freshValue()] };
      await this.receive(keyed ? { kind: "call", message, deadline: Date.now() + 30_000, key: `${run}-${i}` } : { kind: "call", message, deadline: Date.now() + 30_000 });
    }
    return n;
  }

  /** The same `n` puts as SingleObject does them: a transaction each, no behaviour. */
  async probePlain(n: number, mode: "put" | "inc"): Promise<number> {
    this.sql.exec(CREATE);
    this.sql.exec("CREATE TABLE IF NOT EXISTS bench_counter (k INTEGER PRIMARY KEY, n INTEGER NOT NULL)");
    for (let i = 0; i < n; i++) {
      if (mode === "put") {
        const value = freshValue();
        this.ctx.storage.transactionSync(() => this.sql.exec(UPSERT, `key-${i % 1000}`, value));
      } else this.ctx.storage.transactionSync(() => this.sql.exec("INSERT INTO bench_counter (k, n) VALUES (0, 1) ON CONFLICT (k) DO UPDATE SET n = n + 1"));
      await Promise.resolve(); // as `await this.call` yields once per event
    }
    return n;
  }
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
      const client = new Client(env.SINGLE, name);
      return { put: (keys, values) => client.invoke("put", keys, values), get: (keys) => client.invoke("get", keys, null) };
    }
    case "single-client-opts": {
      const client = new Client(env.SINGLE, name, OPTIONS);
      return { put: (keys, values) => client.invoke("put", keys, values), get: (keys) => client.invoke("get", keys, null) };
    }
    case "state-machine":
      return KvMachine.client(env.KV_MACHINE, name);
    case "state-machine-keyed":
      return KvMachine.client(env.KV_MACHINE, name, { retry: { maxAttempts: 3 } });
    case "state-machine-plain": {
      const stub = env.KV_MACHINE.getByName(name);
      return { put: (keys, values) => stub.plainPut(keys, values), get: (keys) => stub.plainGet(keys, null) };
    }
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
  const stub = env.KV_MACHINE.getByName(probe.objectName);
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
  readonly targets?: readonly RpcTarget[];
}

/**
 * Besides the load targets, two that attribute a difference: the DurableStateMachine's object called
 * through a raw stub with an envelope (`state-machine-receive`: the behaviour, without its client),
 * and the same with an idempotency key (`state-machine-receive-keyed`).
 */
type RpcTarget = "single" | BehaviourTarget | "state-machine-receive" | "state-machine-receive-keyed";

/**
 * Times one-key puts end to end, one at a time, from the caller: what a call through each target
 * costs the caller over a raw stub, RPC included. Targets alternate within a round, and each
 * round's order rotates, so drift touches them all alike.
 */
export async function probeRpc(env: BehavioursEnv, probe: ProbeRpc): Promise<{ calls: number; rounds: number; microsPerCall: Record<string, number[]> }> {
  const targets: readonly RpcTarget[] = probe.targets ?? ["single", ...BEHAVIOUR_TARGETS];
  const put = (target: RpcTarget, i: number): Promise<unknown> => {
    const keys = [`key-${i % 1000}`];
    const value = freshValue();
    if (target === "single") return env.SINGLE.getByName(probe.objectName).put(keys, [value]);
    if (target === "state-machine-receive-keyed") return env.KV_MACHINE.getByName(probe.objectName).receive({ kind: "call", message: { op: "put", keys, values: [value] }, key: crypto.randomUUID() });
    if (target === "state-machine-receive") return env.KV_MACHINE.getByName(probe.objectName).receive({ kind: "call", message: { op: "put", keys, values: [value] } });
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
