// Behaviours and Client in the test Worker (RFD0009): XState machines run as Durable Objects (a
// one-state channel pool, for what every behaviour does; a code lock that reports to an audit log)
// and a plain object for Client. The route runs in the Worker, so the classes' client methods run
// here, as in any caller.
import { DurableObject } from "cloudflare:workers";
import { CallError, Client, type Attempt, type CallOptions } from "../../../src/client/index.ts";
import { assign, setup } from "xstate";
import { DurableStateMachine, callOf, durableActions, postpone, reply, replyTo, sendToObject, type From } from "../../../src/behaviours/index.ts";

export interface BehavioursEnv {
  readonly CHANNELS: DurableObjectNamespace<Channels>;
  readonly LOCKS: DurableObjectNamespace<CodeLock>;
  readonly AUDITS: DurableObjectNamespace<Audit>;
  readonly PLAINS: DurableObjectNamespace<Plain>;
}

// ---- A channel pool ----

type Pool = { readonly free: readonly number[]; readonly waiting: readonly From[]; readonly lastCaller: string | null };
type PoolEvent = { readonly type: "alloc" } | { readonly type: "allocWait" } | { readonly type: "hang" } | { readonly type: "explode" } | { readonly type: "state" } | { readonly type: "rows" } | { readonly type: "free"; readonly channel: number };

/** One state, for what every behaviour does: answers now and later, metadata, rollback, SQL. */
const poolMachine = (sql: () => SqlStorage) =>
  setup({
    types: { context: {} as Pool, events: {} as PoolEvent },
    actions: {
      ...durableActions,
      ignore: () => {},
      // Writes a row, then throws: neither the row nor the context change before it may stick.
      boom: () => {
        sql().exec("INSERT INTO pool_log (n) VALUES (1)");
        throw new Error("explode");
      },
    },
    guards: { hasFree: ({ context }) => context.free.length > 0, hasWaiter: ({ context }) => context.waiting.length > 0 },
  }).createMachine({
    context: { free: [0, 1, 2], waiting: [], lastCaller: null },
    initial: "serving",
    states: {
      serving: {
        on: {
          alloc: [
            { guard: "hasFree", actions: [reply(({ context }) => context.free[0]), assign({ free: ({ context }) => context.free.slice(1), lastCaller: ({ context, event }) => callOf(event).metadata.caller ?? context.lastCaller })] },
            { actions: reply(() => null) },
          ],
          allocWait: [
            { guard: "hasFree", actions: [reply(({ context }) => context.free[0]), assign({ free: ({ context }) => context.free.slice(1) })] },
            // No answer now: the caller waits, and a later free answers it.
            { actions: assign({ waiting: ({ context, event }) => [...context.waiting, callOf(event).from!] }) },
          ],
          free: [
            { guard: "hasWaiter", actions: [replyTo(({ context }) => context.waiting[0], ({ event }) => event.channel), assign({ waiting: ({ context }) => context.waiting.slice(1) })] },
            { actions: assign({ free: ({ context, event }) => [...context.free, event.channel] }) },
          ],
          hang: { actions: "ignore" },
          explode: { actions: [assign({ free: [] }), "boom"] },
          state: { actions: reply(({ context }) => context) },
          rows: { actions: reply(() => sql().exec<{ n: number }>("SELECT COUNT(*) AS n FROM pool_log").one().n) },
        },
      },
    },
  });

export class Channels extends DurableStateMachine<BehavioursEnv, ReturnType<typeof poolMachine>> {
  protected readonly machine = poolMachine(() => this.sql);

  constructor(ctx: DurableObjectState, env: BehavioursEnv) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS pool_log (n INTEGER)");
  }

  // ---- client API: runs in the caller ----
  alloc(): Promise<number | null> {
    return this.call({ type: "alloc" }) as Promise<number | null>;
  }
  /** Waits for a channel if none is free. */
  allocWait(): Promise<number> {
    return this.call({ type: "allocWait" }) as Promise<number>;
  }
  free(channel: number): Promise<void> {
    if (!Number.isInteger(channel) || channel < 0) return Promise.reject(new CallError("INVALID_ARGUMENT", `not a channel: ${channel}`));
    return this.cast({ type: "free", channel });
  }
  /** Never answers. */
  hang(): Promise<never> {
    return this.call({ type: "hang" }) as Promise<never>;
  }
  /** Writes a row and changes the context, then throws. */
  explode(): Promise<never> {
    return this.call({ type: "explode" }) as Promise<never>;
  }
  state(): Promise<Pool> {
    return this.call({ type: "state" }) as Promise<Pool>;
  }
  rows(): Promise<number> {
    return this.call({ type: "rows" }) as Promise<number>;
  }

  /** Test only, through a raw stub: ends this instance. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
}

// ---- A code lock that reports to an audit log ----

type LockState = "locked" | "open";
type LockContext = { readonly code: string; readonly entered: string; readonly opened: number; readonly openMs: number; readonly name: string };
type LockEvent = { readonly type: "press"; readonly digit: string } | { readonly type: "lock" } | { readonly type: "status" } | { readonly type: "setCode"; readonly code: string };

/** A lock named "anything:ms" stays open for ms after the right code. */
const lockMachine = (name: string) =>
  setup({
    types: { context: {} as LockContext, events: {} as LockEvent },
    actions: { ...durableActions, ignore: () => {} },
    guards: {
      opens: ({ context, event }) => event.type === "press" && (context.entered + event.digit).slice(-context.code.length) === context.code,
      isCall: ({ event }) => callOf(event).kind === "call",
    },
    delays: { openFor: ({ context }) => context.openMs },
  }).createMachine({
    context: { code: "1234", entered: "", opened: 0, openMs: Number(name.split(":")[1] ?? 10_000), name },
    initial: "locked",
    states: {
      locked: {
        on: {
          press: [
            {
              guard: "opens",
              target: "open",
              actions: [
                assign({ entered: "", opened: ({ context }) => context.opened + 1 }),
                sendToObject("AUDITS", ({ context }) => context.name, ({ context }) => ({ type: "opened", lock: context.name, n: context.opened })),
              ],
            },
            { actions: assign({ entered: ({ context, event }) => (context.entered + event.digit).slice(-context.code.length) }) },
          ],
          status: { actions: reply(({ context }) => ({ state: "locked", opened: context.opened })) },
          // A call waits until the lock opens; a cast is dropped.
          setCode: [{ guard: "isCall", actions: postpone() }, { actions: "ignore" }],
          lock: { actions: "ignore" },
        },
      },
      open: {
        after: { openFor: "locked" },
        on: {
          lock: "locked",
          status: { actions: reply(({ context }) => ({ state: "open", opened: context.opened })) },
          setCode: { actions: [assign({ code: ({ event }) => event.code }), reply(() => true)] },
          // press: no transition here, so presses wait until it locks again.
        },
      },
    },
  });

export class CodeLock extends DurableStateMachine<BehavioursEnv, ReturnType<typeof lockMachine>> {
  protected readonly machine = lockMachine(this.ctx.id.name ?? "");

  // ---- client API ----
  press(digit: string): Promise<void> {
    return this.cast({ type: "press", digit });
  }
  enter(code: string): Promise<void> {
    return [...code].reduce((done, digit) => done.then(() => this.press(digit)), Promise.resolve());
  }
  lock(): Promise<void> {
    return this.cast({ type: "lock" });
  }
  status(): Promise<{ state: LockState; opened: number }> {
    return this.call({ type: "status" }) as Promise<{ state: LockState; opened: number }>;
  }
  /** Only while open; while locked, it waits until the lock opens. */
  setCode(code: string): Promise<boolean> {
    return this.call({ type: "setCode", code }) as Promise<boolean>;
  }

  /** Test only, through a raw stub. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
  /** Test only: this object's name, and the messages its outbox still holds. */
  async outbox(): Promise<{ name: string | null; held: number }> {
    return { name: this.ctx.id.name ?? null, held: this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM behaviour_outbox").one().n };
  }
}

// ---- An audit log: the last events sent to it ----

type AuditEvent = { readonly type: string; readonly lock?: string; readonly n?: number };

const auditMachine = setup({ types: { context: {} as { events: readonly AuditEvent[] }, events: {} as AuditEvent }, actions: durableActions }).createMachine({
  context: { events: [] },
  initial: "recording",
  states: {
    recording: {
      on: {
        "recent?": { actions: reply(({ context }) => context.events) },
        "*": { actions: assign({ events: ({ context, event }) => [...context.events, { ...event }].slice(-10) }) },
      },
    },
  },
});

export class Audit extends DurableStateMachine<BehavioursEnv, typeof auditMachine> {
  protected readonly machine = auditMachine;

  recent(): Promise<readonly AuditEvent[]> {
    return this.call({ type: "recent?" }) as Promise<readonly AuditEvent[]>;
  }
}

// ---- A plain object, for Client ----

export class Plain extends DurableObject<BehavioursEnv> {
  #calls = 0;
  async echo<T>(value: T): Promise<T> {
    return value;
  }
  async sleep(ms: number): Promise<number> {
    await scheduler.wait(ms);
    return ms;
  }
  async fail(message: string): Promise<never> {
    throw new Error(message);
  }
  async calls(): Promise<number> {
    return ++this.#calls;
  }
}

// ---- The route ----

export type BehavioursRequest = { op: string; name: string; options?: CallOptions; args?: unknown[] };

/** A caught CallError as JSON, so tests see its code. */
async function settle(work: () => Promise<unknown>): Promise<unknown> {
  try {
    return { ok: true, value: (await work()) ?? null };
  } catch (error) {
    return { ok: false, code: error instanceof CallError ? error.code : "THROWN", message: error instanceof Error ? error.message : String(error) };
  }
}

export async function behavioursRoute(env: BehavioursEnv, request: BehavioursRequest): Promise<unknown> {
  const { name, options = {} } = request;
  const args = request.args ?? [];
  const channels = Channels.client(env.CHANNELS, name, options);
  const lock = CodeLock.client(env.LOCKS, name, options);
  const audit = Audit.client(env.AUDITS, name, options);
  switch (request.op) {
    case "alloc":
      return settle(() => channels.alloc());
    case "allocWait":
      return settle(() => channels.allocWait());
    case "free":
      return settle(() => channels.free(args[0] as number));
    case "hang":
      return settle(() => channels.hang());
    case "explode":
      return settle(() => channels.explode());
    case "poolState":
      return settle(() => channels.state());
    case "poolRows":
      return settle(() => channels.rows());
    case "allocTwiceWithKey":
      // The same logical call sent twice, as a retry after a lost answer would.
      return settle(async () => [await channels.with({ idempotencyKey: String(args[0]) }).alloc(), await channels.with({ idempotencyKey: String(args[0]) }).alloc()]);
    case "allocConcurrentWithKey":
      return settle(() => Promise.all([channels.with({ idempotencyKey: String(args[0]) }).allocWait(), channels.with({ idempotencyKey: String(args[0]) }).allocWait()]));
    case "allocWaitThenFree": {
      const waiting = settle(() => channels.allocWait());
      await scheduler.wait(50);
      await channels.free(args[0] as number);
      return waiting;
    }
    case "pastDeadline":
      return env.CHANNELS.getByName(name).receive({ kind: "call", message: { type: "alloc" }, deadline: Date.now() - 1_000 });
    case "intercepted": {
      const seen: unknown[] = [];
      const record = (attempt: Attempt, nextAttempt: (a: Attempt) => Promise<unknown>) => {
        seen.push({ method: attempt.method, args: attempt.args, attempt: attempt.attempt });
        return nextAttempt(attempt);
      };
      const value = await channels.with({ interceptors: [record], metadata: { caller: "tests" } }).alloc();
      return { value, seen };
    }
    case "restartPool":
      return settle(() => env.CHANNELS.getByName(name).restart());
    case "hangThenRestart": {
      const waiting = settle(() => channels.hang());
      await scheduler.wait(50);
      await env.CHANNELS.getByName(name).restart().catch(() => undefined);
      return waiting;
    }
    case "reachCallback": {
      // Can a caller reach a protected callback through a raw stub?
      const stub = env.CHANNELS.getByName(name) as unknown as Record<string, (...a: unknown[]) => Promise<unknown>>;
      const migrate = await settle(() => stub.migrate!(1, {}));
      const machine = await settle(async () => typeof (await (stub.machine as unknown as Promise<unknown>)));
      return { migrate, machine };
    }

    case "enter":
      return settle(() => lock.enter(String(args[0])));
    case "press":
      return settle(() => lock.press(String(args[0])));
    case "lock":
      return settle(() => lock.lock());
    case "lockStatus":
      return settle(() => lock.status());
    case "setCode":
      return settle(() => lock.setCode(String(args[0])));
    case "setCodeWhileLocked": {
      // Postponed while locked; answered once the right code opens it.
      const waiting = settle(() => lock.setCode(String(args[0])));
      await scheduler.wait(50);
      await lock.enter(String(args[1]));
      return waiting;
    }
    case "lockOutbox":
      return settle(() => env.LOCKS.getByName(name).outbox());
    case "restartLock":
      return settle(() => env.LOCKS.getByName(name).restart());

    case "auditRecent":
      return settle(() => audit.recent());

    case "plain": {
      const plain = new Client(env.PLAINS, name, options);
      const [method, ...rest] = args as [string, ...unknown[]];
      return settle(() => (plain.invoke as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...rest));
    }
    case "plainProxy": {
      const plain = Client.of(env.PLAINS, name, options);
      return settle(async () => [await plain.echo("hi"), await plain.with({ timeoutMs: 1_000 }).echo(2)]);
    }
    case "plainAbort": {
      const controller = new AbortController();
      const waiting = settle(() => new Client(env.PLAINS, name, { signal: controller.signal }).invoke("sleep", 1_000));
      await scheduler.wait(20);
      controller.abort("test");
      return waiting;
    }
  }
  throw new Error(`no behaviours op ${request.op}`);
}
