// Behaviours and Client in the test Worker (RFD0009): a channel pool (a one-state DurableStateMachine, for
// what every behaviour does), a code lock that reports to an audit log, and a plain object for
// Client. The route
// runs in the Worker, so the classes' client methods run here, as in any caller.
import { DurableObject } from "cloudflare:workers";
import { CallError, Client, type Attempt, type CallOptions } from "../../../src/client/index.ts";
import { DurableStateMachine, keep, next, postpone, replyTo, send, stateTimeout, type From, type Metadata, type StateHandlers } from "../../../src/behaviours/index.ts";

export interface BehavioursEnv {
  readonly CHANNELS: DurableObjectNamespace<Channels>;
  readonly LOCKS: DurableObjectNamespace<CodeLock>;
  readonly AUDITS: DurableObjectNamespace<Audit>;
  readonly PLAINS: DurableObjectNamespace<Plain>;
}

// ---- A channel pool ----

type Pool = { readonly free: readonly number[]; readonly waiting: readonly From[]; readonly lastCaller: string | null };
type PoolEvent = { readonly op: "alloc" } | { readonly op: "allocWait" } | { readonly op: "hang" } | { readonly op: "explode" } | { readonly op: "state" } | { readonly op: "rows" } | { readonly op: "free"; readonly channel: number };

export class Channels extends DurableStateMachine<BehavioursEnv, "serving", Pool, PoolEvent, unknown> {
  // ---- client API: runs in the caller ----
  alloc(): Promise<number | null> {
    return this.call({ op: "alloc" }) as Promise<number | null>;
  }
  /** Waits for a channel if none is free. */
  allocWait(): Promise<number> {
    return this.call({ op: "allocWait" }) as Promise<number>;
  }
  free(channel: number): Promise<void> {
    if (!Number.isInteger(channel) || channel < 0) return Promise.reject(new CallError("INVALID_ARGUMENT", `not a channel: ${channel}`));
    return this.cast({ op: "free", channel });
  }
  /** Never answers. */
  hang(): Promise<never> {
    return this.call({ op: "hang" }) as Promise<never>;
  }
  /** Writes a row and changes the data, then throws. */
  explode(): Promise<never> {
    return this.call({ op: "explode" }) as Promise<never>;
  }
  state(): Promise<Pool> {
    return this.call({ op: "state" }) as Promise<Pool>;
  }
  rows(): Promise<number> {
    return this.call({ op: "rows" }) as Promise<number>;
  }

  // ---- callbacks ----
  protected init() {
    this.sql.exec("CREATE TABLE IF NOT EXISTS pool_log (n INTEGER)");
    return { state: "serving" as const, data: { free: [0, 1, 2], waiting: [], lastCaller: null } };
  }

  protected readonly states: StateHandlers<"serving", Pool, PoolEvent, unknown> = {
    serving: (event, pool, { from, metadata }) => {
      const lastCaller = metadata.caller ?? pool.lastCaller;
      switch (event.op) {
        case "alloc":
        case "allocWait": {
          const [channel, ...free] = pool.free;
          if (channel !== undefined) return keep({ ...pool, free, lastCaller }, { reply: channel });
          if (event.op === "alloc") return keep(pool, { reply: null });
          return keep({ ...pool, waiting: [...pool.waiting, from!] }); // answered by a later free
        }
        case "hang":
          return keep(pool);
        case "explode":
          this.sql.exec("INSERT INTO pool_log (n) VALUES (1)");
          (pool as unknown as { free: number[] }).free = []; // changed in place, then throws: neither may stick
          throw new Error("explode");
        case "state":
          return keep(pool, { reply: pool });
        case "rows":
          return keep(pool, { reply: this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM pool_log").one().n });
        case "free": {
          const [waiter, ...waiting] = pool.waiting;
          if (waiter) return keep({ ...pool, waiting }, { actions: [replyTo(waiter, event.channel)] });
          return keep({ ...pool, free: [...pool.free, event.channel] });
        }
      }
    },
  };

  /** Test only, through a raw stub: ends this instance. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
}

// ---- A code lock that reports to an audit log ----

type LockState = "locked" | "open";
type LockData = { readonly code: string; readonly entered: string; readonly opened: number };
type LockEvent = { readonly type: "press"; readonly digit: string } | { readonly type: "lock" } | { readonly type: "status" } | { readonly type: "setCode"; readonly code: string };

export class CodeLock extends DurableStateMachine<BehavioursEnv, LockState, LockData, LockEvent, unknown> {
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

  // ---- callbacks ----
  protected init() {
    return { state: "locked" as const, data: { code: "1234", entered: "", opened: 0 } };
  }

  protected readonly states: StateHandlers<LockState, LockData, LockEvent, unknown> = {
    locked: (event, data, context) => {
      switch (event.type) {
        case "press": {
          const entered = (data.entered + event.digit).slice(-data.code.length);
          if (entered !== data.code) return keep({ ...data, entered });
          const opened = data.opened + 1;
          const name = this.ctx.id.name ?? "";
          return next("open", { ...data, entered: "", opened }, { actions: [stateTimeout(Number(name.split(":")[1] ?? 10_000), { type: "lock" }), send("AUDITS", name, { type: "opened", lock: name, n: opened })] });
        }
        case "status":
          return keep(data, { reply: { state: "locked", opened: data.opened } });
        case "setCode":
          return context.kind === "call" ? postpone() : keep(data);
        default:
          return keep(data);
      }
    },
    open: (event, data) => {
      switch (event.type) {
        case "lock":
          return next("locked", data);
        case "status":
          return keep(data, { reply: { state: "open", opened: data.opened } });
        case "setCode":
          return keep({ ...data, code: event.code }, { reply: true });
        default:
          return postpone(); // presses wait until it locks again
      }
    },
  };

  /** Test only, through a raw stub. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
  /** Test only: this object's name, and the messages its outbox still holds. */
  async outbox(): Promise<{ name: string | null; held: number }> {
    return { name: this.ctx.id.name ?? null, held: this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM behaviour_outbox").one().n };
  }
}

// ---- An audit log: the last events cast to it ----

type AuditEvent = { readonly type: string; readonly lock?: string; readonly n?: number };
type AuditMessage = AuditEvent | { readonly type: "recent?" };

export class Audit extends DurableStateMachine<BehavioursEnv, "recording", readonly AuditEvent[], AuditMessage, readonly AuditEvent[]> {
  recent(): Promise<readonly AuditEvent[]> {
    return this.call({ type: "recent?" }) as Promise<readonly AuditEvent[]>;
  }

  protected init() {
    return { state: "recording" as const, data: [] as readonly AuditEvent[] };
  }

  protected readonly states: StateHandlers<"recording", readonly AuditEvent[], AuditMessage, readonly AuditEvent[]> = {
    recording: (event, events) => (event.type === "recent?" ? keep(events, { reply: events }) : keep([...events, event].slice(-10))),
  };
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
      return env.CHANNELS.getByName(name).receive({ kind: "call", message: { op: "alloc" }, deadline: Date.now() - 1_000 });
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
      const init = await settle(() => stub.init!());
      const states = await settle(async () => typeof (await (stub.states as unknown as Promise<unknown>)));
      return { init, states };
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
