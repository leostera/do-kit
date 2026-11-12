// Behaviours and DOClient in the test Worker (RFD0009): a channel pool (GenServer), a code lock
// (GenStatem) that reports to an audit log (GenEvent), and a plain object for DOClient. The route
// runs in the Worker, so the classes' client methods run here, as in any caller.
import { DurableObject } from "cloudflare:workers";
import { CallError, DOClient, type Attempt, type CallOptions } from "../../../src/client/index.ts";
import { GenEvent, GenServer, GenStatem, keep, next, postpone, replyTo, send, stateTimeout, type CallResult, type EventHandler, type From, type Metadata, type StateHandlers } from "../../../src/behaviours/index.ts";

export interface BehavioursEnv {
  readonly CHANNELS: DurableObjectNamespace<Channels>;
  readonly LOCKS: DurableObjectNamespace<CodeLock>;
  readonly AUDITS: DurableObjectNamespace<Audit>;
  readonly PLAINS: DurableObjectNamespace<Plain>;
}

// ---- A channel pool ----

type Pool = { readonly free: readonly number[]; readonly waiting: readonly From[]; readonly lastCaller: string | null };
type PoolCall = { readonly op: "alloc" } | { readonly op: "allocWait" } | { readonly op: "hang" } | { readonly op: "explode" } | { readonly op: "state" } | { readonly op: "rows" };
type PoolCast = { readonly op: "free"; readonly channel: number };

export class Channels extends GenServer<BehavioursEnv, Pool, PoolCall, unknown, PoolCast> {
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
  /** Writes a row and changes the state, then throws. */
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
  protected init(): Pool {
    this.sql.exec("CREATE TABLE IF NOT EXISTS pool_log (n INTEGER)");
    return { free: [0, 1, 2], waiting: [], lastCaller: null };
  }

  protected handleCall(request: PoolCall, state: Pool, from: From, metadata: Metadata): CallResult<Pool, unknown> {
    const lastCaller = metadata.caller ?? state.lastCaller;
    switch (request.op) {
      case "alloc":
      case "allocWait": {
        const [channel, ...free] = state.free;
        if (channel !== undefined) return { reply: channel, state: { ...state, free, lastCaller } };
        if (request.op === "alloc") return { reply: null, state };
        return { noreply: true, state: { ...state, waiting: [...state.waiting, from] } };
      }
      case "hang":
        return { noreply: true, state };
      case "explode":
        this.sql.exec("INSERT INTO pool_log (n) VALUES (1)");
        (state as unknown as { free: number[] }).free = []; // changed in place, then throws: neither may stick
        throw new Error("explode");
      case "state":
        return { reply: state, state };
      case "rows":
        return { reply: this.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM pool_log").one().n, state };
    }
  }

  protected override handleCast(message: PoolCast, state: Pool) {
    const [waiter, ...waiting] = state.waiting;
    if (waiter) return { state: { ...state, waiting }, effects: [replyTo(waiter, message.channel)] };
    return { state: { ...state, free: [...state.free, message.channel] } };
  }

  /** Test only, through a raw stub: ends this instance. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
}

// ---- A code lock that reports to an audit log ----

type LockState = "locked" | "open";
type LockData = { readonly code: string; readonly entered: string; readonly opened: number };
type LockEvent = { readonly type: "press"; readonly digit: string } | { readonly type: "lock" } | { readonly type: "status" } | { readonly type: "setCode"; readonly code: string };

export class CodeLock extends GenStatem<BehavioursEnv, LockState, LockData, LockEvent, unknown> {
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
          return next("open", { ...data, entered: "", opened }, { actions: [stateTimeout(Number(name.split(":")[1] ?? 10_000), { type: "lock" }), send("AUDITS", name, GenEvent.notification({ type: "opened", lock: name, n: opened }))] });
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

// ---- An audit log: an event manager ----

type AuditEvent = { readonly type: string; readonly lock?: string; readonly n?: number };

const counter: EventHandler<AuditEvent, number, unknown, "count", number> = {
  init: () => 0,
  handleEvent: (_event, n) => ({ state: n + 1 }),
  handleCall: (_request, n) => ({ reply: n, state: n }),
};

const recent: EventHandler<AuditEvent, AuditEvent[], number, "list", AuditEvent[]> = {
  init: () => [],
  handleEvent: (event, events) => ({ state: [...events, event].slice(-10) }),
  handleCall: (_request, events) => ({ reply: events, state: events }),
};

const fragile: EventHandler<AuditEvent, null> = {
  init: () => null,
  handleEvent: (event) => {
    if (event.type === "boom") throw new Error("fragile handler");
    return { state: null };
  },
};

const once: EventHandler<AuditEvent, null> = {
  init: () => null,
  handleEvent: () => ({ remove: true }),
};

export class Audit extends GenEvent<BehavioursEnv, AuditEvent> {
  protected readonly handlerTypes = { counter, recent, fragile, once };

  /** Test only. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
}

// ---- A plain object, for DOClient ----

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
      const handleCall = await settle(() => stub.handleCall!({ op: "alloc" }, { free: [9], waiting: [], lastCaller: null }, { id: "x" }, {}));
      const lockStub = env.LOCKS.getByName(name) as unknown as Record<string, unknown>;
      const states = await settle(async () => typeof (await (lockStub.states as Promise<unknown>)));
      return { handleCall, states };
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

    case "addHandler":
      return settle(() => audit.addHandler(String(args[0]), String(args[1])));
    case "deleteHandler":
      return settle(() => audit.deleteHandler(String(args[0])));
    case "notify":
      return settle(() => audit.notify(args[0] as AuditEvent));
    case "callHandler":
      return settle(() => audit.callHandler(String(args[0]), args[1]));
    case "whichHandlers":
      return settle(() => audit.whichHandlers());

    case "plain": {
      const plain = new DOClient(env.PLAINS, name, options);
      const [method, ...rest] = args as [string, ...unknown[]];
      return settle(() => (plain.invoke as (m: string, ...a: unknown[]) => Promise<unknown>)(method, ...rest));
    }
    case "plainProxy": {
      const plain = DOClient.of(env.PLAINS, name, options);
      return settle(async () => [await plain.echo("hi"), await plain.with({ timeoutMs: 1_000 }).echo(2)]);
    }
    case "plainAbort": {
      const controller = new AbortController();
      const waiting = settle(() => new DOClient(env.PLAINS, name, { signal: controller.signal }).invoke("sleep", 1_000));
      await scheduler.wait(20);
      controller.abort("test");
      return waiting;
    }
  }
  throw new Error(`no behaviours op ${request.op}`);
}
