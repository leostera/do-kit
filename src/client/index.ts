// Client (RFD0009): the caller's side of a call to any Durable Object. A bare stub call waits as
// long as the object takes, fails with whatever the runtime threw, and leaves retrying, logging and
// tracing to every call site. This puts them on the client: a deadline, retries with backoff on
// transient failures only, cancellation, metadata, interceptors that wrap every attempt, and errors
// with a code that says whether trying again can help. Options are set on the client and narrowed
// per call with `.with({ ... })`. It needs nothing from the object: any class's RPC methods work.
//
// What it can't do alone: stop the object working on a call the caller gave up on. A deadline that
// passes ends the wait, not the work. Behaviours (RFD0009) send the deadline along, and the object
// drops calls that arrive after it.

/** Why a call failed, and so whether trying again can help. */
export type StatusCode =
  /** The deadline passed before an answer. The object may still have done the work. */
  | "DEADLINE_EXCEEDED"
  /** The caller's signal aborted the call. */
  | "CANCELLED"
  /** A transient failure: the object restarted or moved, or the connection dropped. */
  | "UNAVAILABLE"
  /** The object is overloaded. Never retried: retrying adds to the load. */
  | "RESOURCE_EXHAUSTED"
  /** The object's code refused the call's arguments. */
  | "INVALID_ARGUMENT"
  /** The object's code refused the call in its current state. */
  | "FAILED_PRECONDITION"
  /** The object's code found nothing to act on. */
  | "NOT_FOUND"
  /** The object reset while the call waited for its answer. */
  | "ABORTED"
  /** A bug: the object's code threw something that is not a CallError. */
  | "INTERNAL"
  /** The object threw, and nothing says why. */
  | "UNKNOWN";

/** A failed call. `code` says whether retrying can help; `cause` keeps the original error. */
export class CallError extends Error {
  override readonly name = "CallError";
  constructor(
    readonly code: StatusCode,
    message: string,
    options?: { cause?: unknown },
  ) {
    super(message, options);
  }
}

export interface RetryPolicy {
  /** Attempts, the first one included. 1 means no retries. */
  readonly maxAttempts: number;
  /** The first backoff; each later one doubles, with full jitter. Defaults to 50 ms. */
  readonly baseDelayMs?: number;
  /** The longest backoff. Defaults to 2,000 ms. */
  readonly maxDelayMs?: number;
}

export interface CallOptions {
  /** Give up waiting after this long. */
  readonly timeoutMs?: number;
  /** Give up waiting at this time (Unix ms). Wins over `timeoutMs` when both are set. */
  readonly deadline?: number;
  /**
   * Retries on UNAVAILABLE only, within the deadline. Off by default (one attempt). Only retry calls
   * that are safe to run twice: idempotent methods, or behaviour calls, which carry an
   * idempotency key the object deduplicates.
   */
  readonly retry?: RetryPolicy;
  /** Stops waiting when aborted, with CANCELLED. */
  readonly signal?: AbortSignal;
  /** Sent with behaviour calls (RFD0009) and handed to their callbacks. Plain RPC ignores it. */
  readonly metadata?: Readonly<Record<string, string>>;
  /**
   * Identifies one logical call across its retries, for behaviours to answer a repeat with the
   * first answer. Behaviour clients make one per call when retries are on.
   */
  readonly idempotencyKey?: string;
  /** Wrap every attempt, first to last: for logging, tracing, auth headers, metrics. */
  readonly interceptors?: readonly Interceptor[];
}

/** One attempt of one call, as interceptors see it. */
export interface Attempt {
  readonly method: string;
  readonly args: readonly unknown[];
  /** 1 for the first attempt. */
  readonly attempt: number;
  /** When the caller stops waiting (Unix ms), if it ever does. */
  readonly deadline: number | undefined;
  readonly options: CallOptions;
}

export type Interceptor = (attempt: Attempt, next: (attempt: Attempt) => Promise<unknown>) => Promise<unknown>;

/** The resolved shape of one call, fixed before its first attempt and the same for every retry. */
export interface PreparedCall {
  readonly deadline: number | undefined;
  readonly options: CallOptions;
}

/** The RPC methods of a Durable Object class: what a stub of it can call. */
export type RpcMethods<T> = {
  [K in keyof T as K extends "fetch" | "alarm" | "webSocketMessage" | "webSocketClose" | "webSocketError" | "connect" ? never : T[K] extends (...args: any[]) => any ? K : never]: T[K];
};

type Answer<F> = F extends (...args: any[]) => infer R ? Awaited<R> : never;
type Args<F> = F extends (...args: infer A) => any ? A : never;

/** A Client's methods as calls on the object, plus `.with()`. */
export type Remote<T> = {
  readonly [K in keyof RpcMethods<T>]: (...args: Args<RpcMethods<T>[K]>) => Promise<Answer<RpcMethods<T>[K]>>;
} & { with(options: CallOptions): Remote<T> };

/**
 * The stub a client and every client `.with()` made from it share. A stub belongs to the request
 * that made it (a Worker can't use one across requests), so it lives with the client, never in
 * the isolate. A stub can be left broken by an error, so an error that is not the object's own
 * answer drops it, and the next attempt makes a new one.
 */
class SharedStub<T extends Rpc.DurableObjectBranded | undefined> {
  #stub: DurableObjectStub<T> | null = null;
  constructor(
    private readonly namespace: DurableObjectNamespace<T>,
    private readonly name: string,
  ) {}
  get(): DurableObjectStub<T> {
    return (this.#stub ??= this.namespace.getByName(this.name));
  }
  drop(): void {
    this.#stub = null;
  }
}

/**
 * The caller's side of calls to one named Durable Object.
 *
 * ```ts
 * const counter = new Client(env.COUNTERS, "visits", { timeoutMs: 500 });
 * await counter.invoke("increment", 1);
 * await counter.with({ timeoutMs: 50, retry: { maxAttempts: 3 } }).invoke("read");
 * // or, method by method:
 * const remote = Client.of(env.COUNTERS, "visits");
 * await remote.with({ timeoutMs: 50 }).increment(1);
 * ```
 */
export class Client<T extends Rpc.DurableObjectBranded | undefined> {
  #stub: SharedStub<T>;

  constructor(
    readonly namespace: DurableObjectNamespace<T>,
    readonly name: string,
    readonly options: CallOptions = {},
  ) {
    this.#stub = new SharedStub(namespace, name);
  }

  /** The same object, with these options over this client's. Interceptors add up; the rest replace. */
  with(options: CallOptions): Client<T> {
    const narrowed = new Client(this.namespace, this.name, mergeOptions(this.options, options));
    narrowed.#stub = this.#stub;
    return narrowed;
  }

  /** Calls one of the object's RPC methods. */
  invoke<K extends keyof RpcMethods<T> & string>(method: K, ...args: Args<RpcMethods<T>[K]>): Promise<Answer<RpcMethods<T>[K]>> {
    return this.invokePrepared(method, () => args) as Promise<Answer<RpcMethods<T>[K]>>;
  }

  /**
   * Calls `method` with arguments built from the prepared call, once: every retry sends the same
   * arguments. Behaviour clients build their envelope here, with the call's deadline in it.
   */
  invokePrepared(method: string, argsOf: (call: PreparedCall) => readonly unknown[]): Promise<unknown> {
    const call = prepare(this.options);
    const args = argsOf(call);
    const options = call.options;
    if (options.retry === undefined && options.interceptors === undefined && call.deadline === undefined && options.signal === undefined) {
      return this.#bareAttempt(method, args); // the common case pays for nothing it didn't ask for
    }
    return this.#withRetries(method, args, call);
  }

  /** A client whose properties are the object's methods: `Client.of(ns, name).increment(1)`. */
  static of<T extends Rpc.DurableObjectBranded | undefined>(namespace: DurableObjectNamespace<T>, name: string, options?: CallOptions): Remote<T> {
    return remoteOf(new Client(namespace, name, options));
  }

  async #bareAttempt(method: string, args: readonly unknown[]): Promise<unknown> {
    try {
      return await callStub(this.#stub.get(), method, args);
    } catch (error) {
      throw this.#failure(error);
    }
  }

  async #withRetries(method: string, args: readonly unknown[], call: PreparedCall): Promise<unknown> {
    const maxAttempts = Math.max(1, call.options.retry?.maxAttempts ?? 1);
    for (let attempt = 1; ; attempt++) {
      try {
        return await this.#attempt({ method, args, attempt, deadline: call.deadline, options: call.options });
      } catch (error) {
        const failure = error instanceof CallError ? error : this.#failure(error);
        if (failure.code !== "UNAVAILABLE" || attempt >= maxAttempts) throw failure;
        await backoff(attempt, call);
      }
    }
  }

  #attempt(attempt: Attempt): Promise<unknown> {
    const send = (a: Attempt) => callStub(this.#stub.get(), a.method, a.args).catch((error) => Promise.reject(this.#failure(error)));
    const intercepted = (attempt.options.interceptors ?? []).reduceRight<(a: Attempt) => Promise<unknown>>((next, interceptor) => (a) => interceptor(a, next), send);
    return bounded(intercepted(attempt), attempt.deadline, attempt.options.signal);
  }

  /** What an error from the stub means for the caller, dropping the stub unless the object answered. */
  #failure(error: unknown): CallError {
    if (error instanceof CallError) return error;
    const e = error as { message?: string; retryable?: boolean; overloaded?: boolean; remote?: boolean };
    const message = e?.message ?? String(error);
    if (e?.overloaded) {
      this.#stub.drop();
      return new CallError("RESOURCE_EXHAUSTED", message, { cause: error });
    }
    if (e?.retryable) {
      this.#stub.drop();
      return new CallError("UNAVAILABLE", message, { cause: error });
    }
    if (!e?.remote) this.#stub.drop();
    return new CallError("UNKNOWN", message, { cause: error });
  }
}

/** Ends the wait at the deadline or on abort, whichever is first, without stopping `work`. */
function bounded<R>(work: Promise<R>, deadline: number | undefined, signal: AbortSignal | undefined): Promise<R> {
  if (deadline === undefined && signal === undefined) return work;
  if (signal?.aborted) return Promise.reject(cancelled(signal));
  return new Promise<R>((resolve, reject) => {
    const timer = deadline === undefined ? undefined : setTimeout(() => finish(() => reject(deadlineExceeded(deadline))), Math.max(0, deadline - Date.now()));
    const onAbort = () => finish(() => reject(cancelled(signal!)));
    signal?.addEventListener("abort", onAbort, { once: true });
    function finish(settle: () => void) {
      if (timer !== undefined) clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      settle();
    }
    work.then(
      (value) => finish(() => resolve(value)),
      (error) => finish(() => reject(error)),
    );
  });
}

/** Waits a jittered, doubling backoff, or fails if the deadline comes first. */
async function backoff(attempt: number, call: PreparedCall): Promise<void> {
  const base = call.options.retry?.baseDelayMs ?? 50;
  const max = call.options.retry?.maxDelayMs ?? 2_000;
  const delay = Math.random() * Math.min(max, base * 2 ** (attempt - 1));
  if (call.deadline !== undefined && Date.now() + delay >= call.deadline) throw deadlineExceeded(call.deadline);
  await bounded(new Promise((resolve) => setTimeout(resolve, delay)), undefined, call.options.signal);
}

function prepare(options: CallOptions): PreparedCall {
  const deadline = options.deadline ?? (options.timeoutMs === undefined ? undefined : Date.now() + options.timeoutMs);
  return { deadline, options };
}

function mergeOptions(base: CallOptions, over: CallOptions): CallOptions {
  const merged: CallOptions = { ...base, ...over };
  // A per-call timeout replaces the client's deadline, and the reverse.
  if (over.timeoutMs !== undefined && over.deadline === undefined) delete (merged as { deadline?: number }).deadline;
  if (base.metadata && over.metadata) (merged as { metadata?: CallOptions["metadata"] }).metadata = { ...base.metadata, ...over.metadata };
  if (base.interceptors && over.interceptors) (merged as { interceptors?: CallOptions["interceptors"] }).interceptors = [...base.interceptors, ...over.interceptors];
  return merged;
}

/** Calls through the stub itself: its methods are RPC proxies, so even `.apply` on one would be a remote call. */
function callStub(stub: unknown, method: string, args: readonly unknown[]): Promise<unknown> {
  return (stub as Record<string, (...a: unknown[]) => Promise<unknown>>)[method]!(...args);
}

const deadlineExceeded = (deadline: number) => new CallError("DEADLINE_EXCEEDED", `no answer by the deadline (${new Date(deadline).toISOString()})`);
const cancelled = (signal: AbortSignal) => new CallError("CANCELLED", "the caller cancelled the call", { cause: signal.reason });

function remoteOf<T extends Rpc.DurableObjectBranded | undefined>(client: Client<T>): Remote<T> {
  return new Proxy({} as Remote<T>, {
    get(_, property) {
      if (property === "with") return (options: CallOptions) => remoteOf(client.with(options));
      if (typeof property !== "string" || property === "then") return undefined; // not a thenable
      return (...args: unknown[]) => client.invokePrepared(property, () => args);
    },
  });
}
