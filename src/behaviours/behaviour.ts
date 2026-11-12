// What every behaviour shares (RFD0009). A behaviour splits an object into the part every object
// of its kind needs and gets wrong in the same ways (taking messages, answering them, timers,
// retries, storing state) and the part only your object has: a few callbacks that say how an event
// changes the state.
//
// The shared part takes one message at a time, runs your callback, and commits what it decided
// (state, timers, outbox, kept answers) in one transaction before anything leaves the object. That
// fixes three things plain Durable Object code trips on:
// - A method that awaits lets other calls run in the middle of it. Callbacks are synchronous, so
//   each event runs alone, with no lock.
// - A method that throws halfway leaves half its writes behind, and the object restarts into that
//   state. A callback that throws commits nothing: failing leaves the stored state as it was.
// - Work after a write (a message to another object, a timer) is lost if the object resets between
//   them. Callbacks return effects instead, stored with the state and done after the commit.
//
// The same class is its own client. `Behaviour.client(ns, name)` makes an object whose prototype is
// the subclass's, so its public methods run in the caller, and only `call` and `cast` cross the wire,
// as one RPC to `receive` carrying an envelope.
import { DurableObject } from "cloudflare:workers";
import { CallError, DOClient, type CallOptions, type PreparedCall, type StatusCode } from "../client/index.js";
import type { Effect, Envelope, EventKind, From, Metadata, Outcome } from "./envelope.js";

/** @internal One event, as the base class hands it to a behaviour's step. */
export interface BehaviourEvent {
  readonly kind: EventKind;
  readonly message: unknown;
  /** Set for calls: who waits for the answer. */
  readonly from: From | undefined;
  /** The envelope's idempotency key, if it had one. */
  readonly key: string | undefined;
  readonly metadata: Metadata;
}

/** @internal What a behaviour's step decided about one event. */
export interface Step {
  readonly state: unknown;
  /** The answer to this event's caller. A call without one waits for `replyTo`. */
  readonly reply?: { readonly value: unknown };
  readonly effects?: readonly Effect<unknown>[] | undefined;
  /** Keep the event and run it again after the next step that sets `changed`. */
  readonly postpone?: boolean;
  /** A state machine changed state: postponed events run again. */
  readonly changed?: boolean;
}

/** What one committed step leaves to do once its transaction is durable. */
interface Committed {
  current: Outcome | undefined;
  replies: Array<readonly [From, Outcome]>;
  timersChanged: boolean;
  sent: boolean;
}

const NO_METADATA: Metadata = Object.freeze({});
const OK_NOTHING: Outcome = Object.freeze({ ok: true, value: null });
/** Answers kept for retries are pruned at most this often. */
const PRUNE_EVERY_MS = 60_000;
/** Outbox messages sent per drain, and the wait before trying a failed one again. */
const OUTBOX_BATCH = 64;
const OUTBOX_RETRY_MS = 1_000;
/** Rounds of postponed retries one event may trigger, against a machine that cycles forever. */
const MAX_POSTPONED_ROUNDS = 64;
/** Calls waiting for a deferred answer, before the expired ones are swept. */
const SWEEP_PENDING_OVER = 256;

/** Set on an object while one of its callbacks may run. A symbol: never visible over RPC. */
const IN_STEP = Symbol("behaviour.inStep");
/** Callbacks already wrapped by guardCallbacks, so a class is wrapped once. */
const guarded = new WeakSet<Function>();

/**
 * Every method on the prototype is callable over RPC, `protected` or not: TypeScript hides it from
 * the stub's type, the runtime doesn't. A callback called that way would run outside any event,
 * and its writes outside the transaction. So each class's callbacks are wrapped, once, to refuse
 * to run unless the object is running an event.
 */
function guardCallbacks(cls: Function, names: readonly string[]): void {
  for (let proto = cls.prototype; proto && proto !== Behaviour.prototype; proto = Object.getPrototypeOf(proto)) {
    for (const name of names) {
      const descriptor = Object.getOwnPropertyDescriptor(proto, name);
      const callback = descriptor?.value;
      if (typeof callback !== "function" || guarded.has(callback)) continue;
      const guard = function (this: { [IN_STEP]?: boolean }, ...args: unknown[]) {
        if (!this[IN_STEP]) throw new CallError("FAILED_PRECONDITION", `${name} is a callback: it runs inside an event, never as a call`);
        return callback.apply(this, args);
      };
      guarded.add(guard);
      Object.defineProperty(proto, name, { ...descriptor, value: guard });
    }
  }
}

const failed = (code: StatusCode, message: string): Outcome => ({ ok: false, code, message });

/** A callback's error as the caller gets it: a CallError keeps its code, anything else is a bug. */
function failure(error: unknown): Outcome {
  if (error instanceof CallError) return failed(error.code, error.message);
  return failed("INTERNAL", error instanceof Error ? error.message : String(error));
}

function unwrap(outcome: Outcome): unknown {
  if (outcome.ok) return outcome.value;
  throw new CallError(outcome.code, outcome.message);
}

export abstract class Behaviour<Env = unknown, Call = unknown, Reply = unknown, Cast = unknown> extends DurableObject<Env> {
  /** Bump when the stored state's shape changes: the object upgrades older state when it starts. */
  protected readonly version: number = 1;
  /** How long an answer is kept for retries of the same call (same idempotency key). */
  protected readonly idempotencyTtlMs: number = 10 * 60_000;

  #started = false;
  #state: unknown;
  /** Per instance, so a From kept in state from before a restart never matches a new caller. */
  #epoch = "";
  #nextFrom = 0;
  #pending = new Map<string, { readonly resolve: (outcome: Outcome) => void; readonly until: number }>();
  #inflight = new Map<string, Promise<Outcome>>();
  #postponed = 0;
  #alarmAt: number | null = null;
  #draining = false;
  #lastPrune = 0;
  [IN_STEP] = false;

  /** @internal The subclass's callbacks, which only run inside an event. */
  protected static readonly callbacks: readonly string[] = [];

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    if ((this as { alarm: unknown }).alarm !== Behaviour.prototype.alarm) {
      throw new Error(`${new.target.name} overrides alarm(): a behaviour's alarm runs its timers and outbox. Use timer effects instead.`);
    }
    guardCallbacks(new.target, (new.target as unknown as typeof Behaviour).callbacks);
  }

  /** Runs `fn` as the object's code, where callbacks may run. */
  #inStep<R>(fn: () => R): R {
    this[IN_STEP] = true;
    try {
      return fn();
    } finally {
      this[IN_STEP] = false;
    }
  }

  /** @internal The state a new object starts with. */
  protected abstract behaviourInit(): unknown;
  /** @internal The stored state of an older `version`, upgraded. */
  protected abstract behaviourMigrate(fromVersion: number, stored: unknown): unknown;
  /** @internal One event's transition. Synchronous: it runs inside the transaction. */
  protected abstract behaviourStep(event: BehaviourEvent, state: unknown): Step;

  /** The SQLite database, for callbacks that keep more than fits in their state. Write only from a callback: it runs in the event's transaction. */
  protected get sql(): SqlStorage {
    return this.ctx.storage.sql;
  }

  /**
   * Sends a request and waits for its answer. In the object, it runs the callback here; on a
   * client, it is one RPC with the client's options.
   */
  protected call(message: Call): Promise<Reply> {
    return this.#local("call", message) as Promise<Reply>;
  }

  /** Sends a message and returns once it is handled (committed), without an answer. */
  protected cast(message: Cast): Promise<void> {
    return this.#local("cast", message).then(() => undefined);
  }

  /**
   * A client of the object named `name`: the class's public methods, run in the caller, with
   * `call` and `cast` sent to the object. Options apply to every call; narrow them with `.with()`.
   */
  static client<T extends Behaviour<any, any, any, any>>(this: { readonly prototype: T }, namespace: DurableObjectNamespace<T>, name: string, options?: CallOptions): BehaviourClient<T> {
    return behaviourClient(this.prototype, new DOClient(namespace as DurableObjectNamespace<any>, name, options));
  }

  /** The one RPC method clients use. */
  async receive(envelope: Envelope): Promise<Outcome> {
    try {
      this.#start();
    } catch (error) {
      return failure(error);
    }
    if (envelope.deadline !== undefined && Date.now() > envelope.deadline) return failed("DEADLINE_EXCEEDED", "arrived after its deadline: not handled");
    const key = envelope.key;
    if (key === undefined) return this.#deliver(envelope.kind, envelope.message, undefined, envelope.metadata ?? NO_METADATA);
    const kept = this.#keptOutcome(key);
    if (kept) return kept;
    const running = this.#inflight.get(key);
    if (running) return running;
    const outcome = this.#deliver(envelope.kind, envelope.message, key, envelope.metadata ?? NO_METADATA);
    this.#inflight.set(key, outcome);
    try {
      return await outcome;
    } finally {
      this.#inflight.delete(key);
    }
  }

  /** Runs due timers as events, and sends what the outbox holds. */
  override async alarm(): Promise<void> {
    this.#start();
    this.#alarmAt = null; // this alarm is spent
    for (const due of this.sql.exec<{ name: string; message: string }>("SELECT name, message FROM behaviour_timers WHERE due <= ? ORDER BY due", Date.now()).toArray()) {
      const event: BehaviourEvent = { kind: "info", message: JSON.parse(due.message), from: undefined, key: undefined, metadata: NO_METADATA };
      try {
        this.#run(event, due.name);
      } catch (error) {
        // A timer whose callback throws is dropped, not retried forever.
        this.sql.exec("DELETE FROM behaviour_timers WHERE name = ?", due.name);
        console.error(`${this.constructor.name}: timer ${due.name} failed`, error);
      }
    }
    await this.#drainOutbox();
    this.#rearm();
  }

  #local(kind: "call" | "cast", message: unknown): Promise<unknown> {
    this.#start();
    return this.#deliver(kind, message, undefined, NO_METADATA).then(unwrap);
  }

  #start(): void {
    if (this.#started) return;
    const sql = this.sql;
    sql.exec("CREATE TABLE IF NOT EXISTS behaviour_state (k INTEGER PRIMARY KEY CHECK (k = 0), version INTEGER NOT NULL, state TEXT NOT NULL)");
    sql.exec("CREATE TABLE IF NOT EXISTS behaviour_timers (name TEXT PRIMARY KEY, due INTEGER NOT NULL, message TEXT NOT NULL)");
    sql.exec("CREATE TABLE IF NOT EXISTS behaviour_outbox (id INTEGER PRIMARY KEY AUTOINCREMENT, binding TEXT NOT NULL, name TEXT NOT NULL, message TEXT NOT NULL)");
    sql.exec("CREATE TABLE IF NOT EXISTS behaviour_replies (key TEXT PRIMARY KEY, outcome TEXT NOT NULL, at INTEGER NOT NULL)");
    sql.exec("CREATE TABLE IF NOT EXISTS behaviour_postponed (seq INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL)");
    this.#state = this.#loadState();
    this.#postponed = sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM behaviour_postponed").one().n;
    this.#epoch = crypto.randomUUID().slice(0, 8);
    this.#started = true;
    if (sql.exec("SELECT 1 FROM behaviour_outbox LIMIT 1").toArray().length > 0) this.ctx.waitUntil(this.#drainOutbox());
  }

  /** The stored state, upgraded if older; a new object's initial state, stored. */
  #loadState(): unknown {
    const row = this.sql.exec<{ version: number; state: string }>("SELECT version, state FROM behaviour_state WHERE k = 0").toArray()[0];
    if (row && row.version === this.version) return JSON.parse(row.state);
    const state = this.#inStep(() => (row ? this.behaviourMigrate(row.version, JSON.parse(row.state)) : this.behaviourInit()));
    this.#saveState(state);
    return state;
  }

  #saveState(state: unknown): void {
    this.sql.exec("INSERT INTO behaviour_state (k, version, state) VALUES (0, ?, ?) ON CONFLICT (k) DO UPDATE SET version = excluded.version, state = excluded.state", this.version, JSON.stringify(state));
  }

  /** Runs one new event, and answers its caller now, or later if the callback deferred it. */
  #deliver(kind: "call" | "cast", message: unknown, key: string | undefined, metadata: Metadata): Promise<Outcome> {
    const from: From | undefined = kind === "call" ? (key === undefined ? { id: this.#fromId() } : { id: this.#fromId(), key }) : undefined;
    let answer: Outcome | undefined;
    try {
      answer = this.#run({ kind, message, from, key, metadata }, undefined);
    } catch (error) {
      return Promise.resolve(failure(error));
    }
    if (from === undefined) return Promise.resolve(OK_NOTHING);
    if (answer) return Promise.resolve(answer);
    return this.#awaitReply(from);
  }

  #fromId(): string {
    return `${this.#epoch}.${++this.#nextFrom}`;
  }

  #awaitReply(from: From): Promise<Outcome> {
    if (this.#pending.size > SWEEP_PENDING_OVER) this.#sweepPending();
    return new Promise((resolve) => this.#pending.set(from.id, { resolve, until: Date.now() + this.idempotencyTtlMs }));
  }

  /** Callers that have surely given up: their answers would reach no one. */
  #sweepPending(): void {
    const now = Date.now();
    for (const [id, waiting] of this.#pending) if (waiting.until < now) this.#pending.delete(id);
  }

  /**
   * Runs one event in its own transaction, then does what the commit left to do. Returns the
   * answer to the event's caller, if the callback gave one. Throws, having committed nothing, if
   * the callback threw.
   */
  #run(event: BehaviourEvent, firedTimer: string | undefined): Outcome | undefined {
    const committed: Committed = { current: undefined, replies: [], timersChanged: false, sent: false };
    let step: Step | undefined;
    try {
      this.ctx.storage.transactionSync(() => {
        if (firedTimer !== undefined) this.sql.exec("DELETE FROM behaviour_timers WHERE name = ?", firedTimer);
        step = this.#inStep(() => this.behaviourStep(event, this.#state));
        this.#record(event, step, committed);
      });
    } catch (error) {
      this.#state = this.#loadState(); // the callback may have changed the state object before throwing
      throw error;
    }
    if (!step!.postpone) this.#state = step!.state;
    this.#afterCommit(committed);
    if (step!.changed && this.#postponed > 0) this.#retryPostponed();
    return committed.current;
  }

  /** Writes what a step decided. Runs inside the event's transaction. */
  #record(event: BehaviourEvent, step: Step, committed: Committed): void {
    if (step.postpone) {
      this.sql.exec("INSERT INTO behaviour_postponed (event) VALUES (?)", JSON.stringify(event));
      this.#postponed++;
      if (event.kind === "cast" && event.key !== undefined) this.#keepOutcome(event.key, OK_NOTHING);
      return;
    }
    if (step.state !== this.#state) this.#saveState(step.state);
    if (step.effects) for (const effect of step.effects) this.#recordEffect(effect, committed);
    if (event.from && step.reply) {
      committed.current = { ok: true, value: step.reply.value };
      if (event.from.key !== undefined) this.#keepOutcome(event.from.key, committed.current);
    } else if (event.kind === "cast" && event.key !== undefined) {
      this.#keepOutcome(event.key, OK_NOTHING);
    }
  }

  #recordEffect(effect: Effect<unknown>, committed: Committed): void {
    switch (effect.kind) {
      case "timer":
        this.sql.exec("INSERT INTO behaviour_timers (name, due, message) VALUES (?, ?, ?) ON CONFLICT (name) DO UPDATE SET due = excluded.due, message = excluded.message", effect.name, Date.now() + effect.afterMs, JSON.stringify(effect.message ?? null));
        committed.timersChanged = true;
        return;
      case "cancelTimer":
        if (this.sql.exec("DELETE FROM behaviour_timers WHERE name = ?", effect.name).rowsWritten > 0) committed.timersChanged = true;
        return;
      case "send":
        this.sql.exec("INSERT INTO behaviour_outbox (binding, name, message) VALUES (?, ?, ?)", effect.binding, effect.name, JSON.stringify(effect.message ?? null));
        committed.sent = true;
        return;
      case "reply": {
        const outcome: Outcome = { ok: true, value: effect.value };
        if (effect.to.key !== undefined) this.#keepOutcome(effect.to.key, outcome);
        committed.replies.push([effect.to, outcome]);
        return;
      }
    }
  }

  #afterCommit(committed: Committed): void {
    for (const [to, outcome] of committed.replies) this.#answer(to, outcome);
    if (committed.timersChanged) this.#rearm();
    if (committed.sent) this.ctx.waitUntil(this.#drainOutbox());
  }

  #answer(to: From, outcome: Outcome): void {
    const waiting = this.#pending.get(to.id);
    if (!waiting) return; // the caller is gone, or it was a caller of an earlier instance
    this.#pending.delete(to.id);
    waiting.resolve(outcome);
  }

  /**
   * After a state change, runs every postponed event again, oldest first. One that changes the
   * state again starts the round over. Each runs in its own transaction: one that throws is
   * dropped, and fails its caller, without undoing the others.
   */
  #retryPostponed(): void {
    for (let round = 0; round < MAX_POSTPONED_ROUNDS; round++) {
      let changedAgain = false;
      for (const row of this.sql.exec<{ seq: number; event: string }>("SELECT seq, event FROM behaviour_postponed ORDER BY seq").toArray()) {
        const event = JSON.parse(row.event) as BehaviourEvent;
        const outcome = this.#retryOne(row.seq, event);
        if (outcome === "postponed") continue;
        if (event.from && outcome.answer) this.#answer(event.from, outcome.answer);
        if (outcome.changed) {
          changedAgain = true;
          break;
        }
      }
      if (!changedAgain) return;
    }
    console.error(`${this.constructor.name}: postponed events kept changing the state; the rest wait for the next change`);
  }

  #retryOne(seq: number, event: BehaviourEvent): "postponed" | { readonly answer: Outcome | undefined; readonly changed: boolean } {
    const committed: Committed = { current: undefined, replies: [], timersChanged: false, sent: false };
    let step: Step | undefined;
    try {
      this.ctx.storage.transactionSync(() => {
        step = this.#inStep(() => this.behaviourStep(event, this.#state));
        if (step.postpone) return;
        this.sql.exec("DELETE FROM behaviour_postponed WHERE seq = ?", seq);
        this.#record(event, step, committed);
      });
    } catch (error) {
      this.#state = this.#loadState();
      this.sql.exec("DELETE FROM behaviour_postponed WHERE seq = ?", seq);
      this.#postponed--;
      if (event.from) this.#answer(event.from, failure(error));
      return { answer: undefined, changed: false };
    }
    if (step!.postpone) return "postponed";
    this.#postponed--;
    this.#state = step!.state;
    this.#afterCommit(committed);
    return { answer: committed.current, changed: step!.changed === true };
  }

  #keptOutcome(key: string): Outcome | undefined {
    const row = this.sql.exec<{ outcome: string }>("SELECT outcome FROM behaviour_replies WHERE key = ?", key).toArray()[0];
    return row ? (JSON.parse(row.outcome) as Outcome) : undefined;
  }

  #keepOutcome(key: string, outcome: Outcome): void {
    const now = Date.now();
    this.sql.exec("INSERT OR REPLACE INTO behaviour_replies (key, outcome, at) VALUES (?, ?, ?)", key, JSON.stringify(outcome), now);
    if (now - this.#lastPrune > PRUNE_EVERY_MS) {
      this.#lastPrune = now;
      this.sql.exec("DELETE FROM behaviour_replies WHERE at < ?", now - this.idempotencyTtlMs);
    }
  }

  /**
   * Casts what the outbox holds, oldest first, each with a key the receiver deduplicates, so a
   * message sent twice (the object reset before deleting it) is handled once. Stops at the first
   * failure and tries again from the alarm.
   */
  async #drainOutbox(): Promise<void> {
    if (this.#draining) return;
    this.#draining = true;
    const self = this.ctx.id.toString();
    try {
      for (;;) {
        const rows = this.sql.exec<{ id: number; binding: string; name: string; message: string }>("SELECT id, binding, name, message FROM behaviour_outbox ORDER BY id LIMIT ?", OUTBOX_BATCH).toArray();
        if (rows.length === 0) return;
        for (const row of rows) {
          const namespace = (this.env as Record<string, DurableObjectNamespace<Behaviour<any, any, any, any>> | undefined>)[row.binding];
          if (!namespace) throw new Error(`no binding ${row.binding} for an outbox message`);
          const outcome = await namespace.getByName(row.name).receive({ kind: "cast", message: JSON.parse(row.message), key: `${self}/${row.id}` });
          if (!outcome.ok) console.error(`${this.constructor.name}: ${row.binding}/${row.name} refused an outbox message: ${outcome.code} ${outcome.message}`);
          this.sql.exec("DELETE FROM behaviour_outbox WHERE id = ?", row.id);
        }
      }
    } catch (error) {
      console.error(`${this.constructor.name}: outbox delivery failed, retrying`, error);
      this.#armAlarm(Date.now() + OUTBOX_RETRY_MS);
    } finally {
      this.#draining = false;
    }
  }

  /** Sets the alarm for the earliest timer. */
  #rearm(): void {
    const due = this.sql.exec<{ due: number | null }>("SELECT MIN(due) AS due FROM behaviour_timers").one().due;
    if (due !== null) this.#armAlarm(due);
  }

  #armAlarm(at: number): void {
    if (this.#alarmAt !== null && this.#alarmAt <= at && this.#alarmAt > Date.now()) return;
    this.#alarmAt = at;
    void this.ctx.storage.setAlarm(at);
  }
}

/** The public methods a subclass adds: its client API. */
export type BehaviourClient<T> = {
  readonly [K in Exclude<keyof T, keyof Behaviour<any, any, any, any>> as T[K] extends (...args: any[]) => Promise<any> ? K : never]: T[K];
} & {
  /** The same object, with these options over this client's. */
  with(options: CallOptions): BehaviourClient<T>;
};

function behaviourClient<T>(prototype: T, remote: DOClient<any>): BehaviourClient<T> {
  const client = Object.create(prototype as object) as Record<string, unknown>;
  client.call = (message: unknown) => sendEnvelope(remote, "call", message);
  client.cast = (message: unknown) => sendEnvelope(remote, "cast", message).then(() => undefined);
  client.with = (options: CallOptions) => behaviourClient(prototype, remote.with(options));
  return client as BehaviourClient<T>;
}

async function sendEnvelope(remote: DOClient<any>, kind: "call" | "cast", message: unknown): Promise<unknown> {
  const outcome = (await remote.invokePrepared("receive", (call) => [envelopeOf(kind, message, call)])) as Outcome;
  return unwrap(outcome);
}

/** One envelope per logical call: every retry sends the same one, key and deadline included. */
function envelopeOf(kind: "call" | "cast", message: unknown, call: PreparedCall): Envelope {
  const { options, deadline } = call;
  const retried = (options.retry?.maxAttempts ?? 1) > 1;
  const key = options.idempotencyKey ?? (retried ? crypto.randomUUID() : undefined);
  const envelope: { -readonly [K in keyof Envelope]: Envelope[K] } = { kind, message };
  if (deadline !== undefined) envelope.deadline = deadline;
  if (key !== undefined) envelope.key = key;
  if (options.metadata !== undefined) envelope.metadata = options.metadata;
  return envelope;
}
