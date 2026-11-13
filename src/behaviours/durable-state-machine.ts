// DurableStateMachine (RFD0009): an XState machine as a Durable Object, with nothing lost when the
// object is evicted. XState decides; this runs it the way a Durable Object needs: one event at a
// time, each in one transaction with the machine's snapshot, its SQL, its timers and its messages.
//
// XState's own runtime keeps delayed transitions in `setTimeout` and invoked promises in memory, so
// an object evicted between them forgets both. Here every event goes through XState's pure
// `transition()`, which returns the next snapshot and the actions to run without running them. The
// actions that must outlive the object become rows: a delayed event is a timer on the alarm,
// leaving its state cancels it, a message to another object is an outbox entry, and an answer to a
// caller is kept with its idempotency key. The rest run inside the transaction.
import { initialTransition, transition, type AnyMachineSnapshot, type AnyStateMachine, type EventFromLogic, type ExecutableActionObject, type SnapshotFrom } from "xstate";
import { CallError } from "../client/index.js";
import { Behaviour, type BehaviourEvent, type Initial, type Step } from "./behaviour.js";
import { cancelTimer, replyTo as replyEffect, send, timer, type Effect, type EventKind, type From, type Metadata } from "./envelope.js";

const REPLY = "durable.reply";
const REPLY_TO = "durable.replyTo";
const SEND = "durable.send";
const POSTPONE = "durable.postpone";

/** How an event reached the machine. Read it from an action with `callOf(event)`. */
export interface CallInfo {
  readonly kind: EventKind;
  /** Set for calls: answer now with `reply`, or keep it in the context and answer later with `replyTo`. */
  readonly from: From | undefined;
  readonly metadata: Metadata;
}

const CALL = Symbol("durable.call");
const NO_CALL: CallInfo = { kind: "info", from: undefined, metadata: {} };

/** How `event` reached the machine: who waits for an answer, and the caller's metadata. */
export const callOf = (event: object): CallInfo => (event as { [CALL]?: CallInfo })[CALL] ?? NO_CALL;

type ActionArgs = { readonly context: any; readonly event: any };

/**
 * Declare these in `setup({ actions: { ...durableActions } })` so a machine may use the durable
 * actions below. They never run: the object reads them from the transition.
 */
export const durableActions = {
  [REPLY]: (_: unknown, _params: { value: unknown }) => {},
  [REPLY_TO]: (_: unknown, _params: { to: From; value: unknown }) => {},
  [SEND]: (_: unknown, _params: { binding: string; name: string; message: unknown }) => {},
  [POSTPONE]: () => {},
};

/** Answers this event's caller with `value(args)`, once the event commits. */
export const reply = (value: (args: ActionArgs) => unknown) => ({ type: REPLY, params: (args: ActionArgs) => ({ value: value(args) }) }) as const;

/** Answers an earlier caller, kept from `callOf(event).from`, with `value(args)`. */
export const replyTo = (to: (args: ActionArgs) => From, value: (args: ActionArgs) => unknown) =>
  ({ type: REPLY_TO, params: (args: ActionArgs) => ({ to: to(args), value: value(args) }) }) as const;

/** Sends `message(args)` to object `name` of `binding`, once this event commits, at least once. */
export const sendToObject = (binding: string, name: string | ((args: ActionArgs) => string), message: (args: ActionArgs) => unknown) =>
  ({ type: SEND, params: (args: ActionArgs) => ({ binding, name: typeof name === "string" ? name : name(args), message: message(args) }) }) as const;

/** Not now: keep this event, and run it again after the next state change. */
export const postpone = () => ({ type: POSTPONE }) as const;

/**
 * ```ts
 * const seat = setup({ types: { events: {} as SeatEvent }, actions: { ...durableActions } }).createMachine({
 *   initial: "available",
 *   states: {
 *     available: { on: { grab: { target: "held", actions: reply(() => true) } } },
 *     held: { after: { 600_000: "available" }, on: { release: { target: "available", actions: reply(() => true) } } },
 *   },
 * });
 * export class Seat extends DurableStateMachine<Env, typeof seat> {
 *   protected readonly machine = seat;
 *   grab() { return this.call({ type: "grab" }) as Promise<boolean>; }   // client API, run in the caller
 * }
 * ```
 *
 * An event no state of the machine names fails at once with INVALID_ARGUMENT: a misspelled type
 * must not wait for a state that will never come. An event the machine names but the current state
 * takes no transition for waits, and runs again after the next state change (`postponeUnhandled`,
 * on by default); off, such a call fails with FAILED_PRECONDITION and such a cast is dropped.
 */
export abstract class DurableStateMachine<Env, M extends AnyStateMachine> extends Behaviour<Env, EventFromLogic<M>, unknown, EventFromLogic<M>> {
  // The machine is an instance field, which RPC never exposes; migrate is a prototype method.
  protected static override readonly callbacks: readonly string[] = ["migrate"];

  protected abstract readonly machine: M;
  /** Keep events the current state takes no transition for, until a state does. */
  protected readonly postponeUnhandled: boolean = true;

  #names: EventNames | undefined;

  /** Upgrades a snapshot stored by an older `version`. */
  protected migrate(fromVersion: number, _stored: unknown): SnapshotFrom<M> {
    throw new Error(`${this.constructor.name} has a snapshot of version ${fromVersion} and no migrate() for it`);
  }

  protected override behaviourInit(): Initial {
    const [snapshot, actions] = initialTransition(this.machine as AnyStateMachine);
    const { effects, answer, postponed } = this.#translate(actions);
    if (answer || postponed) throw new Error(`${this.constructor.name}: the initial state has no event to answer or postpone`);
    return { state: snapshot, effects };
  }

  protected override behaviourMigrate(fromVersion: number, stored: unknown): unknown {
    return this.migrate(fromVersion, stored);
  }

  protected override behaviourEncode(snapshot: unknown): unknown {
    return this.machine.getPersistedSnapshot(snapshot as SnapshotFrom<M>);
  }

  protected override behaviourDecode(stored: unknown): unknown {
    return this.machine.resolveState(stored as never);
  }

  protected override behaviourStep(event: BehaviourEvent, state: unknown): Step {
    const current = state as AnyMachineSnapshot;
    // Timers carry the events XState itself asked for; only events from callers are checked.
    if (event.kind !== "info") this.#refuseUnknown(event.message);
    const message = { ...(event.message as object), [CALL]: { kind: event.kind === "info" ? "timeout" : event.kind, from: event.from, metadata: event.metadata } satisfies CallInfo };
    const [next, actions] = transition(this.machine as AnyStateMachine, current, message as never) as [AnyMachineSnapshot, ExecutableActionObject[]];
    // No transition and nothing to do: XState would drop the event without a word.
    if (next === current && actions.length === 0 && event.kind !== "info") {
      if (this.postponeUnhandled) return { state, postpone: true };
      if (event.from) throw new CallError("FAILED_PRECONDITION", `${this.constructor.name} takes no ${(event.message as { type: string }).type} in ${JSON.stringify(current.value)}`);
      return { state };
    }
    const { effects, answer, postponed } = this.#translate(actions);
    if (postponed) return { state, postpone: true };
    const changed = next !== current && !sameValue(next.value, current.value);
    return answer && event.from ? { state: next, effects, changed, reply: answer } : { state: next, effects, changed };
  }

  /** An event whose type no state names, or with no type at all, is refused before XState sees it. */
  #refuseUnknown(message: unknown): void {
    const type = (message as { type?: unknown } | null)?.type;
    if (typeof type !== "string") throw new CallError("INVALID_ARGUMENT", `${this.constructor.name} takes events with a string type, got ${JSON.stringify(message)}`);
    this.#names ??= eventNames(this.machine as AnyStateMachine);
    if (!this.#names.accepts(type)) throw new CallError("INVALID_ARGUMENT", `${this.constructor.name} has no event ${JSON.stringify(type)}; its states take ${this.#names.listed}`);
  }

  /** XState's actions as this object's effects; the rest run here, inside the transaction. */
  #translate(actions: readonly ExecutableActionObject[]): { effects: Effect<unknown>[]; answer: { value: unknown } | undefined; postponed: boolean } {
    const effects: Effect<unknown>[] = [];
    let answer: { value: unknown } | undefined;
    for (const action of actions) {
      switch (action.type) {
        case "xstate.raise": {
          const params = action.params as { event: unknown; id?: string; delay?: number };
          // A raise without a delay was already taken, inside this transition.
          if (params.delay !== undefined) effects.push(timer(params.id ?? `xstate.raise.${crypto.randomUUID()}`, params.delay, params.event));
          break;
        }
        case "xstate.cancel":
          effects.push(cancelTimer((action.params as { sendId: string }).sendId));
          break;
        case REPLY:
          answer = { value: (action.params as { value: unknown }).value };
          break;
        case REPLY_TO: {
          const params = action.params as { to: From; value: unknown };
          effects.push(replyEffect(params.to, params.value));
          break;
        }
        case SEND: {
          const params = action.params as { binding: string; name: string; message: unknown };
          effects.push(send(params.binding, params.name, params.message));
          break;
        }
        case POSTPONE:
          return { effects: [], answer: undefined, postponed: true };
        default:
          // Invoked actors, sendTo, emit and the like live in XState's in-memory system: an evicted
          // object would lose them, so they are refused rather than half-supported.
          if (action.type.startsWith("xstate.") && action.type !== "xstate.log") throw new Error(`${this.constructor.name}: ${action.type} is not durable; use delayed transitions (after), reply, replyTo and sendToObject`);
          action.exec?.(action.info, action.params);
      }
    }
    return { effects, answer, postponed: false };
  }
}

/** The event types a machine's states name: exact ones, and wildcards ("*", "user.*"). */
interface EventNames {
  accepts(type: string): boolean;
  readonly listed: string;
}

function eventNames(machine: AnyStateMachine): EventNames {
  const named = (machine.events as string[]).filter((name) => !name.startsWith("xstate."));
  const exact = new Set(named.filter((name) => !name.endsWith("*")));
  const any = named.includes("*");
  const prefixes = named.filter((name) => name.endsWith(".*")).map((name) => name.slice(0, -1));
  return {
    accepts: (type) => any || exact.has(type) || prefixes.some((prefix) => type.startsWith(prefix)),
    listed: named.join(", "),
  };
}

function sameValue(a: unknown, b: unknown): boolean {
  return a === b || (typeof a === "object" && typeof b === "object" && JSON.stringify(a) === JSON.stringify(b));
}
