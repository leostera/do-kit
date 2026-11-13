// StateMachine (RFD0009): an object as a state machine. Orders, sessions, leases, provisioning: things
// that move through states, where what an event means depends on the state, and where "if nothing
// happens in 15 minutes, give up" is part of the logic. Written by hand on a Durable Object, that
// is a status column, an if-ladder in every method, and timeouts multiplexed over the one alarm.
//
// A subclass names one handler per state; each takes an event and the machine's data and returns
// where to go: `next(state, data)`, `keep(data)`, or `postpone()`. Timeouts are durable: a state
// timeout is cancelled when the state changes, a named one when set again or cancelled, and both
// survive the object being evicted. A postponed event ("not in this state") runs again, oldest
// first, after the next state change, so callers don't have to retry.
import { Behaviour, type BehaviourEvent, type Step } from "./behaviour.js";
import { cancelTimer, timer, type Effect, type EventKind, type From, type Metadata } from "./envelope.js";

const STATE_TIMEOUT = "state-machine:state";
const NAMED_TIMEOUT = "state-machine:timeout:";

/** An action besides the effects every behaviour has. */
export type StateMachineAction<E> =
  | Effect<E>
  /** Delivers `event` as a timeout after `afterMs`, unless the state changes first. */
  | { readonly kind: "stateTimeout"; readonly afterMs: number; readonly event: E }
  /** Delivers `event` as a timeout after `afterMs`, unless set again or cancelled by name. */
  | { readonly kind: "timeout"; readonly name: string; readonly afterMs: number; readonly event: E }
  | { readonly kind: "cancelTimeout"; readonly name: string };

export type StateMachineTransition<St, D, E, R> =
  | { readonly next?: St; readonly data: D; readonly reply?: R; readonly actions?: readonly StateMachineAction<E>[] }
  | { readonly postpone: true };

export interface StateMachineContext {
  /** "timeout" for state and named timeouts. */
  readonly kind: EventKind;
  /** Set for calls. Answer with `reply` in the transition, or later with `replyTo(from, value)`. */
  readonly from: From | undefined;
  readonly metadata: Metadata;
}

export type StateHandler<St, D, E, R> = (event: E, data: D, context: StateMachineContext) => StateMachineTransition<St, D, E, R>;

export type StateHandlers<St extends string, D, E, R> = { readonly [K in St]: StateHandler<St, D, E, R> };

interface Stored<St, D> {
  readonly state: St;
  readonly data: D;
}

export interface TransitionOptions<E, R> {
  /** The answer to this event's caller. */
  readonly reply?: R;
  readonly actions?: readonly StateMachineAction<E>[];
}

/** Go to `state` with `data`. */
export function next<St, D, E = never, R = never>(state: St, data: D, options?: TransitionOptions<E, R>): StateMachineTransition<St, D, E, R> {
  return { next: state, data, ...options };
}

/** Stay in this state, with `data`. */
export function keep<D, E = never, R = never>(data: D, options?: TransitionOptions<E, R>): StateMachineTransition<never, D, E, R> {
  return { data, ...options };
}

/** Not now: run this event again after the next state change. */
export function postpone(): { readonly postpone: true } {
  return { postpone: true };
}

export const stateTimeout = <E>(afterMs: number, event: E): StateMachineAction<E> => ({ kind: "stateTimeout", afterMs, event });
export const namedTimeout = <E>(name: string, afterMs: number, event: E): StateMachineAction<E> => ({ kind: "timeout", name, afterMs, event });
export const cancelTimeout = (name: string): StateMachineAction<never> => ({ kind: "cancelTimeout", name });

/**
 * ```ts
 * export class Door extends StateMachine<Env, "locked" | "open", { code: string; entered: string }, DoorEvent, boolean> {
 *   press(digit: string) { return this.cast({ type: "button", digit }); }   // client API
 *
 *   protected init() { return { state: "locked", data: { code: "1234", entered: "" } } as const; }
 *   protected states = {
 *     locked: (event, data) => ... next("open", { ...data, entered: "" }, { actions: [stateTimeout(10_000, { type: "lock" })] }),
 *     open: (event, data) => (event.type === "lock" ? next("locked", data) : postpone()),
 *   };
 * }
 * ```
 */
export abstract class StateMachine<Env, St extends string, D, E, R = unknown> extends Behaviour<Env, E, R, E> {
  // Handlers live in `states`, an instance field, which RPC never exposes.
  protected static override readonly callbacks: readonly string[] = ["init", "migrate"];

  /** The state and data a new object starts in. Runs once in the object's life. */
  protected abstract init(): Stored<St, D>;

  /** One handler per state. */
  protected abstract readonly states: StateHandlers<St, D, E, R>;

  /** Upgrades data stored by an older `version`. */
  protected migrate(fromVersion: number, _stored: unknown): Stored<St, D> {
    throw new Error(`${this.constructor.name} has state of version ${fromVersion} and no migrate() for it`);
  }

  protected override behaviourInit(): unknown {
    return this.init();
  }

  protected override behaviourMigrate(fromVersion: number, stored: unknown): unknown {
    return this.migrate(fromVersion, stored);
  }

  protected override behaviourStep(event: BehaviourEvent, stored: unknown): Step {
    const current = stored as Stored<St, D>;
    const handler = this.states[current.state];
    if (!handler) throw new Error(`${this.constructor.name} has no handler for state ${current.state}`);
    const kind = event.kind === "info" ? "timeout" : event.kind;
    const transition = handler.call(this, event.message as E, current.data, { kind, from: event.from, metadata: event.metadata });
    if ("postpone" in transition) return { state: stored, postpone: true };
    const target = transition.next ?? current.state;
    const changed = target !== current.state;
    const state = changed || transition.data !== current.data ? { state: target, data: transition.data } : stored;
    const effects = effectsOf(transition.actions, changed);
    return "reply" in transition && event.from ? { state, effects, changed, reply: { value: transition.reply } } : { state, effects, changed };
  }
}

/** The machine's actions as effects; a state change cancels the state timeout unless it sets one. */
function effectsOf<E>(actions: readonly StateMachineAction<E>[] | undefined, changed: boolean): Effect<unknown>[] | undefined {
  if (!actions && !changed) return undefined;
  const effects: Effect<unknown>[] = [];
  let setsStateTimeout = false;
  for (const action of actions ?? []) {
    switch (action.kind) {
      case "stateTimeout":
        setsStateTimeout = true;
        effects.push(timer(STATE_TIMEOUT, action.afterMs, action.event));
        break;
      case "timeout":
        effects.push(timer(NAMED_TIMEOUT + action.name, action.afterMs, action.event));
        break;
      case "cancelTimeout":
        effects.push(cancelTimer(NAMED_TIMEOUT + action.name));
        break;
      default:
        effects.push(action);
    }
  }
  if (changed && !setsStateTimeout) effects.unshift(cancelTimer(STATE_TIMEOUT));
  return effects;
}
