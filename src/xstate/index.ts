// Durable XState: an XState machine as a Durable Object, with nothing lost when the object is
// evicted. XState decides; this runs it the way a Durable Object needs: one event at a time, each
// in one transaction with the machine's snapshot, its SQL, its timers and its outgoing messages.
//
// XState's own runtime keeps delayed transitions in `setTimeout` and invoked promises in memory, so
// an object evicted between them forgets both. Here every transition goes through XState's pure
// `transition()`, which returns the next snapshot and the actions to run without running them; the
// actions that must outlive the object become rows (a delayed event is a timer on the alarm, a
// message to another object is an outbox entry) and the rest run inside the transaction.
import { initialTransition, transition, type AnyMachineSnapshot, type AnyStateMachine, type EventFromLogic, type ExecutableActionObject, type SnapshotFrom } from "xstate";
import { Behaviour, type BehaviourEvent, type Step } from "../behaviours/behaviour.js";
import { cancelTimer, send, timer, type Effect } from "../behaviours/envelope.js";

/** The action that answers the event's caller. Declare it in `setup({ actions: durableActions })`. */
export const REPLY = "durable.reply";
/** The action that sends an event to another object through the outbox. */
export const SEND = "durable.send";

type ActionArgs = { readonly context: any; readonly event: any };

/** Implementations for `setup({ actions })`, so machines may name the durable actions. They never run: the object reads them. */
export const durableActions = {
  [REPLY]: (_: unknown, _params: { value: unknown }) => {},
  [SEND]: (_: unknown, _params: { binding: string; name: string; message: unknown }) => {},
};

/** Answers the caller with `value(args)`. */
export const reply = (value: (args: ActionArgs) => unknown) => ({ type: REPLY, params: (args: ActionArgs) => ({ value: value(args) }) }) as const;

/** Sends `message(args)` to object `name` of `binding`, once this event commits. */
export const sendDurable = (binding: string, name: string | ((args: ActionArgs) => string), message: (args: ActionArgs) => unknown) =>
  ({ type: SEND, params: (args: ActionArgs) => ({ binding, name: typeof name === "string" ? name : name(args), message: message(args) }) }) as const;

/**
 * ```ts
 * const seat = setup({ types: { ... }, actions: durableActions }).createMachine({
 *   initial: "available",
 *   states: {
 *     available: { on: { grab: { target: "held", actions: reply(() => true) } } },
 *     held: { after: { 600_000: "available" }, on: { release: { target: "available", actions: reply(() => true) } } },
 *   },
 * });
 * export class Seat extends DurableXState<Env, typeof seat> {
 *   protected readonly machine = seat;
 *   grab() { return this.call({ type: "grab" }) as Promise<boolean>; }   // client API, run in the caller
 * }
 * ```
 *
 * An event no transition takes in the current state waits, and runs again after the next state
 * change (`postponeUnhandled`, on by default); off, such a call fails and such a cast is dropped.
 */
export abstract class DurableXState<Env, M extends AnyStateMachine> extends Behaviour<Env, EventFromLogic<M>, unknown, EventFromLogic<M>> {
  protected static override readonly callbacks: readonly string[] = ["migrate"];

  protected abstract readonly machine: M;
  /** Keep events the current state takes no transition for, until a state does. */
  protected readonly postponeUnhandled: boolean = true;

  /** Upgrades a snapshot stored by an older `version`. */
  protected migrate(fromVersion: number, _stored: unknown): SnapshotFrom<M> {
    throw new Error(`${this.constructor.name} has a snapshot of version ${fromVersion} and no migrate() for it`);
  }

  protected override behaviourInit(): unknown {
    const [snapshot, actions] = initialTransition(this.machine as AnyStateMachine);
    const { effects, answer } = this.#translate(actions);
    if (effects.length > 0 || answer) throw new Error(`${this.constructor.name}: the initial state sets timers, sends or answers; not supported yet`);
    return snapshot;
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
    const [next, actions] = transition(this.machine as AnyStateMachine, current, event.message as never) as [AnyMachineSnapshot, ExecutableActionObject[]];
    // No transition and nothing to do: XState would drop the event silently.
    if (next === current && actions.length === 0 && event.kind !== "info") {
      if (this.postponeUnhandled) return { state, postpone: true };
      if (event.from) throw new Error(`${this.constructor.name} takes no ${(event.message as { type: string }).type} in ${JSON.stringify(current.value)}`);
      return { state };
    }
    const { effects, answer } = this.#translate(actions);
    const changed = next !== current && !sameValue(next.value, current.value);
    return answer && event.from ? { state: next, effects, changed, reply: answer } : { state: next, effects, changed };
  }

  /** XState's actions as this object's effects; the rest run here, inside the transaction. */
  #translate(actions: readonly ExecutableActionObject[]): { effects: Effect<unknown>[]; answer: { value: unknown } | undefined } {
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
        case SEND: {
          const params = action.params as { binding: string; name: string; message: unknown };
          effects.push(send(params.binding, params.name, params.message));
          break;
        }
        default:
          // Invoked actors, sendTo, emit and the like live in XState's in-memory system: an evicted
          // object would lose them, so they are refused rather than half-supported.
          if (action.type.startsWith("xstate.") && action.type !== "xstate.log") throw new Error(`${this.constructor.name}: ${action.type} is not durable; use timers (after), reply and sendDurable`);
          action.exec?.(action.info, action.params);
      }
    }
    return { effects, answer };
  }
}

function sameValue(a: unknown, b: unknown): boolean {
  return a === b || (typeof a === "object" && typeof b === "object" && JSON.stringify(a) === JSON.stringify(b));
}
