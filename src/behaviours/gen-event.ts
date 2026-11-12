// GenEvent (RFD0009): one object that fans events out to handlers added and removed at run time,
// each with its own state: an audit log, counters, a webhook relay, on the same stream of events.
// Without it, every new consumer of an event means changing the code that emits it. Handlers run
// in the order they were added; one that throws is removed and the others still see the event, so a
// broken consumer can't stop the rest. A subclass names the handler types it can run (code can't be
// stored, so a stored handler is a type name and its state).
import { CallError } from "../client/index.js";
import { Behaviour, type BehaviourEvent, type Step } from "./behaviour.js";
import type { Effect } from "./envelope.js";

export interface EventHandler<E, S = any, Args = any, Request = any, Answer = any> {
  /** The handler's state when added. */
  init(args: Args): S;
  /** The handler's next state, or `{ remove: true }` to remove itself. */
  handleEvent(event: E, state: S): { readonly state: S; readonly effects?: readonly Effect<never>[] } | { readonly remove: true };
  /** Answers `callHandler(id, request)`. */
  handleCall?(request: Request, state: S): { readonly reply: Answer; readonly state: S };
}

interface Installed {
  readonly id: string;
  readonly type: string;
  readonly state: unknown;
}

interface ManagerState {
  readonly handlers: readonly Installed[];
}

type ManagerMessage =
  | { readonly op: "add"; readonly id: string; readonly type: string; readonly args: unknown }
  | { readonly op: "delete"; readonly id: string }
  | { readonly op: "notify"; readonly event: unknown }
  | { readonly op: "callHandler"; readonly id: string; readonly request: unknown }
  | { readonly op: "which" };

/**
 * ```ts
 * export class Audit extends GenEvent<Env, AuditEvent> {
 *   protected handlerTypes = { count: counter, recent: lastTen };
 * }
 * const audit = Audit.client(env.AUDIT, "billing");
 * await audit.addHandler("totals", "count");
 * await audit.notify({ type: "charge", cents: 500 });
 * ```
 */
export abstract class GenEvent<Env, E> extends Behaviour<Env, ManagerMessage, unknown, ManagerMessage> {
  /** The handler types this manager can run, by name. */
  protected abstract readonly handlerTypes: Readonly<Record<string, EventHandler<E>>>;

  /** The message that notifies a manager's handlers of `event`, for a `send` effect from another object. */
  static notification<E>(event: E): ManagerMessage {
    return { op: "notify", event };
  }

  // ---- client API: these run in the caller ----

  /** Sends `event` to every handler; returns once they all ran. */
  notify(event: E): Promise<void> {
    return this.cast({ op: "notify", event });
  }

  /** Adds a handler of `type` under `id`. Fails with FAILED_PRECONDITION if `id` is taken. */
  async addHandler(id: string, type: string, args?: unknown): Promise<void> {
    await this.call({ op: "add", id, type, args });
  }

  /** Removes the handler `id`; false if there was none. */
  async deleteHandler(id: string): Promise<boolean> {
    return (await this.call({ op: "delete", id })) as boolean;
  }

  /** Asks the handler `id` with its `handleCall`. */
  async callHandler<Answer = unknown>(id: string, request: unknown): Promise<Answer> {
    return (await this.call({ op: "callHandler", id, request })) as Answer;
  }

  /** The ids of the handlers, in the order they run. */
  async whichHandlers(): Promise<string[]> {
    return (await this.call({ op: "which" })) as string[];
  }

  // ---- the manager ----

  protected override behaviourInit(): unknown {
    return { handlers: [] } satisfies ManagerState;
  }

  protected override behaviourMigrate(_fromVersion: number, stored: unknown): unknown {
    return stored;
  }

  protected override behaviourStep(event: BehaviourEvent, stored: unknown): Step {
    const state = stored as ManagerState;
    const message = event.message as ManagerMessage;
    switch (message.op) {
      case "notify":
        return this.#notify(state, message.event as E);
      case "add":
        return this.#add(state, message.id, message.type, message.args);
      case "delete": {
        const handlers = state.handlers.filter((h) => h.id !== message.id);
        return handlers.length === state.handlers.length ? { state, reply: { value: false } } : { state: { handlers }, reply: { value: true } };
      }
      case "callHandler":
        return this.#callHandler(state, message.id, message.request);
      case "which":
        return { state, reply: { value: state.handlers.map((h) => h.id) } };
      default:
        throw new CallError("INVALID_ARGUMENT", `not a message for an event manager: ${JSON.stringify(message)}; send GenEvent.notification(event)`);
    }
  }

  #add(state: ManagerState, id: string, type: string, args: unknown): Step {
    if (state.handlers.some((h) => h.id === id)) throw new CallError("FAILED_PRECONDITION", `a handler ${id} is already added`);
    const handler = this.#type(type);
    return { state: { handlers: [...state.handlers, { id, type, state: handler.init(args) }] }, reply: { value: null } };
  }

  #notify(state: ManagerState, event: E): Step {
    const handlers: Installed[] = [];
    const effects: Effect<never>[] = [];
    for (const installed of state.handlers) {
      try {
        const result = this.#type(installed.type).handleEvent(event, installed.state);
        if ("remove" in result) continue;
        handlers.push({ ...installed, state: result.state });
        if (result.effects) effects.push(...result.effects);
      } catch (error) {
        console.error(`${this.constructor.name}: handler ${installed.id} threw, removed`, error);
      }
    }
    return { state: { handlers }, effects, reply: { value: null } };
  }

  #callHandler(state: ManagerState, id: string, request: unknown): Step {
    const index = state.handlers.findIndex((h) => h.id === id);
    if (index < 0) throw new CallError("NOT_FOUND", `no handler ${id}`);
    const installed = state.handlers[index]!;
    const handler = this.#type(installed.type);
    if (!handler.handleCall) throw new CallError("INVALID_ARGUMENT", `handler type ${installed.type} takes no calls`);
    const { reply, state: next } = handler.handleCall(request, installed.state);
    const handlers = state.handlers.slice();
    handlers[index] = { ...installed, state: next };
    return { state: { handlers }, reply: { value: reply } };
  }

  #type(type: string): EventHandler<E> {
    const handler = this.handlerTypes[type];
    if (!handler) throw new CallError("INVALID_ARGUMENT", `no handler type ${type}`);
    return handler;
  }
}
