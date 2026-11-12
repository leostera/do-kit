// GenServer (RFD0009): an object that owns some state and serves requests on it: answered ones
// (calls), fire-and-forget ones (casts), and its own timers (info). A subclass writes `init`,
// `handleCall`, `handleCast` and `handleInfo` as pure transitions on its state, and its public
// methods are its client API, run in the caller by `Class.client(ns, name)`. A call can also be
// answered later, from another event: a lock that answers its next waiter when it's released.
import { CallError } from "../client/index.js";
import { Behaviour, type BehaviourEvent, type Step } from "./behaviour.js";
import type { Effect, From, Metadata } from "./envelope.js";

/** A call's transition: answer now, or keep the caller waiting for `replyTo(from, value)`. */
export type CallResult<S, Reply, Info = never> =
  | { readonly reply: Reply; readonly state: S; readonly effects?: readonly Effect<Info>[] }
  | { readonly noreply: true; readonly state: S; readonly effects?: readonly Effect<Info>[] };

export interface ServerTransition<S, Info = never> {
  readonly state: S;
  readonly effects?: readonly Effect<Info>[];
}

/**
 * ```ts
 * export class Channels extends GenServer<Env, { free: number[] }, "alloc", number | null, { free: number }> {
 *   alloc() { return this.call("alloc"); }                    // client API, run in the caller
 *   free(channel: number) { return this.cast({ free: channel }); }
 *
 *   protected init() { return { free: [1, 2, 3] }; }
 *   protected handleCall(_: "alloc", state: { free: number[] }) {
 *     const [channel = null, ...free] = state.free;
 *     return { reply: channel, state: { free } };
 *   }
 *   protected override handleCast(message: { free: number }, state: { free: number[] }) {
 *     return { state: { free: [...state.free, message.free] } };
 *   }
 * }
 * const channels = Channels.client(env.CHANNELS, "pool", { timeoutMs: 200 });
 * await channels.alloc();
 * ```
 *
 * State is stored as JSON after every event that returns a new state object (return the same
 * object to skip the write). Never change it in place. Keep big state in your own tables with
 * `this.sql`, written from callbacks, which run in the event's transaction.
 */
export abstract class GenServer<Env, S, Call, Reply, Cast = never, Info = never> extends Behaviour<Env, Call, Reply, Cast> {
  protected static override readonly callbacks: readonly string[] = ["init", "migrate", "handleCall", "handleCast", "handleInfo"];

  /** The state a new object starts with. Runs once in the object's life, not once per wake. */
  protected abstract init(): S;

  /** Upgrades state stored by an older `version`. Runs at startup, before any event. */
  protected migrate(fromVersion: number, _stored: unknown): S {
    throw new Error(`${this.constructor.name} has state of version ${fromVersion} and no migrate() for it`);
  }

  protected abstract handleCall(request: Call, state: S, from: From, metadata: Metadata): CallResult<S, Reply, Info>;

  protected handleCast(_message: Cast, _state: S, _metadata: Metadata): ServerTransition<S, Info> {
    throw new CallError("INVALID_ARGUMENT", `${this.constructor.name} takes no casts`);
  }

  /** Timers' messages. */
  protected handleInfo(_message: Info, state: S): ServerTransition<S, Info> {
    return { state };
  }

  protected override behaviourInit(): unknown {
    return this.init();
  }

  protected override behaviourMigrate(fromVersion: number, stored: unknown): unknown {
    return this.migrate(fromVersion, stored);
  }

  protected override behaviourStep(event: BehaviourEvent, state: unknown): Step {
    switch (event.kind) {
      case "call": {
        const result = this.handleCall(event.message as Call, state as S, event.from!, event.metadata);
        return "noreply" in result ? { state: result.state, effects: result.effects } : { state: result.state, effects: result.effects, reply: { value: result.reply } };
      }
      case "cast":
        return this.handleCast(event.message as Cast, state as S, event.metadata);
      default:
        return this.handleInfo(event.message as Info, state as S);
    }
  }
}
