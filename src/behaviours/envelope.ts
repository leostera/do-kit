// What crosses the wire between a behaviour's client and its object (RFD0009), and what callbacks
// return. Nothing here touches the runtime, so callers and objects share it.
import type { StatusCode } from "../client/index.js";

export type Metadata = Readonly<Record<string, string>>;

/** One message to a behaviour, as its client sends it. */
export interface Envelope {
  readonly kind: "call" | "cast";
  readonly message: unknown;
  /** The caller stops waiting at this time (Unix ms): the object drops the message after it. */
  readonly deadline?: number;
  /** One logical call across retries: the object answers a repeat with the first answer. */
  readonly key?: string;
  readonly metadata?: Metadata;
}

/** A behaviour's answer to one envelope. Failures are values, so their code survives RPC. */
export type Outcome = { readonly ok: true; readonly value: unknown } | { readonly ok: false; readonly code: StatusCode; readonly message: string };

/**
 * Who is waiting for an answer to a call. Opaque: keep it in state, and answer with
 * `replyTo(from, value)` from any later callback. It names an RPC the caller holds open, so an
 * answer only reaches a caller still waiting in this instance of the object.
 */
export interface From {
  readonly id: string;
  /** @internal the call's idempotency key, so a late answer is kept for the call's retries. */
  readonly key?: string;
}

/** How an event reached a callback. */
export type EventKind = "call" | "cast" | "info" | "timeout";

/** What a callback asks the object to do after its transition commits. */
export type Effect<Info = unknown> =
  /** Delivers `message` to this object as an info event (a timeout, for a state machine) after `afterMs`. Setting a name again replaces it. */
  | { readonly kind: "timer"; readonly name: string; readonly afterMs: number; readonly message: Info }
  | { readonly kind: "cancelTimer"; readonly name: string }
  /** Casts `message` to the behaviour named `name` in the namespace bound as `binding`, at least once, deduplicated there. */
  | { readonly kind: "send"; readonly binding: string; readonly name: string; readonly message: unknown }
  /** Answers a call that returned without answering. */
  | { readonly kind: "reply"; readonly to: From; readonly value: unknown };

export const timer = <Info>(name: string, afterMs: number, message: Info): Effect<Info> => ({ kind: "timer", name, afterMs, message });
export const cancelTimer = (name: string): Effect<never> => ({ kind: "cancelTimer", name });
export const send = (binding: string, name: string, message: unknown): Effect<never> => ({ kind: "send", binding, name, message });
export const replyTo = (to: From, value: unknown): Effect<never> => ({ kind: "reply", to, value });
