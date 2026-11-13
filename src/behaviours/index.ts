// Behaviours (RFD0009): DurableStateMachine, an XState machine as a Durable Object, on a base that
// runs one event at a time, commits each whole, and keeps timers, an outbox and answers to retried
// calls durable. The class is both the object and its client: `Class.client(ns, name, options)`
// runs its public methods in the caller and sends only messages.
export { Behaviour, type BehaviourClient } from "./behaviour.js";
export { DurableStateMachine, callOf, durableActions, postpone, reply, replyTo, sendToObject, type CallInfo } from "./durable-state-machine.js";
export type { Envelope, EventKind, From, Metadata, Outcome } from "./envelope.js";
