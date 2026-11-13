// Behaviours (RFD0009): DurableStateMachine, an object as a state machine, on a base that runs one
// event at a time, commits each whole, and keeps timers and an outbox durable. The class is both
// the object and its client: `Class.client(ns, name, options)` runs its public methods in the
// caller and sends only messages.
export { Behaviour, type BehaviourClient } from "./behaviour.js";
export { DurableStateMachine, cancelTimeout, keep, namedTimeout, next, postpone, stateTimeout, type StateHandler, type StateHandlers, type StateMachineAction, type StateMachineContext, type StateMachineTransition, type TransitionOptions } from "./durable-state-machine.js";
export { cancelTimer, replyTo, send, timer, type Effect, type Envelope, type EventKind, type From, type Metadata, type Outcome } from "./envelope.js";
