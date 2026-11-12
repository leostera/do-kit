// Behaviours (RFD0009): base classes for the shapes most objects take (a server of requests on its
// state, a state machine, an event fan-out), with one-at-a-time events, all-or-nothing transitions,
// durable timers and an outbox done once for all of them. Each class is both the object and its
// client: `Class.client(ns, name, options)` runs its public methods in the caller and sends only
// messages.
export { Behaviour, type BehaviourClient } from "./behaviour.js";
export { GenServer, type CallResult, type ServerTransition } from "./gen-server.js";
export { GenStatem, cancelTimeout, keep, namedTimeout, next, postpone, stateTimeout, type StateHandler, type StateHandlers, type StatemAction, type StatemContext, type StatemTransition, type TransitionOptions } from "./gen-statem.js";
export { GenEvent, type EventHandler } from "./gen-event.js";
export { cancelTimer, replyTo, send, timer, type Effect, type Envelope, type EventKind, type From, type Metadata, type Outcome } from "./envelope.js";
