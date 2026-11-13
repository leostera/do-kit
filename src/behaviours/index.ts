// Behaviours (RFD0009): base classes for the shapes most objects take (a server of requests on its
// state, and a state machine), with one-at-a-time events, all-or-nothing transitions,
// durable timers and an outbox done once for all of them. Each class is both the object and its
// client: `Class.client(ns, name, options)` runs its public methods in the caller and sends only
// messages.
export { Behaviour, type BehaviourClient } from "./behaviour.js";
export { StatefulServer, type CallResult, type StatefulServerTransition } from "./stateful-server.js";
export { StateMachine, cancelTimeout, keep, namedTimeout, next, postpone, stateTimeout, type StateHandler, type StateHandlers, type StateMachineAction, type StateMachineContext, type StateMachineTransition, type TransitionOptions } from "./state-machine.js";
export { cancelTimer, replyTo, send, timer, type Effect, type Envelope, type EventKind, type From, type Metadata, type Outcome } from "./envelope.js";
