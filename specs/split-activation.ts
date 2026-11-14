// Activating a split's copy when the parent's alarm is lost. Once a split commits, the parent
// (the original, now the left half) activates the copy of the right half, and its alarm retries
// until the copy confirms. Alarms can be lost (a failed setAlarm, a deleted alarm, a restore that
// drops it): then nothing activates the copy, and every call to the right half is told "not
// ready" for good, while a client that waited long enough tries the parent and is told "moved".
//
//   - activationBackstop: a call that reaches the parent re-arms its activation if its alarm is
//     gone (the routed-call alarm check), and a copy that is called before it serves nudges its
//     parent to do so. Without it a lost alarm leaves the right half unserved forever. The
//     parent's check alone is not enough: a client may keep retrying the copy, never the parent;
//     so the copy nudges on every call it can't serve (one nudge in flight at a time).
//
// No message is lost here (snapshot-split covers those); the one fault is the lost alarm.
import { atMost, budget, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, spend } from "@leostera/feynman";

const faults = budget("faults", 1);
const activationBackstop = defineRule("activationBackstop", { description: "A call at the parent, or at a copy not serving yet, re-arms the parent's activation when its alarm is gone." });

const Call = defineMessage("Call", {}, { name: "call (right half)" });
const Done = defineMessage("Done", {}, { name: "ok", tone: "reply" });
const NotReady = defineMessage("NotReady", {}, { name: "not ready", tone: "refused" });
const Moved = defineMessage("Moved", {}, { name: "moved", tone: "refused" });
const Activate = defineMessage("Activate", {}, { name: "activate" });
const Activated = defineMessage("Activated", {}, { name: "activated", tone: "reply" });
const Nudge = defineMessage("Nudge", {}, { name: "activate me" });
const Nudged = defineMessage("Nudged", {}, { name: "seen", tone: "reply" });

const callers = defineZone({ name: "Workers" });
const objects = defineZone({ name: "Durable Objects" });

export const Parent = defineActor({
  name: "Parent",
  description: "The original after its split commits: it owns the left half and must activate the copy.",
  zone: objects,
  singleton: true,
  initialState: () => ({ committed: false, activated: false, armed: false, alarmLost: false }),
  display: { icon: "database", status: (s) => (!s.committed ? "splitting" : s.activated ? "copy active" : s.alarmLost ? "alarm lost" : "activating the copy"), mood: (s) => (s.alarmLost && !s.activated ? "bad" : "ok") },
  transitions: {
    commit: {
      description: "The split commits (it gets there: abandoning it is snapshot-split's): the copy is claimed, and the parent's alarm is armed to activate it.",
      sources: ["src/sharded/index.ts#ShardedObject.split"],
      when: (s) => !s.committed,
      starts: ["activateRetry"],
      run(s, _, ctx) {
        s.committed = true;
        s.armed = true;
        ctx.start("activateRetry");
      },
    },
    activateRetry: {
      description: "The alarm: activates the copy. (It comes again until the copy confirms; no message is lost here, so once is enough.)",
      sources: ["src/sharded/index.ts#ShardedObject.alarm", "src/sharded/index.ts#ShardedObject.activateSibling"],
      after: "1 second",
      sends: [Activate],
      run(s, _, ctx) {
        s.armed = false;
        if (!s.activated) ctx.send(ctx.the(Copy), Activate, {});
      },
    },
    activated: { on: Activated, description: "The copy serves.", sources: ["src/sharded/index.ts#ShardedObject.activateSibling"], run: (s, _, ctx) => ((s.activated = true), ctx.cancel("activateRetry")) },
    call: {
      on: Call,
      description: "A call for the right half from a client that forgot the split: moved; and, with the rule, the alarm check re-arms a lost activation.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.checkAlarm"],
      sends: [Moved],
      starts: ["activateRetry"],
      run(s, _, ctx) {
        rearm(s, ctx);
        ctx.reply(Moved, {});
      },
    },
    nudge: {
      on: Nudge,
      description: "The copy asks to be activated: with the rule, a lost activation is re-armed.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedActivate"],
      starts: ["activateRetry"],
      sends: [Nudged],
      run(s, _, ctx) {
        rearm(s, ctx);
        ctx.reply(Nudged, {});
      },
    },
    alarmLost: {
      description: "The parent's alarm is lost before the copy confirmed.",
      sources: ["src/sharded/index.ts#ShardedObject.alarm"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.armed && !s.activated,
      run(s, _, ctx) {
        s.armed = false;
        s.alarmLost = true;
        ctx.cancel("activateRetry");
      },
    },
  },
});

function rearm(s: { committed: boolean; activated: boolean; armed: boolean; alarmLost: boolean }, ctx: { start(timer: "activateRetry"): void }): void {
  if (!activationBackstop.on || !s.committed || s.activated || !s.alarmLost) return;
  s.alarmLost = false;
  s.armed = true;
  ctx.start("activateRetry");
}

export const Copy = defineActor({
  name: "Copy",
  description: "The new object for the right half: claimed, not serving until activated.",
  zone: objects,
  singleton: true,
  initialState: () => ({ active: false, nudging: false }),
  display: { icon: "database", status: (s) => (s.active ? "serving" : "claimed, not serving"), mood: (s) => (s.active ? "ok" : "busy") },
  transitions: {
    activate: { on: Activate, description: "Starts serving.", sources: ["src/sharded/index.ts#ShardedObject.shardedActivate"], sends: [Activated], run: (s, _, ctx) => ((s.active = true), ctx.reply(Activated, {})) },
    nudged: { on: Nudged, description: "The parent saw the nudge.", sources: ["src/sharded/index.ts#ShardedObject.shardedCall"], run: (s) => void (s.nudging = false) },
    call: {
      on: Call,
      description: "Serves once active; before, not ready, and with the rule it nudges its parent (one nudge at a time: in the code, rate-limited).",
      sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.arrive"],
      sends: [Done, NotReady, Nudge],
      run(s, _, ctx) {
        if (s.active) return ctx.reply(Done, {});
        if (activationBackstop.on && !s.nudging) {
          s.nudging = true;
          ctx.send(ctx.the(Parent), Nudge, {});
        }
        ctx.reply(NotReady, {});
      },
    },
  },
});

export const Client = defineActor({
  name: "Client",
  description: "Calls the right half, knowing the split; retries a not-ready answer, and after a while tries the parent.",
  zone: callers,
  singleton: true,
  initialState: () => ({ pending: false, served: false, inFlight: false, lastAnswer: null as null | "notReady" | "moved" }),
  display: { icon: "code", status: (s) => (s.served ? "served" : s.pending ? "waiting" : "idle") },
  actions: {
    call: {
      with: [atMost(1)],
      label: "call the right half",
      description: "Calls a key in the right half.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => !s.pending && !s.served,
      sends: [Call],
      run(s, _, ctx) {
        s.pending = true;
        s.inFlight = true;
        ctx.send(ctx.the(Copy), Call, {});
      },
    },
  },
  transitions: {
    done: { on: Done, description: "Served.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s) => ((s.pending = false), (s.inFlight = false), (s.served = true)) },
    notReady: { on: NotReady, description: "Not served yet: it will try again.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s) => ((s.inFlight = false), (s.lastAnswer = "notReady")) },
    moved: { on: Moved, description: "Learns the split again: it will call the copy.", sources: ["src/sharded/client.ts#ShardedClient.learn"], run: (s) => ((s.inFlight = false), (s.lastAnswer = "moved")) },
    retry: {
      description: "Tries the copy again shortly.",
      sources: ["src/sharded/client.ts#ShardedClient.route", "src/sharded/client.ts#Backoff.next"],
      when: (s) => s.pending && !s.inFlight,
      sends: [Call],
      run(s, _m, ctx) {
        s.inFlight = true;
        ctx.send(ctx.the(Copy), Call, {});
      },
    },
    forget: {
      description: "Has waited long enough on not ready: forgets the split and calls the parent.",
      sources: ["src/sharded/client.ts#ShardedClient.forget"],
      when: (s) => s.pending && !s.inFlight && s.lastAnswer === "notReady",
      sends: [Call],
      run(s, _m, ctx) {
        s.inFlight = true;
        ctx.send(ctx.the(Parent), Call, {});
      },
    },
  },
});

const served = defineProperty("served", {
  description: "A call to the right half is eventually served, even when the parent's alarm is lost.",
  leadsTo: { whenever: (w) => w.the(Client).state.pending, then: (w) => w.the(Client).state.served },
  needs: [activationBackstop],
});
const activates = defineProperty("activates", { description: "The copy can be activated.", reachable: (w) => w.the(Copy).state.active });

const lostAlarm = defineScenario("The parent's alarm is lost", {
  description: "The split commits and the alarm is lost before it activated the copy; a call to the right half still gets served.",
  run(s) {
    s.step(s.the(Parent), "commit");
    s.step(s.the(Parent), "alarmLost");
    s.act(s.the(Client), "call");
    s.settle();
  },
});

export default defineSpec({
  name: "Split activation",
  description: "How a split's copy gets activated, even when the parent's alarm is lost.",
  actors: [Parent, Copy, Client],
  properties: [served, activates],
  scenarios: [lostAlarm],
  finalState: { description: "The copy serves, or nobody called.", when: (w) => w.the(Copy).state.active || !w.the(Client).state.pending },
});
