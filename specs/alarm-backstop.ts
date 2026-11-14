// Alarm backstop: a shard's background work (a split's sibling to activate, rows to clear, a hold
// to end, outcomes to purge, the subclass's onAlarm) is done only by its alarm. An alarm can be
// lost (a setAlarm that failed, an alarm the runtime gave up on), or stranded: getAlarm() keeps
// answering a time in the past for an alarm that will never fire. So every call that reaches the
// shard checks that an alarm will come for its pending work, and sets one if not.
//
// The check, and asking for an alarm later than now, both read getAlarm(). Read as "an alarm is
// set", a stranded alarm passes both: the work waits forever while calls keep arriving.
// --without strandedIsMissing shows it. The rule treats an alarm well past due as missing.
//
// One shard; its calls keep coming (a fair step), as on a shard that serves traffic. At most one
// fault on any path: an alarm lost, or stranded.
import { atMost, budget, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, spend } from "@leostera/feynman";

const faults = budget("faults", 1);

const strandedIsMissing = defineRule("strandedIsMissing", { description: "An alarm getAlarm() answers well past due counts as missing, and is set again." });

const Call = defineMessage("Call", {}, { name: "routed call", over: "DO RPC" });
const Answer = defineMessage("Answer", {}, { name: "answer", over: "DO RPC", tone: "reply" });

const workers = defineZone({ name: "Worker isolate", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

type Alarm = "none" | "set" | "stranded";

export const Client = defineActor({
  name: "Client",
  description: "Traffic for the shard: one call at a time, for as long as the shard serves.",
  zone: workers,
  singleton: true,
  initialState: () => ({ waiting: false }),
  display: { icon: "code", status: (s) => (s.waiting ? "calling" : "idle") },
  transitions: {
    call: {
      description: "Sends the shard a call.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => !s.waiting,
      sends: [Call],
      run(s, _, ctx) {
        s.waiting = true;
        ctx.send(ctx.the(Shard), Call, {});
      },
    },
    answer: { on: Answer, description: "Gets the answer.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s) => void (s.waiting = false) },
  },
});

/** Sets the alarm (the runtime's setAlarm): it will fire. */
function setAlarm(s: { alarm: Alarm }, ctx: { start(label: "fire"): void }): void {
  s.alarm = "set";
  ctx.start("fire");
}

/** getAlarm() read as "an alarm will come": a stranded one passes, unless the rule is on. */
const armed = (alarm: Alarm) => alarm === "set" || (alarm === "stranded" && !strandedIsMissing.on);

export const Shard = defineActor({
  name: "Shard",
  description: "A shard with background work only its alarm does.",
  zone: objects,
  singleton: true,
  initialState: () => ({ pending: false, alarm: "none" as Alarm, done: 0 }),
  display: {
    icon: "database",
    status: (s) => `${s.pending ? "work pending" : "nothing pending"}; alarm ${s.alarm}`,
    mood: (s) => (s.pending && s.alarm !== "set" ? "bad" : s.pending ? "busy" : "ok"),
  },
  actions: {
    workNow: {
      with: [atMost(1)],
      label: "work due now",
      description: "Stores work due now (a split's sibling to activate, rows to clear) and sets the alarm at once, in the same commit.",
      sources: ["src/sharded/index.ts#ShardedObject.armAlarm"],
      when: (s) => !s.pending,
      starts: ["fire"],
      run(s, _, ctx) {
        s.pending = true;
        setAlarm(s, ctx);
      },
    },
    workLater: {
      with: [atMost(1)],
      label: "work due later",
      description: "Stores work due later (a hold's deadline, an outcome's expiry, onAlarm) and asks for the alarm, unless an earlier one is set.",
      sources: ["src/sharded/index.ts#ShardedObject.armAlarm", "src/sharded/index.ts#ShardedObject.scheduleAlarm"],
      when: (s) => !s.pending,
      starts: ["fire"],
      run(s, _, ctx) {
        s.pending = true;
        // A stranded alarm's time is in the past: earlier than any time asked for.
        if (!armed(s.alarm)) setAlarm(s, ctx);
      },
    },
  },
  transitions: {
    call: {
      on: Call,
      description: "Serves the call, and checks that an alarm will come for its pending work (the backstop).",
      sources: ["src/sharded/index.ts#ShardedObject.checkAlarm"],
      sends: [Answer],
      starts: ["fire"],
      run(s, _, ctx) {
        if (s.pending && !armed(s.alarm)) setAlarm(s, ctx);
        ctx.reply(Answer, {});
      },
    },
    fire: {
      description: "The alarm fires and does the pending work.",
      sources: ["src/sharded/index.ts#ShardedObject.alarm"],
      after: "the alarm's time",
      run(s) {
        s.alarm = "none";
        if (s.pending) s.done++;
        s.pending = false;
      },
    },
    alarmLost: {
      description: "The alarm is lost: getAlarm() answers null, and nothing fires.",
      sources: ["src/sharded/index.ts#ShardedObject.armAlarm"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.alarm === "set",
      run(s, _, ctx) {
        s.alarm = "none";
        ctx.cancel("fire");
      },
    },
    alarmStranded: {
      description: "The alarm is stranded: getAlarm() answers a time in the past, and nothing fires.",
      sources: ["src/sharded/index.ts#ShardedObject.armAlarm"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.alarm === "set",
      run(s, _, ctx) {
        s.alarm = "stranded";
        ctx.cancel("fire");
      },
    },
  },
});

const workIsDone = defineProperty("workIsDone", {
  description: "Pending work is done, whatever happened to the alarm, while calls keep reaching the shard.",
  leadsTo: { whenever: (w) => w.the(Shard).state.pending, then: (w) => !w.the(Shard).state.pending },
  needs: [strandedIsMissing],
});
const workCanFinish = defineProperty("workCanFinish", { description: "Work can be done.", reachable: (w) => w.the(Shard).state.done > 0 });

const strandedThenCalled = defineScenario("A stranded alarm, then a call", {
  description: "The alarm for work due now is stranded; the next call sees an alarm well past due, sets it again, and the work is done.",
  run(s) {
    const shard = s.the(Shard);
    s.act(shard, "workNow");
    s.step(shard, "alarmStranded");
    s.step(s.the(Client), "call");
    s.settle();
  },
});

export default defineSpec({
  name: "Alarm backstop",
  description: "How a shard's background work still gets done when its alarm is lost or stranded.",
  actors: [Client, Shard],
  properties: [workIsDone, workCanFinish],
  scenarios: [strandedThenCalled],
});
