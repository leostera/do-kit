// Call deadline: a caller waits on a shard for so long, then gives up and tells its own caller the
// outcome is unknown. A write can wait in the shard (held by a split) past that, and land after it:
// the caller's caller, told "unknown", may have moved on, and the write lands under it.
//
//   - deadlineBeforeWrite: the caller sends a deadline a margin before its own timer; the shard
//     checks it after any wait and before it writes, with nothing in between, and past it answers
//     "expired" and writes nothing. Then nothing lands after the caller gave up, and "expired" is
//     a definite "not applied".
//
// The margin is the assumption: the shard's clock passes the deadline before the caller's timer
// fires. A clock models it: it tells the shard first, and the caller's timer only once the shard
// has taken it in. A caller's own timeout says "unknown", never "not applied": a write committed
// just before the deadline may have its answer on the way.
import { defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone } from "@leostera/feynman";

const deadlineBeforeWrite = defineRule("deadlineBeforeWrite", { description: "The shard refuses, writing nothing, a call past its caller's deadline." });

const Write = defineMessage("Write", {}, { name: "write", over: "DO RPC" });
const Done = defineMessage("Done", {}, { name: "written", over: "DO RPC", tone: "reply" });
const Expired = defineMessage("Expired", {}, { name: "expired", over: "DO RPC", tone: "refused" });
const ShardClock = defineMessage("ShardClock", {}, { name: "deadline passes (shard's clock)", over: "time" });
const Seen = defineMessage("Seen", {}, { name: "seen", over: "time", tone: "reply" });
const CallerTimer = defineMessage("CallerTimer", {}, { name: "caller's timer fires", over: "time" });

const workers = defineZone({ name: "Worker isolate", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

type Report = null | "applied" | "not applied" | "unknown";

export const Caller = defineActor({
  name: "Caller",
  description: "Sends one write and waits for it until its timer fires.",
  zone: workers,
  singleton: true,
  initialState: () => ({ sent: false, report: null as Report, gaveUp: false }),
  display: { icon: "code", status: (s) => s.report ?? (s.sent ? "waiting" : "idle") },
  actions: {
    send: {
      label: "write",
      description: "Sends the write, with its deadline.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => !s.sent,
      sends: [Write],
      run(s, _, ctx) {
        s.sent = true;
        ctx.send(ctx.the(Shard), Write, {});
      },
    },
  },
  transitions: {
    done: { on: Done, description: "The write landed.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s) => void (s.report ??= "applied") },
    expired: { on: Expired, description: "Refused past its deadline: it did not land.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s) => void (s.report ??= "not applied") },
    timer: {
      on: CallerTimer,
      description: "The caller's timer fires: it gives up, and the outcome is unknown.",
      sources: ["src/sharded/client.ts#ShardedClient.onShard"],
      run(s) {
        if (!s.sent || s.report !== null) return;
        s.gaveUp = true;
        s.report = "unknown";
      },
    },
  },
});

export const Shard = defineActor({
  name: "Shard",
  description: "Takes the write, or holds it while a split copies its key, then writes it.",
  zone: objects,
  singleton: true,
  initialState: () => ({ held: false, written: false, past: false, writtenAfterGiveUp: false }),
  display: { icon: "database", status: (s) => (s.written ? "written" : s.held ? "holding the write" : "idle") },
  transitions: {
    write: { on: Write, description: "Holds the write: a split copies its key.", sources: ["src/sharded/index.ts#ShardedObject.waitForHold"], run: (s) => void (s.held = true) },
    clock: {
      on: ShardClock,
      description: "The shard's clock passes the call's deadline.",
      sources: ["src/sharded/index.ts#ShardedObject.transact"],
      sends: [Seen],
      run(s, _, ctx) {
        s.past = true;
        ctx.reply(Seen, {});
      },
    },
    release: {
      description: "The split is over: the held write goes on, checking its deadline first when the rule is on.",
      sources: ["src/sharded/index.ts#ShardedObject.transact"],
      when: (s) => s.held,
      sends: [Done, Expired],
      run(s, _, ctx) {
        s.held = false;
        if (deadlineBeforeWrite.on && s.past) return ctx.send(ctx.the(Caller), Expired, {});
        s.written = true;
        // The caller's timer fired already only if the shard's clock passed first (the margin).
        if (s.past) s.writtenAfterGiveUp = true;
        ctx.send(ctx.the(Caller), Done, {});
      },
    },
  },
});

export const Clock = defineActor({
  name: "Clock",
  description: "Time passing: the shard's clock reaches the deadline before the caller's timer fires (the margin).",
  zone: workers,
  singleton: true,
  initialState: () => ({ ticked: false }),
  display: { icon: "clock", status: (s) => (s.ticked ? "past the deadline" : "before the deadline") },
  actions: {
    tick: {
      label: "the deadline passes",
      description: "The deadline passes, by the shard's clock first.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => !s.ticked,
      sends: [ShardClock],
      run(s, _, ctx) {
        s.ticked = true;
        ctx.send(ctx.the(Shard), ShardClock, {});
      },
    },
  },
  transitions: {
    seen: { on: Seen, description: "Then the caller's timer fires.", sources: ["src/sharded/client.ts#ShardedClient.onShard"], sends: [CallerTimer], run: (_s, _m, ctx) => ctx.send(ctx.the(Caller), CallerTimer, {}) },
  },
});

const reportsTrue = defineProperty("reportsTrue", {
  description: "A write reported not applied never lands, and one reported applied did.",
  always: (w) => {
    const { report } = w.the(Caller).state;
    const { written } = w.the(Shard).state;
    return !(report === "not applied" && written) && !(report === "applied" && !written);
  },
});
const nothingLandsAfterGiveUp = defineProperty("nothingLandsAfterGiveUp", {
  description: "No write lands once its caller gave up on it.",
  always: (w) => !w.the(Shard).state.writtenAfterGiveUp,
  needs: [deadlineBeforeWrite],
});
const writes = defineProperty("writes", { description: "A write can land.", reachable: (w) => w.the(Shard).state.written });
const refused = defineProperty("refused", { description: "A write can be refused past its deadline.", reachable: (w) => w.the(Caller).state.report === "not applied" });

const heldPastDeadline = defineScenario("A write held past its deadline", {
  description: "The split holds the write; the deadline passes; the split ends and the shard refuses the write.",
  run(s) {
    s.act(s.the(Caller), "send");
    s.settle();
    s.act(s.the(Clock), "tick");
    s.settle();
  },
});

export default defineSpec({
  name: "Call deadline",
  description: "What lands, and what a caller can say, once it gave up waiting on a write.",
  actors: [Caller, Shard, Clock],
  properties: [reportsTrue, nothingLandsAfterGiveUp, writes, refused],
  scenarios: [heldPastDeadline],
  finalState: { description: "Nothing is in flight.", when: (w) => w.quiet },
});
