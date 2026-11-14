// Wipe refusal: wiping a shard deletes its storage, and calls still in flight to it must not make
// it again: a late write would bring back an object its operator saw wiped. The shard refuses
// them while it remembers the wipe. Remembered in memory, the refusal goes with the instance: an
// eviction, and the next late write creates the object anew.
//
//   - storedTombstone: the wipe leaves one stored row that says so, and every call to a wiped
//     object is refused until the name is reopened on purpose.
//
// One shard; a writer whose write may arrive at any time, before or after the wipe; the instance
// may be evicted (at most once).
import { atMost, budget, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, spend } from "@leostera/feynman";

const faults = budget("faults", 1);

const storedTombstone = defineRule("storedTombstone", { description: "A wipe leaves a stored tombstone, and a wiped object refuses every call until it is reopened." });

const Write = defineMessage("Write", {}, { name: "write", over: "DO RPC" });
const Wipe = defineMessage("Wipe", {}, { name: "wipe", over: "DO RPC" });
const Wiped = defineMessage("Wiped", {}, { name: "wiped", over: "DO RPC", tone: "reply" });

const workers = defineZone({ name: "Worker isolate", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

export const Writer = defineActor({
  name: "Writer",
  description: "A call sent before the wipe, that may arrive after it.",
  zone: workers,
  singleton: true,
  initialState: () => ({ sent: false }),
  display: { icon: "code", status: (s) => (s.sent ? "sent" : "idle") },
  actions: {
    write: {
      with: [atMost(1)],
      label: "write",
      description: "Sends a write to the shard.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => !s.sent,
      sends: [Write],
      run(s, _, ctx) {
        s.sent = true;
        ctx.send(ctx.the(Shard), Write, {});
      },
    },
  },
});

export const Operator = defineActor({
  name: "Operator",
  description: "Wipes the object, and from then on takes it to be gone.",
  zone: workers,
  singleton: true,
  initialState: () => ({ wiping: false, wiped: false }),
  display: { icon: "code", status: (s) => (s.wiped ? "wiped" : s.wiping ? "wiping" : "idle") },
  actions: {
    wipe: {
      with: [atMost(1)],
      label: "wipe",
      description: "Wipes the object.",
      sources: ["src/sharded/client.ts#ShardedClient.wipe"],
      when: (s) => !s.wiping,
      sends: [Wipe],
      run(s, _, ctx) {
        s.wiping = true;
        ctx.send(ctx.the(Shard), Wipe, {});
      },
    },
  },
  transitions: {
    wiped: { on: Wiped, description: "The wipe is done.", sources: ["src/sharded/client.ts#ShardedClient.wipe"], run: (s) => void (s.wiped = true) },
  },
});

export const Shard = defineActor({
  name: "Shard",
  description: "Holds rows; a wipe deletes them all.",
  zone: objects,
  singleton: true,
  initialState: () => ({
    /** Stored. */
    rows: 0,
    tombstone: false,
    /** In memory: lost on eviction. */
    wipedAt: false,
  }),
  display: { icon: "database", status: (s) => `${s.rows} rows${s.tombstone ? ", tombstone" : ""}${s.wipedAt ? ", refusing" : ""}`, mood: (s) => (s.rows > 0 && s.tombstone ? "bad" : "ok") },
  transitions: {
    write: {
      on: Write,
      description: "Refuses a write while it knows it was wiped; else writes (an object with no storage creates itself).",
      sources: ["src/sharded/index.ts#ShardedObject.assertNotWiped", "src/sharded/index.ts#ShardedObject.arrive"],
      run(s) {
        if (s.wipedAt || s.tombstone) return;
        s.rows++;
      },
    },
    wipe: {
      on: Wipe,
      description: "Deletes every row, and remembers the wipe: in memory, or stored as a tombstone.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedWipe"],
      sends: [Wiped],
      run(s, _, ctx) {
        s.rows = 0;
        s.wipedAt = true;
        if (storedTombstone.on) s.tombstone = true;
        ctx.reply(Wiped, {});
      },
    },
    evicted: {
      description: "The instance is evicted, or its refusal window passes: the wipe it remembered in memory is gone, its storage stays.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedWipe"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.wipedAt,
      run: (s) => void (s.wipedAt = false),
    },
  },
});

const wipedStaysGone = defineProperty("wipedStaysGone", {
  description: "Once its wipe is done, the object holds no rows.",
  always: (w) => !w.the(Operator).state.wiped || w.the(Shard).state.rows === 0,
  needs: [storedTombstone],
});
const writesLand = defineProperty("writesLand", { description: "A write before the wipe lands.", reachable: (w) => w.the(Shard).state.rows > 0 });
const wipesFinish = defineProperty("wipesFinish", { description: "A wipe can be done.", reachable: (w) => w.the(Operator).state.wiped });

const lateWrite = defineScenario("A late write after an eviction", {
  description: "The wipe is done; the instance is evicted; the write sent before the wipe arrives and is refused.",
  run(s) {
    s.act(s.the(Operator), "wipe");
    s.settle();
    s.step(s.the(Shard), "evicted");
    s.act(s.the(Writer), "write");
    s.settle();
  },
});

export default defineSpec({
  name: "Wipe refusal",
  description: "How a wiped object refuses the calls still in flight to it.",
  actors: [Writer, Operator, Shard],
  properties: [wipedStaysGone, writesLand, wipesFinish],
  scenarios: [lateWrite],
  finalState: { description: "Nothing is in flight.", when: (w) => w.quiet },
});
