// Alarm tasks: one alarm per object runs everyone's background work: the library's (index
// deliveries, reservation sweeps) and the app's (onAlarm). App code can throw, every time.
//
//   - isolatedTasks: each task has its own due time, its own try, and its own retry; a task that
//     throws reschedules itself and nothing else. Without it the library's work runs inside the
//     app's onAlarm chain (super), and an app onAlarm that throws before super stops deliveries
//     for good, while it is retried itself.
//
// One shard; a delivery pending; the app's onAlarm may break (a fault: it then throws on every
// run). The alarm fires as long as some task is due.
import { atMost, budget, defineActor, defineProperty, defineRule, defineScenario, defineSpec, defineZone, spend } from "@leostera/feynman";

const faults = budget("faults", 1);
const isolatedTasks = defineRule("isolatedTasks", { description: "Each task runs in its own try, with its own due time and retry." });

const objects = defineZone({ name: "Durable Objects" });

export const Shard = defineActor({
  name: "Shard",
  description: "An object whose alarm runs the app's onAlarm and the library's delivery.",
  zone: objects,
  singleton: true,
  initialState: () => ({ delivery: false, appDue: false, appBroken: false, delivered: 0 }),
  display: { icon: "database", status: (s) => `${s.delivery ? "delivery pending" : "nothing to deliver"}; app ${s.appBroken ? "broken" : "ok"}`, mood: (s) => (s.delivery && s.appBroken ? "bad" : "ok") },
  actions: {
    write: {
      with: [atMost(1)],
      label: "write",
      description: "A write leaves an index change to deliver, and the app schedules its own work.",
      sources: ["src/indexed/table.ts#IndexedTable.indexedTransact", "src/sharded/index.ts#ShardedObject.scheduleAlarm"],
      when: (s) => !s.delivery,
      run(s) {
        s.delivery = true;
        s.appDue = true;
      },
    },
  },
  transitions: {
    alarm: {
      description: "The alarm fires: the app's onAlarm, and the delivery (with the rule, each on its own; without, the delivery inside the app's chain, after super).",
      sources: ["src/sharded/index.ts#ShardedObject.alarm", "src/indexed/table.ts#IndexedTable.layerAlarm"],
      when: (s) => s.delivery || s.appDue,
      run(s) {
        const appThrows = s.appDue && s.appBroken;
        if (s.appDue && !appThrows) s.appDue = false;
        // An app task that throws stays due (its retry); without the rule it takes the delivery with it.
        if (s.delivery && (isolatedTasks.on || !appThrows)) {
          s.delivery = false;
          s.delivered++;
        }
      },
    },
    appBreaks: {
      description: "The app's onAlarm starts throwing on every run (a bug, a dependency down).",
      sources: ["src/sharded/index.ts#ShardedObject.onAlarm"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => !s.appBroken,
      run: (s) => void (s.appBroken = true),
    },
  },
});

const deliveriesHappen = defineProperty("deliveriesHappen", {
  description: "A pending delivery is delivered, whatever the app's onAlarm does.",
  leadsTo: { whenever: (w) => w.the(Shard).state.delivery, then: (w) => !w.the(Shard).state.delivery },
  needs: [isolatedTasks],
});
const delivers = defineProperty("delivers", { description: "A delivery can happen.", reachable: (w) => w.the(Shard).state.delivered > 0 });

const brokenApp = defineScenario("An app onAlarm that throws", {
  description: "The app's onAlarm breaks before a write; the delivery still goes.",
  run(s) {
    const shard = s.the(Shard);
    s.step(shard, "appBreaks");
    s.act(shard, "write");
    s.step(shard, "alarm");
  },
});

export default defineSpec({
  name: "Alarm tasks",
  description: "How one alarm runs the library's and the app's background work, so that app code can't stop the library's.",
  actors: [Shard],
  properties: [deliveriesHappen, delivers],
  scenarios: [brokenApp],
  finalState: { description: "Nothing is due, or only the app's broken task.", when: (w) => !w.the(Shard).state.delivery },
});
