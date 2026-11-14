// Online backfill: a deploy adds a count group (or an index) to a shard with rows. The shard
// counts the stored rows a step at a time, in key order, keeping a cursor (`after`), while writes
// keep coming. Every row must end up counted exactly once, whatever the order of steps, writes and
// crashes.
//
// Two rules:
//   - stepInOneCommit: a step counts its rows and moves the cursor in one commit. Without it, a
//     crash between the two counts the step's rows again (today the whole count is one call,
//     with the record written last, so a storage error before it counts every row again).
//   - writesCountCovered: while the build runs, a write counts its change only if the row is at
//     or before the cursor; the build counts the rest when it gets there. Without it, a row
//     written ahead of the cursor is counted by the write and again by the build.
//
// Three keys in key order; values 0 or 1; a write creates, changes or deletes a row. One crash.
import { atMost, budget, crash, defineActor, defineProperty, defineRule, defineScenario, defineSpec, defineZone, input } from "@leostera/feynman";

const faults = budget("faults", 1);
const stepInOneCommit = defineRule("stepInOneCommit", { description: "A build step counts its rows and moves the cursor in one commit." });
const writesCountCovered = defineRule("writesCountCovered", { description: "During a build, a write counts its change only for a row at or before the cursor." });

const KEYS = ["a", "b", "c"];
const objects = defineZone({ name: "Durable Objects" });

interface State {
  rows: Record<string, number>;
  /** The group's counts by value, as stored. */
  counts: Record<string, number>;
  /** The build's cursor: the last key counted; "" before the first; null once built. */
  after: string | null;
  /** A step's rows counted, its cursor not yet moved (only without stepInOneCommit). */
  counted: string | null;
}

const covered = (s: State, key: string) => s.after === null || (s.after !== "" && key <= s.after);
const bump = (s: State, value: number, delta: number) => void (s.counts[String(value)] = (s.counts[String(value)] ?? 0) + delta);

export const Shard = defineActor({
  name: "Shard",
  description: "A table shard counting a new group over its stored rows, while it serves writes.",
  zone: objects,
  singleton: true,
  initialState: (): State => ({ rows: { a: 0, b: 1 }, counts: {}, after: "", counted: null }),
  display: {
    icon: "database",
    status: (s) => (s.after === null ? "built" : `building, after "${s.after}"`),
    rows: (s) => [...Object.entries(s.rows).map(([k, v]) => ({ group: "rows", label: k, detail: String(v) })), ...Object.entries(s.counts).map(([v, n]) => ({ group: "counts", label: v, detail: String(n) }))],
  },
  actions: {
    write: {
      with: [atMost(2)],
      label: "write a row",
      description: "Creates, changes or deletes a row, and counts the change if the build has passed it.",
      sources: ["src/table/index.ts#ShardedTable.countGroups"],
      inputs: { key: input(String, { examples: () => KEYS }), value: input(Number, { examples: () => [0, 1, -1] }) },
      run(s, { key, value }) {
        const before = s.rows[key];
        if (value === -1) delete s.rows[key];
        else s.rows[key] = value;
        if (writesCountCovered.on && !covered(s, key)) return;
        if (before !== undefined) bump(s, before, -1);
        if (value !== -1) bump(s, value, 1);
      },
    },
  },
  transitions: {
    count: {
      description: "A build step: counts the next stored row after the cursor.",
      sources: ["src/table/index.ts#ShardedTable.migrateCountGroups"],
      when: (s) => s.after !== null && s.counted === null,
      run(s) {
        const next = KEYS.filter((k) => k > s.after! && s.rows[k] !== undefined)[0];
        if (next === undefined) {
          s.after = null;
          return;
        }
        bump(s, s.rows[next]!, 1);
        if (stepInOneCommit.on) s.after = next;
        else s.counted = next;
      },
    },
    advance: {
      description: "Records how far the build got (a separate commit, without the rule).",
      sources: ["src/table/index.ts#ShardedTable.migrateCountGroups"],
      when: (s) => s.counted !== null,
      run(s) {
        s.after = s.counted;
        s.counted = null;
      },
    },
    // Without stepInOneCommit, a crash between counting and advancing keeps the counts and loses
    // the step's place: the step is done again. (A commit is all or nothing: rows, counts and the
    // cursor are what storage keeps.)
    crash: crash({ keeps: ["rows", "counts", "after"], spend: faults }),
  },
});

const countedOnce = defineProperty("countedOnce", {
  description: "Once built, the group's counts equal the stored rows by value.",
  always: (w) => {
    const s = w.the(Shard).state;
    if (s.after !== null) return true;
    const actual: Record<string, number> = {};
    for (const v of Object.values(s.rows)) actual[String(v)] = (actual[String(v)] ?? 0) + 1;
    const stored = Object.fromEntries(Object.entries(s.counts).filter(([, n]) => n !== 0));
    return JSON.stringify(Object.entries(stored).sort()) === JSON.stringify(Object.entries(actual).sort());
  },
  needs: [stepInOneCommit, writesCountCovered],
});
const builds = defineProperty("builds", { description: "A build can finish.", reachable: (w) => w.the(Shard).state.after === null });

const writeAhead = defineScenario("A write ahead of the cursor", {
  description: "A row past the cursor is created while the build runs; the build counts it when it gets there, and the write doesn't.",
  run(s) {
    const shard = s.the(Shard);
    s.step(shard, "count");
    s.act(shard, "write", { key: "c", value: 1 });
    s.settle();
  },
});

export default defineSpec({
  name: "Online backfill",
  description: "How a new count group is counted over stored rows in steps, while writes keep coming, each row exactly once.",
  actors: [Shard],
  properties: [countedOnce, builds],
  scenarios: [writeAhead],
});
