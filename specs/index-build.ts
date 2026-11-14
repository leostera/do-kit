// Index build: a deploy adds a local index to a shard with rows. The shard fills each row's key a
// step at a time, in key order, behind a cursor. Meanwhile calls keep coming, or don't.
//
//   - refuseOnlyListings: while the index builds, only a listing by it is refused ("not ready");
//     gets, puts and other listings are served. Without it the shard refuses every call until
//     the build is done: a shard with many rows is down for the whole build.
//   - alarmDrivesBuild: the shard's alarm takes the steps, and asks for the next one. Without it
//     only calls take steps, and a shard that gets few calls never finishes its build.
//
// Three keys in key order. A put writes the row's key with it, so a row put during the build is
// listed whichever side of the cursor it is on. The calls are few (each at most once): a shard's
// traffic can stop at any time.
import { atMost, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone } from "@leostera/feynman";

const refuseOnlyListings = defineRule("refuseOnlyListings", { description: "While an index builds, only listings by it answer not ready." });
const alarmDrivesBuild = defineRule("alarmDrivesBuild", { description: "The shard's alarm takes the build's steps until it is done." });

const KEYS = ["a", "b", "c"] as const;
type Key = (typeof KEYS)[number];

const Get = defineMessage("Get", {}, { name: "get", over: "DO RPC" });
const Put = defineMessage("Put", { key: String }, { name: "put", over: "DO RPC" });
const List = defineMessage("List", {}, { name: "list by the new index", over: "DO RPC" });
const Served = defineMessage("Served", { op: String }, { name: "served", over: "DO RPC", tone: "reply" });
const Listed = defineMessage("Listed", { rows: Number }, { name: "listed", over: "DO RPC", tone: "reply" });
const NotReady = defineMessage("NotReady", { op: String }, { name: "not ready", over: "DO RPC", tone: "refused" });

const workers = defineZone({ name: "Worker isolate", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

export const Client = defineActor({
  name: "Client",
  description: "A few calls, each at most once: a get, a put of a new row, a listing by the new index.",
  zone: workers,
  singleton: true,
  initialState: () => ({ refused: [] as string[], listed: null as number | null, inFlight: 0 }),
  display: { icon: "code", status: (s) => `${s.inFlight} in flight; refused ${s.refused.join(", ") || "none"}` },
  actions: {
    get: {
      with: [atMost(1)],
      label: "get",
      description: "Gets a row.",
      sources: ["src/table/client.ts#TableClient.get"],
      sends: [Get],
      run(s, _, ctx) {
        s.inFlight++;
        ctx.send(ctx.the(Shard), Get, {});
      },
    },
    put: {
      with: [atMost(1)],
      label: "put",
      description: "Puts a new row, key b.",
      sources: ["src/table/client.ts#TableClient.put"],
      sends: [Put],
      run(s, _, ctx) {
        s.inFlight++;
        ctx.send(ctx.the(Shard), Put, { key: "b" });
      },
    },
    list: {
      with: [atMost(1)],
      label: "list",
      description: "Lists by the new index.",
      sources: ["src/table/client.ts#TableClient.list"],
      sends: [List],
      run(s, _, ctx) {
        s.inFlight++;
        ctx.send(ctx.the(Shard), List, {});
      },
    },
  },
  transitions: {
    served: { on: Served, description: "Served.", sources: ["src/table/client.ts#TableClient.get"], run: (s) => void s.inFlight-- },
    listed: {
      on: Listed,
      description: "Listed.",
      sources: ["src/table/client.ts#TableClient.list"],
      run(s, m) {
        s.inFlight--;
        s.listed = m.rows;
      },
    },
    notReady: {
      on: NotReady,
      description: "Refused: the client waits and retries (not modelled: the refusal is what is checked).",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      run(s, m) {
        s.inFlight--;
        s.refused.push(m.op);
      },
    },
  },
});

interface ShardState {
  /** Rows, and whether each has its key in the new index. */
  rows: Partial<Record<Key, { keyed: boolean }>>;
  /** The build's cursor: keys up to it are keyed; null once built. */
  after: number | null;
  /** An alarm is set for the next step. */
  alarm: boolean;
  /** Rows a listing by the new index missed, when it was answered (bookkeeping for the properties). */
  missed: number;
}

/** One step: keys the next row; done after the last. */
function step(s: ShardState): void {
  if (s.after === null) return;
  const next = s.after + 1;
  if (next >= KEYS.length) {
    s.after = null;
    return;
  }
  const row = s.rows[KEYS[next]!];
  if (row) row.keyed = true;
  s.after = next;
}

/** The migration a call runs first: today, a step, and not ready until the build is done. */
function migrate(s: ShardState): boolean {
  if (s.after === null) return true;
  if (refuseOnlyListings.on) return true;
  step(s);
  return s.after === null;
}

export const Shard = defineActor({
  name: "Shard",
  description: "Rows a and c from before the deploy; a build of the new index under way.",
  zone: objects,
  singleton: true,
  initialState: (): ShardState => ({ rows: { a: { keyed: false }, c: { keyed: false } }, after: -1, alarm: alarmDrivesBuild.on, missed: 0 }),
  display: { icon: "database", status: (s) => (s.after === null ? "built" : `building, after ${s.after < 0 ? "none" : KEYS[s.after]}`), mood: (s) => (s.after === null ? "ok" : "busy") },
  transitions: {
    get: {
      on: Get,
      description: "Serves a get, after its migration step.",
      sources: ["src/table/index.ts#ShardedTable.tableGet", "src/table/index.ts#ShardedTable.migrate"],
      sends: [Served, NotReady],
      run(s, _, ctx) {
        if (!migrate(s)) return ctx.reply(NotReady, { op: "get" });
        ctx.reply(Served, { op: "get" });
      },
    },
    put: {
      on: Put,
      description: "Writes the row with its key in every index, after its migration step.",
      sources: ["src/table/index.ts#ShardedTable.tablePut", "src/table/index.ts#ShardedTable.putRow"],
      sends: [Served, NotReady],
      run(s, m, ctx) {
        if (!migrate(s)) return ctx.reply(NotReady, { op: "put" });
        s.rows[m.key as Key] = { keyed: true };
        ctx.reply(Served, { op: "put" });
      },
    },
    list: {
      on: List,
      description: "Lists by the new index: not ready while it builds.",
      sources: ["src/table/index.ts#ShardedTable.tablePage"],
      sends: [Listed, NotReady],
      run(s, _, ctx) {
        migrate(s);
        if (s.after !== null) return ctx.reply(NotReady, { op: "list" });
        const listed = KEYS.filter((k) => s.rows[k]?.keyed).length;
        s.missed += Object.keys(s.rows).length - listed;
        ctx.reply(Listed, { rows: listed });
      },
    },
    alarm: {
      description: "The alarm takes a step of the build, and asks for the next one until it is done.",
      sources: ["src/table/index.ts#ShardedTable.migrate"],
      when: (s) => s.alarm,
      run(s) {
        step(s);
        s.alarm = s.after !== null;
      },
    },
  },
});

const singleRowsServed = defineProperty("singleRowsServed", {
  description: "A get or a put is never refused because an index is building.",
  always: (w) => w.the(Client).state.refused.every((op) => op === "list"),
  needs: [refuseOnlyListings],
});
const buildEnds = defineProperty("buildEnds", {
  description: "The build ends, however few calls the shard gets.",
  leadsTo: { whenever: (w) => w.the(Shard).state.after !== null, then: (w) => w.the(Shard).state.after === null },
  needs: [alarmDrivesBuild],
});
const listingsComplete = defineProperty("listingsComplete", {
  description: "A listing by the new index lists every row the shard holds as it answers.",
  always: (w) => w.the(Shard).state.missed === 0,
});
const listedOnce = defineProperty("listedOnce", { description: "A listing can be answered.", reachable: (w) => w.the(Client).state.listed !== null });

const putDuringBuild = defineScenario("A put during the build", {
  description: "A new row is put while the build is under way; the alarm finishes the build; the listing has all three rows.",
  run(s) {
    s.act(s.the(Client), "put");
    s.settle();
    s.settle();
    s.act(s.the(Client), "list");
    s.settle();
  },
});

export default defineSpec({
  name: "Index build",
  description: "How a shard builds a new local index while it keeps serving, and what ends the build.",
  actors: [Client, Shard],
  properties: [singleRowsServed, buildEnds, listingsComplete, listedOnce],
  scenarios: [putDuringBuild],
  finalState: { description: "Nothing is in flight.", when: (w) => w.quiet },
});
