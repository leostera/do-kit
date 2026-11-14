// Global index across a rollback: a deploy adds a global index (byEmail); a rollback runs code
// that has never heard of it; the roll forward declares it again, with the same definition, so
// with the same id. Old and new code each serve for a while, in any order with the writes and
// the index's deliveries.
//
// Today, the old code takes the index it doesn't declare for dropped: it asks every index shard
// to drop it, and an index shard keeps a dropped id for good and ignores every delivery of it
// from then on. After the roll forward the row shard builds the index again and delivers every
// entry, and the index shard discards them: the index is empty and reported built.
// --without tombstoneOnlyBefore shows it.
//
// Two rules:
//   - tombstoneOnlyBefore keeps every row found: a drop covers only what was recorded before it.
//     An index recorded again since (by the shard's own clock) is delivered, and kept; its
//     entries replace an older generation's; and a drop that arrives late deletes only the
//     entries from before it. The checker found the last two: deploys reach shards in any order.
//   - keepUnknown keeps a rollback from rebuilding: code older than the schema that recorded an
//     index leaves it alone instead of dropping it, and records the rows it writes meanwhile, for
//     newer code to deliver. Without that record, rows written during the rollback have no entry.
//
// Schemas: 1 (before byEmail), 2 (with it), 3 (a newer one that removes it on purpose, so a
// rollback from 3 to 2 adds it back over a real drop). One row shard and one index shard; up to
// three deploys, which reach the shard in any order (deploys are gradual), and three rows. Times
// are the row shard's own (the time it recorded an index, the time it dropped one). No faults:
// deploys and their order are the point here.
import { atMost, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, input } from "@leostera/feynman";

const tombstoneOnlyBefore = defineRule("tombstoneOnlyBefore", { description: "An index shard ignores a delivery of a dropped index only if the index was recorded before the drop." });
const keepUnknown = defineRule("keepUnknown", { description: "Code older than the schema that recorded an index leaves that index alone instead of dropping it, and records the rows it changes meanwhile, for newer code to deliver." });

const Deploy = defineMessage("Deploy", { schema: Number, deploy: Number }, { name: "deploy", over: "the next call after a deploy" });
const Entry = defineMessage("Entry", { row: String, since: Number }, { name: "index entry", over: "DO RPC (outbox)" });
const Drop = defineMessage("Drop", { at: Number }, { name: "drop index", over: "DO RPC" });

const operators = defineZone({ name: "Deploys", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

const ROWS = ["a", "b", "c"];

export const Operator = defineActor({
  name: "Operator",
  description: "Deploys the next version of the code: schema 2 (with byEmail) or schema 1 (without).",
  zone: operators,
  singleton: true,
  initialState: () => ({ deploys: 0 }),
  display: { icon: "person", status: (s) => `${s.deploys} deploy(s)` },
  actions: {
    deploy: {
      with: [atMost(3)],
      label: "deploy",
      description: "Deploys a schema; the row shard migrates on its next call.",
      sources: ["src/indexed/table.ts#IndexedTable.migrateGlobals"],
      inputs: { schema: input(Number, { examples: () => [1, 2, 3] }) },
      sends: [Deploy],
      run(s, { schema }, ctx) {
        s.deploys++;
        ctx.send(ctx.the(RowShard), Deploy, { schema, deploy: s.deploys });
      },
    },
  },
});

type Schema = 1 | 2 | 3;
/** Schema 1: before byEmail; 2: with it; 3: a newer schema that removes it on purpose. */
const declares = (schema: Schema) => schema === 2;

interface Row {
  schema: Schema;
  /** The highest schema that ran here. */
  newest: Schema;
  /** Whether code older than the newest dropped an index (it couldn't know it was removed). */
  olderDropped: boolean;
  rows: string[];
  /** This shard's clock: its migrations, in the order they ran here. */
  clock: number;
  /** byEmail as recorded here: when (by this shard's clock), or null. */
  recorded: number | null;
  /** Rows whose entry is still to be delivered (the build, and the outbox). */
  outbox: string[];
  /** Rows written while byEmail was recorded but not declared (kept with keepUnknown). */
  missed: string[];
}

export const RowShard = defineActor({
  name: "RowShard",
  description: "The table's row shard: maintains byEmail while its schema declares it.",
  zone: objects,
  singleton: true,
  initialState: (): Row => ({ schema: 1, newest: 1, clock: 0, olderDropped: false, rows: [], recorded: null, outbox: [], missed: [] }),
  display: { icon: "database", status: (s) => `schema ${s.schema}; ${s.rows.length} row(s); byEmail ${s.recorded === null ? "not recorded" : `recorded at deploy ${s.recorded}`}` },
  actions: {
    put: {
      with: [atMost(3)],
      label: "put a row",
      description: "Writes a new row; with byEmail declared, its entry goes into the outbox.",
      sources: ["src/indexed/table.ts#IndexedTable.indexedTransact"],
      inputs: { row: input(String, { examples: (w): string[] => ROWS.filter((r) => !w.the(RowShard).state.rows.includes(r)).slice(0, 1) }) },
      when: (s) => s.rows.length < ROWS.length,
      run(s, { row }) {
        s.rows.push(row);
        if (declares(s.schema)) s.outbox.push(row);
        else if (s.recorded !== null) s.missed.push(row);
      },
    },
  },
  transitions: {
    deploy: {
      on: Deploy,
      description: "Migrates on its first call under the deployed schema: builds an index it declares and hasn't recorded, drops one it has recorded and doesn't declare.",
      sources: ["src/indexed/table.ts#IndexedTable.migrateGlobals", "src/indexed/table.ts#IndexedTable.backfill", "src/indexed/table.ts#IndexedTable.sendDrops"],
      sends: [Drop],
      run(s, msg, ctx) {
        const schema = msg.schema as Schema;
        s.schema = schema;
        s.clock++;
        const older = schema < s.newest;
        s.newest = Math.max(s.newest, schema) as Schema;
        if (declares(schema)) {
          if (s.recorded === null) {
            // A new index, or one recorded again: built over every row.
            s.recorded = s.clock;
            s.outbox = [...s.rows];
          } else {
            // Recorded all along: deliver what older code wrote meanwhile.
            s.outbox.push(...s.missed.filter((r) => !s.outbox.includes(r)));
          }
          s.missed = [];
          return;
        }
        if (s.recorded === null) return;
        // Not declared: dropped, unless this code is older than the schema that may have added it.
        if (keepUnknown.on && older) return;
        if (older) s.olderDropped = true;
        s.recorded = null;
        s.outbox = [];
        s.missed = [];
        ctx.send(ctx.the(IndexShard), Drop, { at: s.clock });
      },
    },
    deliver: {
      description: "Delivers the next entry of byEmail in its outbox (the build, and later writes).",
      sources: ["src/indexed/table.ts#IndexedTable.onAlarm"],
      when: (s) => s.outbox.length > 0 && s.recorded !== null,
      sends: [Entry],
      run(s, _, ctx) {
        const row = s.outbox.shift()!;
        ctx.send(ctx.the(IndexShard), Entry, { row, since: s.recorded! });
      },
    },
  },
});

export const IndexShard = defineActor({
  name: "IndexShard",
  description: "The index shard for byEmail: its entries, and the drops it was told of.",
  zone: objects,
  singleton: true,
  initialState: () => ({ entries: [] as { row: string; since: number }[], droppedAt: null as number | null }),
  display: { icon: "database", status: (s) => `${s.entries.length} entr(ies)${s.droppedAt !== null ? `; dropped at deploy ${s.droppedAt}` : ""}` },
  transitions: {
    entry: {
      on: Entry,
      description: "Keeps the entry, unless the index was dropped.",
      sources: ["src/indexed/global-index.ts#GlobalIndex.apply"],
      run(s, msg) {
        const ignored = s.droppedAt !== null && (!tombstoneOnlyBefore.on || msg.since <= s.droppedAt);
        if (!ignored) {
          // One entry a row: a delivery of a newer generation of the index replaces an older one's.
          const at = s.entries.findIndex((e) => e.row === msg.row);
          if (at === -1) s.entries.push({ row: msg.row, since: msg.since });
          else if (tombstoneOnlyBefore.on && s.entries[at]!.since < msg.since) s.entries[at] = { row: msg.row, since: msg.since };
        }
        // A build of an index recorded again since the drop clears its tombstone.
        if (tombstoneOnlyBefore.on && s.droppedAt !== null && msg.since > s.droppedAt) s.droppedAt = null;
      },
    },
    drop: {
      on: Drop,
      description: "Records the drop for good, and drops the index's entries (with the rule, only those recorded before the drop).",
      sources: ["src/indexed/global-index.ts#GlobalIndex.indexDrop"],
      run(s, msg) {
        s.droppedAt = Math.max(s.droppedAt ?? 0, msg.at);
        s.entries = tombstoneOnlyBefore.on ? s.entries.filter((e) => e.since > msg.at) : [];
      },
    },
  },
});

const everyRowFound = defineProperty("everyRowFound", {
  description: "Once schema 2 serves and its index is delivered, every row has its entry.",
  always: (w) => {
    const r = w.the(RowShard).state;
    if (!w.quiet || !declares(r.schema) || r.outbox.length > 0) return true;
    return r.rows.every((row) => w.the(IndexShard).state.entries.some((e) => e.row === row));
  },
  needs: [tombstoneOnlyBefore],
});
const rollbackDropsNothing = defineProperty("rollbackDropsNothing", {
  description: "Code older than the newest schema that ran never drops an index: a rollback and its roll forward rebuild nothing.",
  always: (w) => !w.the(RowShard).state.olderDropped,
  needs: [keepUnknown],
});
const rolledForward = defineProperty("rolledForward", {
  description: "A rollback and a roll forward, with rows written in each, can happen.",
  reachable: (w) => w.the(Operator).state.deploys === 3 && w.the(RowShard).state.schema === 2 && w.the(RowShard).state.rows.length === 3,
});

const rollbackAndForward = defineScenario("Deploy, roll back, roll forward", {
  description: "byEmail is added with a row, removed by a rollback while a second row is written, and added again with a third.",
  run(s) {
    const op = s.the(Operator);
    const rows = s.the(RowShard);
    s.act(op, "deploy", { schema: 2 });
    s.act(rows, "put", { row: "a" });
    s.settle();
    s.act(op, "deploy", { schema: 1 });
    s.act(rows, "put", { row: "b" });
    s.settle();
    s.act(op, "deploy", { schema: 2 });
    s.act(rows, "put", { row: "c" });
    s.settle();
  },
});

export default defineSpec({
  name: "Global index across a rollback",
  description: "How a global index keeps every row after a deploy adds it, a rollback runs code without it, and a roll forward adds it again.",
  actors: [Operator, RowShard, IndexShard],
  properties: [everyRowFound, rollbackDropsNothing, rolledForward],
  scenarios: [rollbackAndForward],
});
