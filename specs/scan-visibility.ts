// A snapshot leaves the original's old rows in place until background cleanup runs. Scans must
// expose only rows in each shard's owned hash range, not every row still physically stored there.
//
// The rule is needed: --without filterByOwnership has the original return the right half's rows
// it still stores, and breaks eachOwnedKeyAppearsOnce.
import { atMost, budget, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, listOf, lose, oneOf } from "@leostera/feynman";

type Key = "L" | "R";

const faults = budget("faults", 1);

const Scan = defineMessage("Scan", {}, { name: "scan" });
const Rows = defineMessage("Rows", { keys: listOf(oneOf("L", "R")) }, { name: "rows", tone: "reply" });

const filterByOwnership = defineRule("filterByOwnership", { description: "A shard's scan returns only the stored rows in its owned hash range." });

const callers = defineZone({ name: "Workers", description: "collect one result from each shard" });
const objects = defineZone({ name: "Durable Objects", description: "return rows in each shard's owned range" });

export const Client = defineActor({
  name: "Client",
  zone: callers,
  singleton: true,
  initialState: () => ({ started: false, pending: 0, seen: { L: 0, R: 0 } as Record<Key, number> }),
  display: {
    icon: "code",
    status: (s) => (s.pending ? `waiting for ${s.pending} shard result(s)` : s.started ? "scan complete" : "idle"),
    mood: (s) => (s.pending ? "busy" : "ok"),
  },
  actions: {
    scan: {
      label: "Scan all shards",
      description: "Asks both shards for their owned rows.",
      when: (s) => !s.started,
      sources: ["src/sharded/client.ts#ShardedClient.scan"],
      sends: [Scan],
      run(s, _inputs, ctx) {
        s.started = true;
        s.pending = 2;
        s.seen = { L: 0, R: 0 };
        ctx.send(ctx.the(Original), Scan, {});
        ctx.send(ctx.the(Sibling), Scan, {});
      },
    },
  },
  transitions: {
    rows: {
      on: Rows,
      description: "Adds this shard's rows to the scan result.",
      sources: ["src/sharded/client.ts#ShardedClient.scan"],
      run(s, msg) {
        for (const key of msg.keys) s.seen[key]++;
        s.pending--;
      },
    },
    rowsLost: lose(Rows, { spend: faults }),
  },
});

export const Original = defineActor({
  name: "Original",
  description: "The left shard still physically stores the right rows until cleanup, but no longer owns them.",
  zone: objects,
  singleton: true,
  initialState: () => ({ stored: { L: true, R: true } }),
  display: {
    icon: "database",
    status: (s) => `left shard; ${s.stored.R ? "old right rows await cleanup" : "clean"}`,
    mood: (s) => (s.stored.R ? "info" : "ok"),
    rows: (s) => [{ group: "stored rows", label: "L", detail: "owned", mood: "info" }, ...(s.stored.R ? [{ group: "stored rows", label: "R", detail: "not owned; cleanup pending", mood: "off" as const }] : [])],
  },
  transitions: {
    scan: {
      on: Scan,
      description: "Returns only keys in its left-half hash range, even before old rows are deleted.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedScan", "src/sharded/index.ts#ShardedObject.allRows"],
      sends: [Rows],
      run(s, _msg, ctx) {
        ctx.reply(Rows, { keys: filterByOwnership.on || !s.stored.R ? ["L"] : ["L", "R"] });
      },
    },
    scanLost: lose(Scan, { spend: faults }),
  },
  actions: {
    cleanRightRows: {
      with: [atMost(1)],
      label: "Run cleanup",
      description: "Deletes the old right-half rows from the original after ownership has moved: the built-in rows, then, through cleanupForeign, the subclass's. Until then, reads filter by ownership (allRows, owns).",
      when: (s) => s.stored.R,
      sources: ["src/sharded/index.ts#ShardedObject.alarm", "src/sharded/index.ts#ShardedObject.cleanupForeign", "src/sharded/index.ts#ShardedObject.owns"],
      run(s) {
        s.stored.R = false;
      },
    },
  },
});

export const Sibling = defineActor({
  name: "Sibling",
  description: "The right shard owns the rows copied from the original.",
  zone: objects,
  singleton: true,
  initialState: () => ({}),
  display: {
    icon: "database",
    status: () => "right shard",
    mood: () => "ok",
    rows: () => [{ group: "stored rows", label: "R", detail: "owned", mood: "info" }],
  },
  transitions: {
    scan: {
      on: Scan,
      description: "Returns the keys in its right-half hash range.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedScan", "src/sharded/index.ts#ShardedObject.allRows"],
      sends: [Rows],
      run(_s, _msg, ctx) {
        ctx.reply(Rows, { keys: ["R"] });
      },
    },
    scanLost: lose(Scan, { spend: faults }),
  },
});

const eachOwnedKeyAppearsOnce = defineProperty("eachOwnedKeyAppearsOnce", {
  description: "Once a scan has both results, each key appears exactly once, whether or not cleanup has deleted the old copy.",
  always: (w) => {
    const c = w.the(Client).state;
    return !c.started || c.pending > 0 || (c.seen.L === 1 && c.seen.R === 1);
  },
  needs: [filterByOwnership],
});

const noKeyTwice = defineProperty("noKeyTwice", {
  description: "Even before every result is in, no key has come back twice.",
  always: (w) => w.the(Client).state.seen.L <= 1 && w.the(Client).state.seen.R <= 1,
  needs: [filterByOwnership],
});

const beforeCleanup = defineScenario("Scan before cleanup", {
  description: "The original still stores a stale copy of R, but its range-filtered scan returns only L; the sibling returns R.",
  run(s) {
    s.act(s.the(Client), "scan");
    s.settle();
  },
});

const afterCleanup = defineScenario("Scan after cleanup", {
  description: "Cleanup removes the original's stale R row; the scan still returns exactly L and R from their owners.",
  run(s) {
    s.act(s.the(Original), "cleanRightRows");
    s.act(s.the(Client), "scan");
    s.settle();
  },
});

export default defineSpec({
  name: "Scan visibility",
  description: "How scans avoid duplicate rows while a split's old copy is being cleaned up in the background.",
  actors: [Client, Original, Sibling],
  properties: [eachOwnedKeyAppearsOnce, noKeyTwice],
  scenarios: [beforeCleanup, afterCleanup],
  finalState: { description: "The scan has no shard responses in flight; a scan whose result was lost is outside this visibility model.", when: (w) => w.quiet },
});
