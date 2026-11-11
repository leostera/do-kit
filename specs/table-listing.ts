// A listing pages through a sharded table with a cursor that holds only a sort position. Pages run
// while the shard splits: the original stops owning the half that moves, the copy answers "not
// ready" until it is activated, and cleanup deletes the rows the original gave away.
//
// The rule is needed: --without mergeToFrontier returns every row the shards sent, past where one
// of them stopped, and skips the rows that shard had not sent yet (noRowSkipped). The original
// lists only rows it owns, and the merge drops a key two shards sent; neither is needed in this
// model, where a client learns of a split only once it has committed.
import { atMost, budget, defineActor, defineCall, defineLimit, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, failed, listOf, lose, oneOf } from "@leostera/feynman";

/** Rows by sort position; row 2's key hashes into the half that moves. */
const ROWS = [0, 1, 2] as const;
const END = 3;
/** Rows per page. */
const LIMIT = 2;
/** Rows asked of each shard once there are two: fewer than a page (the code asks for its share). */
const SHARD_LIMIT = 1;
type Shard = "root" | "left" | "right";

/** A shard's page: its owned rows after `after`, up to `limit`, and how far it has reported. */
function shardPage(owned: readonly number[], after: number, limit: number): { rows: number[]; frontier: number } {
  const rows = owned.filter((r) => r > after).slice(0, limit);
  return { rows, frontier: rows.length === limit ? rows[limit - 1]! : END };
}

const faults = budget("faults", 1);

const ShardSchema = oneOf("root", "left", "right");
const Page = defineCall("Page", { after: Number, as: ShardSchema, limit: Number }, { name: "list page" });
const Rows = defineMessage("Rows", { shard: ShardSchema, rows: listOf(Number), frontier: Number }, { name: "rows", tone: "reply" });
const Moved = defineMessage("Moved", {}, { name: "moved", tone: "refused" });
const NotReady = defineMessage("NotReady", {}, { name: "not ready", tone: "refused" });
const Copy = defineMessage("Copy", {}, { name: "snapshot" });
const Copied = defineMessage("Copied", {}, { name: "claimed", tone: "reply" });
const Activate = defineMessage("Activate", {}, { name: "activate" });
const Activated = defineMessage("Activated", {}, { name: "activated", tone: "reply" });

const mergeToFrontier = defineRule("mergeToFrontier", { description: "A page holds only the rows up to the smallest frontier any shard reported." });

const callers = defineZone({ name: "Workers", description: "merge one page from every shard" });
const objects = defineZone({ name: "Durable Objects", description: "each lists only the rows it owns" });

const rowsPerPage = defineLimit({ name: "Rows per page", value: LIMIT, unit: "rows", sources: ["src/table/schema.ts#pageRequest"] });

export const Client = defineActor({
  name: "Client",
  zone: callers,
  singleton: true,
  initialState: () => ({
    started: false,
    done: false,
    knowsSplit: false,
    /** Sort position of the last row returned: the cursor. */
    cursor: -1,
    emitted: [] as number[],
    waiting: new Set<Shard>(),
    answers: {} as Partial<Record<Shard, { rows: number[]; frontier: number }>>,
  }),
  display: {
    icon: "code",
    status: (s) => (s.done ? `done: ${s.emitted.join(", ")}` : s.waiting.size ? `page after ${s.cursor}, waiting for ${[...s.waiting].join(", ")}` : s.started ? `listed ${s.emitted.join(", ") || "nothing"}` : "idle"),
    mood: (s) => (s.done ? "ok" : s.waiting.size ? "busy" : "info"),
  },
  actions: {
    list: {
      with: [atMost(1)],
      label: "List the table",
      description: "Starts a listing from the beginning.",
      sources: ["src/table/client.ts#TableClient.list"],
      when: (s) => !s.started,
      run(s) {
        s.started = true;
      },
    },
  },
  transitions: {
    nextPage: {
      description: "Asks every shard it knows for a page after the cursor.",
      sources: ["src/table/client.ts#TableClient.list", "src/table/client.ts#perShardLimit", "src/sharded/client.ts#ShardedClient.scan"],
      when: (s) => s.started && !s.done && s.waiting.size === 0,
      sends: [Page],
      run(s, _, ctx) {
        s.answers = {};
        for (const shard of s.knowsSplit ? (["left", "right"] as const) : (["root"] as const)) {
          s.waiting.add(shard);
          ctx.send(shard === "right" ? ctx.the(Sibling) : ctx.the(Original), Page, { after: s.cursor, as: shard, limit: s.knowsSplit ? SHARD_LIMIT : LIMIT });
        }
      },
    },
    rows: {
      on: Rows,
      description: "Keeps a shard's page; with every shard's, returns the rows up to the smallest frontier and moves the cursor.",
      sources: ["src/sharded/client.ts#ShardedClient.scan", "src/table/schema.ts#mergePages"],
      run(s, msg) {
        if (!s.waiting.delete(msg.shard)) return;
        s.answers[msg.shard] = { rows: msg.rows, frontier: msg.frontier };
        if (s.waiting.size > 0) return;
        const answers = Object.values(s.answers);
        // Without the merge rule: as far as any shard got.
        const frontier = mergeToFrontier.on ? Math.min(...answers.map((a) => a.frontier)) : Math.max(...answers.map((a) => a.frontier));
        const sent = answers.flatMap((a) => a.rows);
        const complete = [...new Set(sent)].filter((r) => r <= frontier).sort((a, b) => a - b);
        const page = complete.slice(0, LIMIT);
        s.emitted.push(...page);
        s.cursor = page.length === LIMIT ? page[LIMIT - 1]! : frontier;
        if (s.cursor === END) s.done = true;
      },
    },
    moved: {
      on: Moved,
      description: "The shard it asked split: asks both halves for the same page.",
      sources: ["src/sharded/client.ts#ShardedClient.scan", "src/sharded/client.ts#ShardedClient.learn"],
      sends: [Page],
      run(s, _msg, ctx) {
        if (!s.waiting.delete("root")) return;
        s.knowsSplit = true;
        for (const shard of ["left", "right"] as const) {
          s.waiting.add(shard);
          ctx.send(shard === "right" ? ctx.the(Sibling) : ctx.the(Original), Page, { after: s.cursor, as: shard, limit: SHARD_LIMIT });
        }
      },
    },
    notReady: {
      on: NotReady,
      description: "The new shard is not serving yet: asks it again shortly.",
      sources: ["src/sharded/client.ts#ShardedClient.scan", "src/sharded/client.ts#Backoff.next"],
      starts: ["retryRight"],
      run: (_s, _msg, ctx) => ctx.start("retryRight"),
    },
    retryRight: {
      description: "Asks the new shard for the page again.",
      sources: ["src/sharded/client.ts#ShardedClient.scan"],
      after: "a few milliseconds",
      sends: [Page],
      run(s, _, ctx) {
        if (s.waiting.has("right")) ctx.send(ctx.the(Sibling), Page, { after: s.cursor, as: "right", limit: SHARD_LIMIT });
      },
    },
    pageFailed: {
      on: failed(Page),
      description: "A page call failed: asks that shard again.",
      sources: ["src/sharded/client.ts#ShardedClient.scan"],
      sends: [Page],
      run(s, failure, ctx) {
        if (s.waiting.has(failure.payload.as)) ctx.send(failure.to, Page, failure.payload);
      },
    },
    rowsLost: lose(Rows, { fails: Page, spend: faults }),
    movedLost: lose(Moved, { fails: Page, spend: faults }),
    notReadyLost: lose(NotReady, { fails: Page, spend: faults }),
  },
});

export const Original = defineActor({
  name: "Original",
  description: "The shard that held every row, and keeps rows 0 and 1 after the split.",
  zone: objects,
  singleton: true,
  initialState: () => ({ splitting: false, committed: false, activated: false, storesMoved: true }),
  display: {
    icon: "database",
    status: (s) => (s.committed ? `owns 0, 1${s.storesMoved ? "; row 2 awaits cleanup" : ""}` : s.splitting ? "copying the right half" : "owns 0, 1, 2"),
    mood: (s) => (s.splitting && !s.committed ? "busy" : "ok"),
  },
  actions: {
    split: {
      with: [atMost(1)],
      label: "Split",
      description: "Copies its storage to the new shard.",
      sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.copyAndClaim"],
      when: (s) => !s.splitting,
      sends: [Copy],
      run(s, _inputs, ctx) {
        s.splitting = true;
        ctx.send(ctx.the(Sibling), Copy, {});
      },
    },
    cleanup: {
      with: [atMost(1)],
      label: "Run cleanup",
      description: "Deletes row 2, which it no longer owns.",
      sources: ["src/sharded/index.ts#ShardedObject.alarm", "src/table/index.ts#ShardedTable.cleanupForeign"],
      when: (s) => s.committed && s.storesMoved,
      run(s) {
        s.storesMoved = false;
      },
    },
  },
  transitions: {
    page: {
      on: Page,
      description: "Lists its owned rows after the cursor; answers moved when asked as the shard it split.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedScan", "src/table/index.ts#ShardedTable.tablePage", "src/table/index.ts#ShardedTable.page"],
      sends: [Rows, Moved],
      run(s, msg, ctx) {
        if (msg.as === "root" && s.committed) return ctx.reply(Moved, {});
        // Owned rows only: row 2 may still be stored, but after the commit it is not listed.
        ctx.reply(Rows, { shard: msg.as, ...shardPage(s.committed ? [0, 1] : [...ROWS], msg.after, msg.limit) });
      },
    },
    copied: {
      on: Copied,
      description: "The copy is claimed: stops owning the right half and activates the copy.",
      sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.activateSibling"],
      sends: [Activate],
      starts: ["activateRetry"],
      run(s, _msg, ctx) {
        s.committed = true;
        ctx.send(ctx.the(Sibling), Activate, {});
        ctx.start("activateRetry");
      },
    },
    activated: {
      on: Activated,
      description: "The copy serves the right half.",
      sources: ["src/sharded/index.ts#ShardedObject.activateSibling"],
      run(s, _msg, ctx) {
        s.activated = true;
        ctx.cancel("activateRetry");
      },
    },
    activateRetry: {
      description: "Activates the copy again until it confirms (the alarm's job).",
      sources: ["src/sharded/index.ts#ShardedObject.alarm", "src/sharded/index.ts#ShardedObject.activateSibling"],
      after: "1 second",
      sends: [Activate],
      starts: ["activateRetry"],
      run(s, _, ctx) {
        if (s.activated) return;
        ctx.send(ctx.the(Sibling), Activate, {});
        ctx.start("activateRetry");
      },
    },
    pageLost: lose(Page, { spend: faults }),
    copiedLost: lose(Copied, { spend: faults }),
    activatedLost: lose(Activated, { spend: faults }),
  },
});

export const Sibling = defineActor({
  name: "Sibling",
  description: "The new shard, which owns row 2 once activated.",
  zone: objects,
  singleton: true,
  initialState: () => ({ copied: false, active: false }),
  display: {
    icon: "database",
    status: (s) => (s.active ? "owns 2" : s.copied ? "copy, not serving yet" : "empty"),
    mood: (s) => (s.active ? "ok" : "off"),
  },
  transitions: {
    copy: {
      on: Copy,
      description: "Restores the snapshot and claims it.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedRestore", "src/sharded/index.ts#ShardedObject.shardedClaim"],
      sends: [Copied],
      run(s, _msg, ctx) {
        s.copied = true;
        ctx.reply(Copied, {});
      },
    },
    activate: {
      on: Activate,
      description: "Starts serving the right half.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedActivate"],
      sends: [Activated],
      run(s, _msg, ctx) {
        s.active = true;
        ctx.reply(Activated, {});
      },
    },
    page: {
      on: Page,
      description: "Not serving yet: not ready. Serving: lists row 2 after the cursor.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedScan", "src/sharded/index.ts#ShardedObject.arriveOrCreate", "src/table/index.ts#ShardedTable.page"],
      sends: [Rows, NotReady],
      run(s, msg, ctx) {
        if (!s.active) return ctx.reply(NotReady, {});
        ctx.reply(Rows, { shard: msg.as, ...shardPage([2], msg.after, msg.limit) });
      },
    },
    pageLost: lose(Page, { spend: faults }),
    copyLost: lose(Copy, { spend: faults }),
    activateLost: lose(Activate, { spend: faults }),
  },
});

const noRowTwice = defineProperty("noRowTwice", {
  description: "No row is returned twice, across pages.",
  always: (w) => new Set(w.the(Client).state.emitted).size === w.the(Client).state.emitted.length,
});
const inOrder = defineProperty("inOrder", {
  description: "Rows come back in sort order, across pages.",
  always: (w) => w.the(Client).state.emitted.every((r, i, all) => i === 0 || all[i - 1]! < r),
});
const noRowSkipped = defineProperty("noRowSkipped", {
  description: "A finished listing returned every row.",
  always: (w) => !w.the(Client).state.done || w.the(Client).state.emitted.length === ROWS.length,
  needs: [mergeToFrontier],
});
const finishes = defineProperty("finishes", {
  description: "A listing that starts finishes.",
  leadsTo: { whenever: (w) => w.the(Client).state.started, then: (w) => w.the(Client).state.done },
});

const noSplit = defineScenario("List with no split", {
  description: "Two pages: rows 0 and 1, then row 2.",
  run(s) {
    s.act(s.the(Client), "list");
    s.settle();
  },
});

const splitBetween = defineScenario("Split between pages", {
  description: "The first page comes from the original; the shard splits; the next page is asked of the shard that split, which answers moved, so it is asked of both halves with the same cursor.",
  run(s) {
    s.act(s.the(Client), "list");
    s.settle({ until: (w) => w.the(Client).state.emitted.length > 0 });
    s.act(s.the(Original), "split");
    s.settle();
  },
});

const notActive = defineScenario("Copy not active yet", {
  description: "The page reaches the new shard before it is activated: it answers not ready, and the client asks again.",
  run(s) {
    s.act(s.the(Original), "split");
    s.deliver(s.the(Sibling), Copy);
    s.deliver(s.the(Original), Copied);
    s.act(s.the(Client), "list");
    s.settle({ until: (w) => w.the(Client).state.knowsSplit });
    s.settle();
  },
});

export default defineSpec({
  name: "Table listing",
  description: "How a listing pages through a sharded table, with a cursor that survives a split, without skipping or repeating a row.",
  actors: [Client, Original, Sibling],
  limits: [rowsPerPage],
  properties: [noRowTwice, inOrder, noRowSkipped, finishes],
  scenarios: [noSplit, splitBetween, notActive],
  finalState: { description: "The listing finished (or never started) and nothing is in flight.", when: (w) => w.quiet && (w.the(Client).state.done || !w.the(Client).state.started) },
});
