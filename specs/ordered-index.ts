// An ordered global index (RFD0012): its entries are split by ranges of the sort key, not by hash,
// so a page of the whole table in order reads one index shard (two at a range's end), however many
// row shards there are. A row moves in the order (its sort column changes) while the index shard
// it is listed from splits by range, and while a listing pages through it.
//
// The index starts as one shard ("Index") holding every position. It splits at SPLIT: it keeps
// the positions below and the new shard ("Upper") takes the rest. Row "a" moves from position 1,
// below the split, to 2, above it: its row shard sends a remove to one side and a put to the other.
//
// Each rule is needed: --without ackAfterIndex is eventual mode, where a write is acknowledged
// before its entries are stored, and a listing started after the ack can miss the move
// (readYourWrites). --without holdMovingHalf lets a put land on the shard that is splitting after
// its snapshot was taken: the copy never has it, and cleanup deletes it (converges). --without
// listOwnedOnly lets the lower shard list positions it gave away, stale copies cleanup has not
// deleted yet, and a cursor past them skips the new entry above the split (readYourWrites).
import { atMost, budget, defineActor, defineCall, defineLimit, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, failed, listOf, lose, oneOf, type } from "@leostera/feynman";

/** Positions below SPLIT stay on the index shard that splits; the rest move to the new one. */
const SPLIT = 2;
/** Entries per page. */
const LIMIT = 2;
/** Where row "a" moves to: above the split. */
const MOVE_TO = 2;

type Leaf = "root" | "lower" | "upper";
type Entry = { key: string; pos: number; version: number; removed: boolean };
type Entries = Record<string, Entry>;
type Change = { id: number; key: string; pos: number; version: number; removed: boolean; sent: boolean };

const entryKey = (key: string, pos: number) => `${pos}:${key}`;
/** The shard a position belongs to, once the split is known. */
const leafOf = (pos: number): Leaf => (pos < SPLIT ? "lower" : "upper");

/** The rows the table starts with, each at version 1, and the index holding all of them. */
const ROWS: Record<string, { pos: number; version: number }> = { d: { pos: 0, version: 1 }, a: { pos: 1, version: 1 }, b: { pos: 3, version: 1 } };
const INDEXED: Entries = Object.fromEntries(Object.entries(ROWS).map(([key, r]) => [entryKey(key, r.pos), { key, pos: r.pos, version: r.version, removed: false }]));

/** Applies a change by version: an older one than the entry has is ignored. */
function applyChange(entries: Entries, c: { key: string; pos: number; version: number; removed: boolean }): void {
  const at = entryKey(c.key, c.pos);
  const was = entries[at];
  if (was && was.version >= c.version) return;
  entries[at] = { key: c.key, pos: c.pos, version: c.version, removed: c.removed };
}

/** A page of the entries after `after` in `range`, in order, up to LIMIT; `end`: the range has no more. */
function pageOf(entries: Entries, after: number, owned: (pos: number) => boolean): { entries: { key: string; pos: number; version: number }[]; end: boolean } {
  const live = Object.values(entries)
    .filter((e) => !e.removed && e.pos > after && owned(e.pos))
    .sort((x, y) => x.pos - y.pos || (x.key < y.key ? -1 : 1));
  const page = live.slice(0, LIMIT).map(({ key, pos, version }) => ({ key, pos, version }));
  return { entries: page, end: live.length <= LIMIT };
}

const faults = budget("faults", 1);

const LeafSchema = oneOf("root", "lower", "upper");
const EntrySchema = { key: String, pos: Number, version: Number };

const Move = defineMessage("Move", { key: String, pos: Number }, { name: "write" });
const Acked = defineMessage("Acked", { key: String, version: Number }, { name: "written", tone: "reply" });

const Deliver = defineCall("Deliver", { id: Number, key: String, pos: Number, version: Number, removed: Boolean, as: LeafSchema }, { name: "index change" });
const Delivered = defineMessage("Delivered", { id: Number }, { name: "applied", tone: "reply" });
const DeliverMoved = defineMessage("DeliverMoved", { id: Number }, { name: "moved", tone: "refused" });
const DeliverHeld = defineMessage("DeliverHeld", { id: Number }, { name: "held: splitting", tone: "refused" });
const DeliverNotReady = defineMessage("DeliverNotReady", { id: Number }, { name: "not ready", tone: "refused" });

const Page = defineCall("Page", { after: Number, as: LeafSchema }, { name: "index page" });
const Entries_ = defineMessage("Entries", { as: LeafSchema, entries: listOf(EntrySchema), end: Boolean }, { name: "entries", tone: "reply" });
const PageMoved = defineMessage("PageMoved", {}, { name: "moved", tone: "refused" });
const PageNotReady = defineMessage("PageNotReady", {}, { name: "not ready", tone: "refused" });

const Copy = defineMessage("Copy", { entries: type<Entries>() }, { name: "snapshot" });
const Copied = defineMessage("Copied", {}, { name: "claimed", tone: "reply" });
const Activate = defineMessage("Activate", {}, { name: "activate" });

const ackAfterIndex = defineRule("ackAfterIndex", { description: 'Consistent mode: a write is acknowledged only once every index change it made is stored ("eventual" acknowledges it first).' });
const holdMovingHalf = defineRule("holdMovingHalf", { description: "While an index shard copies the range that moves, changes to that range wait." });
const listOwnedOnly = defineRule("listOwnedOnly", { description: "An index shard lists only the range it owns, not the copies a split gave away and cleanup has not deleted." });

const app = defineZone({ name: "Worker", description: "writes rows and lists them in order" });
const rowShards = defineZone({ name: "Row shards", description: "rows, by hash of the key" });
const indexShards = defineZone({ name: "Index shards", description: "entries, by range of the sort key" });

const pageSize = defineLimit({ name: "Entries per page", value: LIMIT, unit: "entries" });

export const App = defineActor({
  name: "App",
  description: "A Worker that moves row a in the order, then lists the table in order.",
  zone: app,
  singleton: true,
  initialState: () => ({
    moved: false,
    acked: false,
    started: false,
    done: false,
    /** The listing began after the write was acknowledged: it must show the write. */
    afterAck: false,
    knowsSplit: false,
    /** The cursor: the index shard named in it, and the last position returned. */
    leaf: "root" as Leaf,
    after: -1,
    waiting: false,
    emitted: [] as { key: string; pos: number; version: number }[],
  }),
  display: {
    icon: "code",
    status: (s) => (s.done ? `listed ${s.emitted.map((e) => `${e.key}@${e.pos}`).join(", ")}` : s.started ? `page after ${s.after} from ${s.leaf}` : s.acked ? "moved a" : s.moved ? "moving a" : "idle"),
    mood: (s) => (s.done ? "ok" : s.started || (s.moved && !s.acked) ? "busy" : "info"),
  },
  actions: {
    move: {
      with: [atMost(1)],
      label: "Move a",
      description: `Writes row a with a sort position of ${MOVE_TO}, past the split.`,
      sources: ["src/indexed/client.ts#IndexedTableClient.putMany"],
      sends: [Move],
      run(s, _inputs, ctx) {
        s.moved = true;
        ctx.send(ctx.the(RowShard), Move, { key: "a", pos: MOVE_TO });
      },
    },
    list: {
      with: [atMost(1)],
      label: "List in order",
      description: "Starts a listing of the whole table through the ordered index.",
      when: (s) => !s.started,
      run(s) {
        s.started = true;
        s.afterAck = s.acked;
        s.leaf = s.knowsSplit ? leafOf(0) : "root";
      },
    },
  },
  transitions: {
    acked: {
      on: Acked,
      description: "The write is acknowledged.",
      sources: ["src/indexed/client.ts#IndexedTableClient.settle"],
      run(s) {
        s.acked = true;
      },
    },
    nextPage: {
      description: "Asks the index shard named in the cursor for the entries after it.",
      when: (s) => s.started && !s.done && !s.waiting,
      sends: [Page],
      run(s, _, ctx) {
        s.waiting = true;
        ctx.send(s.leaf === "upper" ? ctx.the(Upper) : ctx.the(Index), Page, { after: s.after, as: s.leaf });
      },
    },
    entries: {
      on: Entries_,
      description: "Returns the page; at the end of its shard's range, the cursor names the next shard.",
      run(s, msg) {
        if (!s.waiting || msg.as !== s.leaf) return;
        s.waiting = false;
        s.emitted.push(...msg.entries);
        if (msg.entries.length > 0) s.after = msg.entries.at(-1)!.pos;
        if (!msg.end) return;
        if (s.leaf === "lower") s.leaf = "upper";
        else s.done = true;
      },
    },
    pageMoved: {
      on: PageMoved,
      description: "The shard named in the cursor split: the cursor names the half holding the positions after it.",
      sends: [Page],
      run(s, _msg, ctx) {
        if (!s.waiting) return;
        s.knowsSplit = true;
        s.leaf = leafOf(s.after + 1);
        ctx.send(s.leaf === "upper" ? ctx.the(Upper) : ctx.the(Index), Page, { after: s.after, as: s.leaf });
      },
    },
    pageNotReady: {
      on: PageNotReady,
      description: "The new shard is not serving yet: asks again shortly.",
      starts: ["retryPage"],
      run: (_s, _msg, ctx) => ctx.start("retryPage"),
    },
    retryPage: {
      description: "Asks the new shard for the page again.",
      after: "a few milliseconds",
      sends: [Page],
      run(s, _, ctx) {
        if (s.waiting) ctx.send(ctx.the(Upper), Page, { after: s.after, as: s.leaf });
      },
    },
    pageFailed: {
      on: failed(Page),
      description: "A page call failed: asks the same shard again.",
      sends: [Page],
      run(s, failure, ctx) {
        if (s.waiting) ctx.send(failure.to, Page, failure.payload);
      },
    },
    entriesLost: lose(Entries_, { fails: Page, spend: faults }),
  },
});

export const RowShard = defineActor({
  name: "Row shard",
  description: "The shard that holds the rows, and an outbox of index changes it has not delivered yet.",
  zone: rowShards,
  singleton: true,
  initialState: () => ({ rows: structuredClone(ROWS), outbox: [] as Change[], nextId: 1, ackWaiting: false, knowsSplit: false }),
  display: {
    icon: "database",
    status: (s) => `a at ${s.rows.a!.pos}${s.outbox.length ? `, ${s.outbox.length} change(s) to deliver` : ""}`,
    mood: (s) => (s.outbox.length ? "busy" : "ok"),
  },
  transitions: {
    move: {
      on: Move,
      description: "Writes the row and, in the same transaction, an outbox change for each entry it removes and adds.",
      sources: ["src/indexed/table.ts#IndexedTable.writeBatch", "src/indexed/table.ts#IndexedTable.rowWritten"],
      sends: [Acked],
      run(s, msg, ctx) {
        const was = s.rows[msg.key]!;
        const version = was.version + 1;
        s.rows[msg.key] = { pos: msg.pos, version };
        s.outbox.push({ id: s.nextId++, key: msg.key, pos: was.pos, version, removed: true, sent: false });
        s.outbox.push({ id: s.nextId++, key: msg.key, pos: msg.pos, version, removed: false, sent: false });
        // Eventual mode acknowledges now; consistent mode once the outbox is delivered.
        if (ackAfterIndex.on) s.ackWaiting = true;
        else ctx.send(ctx.the(App), Acked, { key: msg.key, version });
      },
    },
    deliver: {
      description: "Sends each undelivered change to the index shard whose range holds its position.",
      sources: ["src/indexed/table.ts#IndexedTable.pump", "src/indexed/table.ts#IndexedTable.sendOnce"],
      when: (s) => s.outbox.some((c) => !c.sent),
      sends: [Deliver],
      run(s, _, ctx) {
        for (const c of s.outbox.filter((c) => !c.sent)) {
          c.sent = true;
          const as: Leaf = s.knowsSplit ? leafOf(c.pos) : "root";
          ctx.send(as === "upper" ? ctx.the(Upper) : ctx.the(Index), Deliver, { id: c.id, key: c.key, pos: c.pos, version: c.version, removed: c.removed, as });
        }
      },
    },
    delivered: {
      on: Delivered,
      description: "Deletes the change from the outbox.",
      sources: ["src/indexed/table.ts#IndexedTable.sendOnce"],
      run(s, msg) {
        s.outbox = s.outbox.filter((c) => c.id !== msg.id);
      },
    },
    ack: {
      description: "Consistent mode: every change of the write is stored, so the write is acknowledged.",
      sources: ["src/indexed/table.ts#IndexedTable.deliverWithin"],
      when: (s) => s.ackWaiting && s.outbox.length === 0,
      sends: [Acked],
      run(s, _, ctx) {
        s.ackWaiting = false;
        ctx.send(ctx.the(App), Acked, { key: "a", version: s.rows.a!.version });
      },
    },
    deliverMoved: {
      on: DeliverMoved,
      description: "The index shard split: the change goes to the half whose range holds it.",
      run(s, msg) {
        s.knowsSplit = true;
        const c = s.outbox.find((c) => c.id === msg.id);
        if (c) c.sent = false;
      },
    },
    deliverAgain: {
      on: DeliverHeld,
      description: "The index shard is copying that range: sends the change again.",
      run(s, msg) {
        const c = s.outbox.find((c) => c.id === msg.id);
        if (c) c.sent = false;
      },
    },
    deliverNotReady: {
      on: DeliverNotReady,
      description: "The new index shard is not serving yet: sends the change again.",
      run(s, msg) {
        const c = s.outbox.find((c) => c.id === msg.id);
        if (c) c.sent = false;
      },
    },
    deliverFailed: {
      on: failed(Deliver),
      description: "A delivery failed: the change stays in the outbox, and is sent again.",
      sources: ["src/indexed/table.ts#IndexedTable.layerAlarm"],
      run(s, failure) {
        const c = s.outbox.find((c) => c.id === failure.payload.id);
        if (c) c.sent = false;
      },
    },
    deliveredLost: lose(Delivered, { fails: Deliver, spend: faults }),
  },
});

export const Index = defineActor({
  name: "Index",
  description: `The index shard that held every position, and keeps those below ${SPLIT} after it splits.`,
  zone: indexShards,
  singleton: true,
  initialState: () => ({ entries: structuredClone(INDEXED), splitting: false, committed: false, cleaned: false }),
  display: {
    icon: "database",
    status: (s) => (s.committed ? `owns below ${SPLIT}${s.cleaned ? "" : `; copies of ${SPLIT}+ await cleanup`}` : s.splitting ? `copying ${SPLIT}+` : "owns every position"),
    mood: (s) => (s.splitting && !s.committed ? "busy" : "ok"),
  },
  actions: {
    split: {
      with: [atMost(1)],
      label: "Split by range",
      description: `Splits at ${SPLIT}: snapshots its storage for a new shard that takes ${SPLIT} and above.`,
      when: (s) => !s.splitting,
      sends: [Copy],
      run(s, _inputs, ctx) {
        s.splitting = true;
        ctx.send(ctx.the(Upper), Copy, { entries: structuredClone(s.entries) });
      },
    },
    cleanup: {
      with: [atMost(1)],
      label: "Run cleanup",
      description: "Deletes the entries of the range it gave away.",
      sources: ["src/indexed/global-index.ts#GlobalIndex.cleanupForeign"],
      when: (s) => s.committed && !s.cleaned,
      run(s) {
        s.cleaned = true;
        for (const [at, e] of Object.entries(s.entries)) if (e.pos >= SPLIT) delete s.entries[at];
      },
    },
  },
  transitions: {
    apply: {
      on: Deliver,
      description: "Stores the change by version; refuses it if the range moved, or is being copied.",
      sources: ["src/indexed/global-index.ts#GlobalIndex.indexApply", "src/indexed/global-index.ts#GlobalIndex.applied"],
      sends: [Delivered, DeliverMoved, DeliverHeld],
      run(s, msg, ctx) {
        if (msg.as === "root" && s.committed) return ctx.reply(DeliverMoved, { id: msg.id });
        if (holdMovingHalf.on && s.splitting && !s.committed && msg.pos >= SPLIT) return ctx.reply(DeliverHeld, { id: msg.id });
        applyChange(s.entries, msg);
        ctx.reply(Delivered, { id: msg.id });
      },
    },
    page: {
      on: Page,
      description: "Lists its entries after the cursor, in order; answers moved when asked as the shard it was before the split.",
      sources: ["src/indexed/global-index.ts#GlobalIndex.indexPage", "src/indexed/global-index.ts#GlobalIndex.page"],
      sends: [Entries_, PageMoved],
      run(s, msg, ctx) {
        if (msg.as === "root" && s.committed) return ctx.reply(PageMoved, {});
        const owned = s.committed && listOwnedOnly.on ? (pos: number) => pos < SPLIT : () => true;
        ctx.reply(Entries_, { as: msg.as, ...pageOf(s.entries, msg.after, owned) });
      },
    },
    copied: {
      on: Copied,
      description: "The copy is claimed: stops owning the upper range and activates the copy.",
      sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.activateSibling"],
      sends: [Activate],
      run(s, _msg, ctx) {
        s.committed = true;
        ctx.send(ctx.the(Upper), Activate, {});
      },
    },
    deliverLost: lose(Deliver, { spend: faults }),
  },
});

export const Upper = defineActor({
  name: "Upper",
  description: `The new index shard, which owns ${SPLIT} and above once activated.`,
  zone: indexShards,
  singleton: true,
  initialState: () => ({ entries: {} as Entries, copied: false, active: false }),
  display: {
    icon: "database",
    status: (s) => (s.active ? `owns ${SPLIT}+` : s.copied ? "copy, not serving yet" : "empty"),
    mood: (s) => (s.active ? "ok" : "off"),
  },
  transitions: {
    copy: {
      on: Copy,
      description: "Restores the snapshot and claims it.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedRestore", "src/sharded/index.ts#ShardedObject.shardedClaim"],
      sends: [Copied],
      run(s, msg, ctx) {
        s.copied = true;
        s.entries = msg.entries;
        ctx.reply(Copied, {});
      },
    },
    activate: {
      on: Activate,
      description: "Starts serving the upper range.",
      sources: ["src/sharded/index.ts#ShardedObject.shardedActivate"],
      run(s) {
        s.active = true;
      },
    },
    apply: {
      on: Deliver,
      description: "Not serving yet: not ready. Serving: stores the change by version.",
      sources: ["src/indexed/global-index.ts#GlobalIndex.indexApply", "src/indexed/global-index.ts#GlobalIndex.applied"],
      sends: [Delivered, DeliverNotReady],
      run(s, msg, ctx) {
        if (!s.active) return ctx.reply(DeliverNotReady, { id: msg.id });
        applyChange(s.entries, msg);
        ctx.reply(Delivered, { id: msg.id });
      },
    },
    page: {
      on: Page,
      description: "Not serving yet: not ready. Serving: lists its entries after the cursor.",
      sources: ["src/indexed/global-index.ts#GlobalIndex.indexPage", "src/indexed/global-index.ts#GlobalIndex.page"],
      sends: [Entries_, PageNotReady],
      run(s, msg, ctx) {
        if (!s.active) return ctx.reply(PageNotReady, {});
        ctx.reply(Entries_, { as: msg.as, ...pageOf(s.entries, msg.after, (pos) => pos >= SPLIT) });
      },
    },
    deliverLost: lose(Deliver, { spend: faults }),
  },
});

/** The index's live entries, as the shards that own them hold them. */
function liveIndex(index: { entries: Entries; committed: boolean }, upper: { entries: Entries; active: boolean }): Record<string, number> {
  const owned = (entries: Entries, keep: (pos: number) => boolean) => Object.values(entries).filter((e) => !e.removed && keep(e.pos));
  const live = index.committed ? [...owned(index.entries, (p) => p < SPLIT), ...owned(upper.entries, (p) => p >= SPLIT)] : owned(index.entries, () => true);
  return Object.fromEntries(live.map((e) => [entryKey(e.key, e.pos), e.version]));
}

const readYourWrites = defineProperty("readYourWrites", {
  description: "A listing started after a write was acknowledged shows the row as written, and never an older version of it.",
  always: (w) => {
    const s = w.the(App).state;
    if (!s.afterAck) return true;
    const a = s.emitted.filter((e) => e.key === "a");
    return a.every((e) => e.version === 2) && (!s.done || a.length === 1);
  },
  needs: [ackAfterIndex, listOwnedOnly],
});
const stableRowsOnce = defineProperty("stableRowsOnce", {
  description: "A finished listing shows every row no one wrote during it exactly once.",
  always: (w) => {
    const s = w.the(App).state;
    return !s.done || ["d", "b"].every((key) => s.emitted.filter((e) => e.key === key).length === 1);
  },
});
const noVersionTwice = defineProperty("noVersionTwice", {
  description: "No listing shows the same version of a row twice.",
  always: (w) => {
    const seen = w.the(App).state.emitted.map((e) => `${e.key}@${e.version}`);
    return new Set(seen).size === seen.length;
  },
});
const inOrder = defineProperty("inOrder", {
  description: "A listing returns entries in sort order, across pages and shards.",
  always: (w) => w.the(App).state.emitted.every((e, i, all) => i === 0 || all[i - 1]!.pos <= e.pos),
});
const converges = defineProperty("converges", {
  description: "Once nothing is in flight and the outbox is empty, the index holds exactly one entry per row, at its position and version.",
  always: (w) => {
    const row = w.the(RowShard).state;
    if (!w.quiet || row.outbox.length > 0) return true;
    const live = liveIndex(w.the(Index).state, w.the(Upper).state);
    const want = Object.fromEntries(Object.entries(row.rows).map(([key, r]) => [entryKey(key, r.pos), r.version]));
    return JSON.stringify(Object.entries(live).sort()) === JSON.stringify(Object.entries(want).sort());
  },
  needs: [holdMovingHalf],
});
const finishes = defineProperty("finishes", {
  description: "A listing that starts finishes.",
  leadsTo: { whenever: (w) => w.the(App).state.started, then: (w) => w.the(App).state.done },
});
const writeAcked = defineProperty("writeAcked", {
  description: "A write is acknowledged.",
  leadsTo: { whenever: (w) => w.the(App).state.moved, then: (w) => w.the(App).state.acked },
});
const listsAcrossSplit = defineProperty("listsAcrossSplit", {
  description: "A listing can page from the lower shard into the new one after a split, and see the write.",
  reachable: (w) => w.the(App).state.done && w.the(App).state.afterAck && w.the(App).state.knowsSplit,
});

const moveThenList = defineScenario("Move, then list", {
  description: "Row a moves in the order; once the write is acknowledged, a listing shows it at its new position.",
  run(s) {
    const app = s.the(App);
    s.act(app, "move");
    s.settle();
    s.act(app, "list");
    s.settle();
  },
});

const splitWhileMoving = defineScenario("Split while a row moves", {
  description: "The index shard splits while the move's changes are on their way: the put for the upper range waits for the copy, then goes to the new shard.",
  run(s) {
    const app = s.the(App);
    s.act(app, "move");
    s.step(s.the(RowShard), "move");
    s.act(s.the(Index), "split");
    s.settle();
    s.act(app, "list");
    s.settle();
  },
});

const listAcrossSplit = defineScenario("List across a split", {
  description: "The first page comes from the shard before the split; the next asks it again, hears moved, and goes on from the cursor in the half that holds it.",
  run(s) {
    const app = s.the(App);
    s.act(app, "list");
    s.settle({ until: (w) => w.the(App).state.emitted.length > 0 });
    s.act(s.the(Index), "split");
    s.settle();
  },
});

export default defineSpec({
  name: "Ordered index",
  description: "How an ordered global index split by ranges lists a table in order with one shard a page, while rows move and the index splits, without missing an acknowledged write or showing a row twice.",
  actors: [App, RowShard, Index, Upper],
  limits: [pageSize],
  properties: [readYourWrites, stableRowsOnce, noVersionTwice, inOrder, converges, finishes, writeAcked, listsAcrossSplit],
  scenarios: [moveThenList, splitWhileMoving, listAcrossSplit],
  finalState: {
    description: "The listing finished (or never started), the write was acknowledged (or never made), and nothing is in flight.",
    when: (w) => w.quiet && w.the(RowShard).state.outbox.length === 0 && (w.the(App).state.done || !w.the(App).state.started) && (w.the(App).state.acked || !w.the(App).state.moved),
  },
});
