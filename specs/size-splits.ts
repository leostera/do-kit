// Size splits: a shard grows with its writes; the platform refuses every write past its cap, the
// ones that would shrink the shard too. So a shard splits by size long before the cap, refuses to
// grow past a cap of its own while a split is under way, never refuses what drains it, never makes
// a split that moves nothing, and cleans up after a split it gave up.
//
// Five rules:
//   - sizeTrigger: a shard at splitAt rows starts a split, checked after every write and by its
//     alarm (a split given up is tried again with no write to start it). Without it nothing
//     splits a quiet shard.
//   - admissionCap: at refuseAt rows (below the platform's cap) a write that grows the shard is
//     refused, and one that shrinks it is served. Without it writes during a split run into the
//     platform's cap, where deletes fail too.
//   - floor: a split whose right half would hold none or all of the rows is not started. Without
//     it a shard whose rows share one route makes splits that move nothing.
//   - discardOnAbandon: a split given up tells the sibling to delete its copy. Without it the copy
//     stays, billed, in an object no one routes to.
//   - committedStays: once the claim commits, a step after it that fails (arming the alarm,
//     activating the sibling) is logged and left to the alarm, never taken for the split given up.
//     Without it that failure discards the claimed copy, and the right half's rows with it.
//
// Rows are their routes: "l" (left half) and "r" (right), or one route "s" for a shard whose rows
// all share it. splitAt 3, refuseAt 4, the platform's cap 5 rows. A split copies, then claims (a
// fair step) or is given up (a fault, once).
import { atMost, budget, defineActor, defineProperty, defineRule, defineScenario, defineSpec, defineZone, input, oneOf, spend } from "@leostera/feynman";

const faults = budget("faults", 1);

const sizeTrigger = defineRule("sizeTrigger", { description: "A shard at splitAt rows starts a split: after a write, and from its alarm." });
const admissionCap = defineRule("admissionCap", { description: "At refuseAt rows a write that grows the shard is refused; one that shrinks it is served." });
const floor = defineRule("floor", { description: "A split that would move none or all of the rows is not started." });
const discardOnAbandon = defineRule("discardOnAbandon", { description: "A split given up deletes the sibling's copy." });
const committedStays = defineRule("committedStays", { description: "A step that fails after the claim commits does not give the split up." });

const SPLIT_AT = 3;
const REFUSE_AT = 4;
const PLATFORM_CAP = 5;

type Route = "l" | "r" | "s";
interface ShardState {
  rows: Route[];
  /** "claimed": the claim committed; the alarm and the sibling's activation are still to come. */
  split: "none" | "copying" | "claimed" | "done";
  /** The sibling's storage: a copy of the right half, claimed or not. */
  sibling: "empty" | "copy" | "claimed";
  /** Writes the platform refused at its cap (bookkeeping for the properties). */
  platformRefused: number;
  /** Deletes refused (bookkeeping). */
  deletesRefused: number;
  /** Splits started that would move none or all of the rows (bookkeeping). */
  emptySplits: number;
  /** Rows of the right half deleted with a claimed copy (bookkeeping). */
  lostRows: number;
  /** Rows the claimed copy holds. */
  siblingRows: number;
}

const objects = defineZone({ name: "Durable Objects" });

/** A write as the shard takes it: refused by its own cap, or the platform's, else applied. */
function write(s: ShardState, grows: boolean, apply: () => void): void {
  if (grows && admissionCap.on && s.rows.length >= REFUSE_AT) return;
  if (grows && s.rows.length >= PLATFORM_CAP) return void s.platformRefused++;
  // At the platform's cap every write fails, a delete too (it writes its record first).
  if (!grows && s.rows.length >= PLATFORM_CAP && !admissionCap.on) return void s.deletesRefused++;
  apply();
  if (sizeTrigger.on && s.split === "none" && s.rows.length >= SPLIT_AT) startSplit(s);
}

function startSplit(s: ShardState): void {
  const right = s.rows.filter((r) => r !== "l").length;
  const moves = right > 0 && right < s.rows.length;
  if (!moves) {
    if (floor.on) return;
    s.emptySplits++;
  }
  s.split = "copying";
  s.sibling = "copy";
}

export const Shard = defineActor({
  name: "Shard",
  description: "One shard, its sibling's storage, and the writes that reach it.",
  zone: objects,
  singleton: true,
  initialState: (): ShardState => ({ rows: [], split: "none", sibling: "empty", platformRefused: 0, deletesRefused: 0, emptySplits: 0, lostRows: 0, siblingRows: 0 }),
  display: { icon: "database", status: (s) => `${s.rows.length} rows (${s.rows.join("")}); split ${s.split}; sibling ${s.sibling}`, mood: (s) => (s.rows.length >= PLATFORM_CAP ? "bad" : s.split === "copying" ? "busy" : "ok") },
  actions: {
    put: {
      with: [atMost(6)],
      label: "put",
      description: "Writes a new row of a route: a half's, or the one route of a shard that has only one.",
      sources: ["src/sharded/index.ts#ShardedObject.update"],
      inputs: { route: input(oneOf("l", "r", "s")) },
      when: (s) => s.split !== "done" && s.split !== "claimed",
      run(s, { route }) {
        write(s, true, () => void s.rows.push(route as Route));
      },
    },
    remove: {
      with: [atMost(2)],
      label: "delete",
      description: "Deletes a row.",
      sources: ["src/sharded/index.ts#ShardedObject.update"],
      when: (s) => s.rows.length > 0 && s.split !== "done" && s.split !== "claimed",
      run(s) {
        write(s, false, () => void s.rows.pop());
      },
    },
  },
  transitions: {
    alarm: {
      description: "The alarm checks the shard's size, as a write does: a split given up is tried again.",
      sources: ["src/sharded/index.ts#ShardedObject.alarm"],
      when: (s) => sizeTrigger.on && big(s),
      run: (s) => startSplit(s),
    },
    claim: {
      description: "The copy is claimed, and the shard commits: the right half is the sibling's; the shard keeps the left.",
      sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.shardedClaim"],
      when: (s) => s.split === "copying",
      run(s) {
        s.siblingRows = s.rows.filter((r) => r !== "l").length;
        s.rows = s.rows.filter((r) => r === "l");
        s.sibling = "claimed";
        s.split = "claimed";
      },
    },
    finish: {
      description: "After the commit: the shard arms its alarm and activates the sibling.",
      sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.activateSibling"],
      when: (s) => s.split === "claimed",
      run(s) {
        s.split = "done";
      },
    },
    failAfterClaim: {
      description: "A step after the commit fails (a storage error arming the alarm).",
      sources: ["src/sharded/index.ts#ShardedObject.split"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.split === "claimed",
      run(s) {
        if (committedStays.on) return void (s.split = "done"); // logged; the alarm, or a call, asks the sibling again
        // Taken for the split given up: the claimed copy is discarded, and its rows with it.
        s.split = "none";
        if (discardOnAbandon.on) {
          s.sibling = "empty";
          s.lostRows += s.siblingRows;
        }
      },
    },
    abandon: {
      description: "The split is given up (its copy did not finish in time); the shard may split again.",
      sources: ["src/sharded/index.ts#ShardedObject.split"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.split === "copying",
      run(s) {
        s.split = "none";
        if (discardOnAbandon.on) s.sibling = "empty";
      },
    },
  },
});

const neverAtPlatformCap = defineProperty("neverAtPlatformCap", {
  description: "No write is refused by the platform's cap: the shard's own cap, or a split, comes first.",
  always: (w) => w.the(Shard).state.platformRefused === 0,
  needs: [admissionCap],
});
const deletesServed = defineProperty("deletesServed", {
  description: "A delete is never refused: a shard can always shrink.",
  always: (w) => w.the(Shard).state.deletesRefused === 0,
  needs: [admissionCap],
});
const splitsMove = defineProperty("splitsMove", {
  description: "No split starts that would move none or all of the rows.",
  always: (w) => w.the(Shard).state.emptySplits === 0,
  needs: [floor],
});
/** Past splitAt with rows of both halves: a split would help. */
const big = (s: ShardState) => s.split === "none" && s.rows.length >= SPLIT_AT && s.rows.includes("l") && s.rows.some((r) => r !== "l");
const bigShardsSplit = defineProperty("bigShardsSplit", {
  description: "A shard with rows of both halves past splitAt is split, unless it shrinks back first.",
  leadsTo: { whenever: (w) => big(w.the(Shard).state), then: (w) => !big(w.the(Shard).state) },
  needs: [sizeTrigger],
});
const noOrphanCopy = defineProperty("noOrphanCopy", {
  description: "A sibling holds a copy only while its split is under way.",
  always: (w) => {
    const s = w.the(Shard).state;
    return s.sibling !== "copy" || s.split === "copying";
  },
  needs: [discardOnAbandon],
});
const claimedKept = defineProperty("claimedKept", {
  description: "A copy the shard claimed is never deleted: the right half's rows live there.",
  always: (w) => w.the(Shard).state.lostRows === 0,
  needs: [committedStays],
});
const splitDone = defineProperty("splitDone", { description: "A shard can split by size.", reachable: (w) => w.the(Shard).state.split === "done" });

const fillAndSplit = defineScenario("A shard filled past splitAt", {
  description: "Three rows of both halves: the third starts a split, which is claimed.",
  run(s) {
    const shard = s.the(Shard);
    s.act(shard, "put", { route: "l" });
    s.act(shard, "put", { route: "r" });
    s.act(shard, "put", { route: "l" });
    s.step(shard, "claim");
    s.step(shard, "finish");
  },
});

export default defineSpec({
  name: "Size splits",
  description: "How a shard keeps clear of the platform's size cap: splitting by size, refusing growth, and cleaning up after a split it gave up.",
  actors: [Shard],
  properties: [neverAtPlatformCap, deletesServed, splitsMove, bigShardsSplit, noOrphanCopy, claimedKept, splitDone],
  scenarios: [fillAndSplit],
  finalState: { description: "No split is under way.", when: (w) => w.the(Shard).state.split !== "copying" && w.the(Shard).state.split !== "claimed" },
});
