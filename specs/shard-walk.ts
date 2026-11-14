// Shard-order walks through splits: a walk lists one shard at a time, by key within each, and
// carries its position in a cursor. When the shard it is in splits, its halves each hold rows the
// walk already listed (up to its position) and rows it hasn't; so the cursor keeps a floor, "under
// these bits, everything up to here is listed", and starts each half from it.
//
//   - floorPerSubtree: a cursor keeps one floor per shard that split under it, and a shard starts
//     from the floor of its nearest such ancestor. With one floor (the first split's), a shard that
//     splits again starts its second half from the old floor, below what the walk listed in that
//     shard: those rows are listed twice. (Keeping only the newest floor instead skips rows in the
//     first split's other half.)
//
// Five keys over a root that splits, and its left half that splits again, both perhaps mid-walk.
// Every key exists throughout. Before cleanup, a shard holds the keys under its bits.
import { atMost, defineActor, defineProperty, defineRule, defineScenario, defineSpec, defineZone } from "@leostera/feynman";

const floorPerSubtree = defineRule("floorPerSubtree", { description: "A cursor keeps a floor per shard that split under it; a shard starts from its nearest one." });

/** Each key's hash bits: a and c under "00", b and e under "01", d under "1". */
const HASH: Record<string, string> = { a: "00", b: "01", c: "00", d: "10", e: "01" };
const KEYS = Object.keys(HASH).sort();

type Floor = { bits: string; after: string };
type Walk = { leaf: string; after: string | null; floors: Floor[] } | null;

const zone = defineZone({ name: "Worker isolate" });

/** Where a walk goes once shard `leaf` is done: the sibling of its nearest left half, or the end. */
function next(walk: NonNullable<Walk>): Walk {
  const left = walk.leaf.lastIndexOf("0");
  if (left < 0) return null;
  const leaf = `${walk.leaf.slice(0, left)}1`;
  const floors = walk.floors.filter((f) => leaf.startsWith(f.bits));
  // The nearest floor is the deepest: the walk listed further in that shard before it split.
  const floor = floors.sort((x, y) => y.bits.length - x.bits.length)[0];
  return { leaf, after: floor?.after ?? null, floors };
}

export const Walker = defineActor({
  name: "Walker",
  description: "A walk in shard order, a row a page, and the tree it walks, which may split under it.",
  zone,
  singleton: true,
  initialState: () => ({ leaves: [""], walk: { leaf: "", after: null, floors: [] } as Walk, listed: [] as string[] }),
  display: { icon: "code", status: (s) => (s.walk === null ? `done: ${s.listed.join(",")}` : `at ${s.walk.leaf || "root"} after ${s.walk.after ?? "start"}; listed ${s.listed.join(",")}`) },
  actions: {
    page: {
      label: "list a page",
      description: "Lists the next row of the cursor's shard, or follows its split, or moves on to the next shard.",
      sources: ["src/table/client.ts#TableClient.listByShard", "src/table/schema.ts#nextShard"],
      when: (s) => s.walk !== null,
      run(s) {
        const walk = s.walk!;
        if (!s.leaves.includes(walk.leaf)) {
          // The shard split since: its left half, from where the walk was.
          const floor = walk.after === null ? null : { bits: walk.leaf, after: walk.after };
          const floors = floorPerSubtree.on ? [...walk.floors, ...(floor ? [floor] : [])] : walk.floors.length ? walk.floors : floor ? [floor] : [];
          s.walk = { leaf: `${walk.leaf}0`, after: walk.after, floors };
          return;
        }
        const row = KEYS.find((k) => HASH[k]!.startsWith(walk.leaf) && (walk.after === null || k > walk.after));
        if (row === undefined) return void (s.walk = next(walk));
        s.listed.push(row);
        s.walk = { ...walk, after: row };
      },
    },
    splitRoot: {
      with: [atMost(1)],
      label: "the root splits",
      description: "The root splits into 0 and 1.",
      sources: ["src/sharded/index.ts#ShardedObject.split"],
      when: (s) => s.leaves.includes(""),
      run: (s) => void (s.leaves = ["0", "1"]),
    },
    splitLeft: {
      with: [atMost(1)],
      label: "shard 0 splits",
      description: "Shard 0 splits into 00 and 01.",
      sources: ["src/sharded/index.ts#ShardedObject.split"],
      when: (s) => s.leaves.includes("0"),
      run: (s) => void (s.leaves = ["00", "01", "1"]),
    },
  },
});

const once = defineProperty("once", {
  description: "No row is listed twice.",
  always: (w) => new Set(w.the(Walker).state.listed).size === w.the(Walker).state.listed.length,
  needs: [floorPerSubtree],
});
const complete = defineProperty("complete", {
  description: "A walk that ended listed every row (every row exists throughout).",
  always: (w) => w.the(Walker).state.walk !== null || KEYS.every((k) => w.the(Walker).state.listed.includes(k)),
});
const splitTwiceMidWalk = defineProperty("splitTwiceMidWalk", {
  description: "A walk can end after both splits happened mid-walk.",
  reachable: (w) => w.the(Walker).state.walk === null && w.the(Walker).state.leaves.length === 3 && w.the(Walker).state.listed.length === KEYS.length,
});

const twice = defineScenario("A shard splits twice under a walk", {
  description: "The walk lists a row from the root, the root splits, it lists in shard 0, which splits too; then it walks on to the end.",
  run(s) {
    const walker = s.the(Walker);
    s.act(walker, "page");
    s.act(walker, "splitRoot");
    s.act(walker, "page");
    s.act(walker, "page");
    s.act(walker, "page");
    s.act(walker, "splitLeft");
    while (walker.state.walk !== null) s.act(walker, "page");
  },
});

export default defineSpec({
  name: "Shard-order walk",
  description: "A walk in shard order through shards that split under it lists every row once.",
  actors: [Walker],
  properties: [once, complete, splitTwiceMidWalk],
  scenarios: [twice],
  finalState: { description: "The walk ended.", when: (w) => w.the(Walker).state.walk === null },
});
