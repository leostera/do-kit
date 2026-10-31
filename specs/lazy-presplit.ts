// Lazy presplit: with `initialDepth` N, no shard above depth N is ever created. The root starts
// as the all-zeros shard at depth N and records N; every other depth-N shard creates itself on
// its first call, at the depth the client learned from the tree (`presplit` on "moved"). Clients
// that know N route straight to depth N; load splits go deeper as usual.
//
// Safe because a key's rows only ever live in one serving shard: a shard created lazily owns
// keys no other shard has held (nothing above depth N holds rows, and below it only splits hand
// rows over). The name's storage may be deleted and the name used again, with another
// `initialDepth`, while isolates still remember the old depth: so a new shard asks the root for
// the tree's depth before creating itself (once per shard, ever), and a client whose depth is not
// the root's is told its map is stale. ASK_THE_ROOT = false trusts the client's depth instead,
// and two serving shards own one key (9 events).
//
// Keys: k0 (hash 00…), k1 (10…), k2 (11…). Depth 1 by default; two isolates' clients (checked
// with --instances Client=2); one split, below the depth, of the shard holding k1 and k2.
import { defineSpec, message } from "@leostera/feynman";

/** The rule under test: a new shard checks the client's depth with the root's (false trusts the client). */
const ASK_THE_ROOT = true;

type Key = "k0" | "k1" | "k2";
const HASH: Record<Key, string> = { k0: "00", k1: "10", k2: "11" };
const KEYS: Key[] = ["k0", "k1", "k2"];

const hostBits = (bits: string) => bits.replace(/0+$/, "");
const owns = (bits: string, key: Key) => HASH[key].startsWith(bits);

interface Shard {
  bits: string;
  presplit: number;
  rows: Key[];
}

export default defineSpec(
  "Lazy presplit",
  {
    description: "Shards at the initial depth create themselves; clients route straight to them; no key is ever owned twice.",
    messages: {
      Call: message<{ key: Key; bits: string; presplitTo: number }>({ name: "shardedCall", over: "DO RPC" }),
      Ok: message<{ key: Key }>({ name: "ok", over: "DO RPC", tone: "reply" }),
      Moved: message<{ key: Key; moved: string; presplit: number }>({ name: "moved", over: "DO RPC", tone: "refused" }),
      Stale: message<{ key: Key }>({ name: "stale", over: "DO RPC", tone: "refused" }),
      NotReady: message<{ key: Key }>({ name: "not ready", over: "DO RPC", tone: "refused" }),
    },
  },
  (spec) => {
    const { Call, Ok, Moved, Stale, NotReady } = spec.messages;
    const workers = spec.defineZone({ name: "Worker isolate", place: { column: 0 } });
    const objects = spec.defineZone({ name: "Durable Objects", description: "one per host bits", place: { column: 1 } });

    const Client = spec.defineActor({
      name: "Client",
      description: "Routes each key by the splits and presplit depth it knows (each isolate has its own).",
      zone: workers,
      initialState: () => ({ splits: [] as string[], presplitTo: 0, inFlight: null as Key | null, acked: [] as Key[], hops: 0 }),
      display: { icon: "code", status: (s) => `depth ${s.presplitTo}, ${s.splits.length} split(s) seen` },
      actions: {
        write: {
          label: "write a key",
          description: "Writes a key at the deepest node it knows owns it.",
          sources: ["src/sharded/client.ts#ShardedClient.route", "src/sharded/client.ts#ShardedClient.shardFor"],
          inputs: { key: spec.defineInput(String, { examples: () => KEYS, accepts: "key" }) },
          when: (s) => s.inFlight === null,
          sends: [Call],
          run(state, { key }, ctx) {
            const k = key as Key;
            state.inFlight = k;
            ctx.send(ctx.the(Objects), Call, { key: k, bits: shardFor(state, k), presplitTo: state.presplitTo });
          },
        },
      },
      transitions: {
        [Ok]: { description: "Acknowledged.", run: (s, msg) => void (s.acked.includes(msg.key) || s.acked.push(msg.key), (s.inFlight = null)) },
        [Moved]: {
          description: "Learns the split (and the depth), and calls again.",
          sources: ["src/sharded/client.ts#ShardedClient.learn"],
          sends: [Call],
          run(state, msg, ctx) {
            if (msg.presplit) state.presplitTo = msg.presplit;
            for (let i = state.presplitTo; i <= msg.moved.length; i++) if (!state.splits.includes(msg.moved.slice(0, i))) state.splits.push(msg.moved.slice(0, i));
            state.hops++;
            ctx.send(ctx.the(Objects), Call, { key: msg.key, bits: shardFor(state, msg.key), presplitTo: state.presplitTo });
          },
        },
        [Stale]: {
          description: "Its map outlived the storage: forgets it, depth included, and calls again from the root.",
          sources: ["src/sharded/client.ts#ShardedClient.forget"],
          sends: [Call],
          run(state, msg, ctx) {
            state.splits = [];
            state.presplitTo = 0;
            state.hops++;
            ctx.send(ctx.the(Objects), Call, { key: msg.key, bits: "", presplitTo: 0 });
          },
        },
        [NotReady]: { description: "A copy not here yet: gives up (the real client backs off and retries).", run: (s) => void (s.inFlight = null) },
      },
    });

    function shardFor(state: { splits: string[]; presplitTo: number }, key: Key): string {
      let bits = "";
      while (bits.length < state.presplitTo || state.splits.includes(bits)) bits += HASH[key][bits.length];
      return bits;
    }

    const Objects = spec.defineActor({
      name: "Shard objects",
      description: "The Durable Objects of one name, by host bits (a shard's bits without trailing zeros).",
      zone: objects,
      singleton: true,
      durable: ["objects", "initialDepth", "deleted"],
      initialState: () => ({ objects: {} as Record<string, Shard>, initialDepth: 1, deleted: 0, lost: false }),
      display: { icon: "database", status: (s) => Object.values(s.objects).map((o) => `${o.bits || "∅"}:[${o.rows.join(",")}]`).join(" ") || "none" },
      actions: {
        split: {
          label: "load split",
          description: "The shard holding k1 and k2 splits below the initial depth; the copy takes its parent's depth.",
          sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.shardedClaim"],
          when: (s) => Object.values(s.objects).some((o) => o.bits === "1"),
          run(state) {
            const o = Object.values(state.objects).find((x) => x.bits === "1")!;
            state.objects[hostBits("11")] = { bits: "11", presplit: o.presplit, rows: o.rows.filter((k) => owns("11", k)) };
            o.bits = "10";
            o.rows = o.rows.filter((k) => owns("10", k));
          },
        },
        deleteStorage: {
          label: "delete and reuse the name",
          description: "Every object's storage is deleted, and the name is used again with initialDepth 2. Clients still remember the old tree.",
          when: (s) => s.deleted < 1 && Object.keys(s.objects).length > 0,
          run(state) {
            state.objects = {};
            state.deleted++;
            state.initialDepth = 2; // reused with another initial depth
          },
        },
      },
      transitions: {
        [Call]: {
          description: "Serves the key if it hosts that shard; creates itself first if new and allowed (the root, or at the depth the client learned); else moved or stale.",
          // The new shard's call to the root is taken as atomic: what could come between is the
          // operator deleting the name's storage.
          sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.arriveOrCreate", "src/sharded/index.ts#ShardedObject.shardedTreeDepth", "src/sharded/index.ts#ShardedObject.arrive", "src/sharded/index.ts#ShardedObject.moved"],
          sends: [Ok, Moved, Stale, NotReady],
          run(state, msg, ctx) {
            const host = hostBits(msg.bits);
            const root = () => (state.objects[""] ??= { bits: "0".repeat(state.initialDepth), presplit: state.initialDepth, rows: [] });
            let o = state.objects[host];
            if (!o && host === "") o = root();
            if (!o) {
              if (!(msg.presplitTo > 0 && msg.bits.length <= msg.presplitTo)) return ctx.reply(NotReady, { key: msg.key });
              if (ASK_THE_ROOT && root().presplit !== msg.presplitTo) return ctx.reply(Stale, { key: msg.key });
              const bits = msg.bits.padEnd(msg.presplitTo, "0");
              o = state.objects[host] = { bits, presplit: bits.length, rows: [] };
            }
            if (o.bits === msg.bits) {
              if (!o.rows.includes(msg.key)) o.rows.push(msg.key);
              return ctx.reply(Ok, { key: msg.key });
            }
            if (o.bits.startsWith(msg.bits)) return ctx.reply(Moved, { key: msg.key, moved: o.bits.length > msg.bits.length ? o.bits.slice(0, -1) : msg.bits, presplit: o.presplit });
            ctx.reply(Stale, { key: msg.key });
          },
        },
      },
    });

    spec.defineFinalState({ description: "No call in flight.", when: (w) => w.quiet && w.all(Client).every((c) => c.state.inFlight === null) });

    spec.defineProperties({
      oneOwner: {
        description: "No key is owned by two shards that exist.",
        always: (w) => KEYS.every((k) => Object.values(w.the(Objects).state.objects).filter((o) => owns(o.bits, k)).length <= 1),
      },
      sameDepthEverywhere: {
        description: "Every shard of the tree records the same presplit depth.",
        always: (w) => new Set(Object.values(w.the(Objects).state.objects).map((o) => o.presplit)).size <= 1,
      },
      ackedWritesAreThere: {
        description: "A key acknowledged since the storage was last deleted is in the shard that owns it.",
        always: (w) => w.the(Objects).state.deleted > 0 || w.all(Client).every((c) => c.state.acked.every((k) => Object.values(w.the(Objects).state.objects).some((o) => owns(o.bits, k) && o.rows.includes(k)))),
      },
    });

    spec.defineScenario({
      name: "Straight to depth N after one answer",
      description: "The first write reaches the root, which answers moved with the depth; the next write of another key goes straight to its shard, which creates itself.",
      run(s) {
        const c = s.spawn(Client, "Isolate A");
        s.act(c, "write", { key: "k0" });
        s.settle();
        s.act(c, "write", { key: "k2" });
        s.settle();
      },
    });

    return { Client, Objects };
  },
);
