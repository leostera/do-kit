// How a shard splits with one storage copy.
//
// The Durable Object that hosts a shard keeps the left half of its range (it is also the host of
// the left child: hosts are named by the shard's bits without trailing zeros) and copies its
// storage once into a new object for the right half. While the copy is made, writes to the right
// half wait on the original; reads keep being served there. Once the copy reports it is ready
// the original stops owning the right half (answers "moved"), and only then tells the copy to
// start serving. A split that does not finish in time is abandoned, and the writes it held go
// ahead on the original. Every attempt has an epoch, written into the storage before the
// snapshot, so the copy carries the epoch of the attempt that made it: a copy from an abandoned
// attempt is never claimed or activated. "Restore" stands for the snapshot restore (or the row
// copy where snapshots are not available) and "copy ready" for a successful claim.
import { defineSpec, message } from "@leostera/feynman";

type Key = "L" | "R";
const KEYS: Key[] = ["L", "R"];

export default defineSpec(
  "Snapshot split",
  {
    description: "How a shard hands the right half of its keys to a new object with a single storage copy, without losing or double-serving a write.",
    messages: {
      Write: message<{ key: Key }>({ name: "write" }),
      Read: message<{ key: Key }>({ name: "read" }),
      Done: message<{ key: Key; op: "write" | "read"; value: number }>({ name: "ok", tone: "reply" }),
      Moved: message<{ key: Key }>({ name: "moved", tone: "refused" }),
      NotReady: message<{ key: Key }>({ name: "not ready", tone: "refused" }),
      Restore: message<{ epoch: number; right: number }>({ name: "restore from snapshot", over: "rpc" }),
      Restored: message<{ epoch: number }>({ name: "copy ready", tone: "reply" }),
      Activate: message<{ epoch: number }>({ name: "activate" }),
      Activated: message<{ epoch: number }>({ name: "activated", tone: "reply" }),
    },
  },
  (spec) => {
    const { Write, Read, Done, Moved, NotReady, Restore, Restored, Activate, Activated } = spec.messages;
    const callers = spec.defineZone({ name: "Workers", description: "route keys through the splits they know" });
    const objects = spec.defineZone({ name: "Durable Objects", description: "one shard per object" });

    spec.defineLimit({ name: "Split timeout", value: 5, unit: "minutes" });

    const Client = spec.defineActor({
      name: "Client",
      zone: callers,
      singleton: true,
      initialState: () => ({
        knowsSplit: false,
        pending: null as null | { key: Key; op: "write" | "read" },
        acked: { L: 0, R: 0 } as Record<Key, number>,
        writes: { L: 0, R: 0 } as Record<Key, number>,
        staleRead: false,
      }),
      display: {
        icon: "code",
        status: (s) => (s.pending ? `${s.pending.op} ${s.pending.key}` : s.knowsSplit ? "knows the split" : "idle"),
        mood: (s) => (s.staleRead ? "bad" : "ok"),
      },
      actions: {
        write: {
          label: "Write a key",
          description: "Increments a key on the object it believes owns it.",
          sources: ["src/sharded/client.ts#ShardedClient.call", "src/sharded/client.ts#ShardedClient.route"],
          inputs: { key: spec.defineInput(String, { examples: KEYS }) },
          when: (s) => s.pending === null,
          sends: [Write],
          run(s, { key }, ctx) {
            const k = key as Key;
            s.pending = { key: k, op: "write" };
            s.writes[k]++;
            ctx.send(k === "R" && s.knowsSplit ? ctx.the(Sibling) : ctx.the(Original), Write, { key: k });
          },
        },
        read: {
          label: "Read a key",
          description: "Reads a key from the object it believes owns it.",
          sources: ["src/sharded/client.ts#ShardedClient.call", "src/sharded/client.ts#ShardedClient.route"],
          inputs: { key: spec.defineInput(String, { examples: KEYS }) },
          when: (s) => s.pending === null,
          sends: [Read],
          run(s, { key }, ctx) {
            const k = key as Key;
            s.pending = { key: k, op: "read" };
            ctx.send(k === "R" && s.knowsSplit ? ctx.the(Sibling) : ctx.the(Original), Read, { key: k });
          },
        },
      },
      transitions: {
        [Done]: {
          description: "Gets the answer.",
          sources: ["src/sharded/client.ts#ShardedClient.route"],
          run(s, msg) {
            if (msg.op === "read" && msg.value < s.acked[msg.key]) s.staleRead = true;
            if (msg.op === "write") s.acked[msg.key] = Math.max(s.acked[msg.key], msg.value);
            s.pending = null;
          },
        },
        [Moved]: {
          description: "Learns the split and sends the call to the new owner.",
          sources: ["src/sharded/client.ts#ShardedClient.route", "src/sharded/client.ts#ShardedClient.learn"],
          sends: [Write, Read],
          run(s, msg, ctx) {
            s.knowsSplit = true;
            if (!s.pending || s.pending.key !== msg.key) return;
            const target = msg.key === "R" ? ctx.the(Sibling) : ctx.the(Original);
            if (s.pending.op === "write") ctx.send(target, Write, { key: msg.key });
            else ctx.send(target, Read, { key: msg.key });
          },
        },
        [NotReady]: {
          description: "The new owner is not serving yet; tries again shortly.",
          sources: ["src/sharded/client.ts#ShardedClient.route", "src/sharded/client.ts#Backoff.next"],
          starts: ["retry"],
          run: (_s, _msg, ctx) => ctx.start("retry"),
        },
        retry: {
          description: "Sends the waiting call again.",
          sources: ["src/sharded/client.ts#ShardedClient.route"],
          after: "a few milliseconds",
          sends: [Write, Read],
          run(s, ctx) {
            if (!s.pending) return;
            const target = s.pending.key === "R" && s.knowsSplit ? ctx.the(Sibling) : ctx.the(Original);
            if (s.pending.op === "write") ctx.send(target, Write, { key: s.pending.key });
            else ctx.send(target, Read, { key: s.pending.key });
          },
        },
      },
    });

    const Original = spec.defineActor({
      name: "Original",
      description: "The object hosting the shard; after the split it hosts the left half.",
      zone: objects,
      singleton: true,
      initialState: () => ({
        owns: "both" as "both" | "left",
        rows: { L: 0, R: 0 } as Record<Key, number>,
        epoch: 0,
        splitting: null as number | null,
        held: 0,
        activated: false,
      }),
      durable: ["owns", "rows", "epoch", "activated"],
      display: {
        icon: "database",
        status: (s) => (s.splitting !== null ? `splitting (epoch ${s.splitting}), ${s.held} write(s) held` : s.owns === "both" ? "shard: L and R" : s.activated ? "shard: L" : "shard: L, activating copy"),
        mood: (s) => (s.splitting !== null ? "busy" : "ok"),
        rows: (s) => KEYS.map((k) => ({ group: "rows", label: k, detail: String(s.rows[k]), mood: k === "R" && s.owns === "left" ? "off" : "info" })),
      },
      actions: {
        split: {
          label: "Split",
          description: "Holds writes to the right half, snapshots its storage and asks the new object to restore from it.",
          sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.copyAndClaim", "src/sharded/index.ts#ShardedObject.copyTo"],
          when: (s) => s.owns === "both" && s.splitting === null && s.epoch < 2,
          sends: [Restore],
          starts: ["splitTimeout"],
          run(s, _i, ctx) {
            s.epoch++;
            s.splitting = s.epoch;
            ctx.send(ctx.the(Sibling), Restore, { epoch: s.epoch, right: s.rows.R });
            ctx.start("splitTimeout");
          },
        },
      },
      transitions: {
        [Write]: {
          description: "Applies a write it owns; holds right-half writes while splitting; answers moved for keys it gave away.",
          sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.update", "src/sharded/index.ts#ShardedObject.holding", "src/sharded/index.ts#ShardedObject.waitForHold"],
          sends: [Done, Moved],
          starts: ["holdWait"],
          run(s, msg, ctx) {
            if (msg.key === "R" && s.owns === "left") return ctx.reply(Moved, { key: msg.key });
            if (msg.key === "R" && s.splitting !== null) {
              s.held++;
              return void ctx.start("holdWait");
            }
            s.rows[msg.key]++;
            ctx.reply(Done, { key: msg.key, op: "write", value: s.rows[msg.key] });
          },
        },
        [Read]: {
          description: "Serves reads of keys it owns, including the right half while splitting.",
          sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.read"],
          sends: [Done, Moved],
          run(s, msg, ctx) {
            if (msg.key === "R" && s.owns === "left") return ctx.reply(Moved, { key: msg.key });
            ctx.reply(Done, { key: msg.key, op: "read", value: s.rows[msg.key] });
          },
        },
        [Restored]: {
          description: "The copy is ready: stops owning the right half, turns held writes away, then activates the copy.",
          sources: ["src/sharded/index.ts#ShardedObject.split", "src/sharded/index.ts#ShardedObject.activateSibling"],
          sends: [Moved, Activate],
          starts: ["activateRetry"],
          run(s, msg, ctx) {
            if (s.splitting !== msg.epoch) return; // an abandoned attempt
            s.owns = "left";
            s.splitting = null;
            ctx.cancel("splitTimeout");
            for (; s.held > 0; s.held--) ctx.send(ctx.the(Client), Moved, { key: "R" });
            ctx.send(ctx.the(Sibling), Activate, { epoch: msg.epoch });
            ctx.start("activateRetry");
          },
        },
        [Activated]: {
          description: "The copy serves the right half.",
          sources: ["src/sharded/index.ts#ShardedObject.activateSibling"],
          run(s, _msg, ctx) {
            s.activated = true;
            ctx.cancel("activateRetry");
          },
        },
        holdWait: {
          description: "Held writes waited holdWaitMs: they are turned away as not ready, unapplied, and the client retries later instead of queueing here.",
          sources: ["src/sharded/index.ts#ShardedObject.waitForHold", "src/sharded/index.ts#ShardedObject.shardedCall"],
          after: "2 seconds",
          sends: [NotReady],
          run(s, ctx) {
            if (s.splitting === null) return;
            for (; s.held > 0; s.held--) ctx.send(ctx.the(Client), NotReady, { key: "R" });
          },
        },
        splitTimeout: {
          description: "The copy did not get ready in time: abandons the split and applies the held writes.",
          sources: ["src/sharded/index.ts#ShardedObject.split"],
          after: "5 minutes",
          sends: [Done],
          run(s, ctx) {
            if (s.splitting === null) return;
            s.splitting = null;
            for (; s.held > 0; s.held--) {
              s.rows.R++;
              ctx.send(ctx.the(Client), Done, { key: "R", op: "write", value: s.rows.R });
            }
          },
        },
        activateRetry: {
          description: "Activates the copy again until it confirms (the alarm's job).",
          sources: ["src/sharded/index.ts#ShardedObject.alarm", "src/sharded/index.ts#ShardedObject.activateSibling"],
          after: "1 second",
          sends: [Activate],
          run(s, ctx) {
            if (s.owns === "left" && !s.activated) ctx.send(ctx.the(Sibling), Activate, { epoch: s.epoch });
          },
        },
      },
    });

    const Sibling = spec.defineActor({
      name: "Sibling",
      description: "The new object for the right half, restored from the original's snapshot.",
      zone: objects,
      singleton: true,
      initialState: () => ({ copyEpoch: null as number | null, right: 0, active: false }),
      durable: ["copyEpoch", "right", "active"],
      display: {
        icon: "database",
        status: (s) => (s.active ? "shard: R" : s.copyEpoch !== null ? `copy of epoch ${s.copyEpoch}, waiting` : "not created"),
        mood: (s) => (s.active ? "ok" : s.copyEpoch !== null ? "busy" : "off"),
        rows: (s) => [{ group: "rows", label: "R", detail: String(s.right), mood: s.active ? "info" : "off" }],
      },
      transitions: {
        [Restore]: {
          description: "Restores its storage from the snapshot, drops what is not its range, and says it is ready (not serving yet).",
          sources: ["src/sharded/index.ts#ShardedObject.shardedRestore", "src/sharded/index.ts#ShardedObject.shardedReceive", "src/sharded/index.ts#ShardedObject.shardedClaim"],
          sends: [Restored],
          run(s, msg, ctx) {
            if (s.active) return; // an active shard is never overwritten
            if (s.copyEpoch !== null && msg.epoch < s.copyEpoch) return; // a late restore from an abandoned attempt
            s.copyEpoch = msg.epoch;
            s.right = msg.right;
            ctx.reply(Restored, { epoch: msg.epoch });
          },
        },
        [Activate]: {
          description: "Starts serving the right half, if its copy is from this attempt.",
          sources: ["src/sharded/index.ts#ShardedObject.shardedActivate"],
          sends: [Activated],
          run(s, msg, ctx) {
            if (s.copyEpoch !== msg.epoch) return;
            s.active = true;
            ctx.reply(Activated, { epoch: msg.epoch });
          },
        },
        [Write]: {
          description: "Applies writes once active; until then asks the caller to retry.",
          sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.arrive"],
          sends: [Done, NotReady],
          run(s, msg, ctx) {
            if (!s.active) return ctx.reply(NotReady, { key: msg.key });
            s.right++;
            ctx.reply(Done, { key: msg.key, op: "write", value: s.right });
          },
        },
        [Read]: {
          description: "Serves reads once active.",
          sources: ["src/sharded/index.ts#ShardedObject.shardedCall", "src/sharded/index.ts#ShardedObject.arrive"],
          sends: [Done, NotReady],
          run(s, msg, ctx) {
            if (!s.active) return ctx.reply(NotReady, { key: msg.key });
            ctx.reply(Done, { key: msg.key, op: "read", value: s.right });
          },
        },
      },
    });

    spec.defineFinalState({ description: "Nothing is in flight. A call whose message was lost stays waiting here; in the code the caller gets an RPC error and retries.", when: (w) => w.quiet });

    spec.defineProperties({
      noLostWrite: {
        description: "The owner of a key always holds at least every write acknowledged for it.",
        always: (w) => {
          const o = w.the(Original).state;
          const c = w.the(Client).state;
          const right = o.owns === "both" ? o.rows.R : w.the(Sibling).state.right;
          return o.rows.L >= c.acked.L && right >= c.acked.R;
        },
      },
      noDoubleApply: {
        description: "No write is applied twice: an owner never holds more than the writes issued.",
        always: (w) => {
          const o = w.the(Original).state;
          const c = w.the(Client).state;
          const right = o.owns === "both" ? o.rows.R : w.the(Sibling).state.right;
          return o.rows.L <= c.writes.L && right <= c.writes.R;
        },
      },
      oneOwner: {
        description: "The copy never serves while the original still owns the right half.",
        always: (w) => !(w.the(Sibling).state.active && w.the(Original).state.owns === "both"),
      },
      noStaleRead: {
        description: "A read never returns less than a write already acknowledged.",
        always: (w) => !w.the(Client).state.staleRead,
      },
    });

    spec.defineScenario({
      name: "Split while writing the right half",
      description: "A right-half write arrives during the copy, is held, turned away when the copy is ready, and lands on the new object once it is active.",
      run(s) {
        const client = s.the(Client);
        const original = s.the(Original);
        s.act(client, "write", { key: "R" });
        s.settle();
        s.act(original, "split");
        s.act(client, "write", { key: "R" });
        s.deliver(original, "Write");
        s.settle();
        s.act(client, "read", { key: "R" });
        s.settle();
      },
    });

    spec.defineScenario({
      name: "The left half never stops",
      description: "Writes to the left half are served throughout the split.",
      run(s) {
        const client = s.the(Client);
        s.act(s.the(Original), "split");
        s.act(client, "write", { key: "L" });
        s.deliver(s.the(Original), "Write");
        s.deliver(client, "Done");
        s.settle();
      },
    });

    spec.defineScenario({
      name: "An abandoned split",
      description: "The copy is lost; the split times out and the held write is applied on the original. A later copy from that attempt is never activated.",
      run(s) {
        const client = s.the(Client);
        const original = s.the(Original);
        s.act(original, "split");
        s.drop(s.the(Sibling), "Restore");
        s.act(client, "write", { key: "R" });
        s.deliver(original, "Write");
        s.fire(original, "splitTimeout");
        s.settle();
      },
    });

    return { Client, Original, Sibling };
  },
);
