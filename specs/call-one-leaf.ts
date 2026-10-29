// A stale client may not know that a split happened. A leaf checks every key before invoking a
// method, so callOneLeaf can discover a split and refuse the now-cross-leaf batch without having
// applied any part of it.
import { defineSpec, message } from "@leostera/feynman";

type Key = "L" | "R";

export default defineSpec(
  "One-leaf calls",
  {
    description: "How callOneLeaf refuses a batch that spans leaves, even when the client learns about the split only after reaching the old owner.",
    messages: {
      Call: message<{ keys: Key[] }>({ name: "call" }),
      Moved: message<{ keys: Key[] }>({ name: "moved", tone: "refused" }),
    },
  },
  (spec) => {
    const { Call, Moved } = spec.messages;
    const workers = spec.defineZone({ name: "Workers", description: "group keys by the splits they know" });
    const objects = spec.defineZone({ name: "Durable Objects", description: "check ownership before running a method" });

    const Client = spec.defineActor({
      name: "Client",
      zone: workers,
      singleton: true,
      initialState: () => ({ knowsSplit: false, rejected: false, callsSent: 0 }),
      display: {
        icon: "code",
        status: (s) => (s.rejected ? "refused: spans leaves" : s.knowsSplit ? "knows the split" : "stale map"),
        mood: (s) => (s.rejected ? "warn" : "ok"),
      },
      actions: {
        callOneLeaf: {
          label: "Call one leaf",
          description: "Requires every key to stay on one leaf.",
          sources: ["src/client.ts:95-116"],
          sends: [Call],
          run(s, _inputs, ctx) {
            s.callsSent++;
            // Before learning the split, both keys appear to belong to the root.
            ctx.send(ctx.the(Original), Call, { keys: ["L", "R"] });
          },
        },
      },
      transitions: {
        [Moved]: {
          description: "Learns the split, then rejects the batch before retrying any key.",
          sources: ["src/client.ts:91-116", "src/client.ts:124-126"],
          run(s, msg) {
            s.knowsSplit = true;
            if (msg.keys.includes("L") && msg.keys.includes("R")) s.rejected = true;
          },
        },
      },
    });

    const Original = spec.defineActor({
      name: "Original",
      description: "After splitting, this object hosts only the left half but may still be reached by a stale client.",
      zone: objects,
      singleton: true,
      initialState: () => ({ applied: 0 }),
      display: {
        icon: "database",
        status: (s) => `left leaf; ${s.applied} applied`,
        mood: () => "ok",
      },
      transitions: {
        [Call]: {
          description: "Detects that the batch no longer belongs entirely to this leaf and answers moved before invoking the method.",
          sources: ["src/index.ts#SplittableObject.splittableCall"],
          sends: [Moved],
          run(_s, msg, ctx) {
            ctx.reply(Moved, msg);
          },
        },
      },
    });

    spec.defineFinalState({
      description: "The batch was refused and no messages remain in flight.",
      when: (w) => w.quiet && w.the(Client).state.rejected,
    });

    spec.defineProperties({
      rejectedBatchHasNoEffects: {
        description: "A callOneLeaf request that spans the split is rejected without applying either key.",
        always: (w) => !w.the(Client).state.rejected || w.the(Original).state.applied === 0,
      },
      learnsFromMoved: {
        description: "A moved answer teaches the client that the root has split.",
        always: (w) => !w.the(Client).state.rejected || w.the(Client).state.knowsSplit,
      },
    });

    spec.defineScenario({
      name: "Refuse a cross-leaf update",
      description: "The stale client sends both keys to the original; it answers moved without running the method, and the client refuses before retrying either key.",
      run(s) {
        s.act(s.the(Client), "callOneLeaf");
        s.settle();
      },
    });

    return { Client, Original };
  },
);
