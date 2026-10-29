import { defineSpec, message } from "@leostera/feynman";

export default defineSpec(
  "Splitting and routing",
  {
    description: "How stale clients retry after a permanent hash-prefix split, and how a child inherits rows before the parent releases them.",
    messages: {
      Call: message<{ key: "left" | "right"; value: number }>({ name: "call" }),
      Moved: message<{ prefix: string }>({ name: "moved", tone: "refused" }),
      HandOut: message<{ prefix: string }>({ name: "hand out rows" }),
      Rows: message<{ prefix: string; value: number }>({ name: "inherited rows", tone: "reply" }),
      Release: message<{ prefix: string }>({ name: "release parent rows" }),
      Done: message<{ value: number }>({ name: "call completed", tone: "reply" }),
    },
  },
  (spec) => {
    const { Call, Moved, HandOut, Rows, Release, Done } = spec.messages;
    const zone = spec.defineZone({ name: "Durable Objects", description: "A root node may split into children; clients cache the split tree." });

    const Client = spec.defineActor({
      name: "Client",
      zone,
      singleton: true,
      initialState: () => ({ knowsSplit: false, pending: null as ("left" | "right") | null, result: null as number | null }),
      display: { icon: "browser", status: (s) => (s.pending ? `waiting on ${s.pending}` : s.knowsSplit ? "knows the split" : "stale split map") },
      actions: {
        call: {
          label: "Call a key",
          description: "Sends a key through the locally known route.",
          sources: ["src/client.ts#SplittableClient"],
          inputs: { key: spec.defineInput(String, { examples: ["left", "right"] }) },
          sends: [Call],
          run(state, { key }, ctx) {
            state.pending = key as "left" | "right";
            ctx.send(ctx.the(Root), Call, { key: state.pending, value: 1 });
          },
        },
      },
      transitions: {
        [Moved]: {
          description: "Learns the split and retries the original call at its child.",
          sources: ["src/client.ts#SplittableClient"],
          sends: [Call],
          run(state, _msg, ctx) {
            state.knowsSplit = true;
            ctx.send(ctx.the(Leaf), Call, { key: state.pending!, value: 1 });
          },
        },
        [Done]: { description: "Returns the successful leaf result.", sources: ["src/client.ts#SplittableClient"], run: (s, msg) => { s.result = msg.value; s.pending = null; } },
      },
    });

    const Root = spec.defineActor({
      name: "Root",
      zone,
      singleton: true,
      initialState: () => ({ split: false, rows: { left: 0, right: 0 }, childHasRows: false, released: false }),
      display: { icon: "database", status: (s) => (s.split ? s.released ? "split; parent rows released" : "split; handing off rows" : "leaf") },
      actions: {
        split: { label: "Split this node", description: "Creates children and freezes this leaf.", when: (s) => !s.split, sources: ["src/index.ts#SplittableObject"], run: (s) => { s.split = true; } },
      },
      transitions: {
        [Call]: {
          description: "Accepts calls while a leaf; after splitting, tells stale callers to retry.",
          sources: ["src/index.ts#SplittableObject"],
          sends: [Moved, Done],
          run(state, msg, ctx) {
            if (state.split) return ctx.reply(Moved, { prefix: "" });
            state.rows[msg.key] += msg.value;
            ctx.reply(Done, { value: state.rows[msg.key] });
          },
        },
        [HandOut]: {
          description: "Returns the child's hash range only after the parent is frozen.",
          sources: ["src/index.ts#SplittableObject"],
          sends: [Rows],
          run(state, msg, ctx) {
            if (state.split) ctx.reply(Rows, { prefix: msg.prefix, value: state.rows.left });
          },
        },
        [Release]: { description: "Drops the parent's copy after child inheritance is complete.", sources: ["src/index.ts#SplittableObject"], run: (s) => { if (s.childHasRows) s.released = true; } },
      },
    });

    const Leaf = spec.defineActor({
      name: "Leaf",
      zone,
      singleton: true,
      initialState: () => ({ value: 0, inherited: false }),
      display: { icon: "database", status: (s) => s.inherited ? `inherited; value ${s.value}` : `value ${s.value}` },
      actions: {
        inherit: {
          label: "Inherit rows",
          description: "Asks the split parent for this child's rows.",
          sources: ["src/index.ts#SplittableObject"],
          when: (s) => !s.inherited,
          sends: [HandOut],
          run: (_s, _inputs, ctx) => ctx.send(ctx.the(Root), HandOut, { prefix: "0" }),
        },
      },
      transitions: {
        [Rows]: {
          description: "Stores inherited rows before notifying the parent that it can release them.",
          sources: ["src/index.ts#SplittableObject"],
          sends: [Release],
          run(state, msg, ctx) {
            state.value = msg.value;
            state.inherited = true;
            ctx.send(ctx.the(Root), Release, { prefix: msg.prefix });
          },
        },
        [Call]: {
          description: "Handles a key after it has reached its child leaf.",
          sources: ["src/index.ts#SplittableObject"],
          sends: [Done],
          run(state, msg, ctx) {
            state.value += msg.value;
            ctx.reply(Done, { value: state.value });
          },
        },
      },
    });

    spec.defineProperties({
      movedRouteIsRemembered: {
        description: "After a node tells a client it moved, the client remembers the split and retries at a child.",
        always: (w) => !w.the(Client).state.knowsSplit || w.the(Client).state.pending !== null || w.the(Client).state.result !== null,
      },
      parentDoesNotReleaseEarly: {
        description: "The parent never releases rows before the child has inherited them.",
        always: (w) => !w.the(Root).state.released || w.the(Leaf).state.inherited,
      },
    });

    spec.defineScenario({
      name: "A stale route catches up",
      description: "The node splits behind the client; the moved response teaches it the tree and retry succeeds.",
      run(s) {
        s.act(s.the(Root), "split");
        s.act(s.the(Client), "call", { key: "left" });
        s.settle();
      },
    });
    spec.defineScenario({
      name: "Inherit before releasing",
      description: "A child asks for rows, stores them, and only then lets the parent delete its copy.",
      run(s) {
        s.act(s.the(Root), "split");
        s.act(s.the(Leaf), "inherit");
        s.settle();
      },
    });
    return { Client, Root, Leaf };
  },
);
