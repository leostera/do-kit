// A snapshot leaves the original's old rows in place until background cleanup runs. Scans must
// expose only rows in each leaf's owned hash range, not every row still physically stored there.
import { defineSpec, message } from "@leostera/feynman";

type Key = "L" | "R";

export default defineSpec(
  "Scan visibility",
  {
    description: "How scans avoid duplicate rows while a split's old copy is being cleaned up in the background.",
    messages: {
      Scan: message<{}>({ name: "scan" }),
      Rows: message<{ keys: Key[] }>({ name: "rows", tone: "reply" }),
    },
  },
  (spec) => {
    const { Scan, Rows } = spec.messages;
    const callers = spec.defineZone({ name: "Workers", description: "collect one result from each leaf" });
    const objects = spec.defineZone({ name: "Durable Objects", description: "return rows in each leaf's owned range" });

    const Client = spec.defineActor({
      name: "Client",
      zone: callers,
      singleton: true,
      initialState: () => ({ started: false, pending: 0, seen: { L: 0, R: 0 } as Record<Key, number> }),
      display: {
        icon: "code",
        status: (s) => (s.pending ? `waiting for ${s.pending} leaf result(s)` : s.started ? "scan complete" : "idle"),
        mood: (s) => (s.pending ? "busy" : "ok"),
      },
      actions: {
        scan: {
          label: "Scan all leaves",
          description: "Asks both leaves for their owned rows.",
          when: (s) => !s.started,
          sources: ["src/client.ts:142-160"],
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
        [Rows]: {
          description: "Adds this leaf's rows to the scan result.",
          sources: ["src/client.ts:142-160"],
          run(s, msg) {
            for (const key of msg.keys) s.seen[key]++;
            s.pending--;
          },
        },
      },
    });

    const Original = spec.defineActor({
      name: "Original",
      description: "The left leaf still physically stores the right rows until cleanup, but no longer owns them.",
      zone: objects,
      singleton: true,
      initialState: () => ({ stored: { L: true, R: true } }),
      display: {
        icon: "database",
        status: (s) => `left leaf; ${s.stored.R ? "old right rows await cleanup" : "clean"}`,
        mood: (s) => (s.stored.R ? "info" : "ok"),
        rows: (s) => [
          { group: "stored rows", label: "L", detail: "owned", mood: "info" },
          ...(s.stored.R ? [{ group: "stored rows", label: "R", detail: "not owned; cleanup pending", mood: "off" as const }] : []),
        ],
      },
      transitions: {
        [Scan]: {
          description: "Returns only keys in its left-half hash range, even before old rows are deleted.",
          sources: ["src/index.ts#SplittableObject.splittableScan", "src/index.ts#SplittableObject.allRows"],
          sends: [Rows],
          run(_s, _msg, ctx) {
            ctx.reply(Rows, { keys: ["L"] });
          },
        },
      },
      actions: {
        cleanRightRows: {
          label: "Run cleanup",
          description: "Deletes the old right-half rows from the original after ownership has moved.",
          when: (s) => s.stored.R,
          sources: ["src/index.ts#SplittableObject.alarm"],
          run(s) {
            s.stored.R = false;
          },
        },
      },
    });

    const Sibling = spec.defineActor({
      name: "Sibling",
      description: "The right leaf owns the rows copied from the original.",
      zone: objects,
      singleton: true,
      initialState: () => ({ stored: true }),
      display: {
        icon: "database",
        status: () => "right leaf",
        mood: () => "ok",
        rows: () => [{ group: "stored rows", label: "R", detail: "owned", mood: "info" }],
      },
      transitions: {
        [Scan]: {
          description: "Returns the keys in its right-half hash range.",
          sources: ["src/index.ts#SplittableObject.splittableScan", "src/index.ts#SplittableObject.allRows"],
          sends: [Rows],
          run(s, _msg, ctx) {
            ctx.reply(Rows, { keys: s.stored ? ["R"] : [] });
          },
        },
      },
    });

    spec.defineFinalState({
      description: "The scan has no leaf responses in flight; incomplete scans are outside this visibility model.",
      when: (w) => w.quiet,
    });

    spec.defineProperties({
      eachOwnedKeyAppearsOnce: {
        description: "Once a scan has both results, each key appears exactly once, whether or not cleanup has deleted the old copy.",
        always: (w) => {
          const c = w.the(Client).state;
          return !c.started || c.pending > 0 || (c.seen.L === 1 && c.seen.R === 1);
        },
      },
      cleanupDoesNotChangeScan: {
        description: "Deleting unowned physical rows does not change which keys the scan returns.",
        always: (w) => !w.the(Client).state.started || w.the(Client).state.pending > 0 || !w.quiet || w.the(Client).state.seen.L + w.the(Client).state.seen.R === 2,
      },
    });

    spec.defineScenario({
      name: "Scan before cleanup",
      description: "The original still stores a stale copy of R, but its range-filtered scan returns only L; the sibling returns R.",
      run(s) {
        s.act(s.the(Client), "scan");
        s.settle();
      },
    });

    spec.defineScenario({
      name: "Scan after cleanup",
      description: "Cleanup removes the original's stale R row; the scan still returns exactly L and R from their owners.",
      run(s) {
        s.act(s.the(Original), "cleanRightRows");
        s.act(s.the(Client), "scan");
        s.settle();
      },
    });

    return { Client, Original, Sibling };
  },
);
