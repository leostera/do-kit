// Partial outcomes: a call over several shards is one call per shard, and each commits on its own.
// When one shard's call fails, the others may have committed, and so may the one that failed: an
// error can come after its transaction (an answer lost on the way back, a method that throws once
// its write is in). The caller must learn what it can, and be able to retry the rest safely.
//
//   - settledOutcomes: the client waits for every shard's answer and reports each: committed, or
//     unknown (it failed, and may have committed). Without it (Promise.all) the first failure
//     rejects the call and the answers of the shards that committed are dropped.
//   - operationIds: a write carries an id its shard keeps with the write; the same id again
//     answers the first outcome. Without it, retrying a write whose outcome is unknown, made
//     only if the row is new, conflicts with itself.
//
// One write of a new row on each of two shards, then a retry of
// every shard not reported committed. At most one fault: a shard's call fails before or after
// it commits.
import { atMost, budget, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, spend } from "@leostera/feynman";

const faults = budget("faults", 1);

const settledOutcomes = defineRule("settledOutcomes", { description: "The client waits for every shard and reports each shard's outcome: committed, or unknown." });
const operationIds = defineRule("operationIds", { description: "A write carries an operation id; its shard answers a repeat with the first outcome." });

const Write = defineMessage("Write", { op: Number }, { name: "write, if the row is new", over: "DO RPC" });
const Ok = defineMessage("Ok", { shard: Number }, { name: "committed", over: "DO RPC", tone: "reply" });
const Failed = defineMessage("Failed", { shard: Number }, { name: "error", over: "DO RPC", tone: "refused" });
const Conflict = defineMessage("Conflict", { shard: Number }, { name: "the row exists", over: "DO RPC", tone: "refused" });

const workers = defineZone({ name: "Worker isolate", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

type Answer = "ok" | "failed" | "conflict";
/** What the client tells its caller about a shard: its write committed, may have, or nothing. */
type Reported = "committed" | "unknown" | null;
interface ClientState {
  phase: "idle" | "sent" | "reported" | "retried";
  /** Every answer that arrived, by shard, the retry's included. */
  answers: (Answer | null)[];
  /** The first call's report, by shard. */
  report: Reported[];
  /** Answers to the retry. */
  retried: (Answer | null)[];
}

export const Client = defineActor({
  name: "Client",
  description: "Writes a new row to each shard in one call, reports the outcome, then retries the shards not reported committed.",
  zone: workers,
  singleton: true,
  initialState: (): ClientState => ({ phase: "idle", answers: [null, null], report: [null, null], retried: [null, null] }),
  display: { icon: "code", status: (s) => `${s.phase}; report ${s.report.map((r) => r ?? "-").join(", ")}` },
  actions: {
    write: {
      with: [atMost(1)],
      label: "write",
      description: "Sends each shard its write.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => s.phase === "idle",
      sends: [Write],
      run(s, _, ctx) {
        s.phase = "sent";
        for (const shard of shards()) ctx.send(ctx.the(shard), Write, { op: 1 });
      },
    },
    retry: {
      with: [atMost(1)],
      label: "retry",
      description: "Writes again to every shard the report did not say committed.",
      sources: ["src/sharded/client.ts#ShardedClient.route"],
      when: (s) => s.phase === "reported" && s.report.some((r) => r !== "committed"),
      sends: [Write],
      run(s, _, ctx) {
        s.phase = "retried";
        shards().forEach((shard, i) => s.report[i] !== "committed" && ctx.send(ctx.the(shard), Write, { op: 1 }));
      },
    },
  },
  transitions: {
    ok: { on: Ok, description: "A shard committed.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s, m) => answer(s, m.shard, "ok") },
    failed: { on: Failed, description: "A shard's call threw.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s, m) => answer(s, m.shard, "failed") },
    conflict: { on: Conflict, description: "A shard already had the row.", sources: ["src/sharded/client.ts#ShardedClient.route"], run: (s, m) => answer(s, m.shard, "conflict") },
  },
});

/** Records an answer; and, for the first call, reports once it can. */
function answer(s: ClientState, shard: number, a: Answer): void {
  if (s.phase === "retried") {
    s.retried[shard] = a;
    return;
  }
  s.answers[shard] = a;
  if (s.phase !== "sent") return;
  if (settledOutcomes.on) {
    // Promise.allSettled: every answer, then a report of each.
    if (s.answers.some((x) => x === null)) return;
    s.report = s.answers.map((x) => (x === "ok" ? "committed" : "unknown"));
    s.phase = "reported";
    return;
  }
  // Promise.all: the first failure rejects the call, with nothing of the other shards.
  if (a !== "ok") {
    s.report = [null, null];
    s.phase = "reported";
  } else if (s.answers.every((x) => x === "ok")) {
    s.report = ["committed", "committed"];
    s.phase = "reported";
  }
}

function makeShard(name: string, shard: number) {
  return defineActor({
  name,
  singleton: true,
  description: "Writes the row if it is new, in one transaction; its call may fail before or after the commit.",
  zone: objects,
  initialState: () => ({
    row: false,
    /** Operation ids it committed, kept with the write. */
    ops: [] as number[],
    /** A fault armed for the next call: it throws before its commit, or after. */
    breaks: null as "before" | "after" | null,
    calls: 0,
  }),
  display: { icon: "database", status: (s) => (s.row ? "has the row" : "empty"), mood: (s) => (s.breaks ? "bad" : "ok") },
  transitions: {
    write: {
      on: Write,
      description: "Writes the row if it has none; answers a repeat of a committed operation with its outcome, when writes carry ids.",
      sources: ["src/table/index.ts#ShardedTable.putRow", "src/sharded/index.ts#ShardedObject.shardedCall"],
      sends: [Ok, Failed, Conflict],
      run(s, m, ctx) {
        s.calls++;
        const breaks = s.breaks;
        s.breaks = null;
        if (breaks === "before") return ctx.reply(Failed, { shard });
        if (operationIds.on && s.ops.includes(m.op)) return ctx.reply(Ok, { shard });
        if (s.row) return ctx.reply(Conflict, { shard });
        s.row = true;
        s.ops.push(m.op);
        ctx.reply(breaks === "after" ? Failed : Ok, { shard });
      },
    },
    failsBeforeCommit: {
      description: "The next call to this shard throws before it commits (a storage error, an overloaded object).",
      sources: ["src/sharded/index.ts#ShardedObject.shardedCall"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.breaks === null && s.calls === 0,
      run: (s) => void (s.breaks = "before"),
    },
    failsAfterCommit: {
      description: "The next call to this shard commits, then throws (its answer lost, or a method that throws after its write).",
      sources: ["src/sharded/index.ts#ShardedObject.shardedCall"],
      tags: ["fault"],
      fair: false,
      with: [spend(faults)],
      when: (s) => s.breaks === null && s.calls === 0,
      run: (s) => void (s.breaks = "after"),
    },
  },
});
}

export const ShardA = makeShard("Shard 1", 0);
export const ShardB = makeShard("Shard 2", 1);
const shards = () => [ShardA, ShardB];

const committedAnswersKept = defineProperty("committedAnswersKept", {
  description: "Once the call is reported, every shard that answered committed is reported committed.",
  always: (w) => {
    const c = w.the(Client).state;
    return c.phase === "idle" || c.phase === "sent" || c.answers.every((a, i) => a !== "ok" || c.report[i] === "committed");
  },
  needs: [settledOutcomes],
});
const noUnknownReportedFailed = defineProperty("noUnknownReportedFailed", {
  description: "A shard reported unknown may hold the row; one not reported at all is a shard the caller knows nothing of.",
  always: (w) => {
    const c = w.the(Client).state;
    return c.phase !== "reported" || c.report.every((r) => r !== null);
  },
  needs: [settledOutcomes],
});
const ownWriteNeverConflicts = defineProperty("ownWriteNeverConflicts", {
  description: "A retry of the client's own write never answers that the row exists.",
  always: (w) => w.the(Client).state.retried.every((a) => a !== "conflict"),
  needs: [operationIds],
});
const bothWritten = defineProperty("bothWritten", { description: "Both rows can be written, and the call reported committed.", reachable: (w) => w.the(Client).state.report.every((r) => r === "committed") });
const retriedToCommit = defineProperty("retriedToCommit", { description: "A failed shard can be retried into its row.", reachable: (w) => w.the(Client).state.retried.some((a) => a === "ok") });

const lostAnswer = defineScenario("A commit whose answer is lost", {
  description: "Shard 2 commits and throws. The call reports shard 1 committed and shard 2 unknown; the retry finds its own write.",
  run(s) {
    s.step(s.the(ShardB), "failsAfterCommit");
    s.act(s.the(Client), "write");
    s.settle();
    s.act(s.the(Client), "retry");
    s.settle();
  },
});

export default defineSpec({
  name: "Partial outcomes",
  description: "What a call over several shards tells its caller when one of them fails, and how its retry stays safe.",
  actors: [Client, ShardA, ShardB],
  properties: [committedAnswersKept, noUnknownReportedFailed, ownWriteNeverConflicts, bothWritten, retriedToCommit],
  scenarios: [lostAnswer],
  finalState: { description: "Nothing is in flight: the call was reported, and retried or not.", when: (w) => w.quiet },
});
