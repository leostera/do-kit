// How a work graph run's chunks get done (RFD0005): runners pull leases on chunks from the coordinator,
// compute them, write their results to the results store, and report them done in their next
// call, which also leases them the next chunk. A runner's alarm drives it: a crashed runner's
// alarm is retried and leases the chunk it held again. A runner lost for good keeps its lease
// until the lease runs out, and the chunk passes to the other runner.
//
// Three chunks: B needs a value A exports (a cut that stores), C needs nothing. Two runners. The
// coordinator keeps everything in storage and its alarm is durable, so it is not crashed here.
// Calls and their replies may be lost (the caller sees them fail), and, with --set crashes=1,
// runners may crash: one that does loses the report it had in hand.
//
// A run also leaves nothing behind. It ends when its chunks are done, when the app deletes it, or
// when its deadline passes. Then the coordinator stops each runner (a call, retried from its alarm
// until it is answered), and a stopped runner deletes its storage and alarm: a runner lost for good
// too, since the stop reaches a new instance of its object. Once every runner has stopped, the
// coordinator keeps only its status for a while (not on delete), then wipes the results store
// and itself. A write that reaches a wiped results shard is refused. (The code also has a stop wait
// for the chunk in hand, since a shard refuses only for a few seconds, so that its name can be
// used again; the spec's shard keeps refusing.)
//
// Each rule is needed: --without waitForNeeds breaks resultsAreRight, --without
// resultsBeforeDone breaks doneMeansResults, --without expireLeases breaks finishes, --without
// stopRunners or wipeAfterAcks breaks nothingLeftBehind, and --without refuseAfterWipe breaks
// staysEmpty.
import { atMost, budget, crash, defineActor, defineCall, defineLimit, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, failed, lose, nullable, oneOf } from "@leostera/feynman";

type Chunk = "A" | "B" | "C";
const CHUNKS: Chunk[] = ["A", "B", "C"];
/** The chunk each one needs done first (B loads a value A exports). */
const NEEDS: Partial<Record<Chunk, Chunk>> = { B: "A" };
type Runner = 1 | 2;

interface ChunkState {
  state: "ready" | "leased" | "done";
  runner: Runner | null;
}

/** Lost calls and replies on one path. */
const faults = budget("faults", 1);
/**
 * Runner crashes on one path, apart from faults: with both, the search does not finish. The
 * default checks lost calls; specs:check also runs --set faults=0 --set crashes=1.
 */
const crashes = budget("crashes", 0);

const ChunkSchema = oneOf("A", "B", "C");
const RunnerSchema = oneOf(1, 2);
const Next = defineCall("Next", { runner: RunnerSchema, finished: nullable(ChunkSchema) }, { name: "next (and finished)", over: "DO RPC" });
const Lease = defineMessage("Lease", { chunk: ChunkSchema }, { name: "lease", tone: "reply" });
const Wait = defineMessage("Wait", {}, { name: "wait", tone: "reply" });
const Done = defineMessage("Done", {}, { name: "run done", tone: "reply" });
const Load = defineCall("Load", { chunk: ChunkSchema }, { name: "load needed value", over: "DO RPC" });
const Loaded = defineMessage("Loaded", { present: Boolean }, { name: "value (or none)", tone: "reply" });
const Put = defineCall("Put", { chunk: ChunkSchema, right: Boolean }, { name: "put results", over: "DO RPC" });
const Stored = defineMessage("Stored", { chunk: ChunkSchema }, { name: "stored", tone: "reply" });
const Stop = defineCall("Stop", { runner: RunnerSchema }, { name: "stop", over: "DO RPC" });
const Stopped = defineMessage("Stopped", { runner: RunnerSchema }, { name: "stopped", tone: "reply" });
const Wipe = defineCall("Wipe", {}, { name: "wipe", over: "DO RPC" });
const Wiped = defineMessage("Wiped", {}, { name: "wiped", tone: "reply" });

const runners = defineZone({ name: "Runners", description: "pull and compute chunks", place: { column: 0, arrange: "stack" } });
const control = defineZone({ name: "Coordinator", description: "the run's chunks and leases", place: { column: 1 } });
const store = defineZone({ name: "Results", description: "results and values by unit hash", place: { column: 2 } });

const waitForNeeds = defineRule("waitForNeeds", { description: "The coordinator leases a chunk only once every chunk it needs is done." });
const resultsBeforeDone = defineRule("resultsBeforeDone", { description: "A runner reports a chunk finished only after the results store has its results." });
const expireLeases = defineRule("expireLeases", { description: "A lease that runs out goes back to ready, for any runner to take." });
const stopRunners = defineRule("stopRunners", { description: "Once the run ends, the coordinator tells every runner to stop, until each has answered." });
const wipeAfterAcks = defineRule("wipeAfterAcks", { description: "The coordinator wipes the run only once every runner has said it stopped." });
const refuseAfterWipe = defineRule("refuseAfterWipe", { description: "A wiped results shard refuses writes: it is not made again." });

const leaseLength = defineLimit({ name: "Lease length", value: 120, unit: "seconds" });

function makeRunner(name: string, id: Runner, canVanish: boolean) {
  return defineActor({
    name,
    description: `Runner ${id}: one chunk per alarm invocation.`,
    zone: runners,
    singleton: true,
    initialState: () => ({
      id,
      phase: "idle" as "idle" | "asking" | "loading" | "writing",
      holding: null as Chunk | null,
      right: true,
      finished: null as Chunk | null,
      stopped: false,
      gone: false,
      /** Its storage holds the run (its name, its finished chunk) or its alarm is set. */
      held: true,
    }),
    // The chunk it finished is kept in hand while it goes on to the next in the same invocation
    // (stored only before it sets its alarm): a crash may lose the report, never the results.
    // The chunk's lease then runs out, and whoever takes it finds the results stored.
    display: {
      icon: "server",
      status: (s) => (!s.held ? "wiped" : s.gone ? "gone" : s.stopped ? "stopped" : s.holding ? `${s.phase} ${s.holding}` : s.phase),
      mood: (s) => (s.gone ? "warn" : s.stopped ? "off" : s.holding ? "busy" : "ok"),
    },
    actions: {
      vanish: {
        with: [atMost(1)],
        label: "Lose the runner",
        description: "The runner's object never runs again, whatever it held.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.chunkStarting"],
        when: (s) => canVanish && !s.gone && !s.stopped,
        run(s) {
          s.gone = true;
        },
      },
    },
    transitions: {
      alarm: {
        description: "Its alarm: reports the chunk it finished, and asks for the next one.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        routine: true,
        when: (s) => !s.gone && !s.stopped && s.held && s.phase === "idle",
        sends: [Next],
        run(s, _, ctx) {
          s.phase = "asking";
          ctx.send(ctx.the(Coordinator), Next, { runner: s.id, finished: s.finished });
        },
      },
      lease: {
        on: Lease,
        description: "Holds a chunk: loads what it needs, or computes it at once.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.runChunk"],
        sends: [Load, Put],
        run(s, msg, ctx) {
          if (s.gone || s.stopped || s.phase !== "asking") return;
          // The coordinator took its report: it is not reported again.
          s.finished = null;
          s.holding = msg.chunk;
          const need = NEEDS[msg.chunk];
          if (need) {
            s.phase = "loading";
            ctx.send(ctx.the(Results), Load, { chunk: need });
            return;
          }
          s.right = true;
          s.phase = "writing";
          ctx.send(ctx.the(Results), Put, { chunk: msg.chunk, right: true });
          // Without the rule, the runner counts the chunk finished as soon as it has computed it.
          if (!resultsBeforeDone.on) s.finished = msg.chunk;
        },
      },
      loaded: {
        on: Loaded,
        description: "Computes the chunk with the value it loaded (a chunk computed without it has wrong results).",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.runChunk"],
        sends: [Put],
        run(s, msg, ctx) {
          if (s.gone || s.stopped || s.phase !== "loading") return;
          s.right = msg.present;
          s.phase = "writing";
          ctx.send(ctx.the(Results), Put, { chunk: s.holding!, right: s.right });
          if (!resultsBeforeDone.on) s.finished = s.holding;
        },
      },
      stored: {
        on: Stored,
        description: "The results are durable: the chunk is finished, and the next alarm reports it.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.runChunk"],
        run(s, msg) {
          if (s.gone || s.stopped || s.phase !== "writing" || s.holding !== msg.chunk) return;
          if (resultsBeforeDone.on) s.finished = s.holding;
          s.holding = null;
          s.phase = "idle";
        },
      },
      wait: {
        on: Wait,
        description: "Nothing is ready: its alarm asks again later.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        run(s) {
          if (s.phase === "asking") s.phase = "idle";
        },
      },
      done: {
        on: Done,
        description: "The run is over: the runner deletes its storage and its alarm.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.step", "src/workgraph/runner.ts#WorkGraphRunner.wipe"],
        run(s) {
          s.stopped = true;
          s.held = false;
          s.phase = "idle";
          s.holding = null;
          s.finished = null;
        },
      },
      stop: {
        on: Stop,
        description: "Told to stop: deletes its storage and its alarm, and says so. A lost runner too: the call reaches a new instance of its object.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.graphStop"],
        sends: [Stopped],
        run(s, msg, ctx) {
          s.stopped = true;
          s.held = false;
          s.phase = "idle";
          s.holding = null;
          s.finished = null;
          ctx.reply(Stopped, { runner: msg.runner });
        },
      },
      nextFailed: {
        on: failed(Next),
        description: "The call threw: the alarm is retried, and reports the same chunk again.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        run(s) {
          if (s.phase === "asking") s.phase = "idle";
        },
      },
      loadFailed: {
        on: failed(Load),
        description: "Loading threw: the alarm is retried, and leases the same chunk again.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        run(s) {
          if (s.phase !== "loading") return;
          s.holding = null;
          s.phase = "idle";
        },
      },
      putFailed: {
        on: failed(Put),
        description: "Writing threw: the alarm is retried, and leases the same chunk again.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        run(s) {
          if (s.phase !== "writing") return;
          s.holding = null;
          s.phase = "idle";
        },
      },
      leaseLost: lose(Lease, { fails: Next, spend: faults }),
      waitLost: lose(Wait, { fails: Next, spend: faults }),
      doneLost: lose(Done, { fails: Next, spend: faults }),
      loadedLost: lose(Loaded, { fails: Load, spend: faults }),
      storedLost: lose(Stored, { fails: Put, spend: faults }),
      stopLost: lose(Stop, { spend: faults }),
      // Its storage, and whether it is lost for good (a lost runner does not come back by crashing).
          crash: crash({ keeps: ["held", "stopped", "gone"], spend: crashes }),
    },
  });
}

export const Runner1 = makeRunner("Runner 1", 1, true);
export const Runner2 = makeRunner("Runner 2", 2, false);

export const Coordinator = defineActor({
  name: "Coordinator",
  description: "The run's chunks, their leases, and what each needs, in SQLite.",
  zone: control,
  singleton: true,
  initialState: () => ({
    chunks: { A: { state: "ready", runner: null }, B: { state: "ready", runner: null }, C: { state: "ready", runner: null } } as Record<Chunk, ChunkState>,
    ended: null as null | "done" | "failed" | "cancelled",
    deleting: false,
    /** Runners that said they stopped, and stops in flight. */
    stopped: { 1: false, 2: false } as Record<Runner, boolean>,
    asked: { 1: false, 2: false } as Record<Runner, boolean>,
    /** live: running or stopping its runners; kept: only its status; wiping: the results store; wiped: nothing. */
    status: "live" as "live" | "kept" | "wiping" | "wiped",
  }),
  display: {
    icon: "database",
    status: (s) => (s.status !== "live" ? s.status : s.ended ? `${s.ended}, stopping runners` : `${CHUNKS.filter((c) => s.chunks[c].state === "done").length} of ${CHUNKS.length} done`),
    rows: (s) =>
      CHUNKS.map((c) => ({
        label: c,
        detail: s.chunks[c].state + (s.chunks[c].runner ? ` by ${s.chunks[c].runner}` : ""),
        mood: s.chunks[c].state === "done" ? ("ok" as const) : s.chunks[c].state === "leased" ? ("busy" as const) : ("info" as const),
      })),
  },
  actions: {
    deadline: {
      with: [atMost(1)],
      label: "Pass the deadline",
      description: "The alarm finds the run past its deadline: it fails.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.alarm"],
      when: (s) => s.ended === null,
      run(s) {
        s.ended = "failed";
      },
    },
    delete: {
      with: [atMost(1)],
      label: "Delete the run",
      description: "The app deletes the run: it is cancelled if it has not ended, and nothing is kept.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.graphDelete"],
      when: (s) => s.status !== "wiped" && !s.deleting,
      run(s) {
        if (s.ended === null) s.ended = "cancelled";
        s.deleting = true;
      },
    },
  },
  transitions: {
    next: {
      on: Next,
      description: "Marks the reported chunk done, then gives the runner its own lease back, or the next ready chunk, or says wait or done.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.graphNext", "src/workgraph/coordinator.ts#WorkGraphCoordinator.complete", "src/workgraph/coordinator.ts#WorkGraphCoordinator.lease"],
      sends: [Lease, Wait, Done],
      run(s, msg, ctx) {
        // Ended, or gone: the runner stops. Nothing changes here.
        if (s.ended || s.status !== "live") return ctx.reply(Done, {});
        if (msg.finished) {
          const done = s.chunks[msg.finished];
          done.state = "done";
          done.runner = null;
        }
        const own = CHUNKS.find((c) => s.chunks[c].state === "leased" && s.chunks[c].runner === msg.runner);
        if (own) return ctx.reply(Lease, { chunk: own });
        const ready = CHUNKS.find((c) => s.chunks[c].state === "ready" && (!waitForNeeds.on || !NEEDS[c] || s.chunks[NEEDS[c]!].state === "done"));
        if (ready) {
          s.chunks[ready].state = "leased";
          s.chunks[ready].runner = msg.runner;
          return ctx.reply(Lease, { chunk: ready });
        }
        if (CHUNKS.every((c) => s.chunks[c].state === "done")) {
          s.ended = "done";
          return ctx.reply(Done, {});
        }
        ctx.reply(Wait, {});
      },
    },
    stopped: {
      on: Stopped,
      description: "A runner has stopped, and holds nothing.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.cleanUpOnce"],
      run(s, msg) {
        s.stopped[msg.runner] = true;
        s.asked[msg.runner] = false;
      },
    },
    stopFailed: {
      on: failed(Stop),
      description: "The stop threw: the alarm asks again.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.cleanUpOnce"],
      run(s, msg) {
        s.asked[msg.payload.runner] = false;
      },
    },
    wiped: {
      on: Wiped,
      description: "The results store is wiped: the coordinator deletes its own storage and alarm.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.wipe"],
      run(s) {
        s.status = "wiped";
      },
    },
    wipeFailed: {
      on: failed(Wipe),
      description: "Wiping the results store threw: the alarm tries again.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.wipe"],
      run(s) {
        if (s.status === "wiping") s.status = "kept";
      },
    },
    stopRunners: {
      description: "The alarm, once the run ended: tells each runner that has not said so to stop.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.cleanUpOnce"],
      after: "a tick",
      routine: true,
      when: (s) => stopRunners.on && s.ended !== null && s.status === "live" && ([1, 2] as Runner[]).some((r) => !s.stopped[r] && !s.asked[r]),
      sends: [Stop],
      run(s, _, ctx) {
        for (const [r, actor] of [
          [1, ctx.the(Runner1)],
          [2, ctx.the(Runner2)],
        ] as const) {
          if (s.stopped[r] || s.asked[r]) continue;
          s.asked[r] = true;
          ctx.send(actor, Stop, { runner: r });
        }
      },
    },
    keepStatus: {
      description: "Every runner has stopped: the chunks go, and only the status stays (on delete, not even that).",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.cleanUpOnce"],
      after: "a tick",
      routine: true,
      when: (s) => s.ended !== null && s.status === "live" && (!wipeAfterAcks.on || (s.stopped[1] && s.stopped[2])),
      run(s) {
        s.status = "kept";
      },
    },
    expireStatus: {
      description: "keepStatusMs is up (at once, on delete): wipes the run's results store.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.wipe"],
      after: "keepStatusMs",
      routine: true,
      when: (s) => s.status === "kept",
      sends: [Wipe],
      run(s, _, ctx) {
        s.status = "wiping";
        ctx.send(ctx.the(Results), Wipe, {});
      },
    },
    expire: {
      description: "The alarm: leases that ran out go back to ready (a slow runner's too; its completion still counts).",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.alarm"],
      after: "the lease length",
      routine: true,
      when: (s) => expireLeases.on && s.ended === null && CHUNKS.some((c) => s.chunks[c].state === "leased"),
      run(s) {
        for (const c of CHUNKS) {
          if (s.chunks[c].state !== "leased") continue;
          s.chunks[c].state = "ready";
          s.chunks[c].runner = null;
        }
      },
    },
    nextLost: lose(Next, { spend: faults }),
    stoppedLost: lose(Stopped, { fails: Stop, spend: faults }),
    wipedLost: lose(Wiped, { fails: Wipe, spend: faults }),
  },
});

export const Results = defineActor({
  name: "Results",
  description: "Results and exported values, keyed by unit hash: a put is idempotent.",
  zone: store,
  singleton: true,
  // right: every put of the chunk's results was computed with what the chunk needs.
  initialState: () => ({ results: {} as Partial<Record<Chunk, boolean>>, wiped: false }),
  display: {
    icon: "storage",
    status: (s) => `${Object.keys(s.results).length} chunk(s) of results`,
  },
  transitions: {
    load: {
      on: Load,
      description: "Answers with a needed chunk's exported value, if it is stored.",
      sources: ["src/workgraph/results.ts#WorkGraphResults.graphGet"],
      sends: [Loaded],
      run(s, msg, ctx) {
        ctx.reply(Loaded, { present: s.results[msg.chunk] !== undefined });
      },
    },
    put: {
      on: Put,
      description: "Stores the chunk's results (and the values it exports).",
      sources: ["src/workgraph/results.ts#WorkGraphResults.graphPut"],
      sends: [Stored],
      run(s, msg, ctx) {
        // A late write, from a runner stopped (or lost) while writing.
        if (s.wiped && refuseAfterWipe.on) return;
        s.results[msg.chunk] = (s.results[msg.chunk] ?? true) && msg.right;
        ctx.reply(Stored, { chunk: msg.chunk });
      },
    },
    wipe: {
      on: Wipe,
      description: "Deletes every shard's storage and alarm.",
      sources: ["src/sharded/client.ts#ShardedClient.wipe", "src/sharded/index.ts#ShardedObject.shardedWipe"],
      sends: [Wiped],
      run(s, _msg, ctx) {
        s.results = {};
        s.wiped = true;
        ctx.reply(Wiped, {});
      },
    },
    loadLost: lose(Load, { spend: faults }),
    putLost: lose(Put, { spend: faults }),
    wipeLost: lose(Wipe, { spend: faults }),
  },
});

const allDone = (w: { the: (a: typeof Coordinator) => { state: { chunks: Record<Chunk, ChunkState> } } }) => CHUNKS.every((c) => w.the(Coordinator).state.chunks[c].state === "done");

const nothingLeft = (w: { the: (a: never) => { state: Record<string, unknown> } }) =>
  (w.the(Coordinator as never).state as { status: string }).status === "wiped" &&
  (w.the(Results as never).state as { wiped: boolean }).wiped &&
  [w.the(Runner1 as never).state, w.the(Runner2 as never).state].every((r) => !(r as { held: boolean }).held);

const resultsAreRight = defineProperty("resultsAreRight", {
  description: "Every stored result was computed with the values its chunk needs.",
  always: (w) => CHUNKS.every((c) => w.the(Results).state.results[c] !== false),
  needs: [waitForNeeds],
});
const doneMeansResults = defineProperty("doneMeansResults", {
  description: "Once a chunk is done, its results are stored (until the store is wiped).",
  always: (w) => w.the(Results).state.wiped || CHUNKS.every((c) => w.the(Coordinator).state.chunks[c].state !== "done" || w.the(Results).state.results[c] !== undefined),
  needs: [resultsBeforeDone],
});
const finishes = defineProperty("finishes", {
  description: "Every chunk is eventually done, even if Runner 1 is lost for good, unless the run is deleted or passes its deadline first.",
  eventually: (w) => allDone(w as never) || w.the(Coordinator).state.ended === "failed" || w.the(Coordinator).state.ended === "cancelled",
  needs: [expireLeases],
});
const nothingLeftBehind = defineProperty("nothingLeftBehind", {
  description: "Eventually every object the run touched holds nothing and has no alarm: each runner (a lost one too), the results store, and the coordinator.",
  eventually: (w) => nothingLeft(w as never),
  needs: [stopRunners, wipeAfterAcks],
});
const staysEmpty = defineProperty("staysEmpty", {
  description: "Once the results store is wiped, it holds nothing.",
  always: (w) => !w.the(Results).state.wiped || Object.keys(w.the(Results).state.results).length === 0,
  needs: [refuseAfterWipe],
});

const scenario1 = defineScenario("Two runners finish a job", {
  description: "Runner 1 takes A and Runner 2 takes C; B waits for A, and whoever is free takes it.",
  run(s) {
    s.settle();
  },
});

const scenario2 = defineScenario("A runner is lost holding a chunk", {
  description: "Runner 1 leases A and is lost. Its lease runs out, Runner 2 takes A, then B.",
  run(s) {
    s.step(s.the(Runner1), "alarm");
    s.deliver(s.the(Coordinator), Next);
    s.deliver(s.the(Runner1), Lease);
    s.act(s.the(Runner1), "vanish");
    s.settle();
  },
});

const scenario3 = defineScenario("A run is deleted while a runner writes", {
  description: "Runner 1 leases A and sends its results; the app deletes the run. Runner 1 is stopped and wiped, and its late write finds the store wiped.",
  run(s) {
    s.step(s.the(Runner1), "alarm");
    s.deliver(s.the(Coordinator), Next);
    s.deliver(s.the(Runner1), Lease);
    s.act(s.the(Coordinator), "delete");
    s.settle();
  },
});

export default defineSpec({
  name: "Work graph runs",
  description: "How runners pull chunks from a run's coordinator, compute them, and make their results durable before the chunk is done.",
  actors: [Runner1, Runner2, Coordinator, Results],
  limits: [leaseLength],
  properties: [resultsAreRight, doneMeansResults, finishes, nothingLeftBehind, staysEmpty],
  scenarios: [scenario1, scenario2, scenario3],
  finalState: { description: "The run ended, nothing is in flight, and nothing is left: no runner, coordinator or results store holds anything.", when: (w) => w.quiet && nothingLeft(w as never) },
});
