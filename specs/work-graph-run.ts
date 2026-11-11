// How a work graph run's chunks get done (RFD0005): runners pull leases on chunks from the coordinator,
// compute them, write their results to the results store, and report them done in their next
// call, which also leases them the next chunk. A runner's alarm drives it: a crashed runner's
// alarm is retried and leases the chunk it held again. A runner lost for good keeps its lease
// until the lease runs out, and the chunk passes to the other runner.
//
// Three chunks: B needs a value A exports (a cut that stores), C needs nothing. Two runners. The
// coordinator keeps everything in storage and its alarm is durable, so it is not crashed here.
// Calls may be lost (the caller sees them fail) and runners may crash.
//
// Each rule is needed: --without waitForNeeds breaks resultsAreRight, --without
// resultsBeforeDone breaks doneMeansResults, and --without expireLeases breaks finishes.
import { call, defineSpec, message } from "@leostera/feynman";

type Chunk = "A" | "B" | "C";
const CHUNKS: Chunk[] = ["A", "B", "C"];
/** The chunk each one needs done first (B loads a value A exports). */
const NEEDS: Partial<Record<Chunk, Chunk>> = { B: "A" };
type Runner = 1 | 2;

interface ChunkState {
  state: "ready" | "leased" | "done";
  runner: Runner | null;
}

export default defineSpec(
  "Work graph runs",
  {
    description: "How runners pull chunks from a run's coordinator, compute them, and make their results durable before the chunk is done.",
    messages: {
      Next: call<{ runner: Runner; finished: Chunk | null }>({ name: "next (and finished)", over: "DO RPC" }),
      Lease: message<{ chunk: Chunk }>({ name: "lease", tone: "reply" }),
      Wait: message<Record<string, never>>({ name: "wait", tone: "reply" }),
      Done: message<Record<string, never>>({ name: "run done", tone: "reply" }),
      Load: call<{ chunk: Chunk }>({ name: "load needed value", over: "DO RPC" }),
      Loaded: message<{ present: boolean }>({ name: "value (or none)", tone: "reply" }),
      Put: call<{ chunk: Chunk; right: boolean }>({ name: "put results", over: "DO RPC" }),
      Stored: message<{ chunk: Chunk }>({ name: "stored", tone: "reply" }),
    },
  },
  (spec) => {
    const { Next, Lease, Wait, Done, Load, Loaded, Put, Stored } = spec.messages;

    const runners = spec.defineZone({ name: "Runners", description: "pull and compute chunks", place: { column: 0, arrange: "stack" } });
    const control = spec.defineZone({ name: "Coordinator", description: "the run's chunks and leases", place: { column: 1 } });
    const store = spec.defineZone({ name: "Results", description: "results and values by unit hash", place: { column: 2 } });

    const waitForNeeds = spec.defineRule({ name: "waitForNeeds", description: "The coordinator leases a chunk only once every chunk it needs is done." });
    const resultsBeforeDone = spec.defineRule({ name: "resultsBeforeDone", description: "A runner reports a chunk finished only after the results store has its results." });
    const expireLeases = spec.defineRule({ name: "expireLeases", description: "A lease that runs out goes back to ready, for any runner to take." });

    spec.defineLimit({ name: "Lease length", value: 120, unit: "seconds" });

    function makeRunner(name: string, id: Runner, canVanish: boolean) {
      return spec.defineActor({
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
        }),
        // Its finished chunk is stored before its alarm is set; its alarm survives a crash.
        durable: ["finished"],
        display: {
          icon: "server",
          status: (s) => (s.gone ? "gone" : s.stopped ? "stopped" : s.holding ? `${s.phase} ${s.holding}` : s.phase),
          mood: (s) => (s.gone ? "warn" : s.stopped ? "off" : s.holding ? "busy" : "ok"),
        },
        actions: {
          vanish: {
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
            when: (s) => !s.gone && !s.stopped && s.phase === "idle",
            sends: [Next],
            run(s, ctx) {
              s.phase = "asking";
              ctx.send(ctx.the(Coordinator), Next, { runner: s.id, finished: s.finished });
            },
          },
          [Lease]: {
            description: "Holds a chunk: loads what it needs, or computes it at once.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.runChunk"],
            sends: [Load, Put],
            run(s, msg, ctx) {
              if (s.gone || s.phase !== "asking") return;
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
          [Loaded]: {
            description: "Computes the chunk with the value it loaded (a chunk computed without it has wrong results).",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.runChunk"],
            sends: [Put],
            run(s, msg, ctx) {
              if (s.gone || s.phase !== "loading") return;
              s.right = msg.present;
              s.phase = "writing";
              ctx.send(ctx.the(Results), Put, { chunk: s.holding!, right: s.right });
              if (!resultsBeforeDone.on) s.finished = s.holding;
            },
          },
          [Stored]: {
            description: "The results are durable: the chunk is finished, and the next alarm reports it.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.runChunk"],
            run(s, msg) {
              if (s.gone || s.phase !== "writing" || s.holding !== msg.chunk) return;
              if (resultsBeforeDone.on) s.finished = s.holding;
              s.holding = null;
              s.phase = "idle";
            },
          },
          [Wait]: {
            description: "Nothing is ready: its alarm asks again later.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
            run(s) {
              if (s.phase === "asking") s.phase = "idle";
            },
          },
          [Done]: {
            description: "The run is done: the runner stops.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
            run(s) {
              s.stopped = true;
              s.phase = "idle";
            },
          },
          [spec.failed(Next)]: {
            description: "The call threw: the alarm is retried, and reports the same chunk again.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
            run(s) {
              if (s.phase === "asking") s.phase = "idle";
            },
          },
          [spec.failed(Load)]: {
            description: "Loading threw: the alarm is retried, and leases the same chunk again.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
            run(s) {
              if (s.phase !== "loading") return;
              s.holding = null;
              s.phase = "idle";
            },
          },
          [spec.failed(Put)]: {
            description: "Writing threw: the alarm is retried, and leases the same chunk again.",
            sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
            run(s) {
              if (s.phase !== "writing") return;
              s.holding = null;
              s.phase = "idle";
            },
          },
        },
      });
    }

    const Runner1 = makeRunner("Runner 1", 1, true);
    const Runner2 = makeRunner("Runner 2", 2, false);

    const Coordinator = spec.defineActor({
      name: "Coordinator",
      description: "The run's chunks, their leases, and what each needs, in SQLite.",
      zone: control,
      singleton: true,
      initialState: () => ({ chunks: { A: { state: "ready", runner: null }, B: { state: "ready", runner: null }, C: { state: "ready", runner: null } } as Record<Chunk, ChunkState> }),
      durable: ["chunks"],
      display: {
        icon: "database",
        status: (s) => `${CHUNKS.filter((c) => s.chunks[c].state === "done").length} of ${CHUNKS.length} done`,
        rows: (s) => CHUNKS.map((c) => ({ label: c, detail: s.chunks[c].state + (s.chunks[c].runner ? ` by ${s.chunks[c].runner}` : ""), mood: s.chunks[c].state === "done" ? ("ok" as const) : s.chunks[c].state === "leased" ? ("busy" as const) : ("info" as const) })),
      },
      transitions: {
        [Next]: {
          description: "Marks the reported chunk done, then gives the runner its own lease back, or the next ready chunk, or says wait or done.",
          sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.graphNext", "src/workgraph/coordinator.ts#WorkGraphCoordinator.complete", "src/workgraph/coordinator.ts#WorkGraphCoordinator.lease"],
          sends: [Lease, Wait, Done],
          run(s, msg, ctx) {
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
            if (CHUNKS.every((c) => s.chunks[c].state === "done")) return ctx.reply(Done, {});
            ctx.reply(Wait, {});
          },
        },
        expire: {
          description: "The alarm: leases that ran out go back to ready (a slow runner's too; its completion still counts).",
          sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.alarm"],
          after: "the lease length",
          routine: true,
          when: (s) => expireLeases.on && CHUNKS.some((c) => s.chunks[c].state === "leased"),
          run(s) {
            for (const c of CHUNKS) {
              if (s.chunks[c].state !== "leased") continue;
              s.chunks[c].state = "ready";
              s.chunks[c].runner = null;
            }
          },
        },
      },
    });

    const Results = spec.defineActor({
      name: "Results",
      description: "Results and exported values, keyed by unit hash: a put is idempotent.",
      zone: store,
      singleton: true,
      // right: every put of the chunk's results was computed with what the chunk needs.
      initialState: () => ({ results: {} as Partial<Record<Chunk, boolean>> }),
      durable: ["results"],
      display: {
        icon: "storage",
        status: (s) => `${Object.keys(s.results).length} chunk(s) of results`,
      },
      transitions: {
        [Load]: {
          description: "Answers with a needed chunk's exported value, if it is stored.",
          sources: ["src/workgraph/results.ts#WorkGraphResults.graphGet"],
          sends: [Loaded],
          run(s, msg, ctx) {
            ctx.reply(Loaded, { present: s.results[msg.chunk] !== undefined });
          },
        },
        [Put]: {
          description: "Stores the chunk's results (and the values it exports).",
          sources: ["src/workgraph/results.ts#WorkGraphResults.graphPut"],
          sends: [Stored],
          run(s, msg, ctx) {
            s.results[msg.chunk] = (s.results[msg.chunk] ?? true) && msg.right;
            ctx.reply(Stored, { chunk: msg.chunk });
          },
        },
      },
    });

    const allDone = (w: { the: (a: typeof Coordinator) => { state: { chunks: Record<Chunk, ChunkState> } } }) => CHUNKS.every((c) => w.the(Coordinator).state.chunks[c].state === "done");

    spec.defineFinalState({
      description: "Every chunk is done, nothing is in flight, and every runner has stopped or is gone.",
      when: (w) => w.quiet && allDone(w as never) && [w.the(Runner1).state, w.the(Runner2).state].every((r) => r.stopped || r.gone),
    });

    spec.defineProperties({
      resultsAreRight: {
        description: "Every stored result was computed with the values its chunk needs.",
        always: (w) => CHUNKS.every((c) => w.the(Results).state.results[c] !== false),
        needs: [waitForNeeds],
      },
      doneMeansResults: {
        description: "Once a chunk is done, its results are stored.",
        always: (w) => CHUNKS.every((c) => w.the(Coordinator).state.chunks[c].state !== "done" || w.the(Results).state.results[c] !== undefined),
        needs: [resultsBeforeDone],
      },
      finishes: {
        description: "Every chunk is eventually done, even if Runner 1 is lost for good.",
        eventually: (w) => allDone(w as never),
        needs: [expireLeases],
      },
    });

    spec.defineScenario({
      name: "Two runners finish a job",
      description: "Runner 1 takes A and Runner 2 takes C; B waits for A, and whoever is free takes it.",
      run(s) {
        s.settle();
      },
    });

    spec.defineScenario({
      name: "A runner is lost holding a chunk",
      description: "Runner 1 leases A and is lost. Its lease runs out, Runner 2 takes A, then B.",
      run(s) {
        s.step(s.the(Runner1), "alarm");
        s.deliver(s.the(Coordinator), "Next");
        s.deliver(s.the(Runner1), "Lease");
        s.act(s.the(Runner1), "vanish");
        s.settle();
      },
    });

    return { Runner1, Runner2, Coordinator, Results };
  },
);
