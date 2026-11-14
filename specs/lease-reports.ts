// Lease reports: a work graph's coordinator leases a chunk to one runner at a time, and a lease
// that runs out goes back to ready, for another runner. The runner it ran out on doesn't know: it
// may still finish, or fail, and report. Its report is about its own lease.
//
//   - fenceReports: a failure moves the chunk back to ready only if it comes from the runner that
//     holds the chunk, on the attempt it holds. Without it, a late failure from a lease that ran
//     out resets the chunk under the runner that holds it now, and a third runner leases it too:
//     two live leases on one chunk, and a failure counted that was not the current lease's.
//
// One chunk; three runners; a lease may run out (the coordinator's alarm), at most twice. A runner computes and finishes or fails, in any order with the other events.
import { atMost, defineActor, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone } from "@leostera/feynman";

const fenceReports = defineRule("fenceReports", { description: "A failure counts only from the runner holding the chunk, on the attempt it holds." });

const Lease = defineMessage("Lease", { attempt: Number }, { name: "lease", over: "DO RPC", tone: "reply" });
const Report = defineMessage("Report", { runner: Number, attempt: Number, failed: Boolean }, { name: "report", over: "DO RPC" });
const Ask = defineMessage("Ask", { runner: Number }, { name: "next?", over: "DO RPC" });

const objects = defineZone({ name: "Durable Objects", place: { column: 0 } });

interface Chunk {
  state: "ready" | "leased" | "done";
  runner: number | null;
  attempts: number;
  failures: number;
  /** Attempts whose lease ran out (bookkeeping for the properties: a runner can't tell). */
  lapsed: number[];
  /** Attempts whose lease ended by its own runner's report. */
  ended: number[];
}

export const Coordinator = defineActor({
  name: "Coordinator",
  description: "Leases the chunk to one runner at a time; its alarm lets a lease run out.",
  zone: objects,
  singleton: true,
  initialState: (): { chunk: Chunk; leases: { runner: number; attempt: number }[] } => ({ chunk: { state: "ready", runner: null, attempts: 0, failures: 0, lapsed: [], ended: [] }, leases: [] }),
  display: { icon: "database", status: (s) => `chunk ${s.chunk.state}${s.chunk.runner === null ? "" : ` by ${s.chunk.runner}`}, attempt ${s.chunk.attempts}, ${s.chunk.failures} failed` },
  actions: {
    lapse: {
      with: [atMost(2)],
      label: "a lease runs out",
      description: "The alarm finds the lease past its time: the chunk is ready again.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.alarm"],
      when: (s) => s.chunk.state === "leased",
      run(s) {
        s.chunk.lapsed.push(s.chunk.attempts);
        s.chunk.state = "ready";
        s.chunk.runner = null;
      },
    },
  },
  transitions: {
    ask: {
      on: Ask,
      description: "Leases the chunk if it is ready.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.lease"],
      sends: [Lease],
      run(s, m, ctx) {
        if (s.chunk.state !== "ready") return;
        s.chunk.state = "leased";
        s.chunk.runner = m.runner;
        s.chunk.attempts++;
        s.leases.push({ runner: m.runner, attempt: s.chunk.attempts });
        ctx.reply(Lease, { attempt: s.chunk.attempts });
      },
    },
    report: {
      on: Report,
      description: "A finish marks the chunk done, from any attempt (each writes the same results). A failure makes it ready again.",
      sources: ["src/workgraph/coordinator.ts#WorkGraphCoordinator.graphNext", "src/workgraph/coordinator.ts#WorkGraphCoordinator.fail", "src/workgraph/coordinator.ts#WorkGraphCoordinator.complete"],
      run(s, m) {
        if (s.chunk.state === "done") return;
        if (s.chunk.state === "leased" && s.chunk.runner === m.runner && s.chunk.attempts === m.attempt) s.chunk.ended.push(m.attempt);
        if (!m.failed) {
          s.chunk.state = "done";
          s.chunk.runner = null;
          return;
        }
        if (fenceReports.on && !(s.chunk.state === "leased" && s.chunk.runner === m.runner && s.chunk.attempts === m.attempt)) return;
        s.chunk.state = "ready";
        s.chunk.runner = null;
        s.chunk.failures++;
      },
    },
  },
});

function makeRunner(name: string, runner: number) {
  return defineActor({
    name,
    description: "Asks for the chunk once, computes it, and reports: finished or failed.",
    zone: objects,
    singleton: true,
    initialState: () => ({ asked: false, holds: null as number | null, reported: false }),
    display: { icon: "cpu", status: (s) => (s.holds === null ? (s.reported ? "reported" : "idle") : `computing attempt ${s.holds}`) },
    actions: {
      ask: {
        with: [atMost(1)],
        label: "ask for work",
        description: "Asks the coordinator for a chunk.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        when: (s) => !s.asked,
        sends: [Ask],
        run(s, _, ctx) {
          s.asked = true;
          ctx.send(ctx.the(Coordinator), Ask, { runner });
        },
      },
      finish: {
        label: "finish",
        description: "Computes the chunk and reports it finished.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        when: (s) => s.holds !== null,
        sends: [Report],
        run(s, _, ctx) {
          ctx.send(ctx.the(Coordinator), Report, { runner, attempt: s.holds!, failed: false });
          s.holds = null;
          s.reported = true;
        },
      },
      fail: {
        label: "fail",
        description: "The chunk's compute throws; the runner reports the failure.",
        sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"],
        when: (s) => s.holds !== null,
        sends: [Report],
        run(s, _, ctx) {
          ctx.send(ctx.the(Coordinator), Report, { runner, attempt: s.holds!, failed: true });
          s.holds = null;
          s.reported = true;
        },
      },
    },
    transitions: {
      lease: { on: Lease, description: "Takes the lease.", sources: ["src/workgraph/runner.ts#WorkGraphRunner.alarm"], run: (s, m) => void (s.holds = m.attempt) },
    },
  });
}

export const Runner1 = makeRunner("Runner 1", 1);
export const Runner2 = makeRunner("Runner 2", 2);
export const Runner3 = makeRunner("Runner 3", 3);

const oneLiveLease = defineProperty("oneLiveLease", {
  description: "At most one lease on the chunk is live: not run out, and not ended by its runner's report.",
  always: (w) => {
    const c = w.the(Coordinator).state.chunk;
    const live = w.the(Coordinator).state.leases.filter((l) => !c.lapsed.includes(l.attempt) && !c.ended.includes(l.attempt));
    return live.length <= 1;
  },
  needs: [fenceReports],
});
const failuresAreOwn = defineProperty("failuresAreOwn", {
  description: "Failures counted are no more than the leases that were not lapsed when they failed.",
  always: (w) => {
    const c = w.the(Coordinator).state.chunk;
    return c.failures <= c.attempts - c.lapsed.length;
  },
  needs: [fenceReports],
});
const finishes = defineProperty("finishes", { description: "The chunk can be done.", reachable: (w) => w.the(Coordinator).state.chunk.state === "done" });
const handedOn = defineProperty("handedOn", { description: "A lease can run out and the chunk go to another runner.", reachable: (w) => w.the(Coordinator).state.chunk.attempts >= 2 });

const lateFailure = defineScenario("A failure from a lease that ran out", {
  description: "Runner 1's lease runs out; runner 2 takes the chunk; runner 1's failure arrives and is ignored; runner 2 finishes.",
  run(s) {
    s.act(s.the(Runner1), "ask");
    s.settle();
    s.act(s.the(Coordinator), "lapse");
    s.act(s.the(Runner2), "ask");
    s.settle();
    s.act(s.the(Runner1), "fail");
    s.settle();
    s.act(s.the(Runner2), "finish");
    s.settle();
  },
});

export default defineSpec({
  name: "Lease reports",
  description: "How a work graph's coordinator takes a runner's report about a lease that may have run out.",
  actors: [Coordinator, Runner1, Runner2, Runner3],
  properties: [oneLiveLease, failuresAreOwn, finishes, handedOn],
  scenarios: [lateFailure],
  finalState: { description: "Nothing is in flight.", when: (w) => w.quiet },
});
