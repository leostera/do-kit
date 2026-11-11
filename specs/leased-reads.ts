// Leased reads (RFD0004): one hot key, read through a relay without ever being stale. The source
// leases the key to the relay; the relay answers isolates from the lease and gives them
// sub-leases inside it. A write to a leased key commits at once, but is shown to reads (and
// acknowledged) only once the lease is over or given back. A held write asks the relay for the
// lease back; the relay stops answering from it, and says whether sub-leases it gave are still out.
//
// Time. Specs have no clock, but the code's safety rests on how leases nest in time: a holder
// times its lease from before it asked, less a margin, and the source from after it granted,
// plus the margin. So an isolate's sub-lease ends before the relay's lease, which ends before the
// source stops counting it. The spec keeps that real time in model variables that only guards
// read: `relayWindow` (the relay's lease is running), `grantInFlight` (a lease on its way), and
// the sub-leases running or on their way. The relay's lease can end only once no sub-lease
// runs; the source's count only once the relay's lease has ended. A restarted source does not
// know its leases: it holds writes, and answers no read of a key it held a write on, until every
// lease from before could have ended (its stored horizon, plus the skew).
//
// specs:check runs it with one isolate, one lost message or relay restart, and one source restart:
// exhaustive, and it shows four rules needed. Two need a second isolate:
//   --instances Reader=2 --set faults=0 --set crashes=0 --without matchLease --only freshReads
//   --instances Reader=2 --set faults=0 --without storeHeldKeys --only neverGoBack
// Not modelled: the
// relay count and relay nesting (relay-plan.ts), unnamed holders, and WRITTEN_MS (a key written
// recently is not leased to relays: fewer leases, never a weaker guarantee). The client side
// (`leased(...)`, isolates' memory) is not built yet: its steps have no sources.
import { atMost, budget, crash, defineActor, defineCall, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineVariable, defineZone, failed, lose, oneOf } from "@leostera/feynman";

/** Lost calls and replies, and relay restarts, on one path. */
const faults = budget("faults", 1);
/** Restarts of the source on one path. */
const crashes = budget("crashes", 1);

// ---- Real time, as the guards see it (never read by what an actor decides) ----

/** The relay's lease is running in real time (it may have lost it by restarting). */
const relayWindow = defineVariable("relayWindow", false);
/** Leases the source granted that are on their way to the relay, by the read they answer. */
const grantInFlight = defineVariable("grantInFlight", [] as number[]);
/** Isolates whose sub-lease is running, or on its way to them. */
const subsOut = defineVariable("subsOut", [] as string[]);

// ---- What the properties read: the newest write acknowledged, and the newest value a finished read saw ----

const acked = defineVariable("acked", 0);
const seen = defineVariable("seen", 0);

const Ask = defineCall("Ask", { attempt: Number }, { name: "read", over: "DO RPC" });
const Answer = defineMessage("Answer", { value: Number, sub: Boolean, attempt: Number }, { name: "value (and a sub-lease)", tone: "reply" });
const SourceRead = defineCall("SourceRead", { fresh: Boolean, read: Number }, { name: "read the source", over: "DO RPC" });
const SourceAnswer = defineMessage("SourceAnswer", { value: Number, lease: Boolean, read: Number }, { name: "value (and a lease)", tone: "reply" });
const Revoke = defineCall("Revoke", { lease: Number }, { name: "give the lease back", over: "DO RPC" });
const Returned = defineMessage("Returned", { subs: Boolean, lease: Number }, { name: "given back", tone: "reply" });

const holdWrites = defineRule("holdWrites", { description: "A write to a leased key is shown, and acknowledged, only once every lease on it is over or given back." });
const joinLaterReads = defineRule("joinLaterReads", { description: "A relay's ask without a lease joins only a read of the source that started after it arrived." });
const ignoreRevokedLease = defineRule("ignoreRevokedLease", { description: "A lease that a read brings back after the relay gave its lease back is never taken up." });
const waitForSubs = defineRule("waitForSubs", { description: "A lease given back with sub-leases out ends only when they do." });
const matchLease = defineRule("matchLease", { description: "A lease given back ends only the lease that was asked for, not a later one to the same relay." });
const restartHold = defineRule("restartHold", { description: "A restarted source holds every write until the leases it may have granted before could have ended (its stored horizon)." });
const storeHeldKeys = defineRule("storeHeldKeys", { description: "A restarted source answers no read of a key it held a write on until that hold could have ended." });

const readers = defineZone({ name: "Worker isolates", description: "read the hot key", place: { column: 0, arrange: "stack" } });
const relays = defineZone({ name: "Relay", description: "answers from its lease", place: { column: 1 } });
const shard = defineZone({ name: "Shard", description: "the key's only owner", place: { column: 2 } });

/** Every lease and sub-lease is over in real time. */
const allOver = (m: { get<T>(v: { name: string; initial: T }): T }) => !m.get(relayWindow) && m.get(grantInFlight).length === 0 && m.get(subsOut).length === 0;

export const Source = defineActor({
  name: "Source",
  description: "The shard that owns the key (ReadLeases): grants leases, holds writes, and asks leases back.",
  zone: shard,
  singleton: true,
  initialState: () => ({
    /** Durable: the newest committed version, whether leases may be out (the horizon), and whether a write was held. */
    committed: 0,
    horizon: false,
    heldKey: false,
    /** In memory. */
    started: false,
    shown: 0,
    held: false,
    leaseOut: false,
    /** Numbers each grant. Kept across a restart: it stands for the call a reply goes back to, which a restarted source no longer has. */
    lease: 0,
    asked: false,
    returnedWithSubs: false,
    holdAll: false,
    unsettled: false,
  }),
  display: {
    icon: "database",
    status: (s) => (!s.started ? "starting" : s.held ? `v${s.committed} held; reads see v${s.shown}` : `v${s.shown}${s.leaseOut ? ", leased" : ""}`),
    mood: (s) => (s.held ? "busy" : s.unsettled ? "warn" : "ok"),
    rows: (s) => [
      { group: "stored", label: "version", detail: `v${s.committed}` },
      { group: "stored", label: "horizon", detail: s.horizon ? "leases may be out" : "none", mood: s.horizon ? "info" : "off" },
      { group: "stored", label: "held key", detail: s.heldKey ? "a write was held" : "none", mood: s.heldKey ? "info" : "off" },
      { group: "memory", label: "lease", detail: s.leaseOut ? (s.returnedWithSubs ? "given back, sub-leases out" : s.asked ? "asked back" : "out") : "none" },
    ],
  },
  actions: {
    write: {
      with: [atMost(2)],
      label: "Write the key",
      description: "Commits a new version; with a lease out, holds it (reads keep the old one) and asks the relay for the lease back.",
      sources: ["src/leased/read-leases.ts#ReadLeases.holdUntil", "src/leased/read-leases.ts#ReadLeases.toRevoke"],
      when: (s) => s.started,
      uses: [acked],
      sends: [Revoke],
      run(s, _inputs, ctx) {
        s.committed++;
        const wait = holdWrites.on && (s.leaseOut || (restartHold.on && s.holdAll));
        if (!wait) {
          s.shown = s.committed;
          ctx.set(acked, s.committed);
          return;
        }
        s.held = true;
        s.heldKey = true;
        if (s.leaseOut && !s.asked) {
          s.asked = true;
          ctx.send(ctx.the(Relay), Revoke, { lease: s.lease });
        }
      },
    },
  },
  transitions: {
    start: {
      description: "Starts (again): a horizon in the future holds every write, and a key it held a write on waits too.",
      sources: ["src/leased/read-leases.ts:107-118"],
      when: (s) => !s.started,
      run(s) {
        s.started = true;
        s.shown = s.committed; // the row from before a held write is gone with the memory
        s.holdAll = s.horizon;
        s.unsettled = storeHeldKeys.on && s.heldKey;
      },
    },
    sourceRead: {
      on: SourceRead,
      description: "Answers the visible value, with a lease unless the read is fresh or a write is held. The horizon is stored before the lease leaves.",
      sources: ["src/leased/read-leases.ts#ReadLeases.readableAt", "src/leased/read-leases.ts#ReadLeases.visible", "src/leased/read-leases.ts#ReadLeases.grant"],
      // A restarted source answers no read of a key it held a write on until that hold is over.
      when: (s) => s.started && !s.unsettled,
      uses: [grantInFlight],
      sends: [SourceAnswer],
      run(s, msg, ctx) {
        const lease = !msg.fresh && !s.held;
        if (lease) {
          s.leaseOut = true;
          s.lease++;
          s.asked = false;
          s.returnedWithSubs = false;
          s.horizon = true;
          ctx.set(grantInFlight, [...ctx.get(grantInFlight), msg.read]);
        }
        ctx.reply(SourceAnswer, { value: s.shown, lease, read: msg.read });
      },
    },
    returned: {
      on: Returned,
      description: "The relay gave the lease back: it ends now, or when the sub-leases it gave are over.",
      sources: ["src/leased/read-leases.ts#ReadLeases.returned", "src/leased/read-leases.ts#ReadLeases.toRevoke"],
      run(s, msg) {
        if (!s.leaseOut || (matchLease.on && msg.lease !== s.lease)) return;
        if (msg.subs && waitForSubs.on) s.returnedWithSubs = true;
        else s.leaseOut = false;
      },
    },
    revokeFailed: {
      on: failed(Revoke),
      description: "Asking the lease back threw: the write waits for the lease to end.",
      sources: ["src/leased/read-leases.ts#ReadLeases.returned"],
      run() {},
    },
    leaseEnds: {
      description: "The lease's end, by the source's clock: after the relay's, margin included.",
      sources: ["src/leased/read-leases.ts#ReadLeases.leaseEnd"],
      routine: true,
      uses: [relayWindow, grantInFlight],
      when: (s, m) => s.leaseOut && !m.get(relayWindow) && m.get(grantInFlight).length === 0,
      run(s) {
        s.leaseOut = false;
      },
    },
    subsEnd: {
      description: "A lease given back with sub-leases out ends when they do.",
      sources: ["src/leased/read-leases.ts#ReadLeases.returned"],
      routine: true,
      uses: [subsOut],
      when: (s, m) => s.leaseOut && s.returnedWithSubs && m.get(subsOut).length === 0,
      run(s) {
        s.leaseOut = false;
      },
    },
    horizonPasses: {
      description: "Every lease from before the restart (its stored horizon, plus the skew) could have ended.",
      sources: ["src/leased/read-leases.ts#ReadLeases.readableAt", "src/leased/read-leases.ts#ReadLeases.sweep"],
      routine: true,
      uses: [relayWindow, grantInFlight, subsOut],
      when: (s, m) => s.started && (s.holdAll || s.unsettled || (s.horizon && !s.leaseOut)) && allOver(m),
      run(s) {
        s.holdAll = false;
        s.unsettled = false;
        if (!s.leaseOut) s.horizon = false;
      },
    },
    show: {
      description: "Nothing holds the write any more: reads see it, and it is acknowledged.",
      sources: ["src/leased/read-leases.ts#ReadLeases.visible", "src/leased/read-leases.ts#ReadLeases.release"],
      when: (s) => s.held && !s.leaseOut && !s.holdAll,
      uses: [acked],
      run(s, _, ctx) {
        s.held = false;
        s.heldKey = false;
        s.shown = s.committed;
        ctx.set(acked, s.committed);
      },
    },
    sourceReadLost: lose(SourceRead, { spend: faults }),
    returnedLost: lose(Returned, { fails: Revoke, spend: faults }),
    crash: crash({ keeps: ["committed", "horizon", "heldKey", "lease"], spend: crashes }),
  },
});

export const Relay = defineActor({
  name: "Relay",
  description: "A relay of the key (RelayReads): answers from its lease, or shares one read of the source among the asks that wait.",
  zone: relays,
  singleton: true,
  initialState: () => ({
    lease: null as number | null,
    quiet: false,
    /** Numbers its reads of the source: stands for the call a reply goes back to, which a restarted relay no longer has. */
    reads: 0,
    reading: null as null | { fresh: boolean; revoked: boolean; read: number },
    joined: [] as { reader: string; attempt: number }[],
    next: [] as { reader: string; attempt: number }[],
  }),
  display: {
    icon: "server",
    status: (s) => (s.lease !== null ? `leased v${s.lease}` : s.reading ? `reading the source for ${s.joined.length} ask(s)` : s.quiet ? "quiet: reads through" : "no lease"),
    mood: (s) => (s.lease !== null ? "ok" : s.reading ? "busy" : "off"),
  },
  actions: {
    renew: {
      with: [atMost(1)],
      label: "Renew the lease",
      description: "In the lease's last RENEW_MS, reads the source again in the background.",
      sources: ["src/leased/relay-reads.ts#RelayReads.read", "src/leased/relay-reads.ts#RelayReads.start"],
      when: (s) => s.lease !== null && s.reading === null && !s.quiet,
      sends: [SourceRead],
      run(s, _inputs, ctx) {
        s.reading = { fresh: false, revoked: false, read: ++s.reads };
        ctx.send(ctx.the(Source), SourceRead, { fresh: false, read: s.reads });
      },
    },
  },
  transitions: {
    ask: {
      on: Ask,
      description: "From the lease, with a sub-lease; else joins a read of the source that started after the ask arrived, or starts one.",
      sources: ["src/leased/relay-reads.ts#RelayReads.read", "src/leased/relay-reads.ts#RelayReads.fromLease", "src/leased/relay-reads.ts#RelayReads.readAfter", "src/leased/relay-reads.ts#RelayReads.fresh"],
      uses: [subsOut],
      sends: [Answer, SourceRead],
      run(s, msg, ctx) {
        const ask = { reader: ctx.delivery!.from.id, attempt: msg.attempt };
        if (s.lease !== null && !s.quiet) {
          ctx.set(subsOut, [...ctx.get(subsOut), ask.reader]);
          return ctx.reply(Answer, { value: s.lease, sub: true, attempt: msg.attempt });
        }
        if (s.reading === null) {
          s.reading = { fresh: s.quiet, revoked: false, read: ++s.reads };
          s.joined.push(ask);
          return ctx.send(ctx.the(Source), SourceRead, { fresh: s.quiet, read: s.reads });
        }
        // A read in flight started before this ask arrived: wait for the next one.
        if (joinLaterReads.on) s.next.push(ask);
        else s.joined.push(ask);
      },
    },
    sourceAnswer: {
      on: SourceAnswer,
      description: "Takes up the lease (unless it gave its lease back meanwhile) and answers the asks that joined the read; the asks that came after start the next read.",
      sources: ["src/leased/relay-reads.ts#RelayReads.start", "src/leased/relay-reads.ts#RelayReads.readAfter"],
      uses: [relayWindow, grantInFlight, subsOut],
      sends: [Answer, SourceRead],
      run(s, msg, ctx) {
        if (msg.lease)
          ctx.set(
            grantInFlight,
            ctx.get(grantInFlight).filter((r) => r !== msg.read),
          );
        if (!s.reading || s.reading.read !== msg.read) return; // a restarted relay: the read was not its own
        if (msg.lease && (!s.reading.revoked || !ignoreRevokedLease.on)) {
          s.lease = msg.value;
          ctx.set(relayWindow, true);
        }
        if (!msg.lease) s.quiet = true;
        const sub = s.lease !== null && !s.quiet;
        for (const ask of s.joined) {
          if (sub) ctx.set(subsOut, [...ctx.get(subsOut), ask.reader]);
          ctx.send(
            ctx.all(Reader).find((r) => r.id === ask.reader)!,
            Answer,
            { value: sub ? s.lease! : msg.value, sub, attempt: ask.attempt },
          );
        }
        s.joined = s.next;
        s.next = [];
        s.reading = null;
        if (s.joined.length) {
          s.reading = { fresh: s.quiet, revoked: false, read: ++s.reads };
          ctx.send(ctx.the(Source), SourceRead, { fresh: s.quiet, read: s.reads });
        }
      },
    },
    sourceReadFailed: {
      on: failed(SourceRead),
      description: "The read of the source threw: the asks that joined it fail too.",
      sources: ["src/leased/relay-reads.ts#RelayReads.start"],
      uses: [grantInFlight],
      run(s, f, ctx) {
        // Its lease, if one was granted, never arrived: it is over by the time the call fails.
        ctx.set(
          grantInFlight,
          ctx.get(grantInFlight).filter((r) => r !== f.payload.read),
        );
        if (!s.reading || s.reading.read !== f.payload.read) return;
        s.joined = s.next;
        s.next = [];
        s.reading = null;
      },
    },
    revoke: {
      on: Revoke,
      description: "Gives the lease back: answers no ask from it, takes up no lease a read in flight brings, reads through for QUIET_MS, and says whether sub-leases it gave are still out.",
      sources: ["src/leased/relay-reads.ts#RelayReads.revoke"],
      sends: [Returned],
      run(s, msg, ctx) {
        s.lease = null;
        s.quiet = true;
        if (s.reading) s.reading.revoked = true;
        // Its subsUntil, by its own clock: whether some sub-lease it gave may still run.
        ctx.reply(Returned, { subs: ctx.get(subsOut).length > 0, lease: msg.lease });
      },
      uses: [subsOut],
    },
    quietEnds: {
      description: "QUIET_MS after the revoke: it leases again.",
      sources: ["src/leased/relay-reads.ts#RelayReads.read"],
      after: "QUIET_MS (10 s)",
      routine: true,
      when: (s) => s.quiet && s.reading === null,
      run(s) {
        s.quiet = false;
      },
    },
    leaseEnds: {
      description: "Its lease's end, by its clock: after every sub-lease it gave.",
      sources: ["src/leased/relay-reads.ts#RelayReads.fromLease"],
      routine: true,
      when: (_s, m) => m.get(relayWindow) && m.get(subsOut).length === 0,
      uses: [relayWindow, subsOut],
      run(s, _, ctx) {
        s.lease = null;
        ctx.set(relayWindow, false);
      },
    },
    askLost: lose(Ask, { spend: faults }),
    sourceAnswerLost: lose(SourceAnswer, { fails: SourceRead, spend: faults }),
    revokeLost: lose(Revoke, { spend: faults }),
    // A relay keeps nothing: a restarted one has no lease (its lease's time still runs out).
    crash: crash({ keeps: ["reads"], spend: faults }),
  },
});

export const Reader = defineActor({
  name: "Reader",
  description: "A Worker isolate reading the hot key: from its sub-lease while it lasts, else through the relay.",
  zone: readers,
  initialState: () => ({ me: "", attempt: 0, sub: null as number | null, reading: false, floorAcked: 0, floorSeen: 0, last: null as number | null, stale: false, wentBack: false }),
  display: {
    icon: "code",
    names: ["Isolate A", "Isolate B"],
    status: (s) => (s.reading ? "reading" : s.sub !== null ? `sub-lease on v${s.sub}` : s.last !== null ? `read v${s.last}` : "idle"),
    mood: (s) => (s.stale || s.wentBack ? "bad" : s.reading ? "busy" : "ok"),
  },
  actions: {
    read: {
      with: [atMost(2)],
      label: "Read the key",
      description: "From its sub-lease while it lasts; else asks the relay.",
      when: (s) => !s.reading,
      uses: [acked, seen],
      sends: [Ask],
      starts: ["askTimeout"],
      run(s, _inputs, ctx) {
        s.me = ctx.id; // for subLapses' guard
        s.floorAcked = ctx.get(acked);
        s.floorSeen = ctx.get(seen);
        if (s.sub !== null) return finish(s, s.sub, ctx);
        s.reading = true;
        s.attempt++;
        ctx.send(ctx.the(Relay), Ask, { attempt: s.attempt });
        ctx.start("askTimeout");
      },
    },
  },
  transitions: {
    answer: {
      on: Answer,
      description: "Gets the value; with a sub-lease, keeps it for the next reads until the sub-lease ends.",
      uses: [subsOut, seen],
      starts: ["subEnds"],
      run(s, msg, ctx) {
        if (!s.reading || msg.attempt !== s.attempt) {
          // Late, after the read gave up: a sub-lease in it is not taken up.
          ctx.set(
            subsOut,
            ctx.get(subsOut).filter((r) => r !== ctx.id),
          );
          return;
        }
        ctx.cancel("askTimeout");
        // A sub-lease times from before the ask: it may have run out on the way.
        if (msg.sub && ctx.get(subsOut).includes(ctx.id)) {
          s.sub = msg.value;
          ctx.start("subEnds");
        } else
          ctx.set(
            subsOut,
            ctx.get(subsOut).filter((r) => r !== ctx.id),
          );
        finish(s, msg.value, ctx);
      },
    },
    askFailed: {
      on: failed(Ask),
      description: "The relay call threw: the read fails (the code reads around the relay, at the shard).",
      uses: [subsOut],
      run(s, f, ctx) {
        if (!s.reading || f.payload.attempt !== s.attempt) return;
        s.reading = false;
        ctx.cancel("askTimeout");
      },
    },
    askTimeout: {
      description: "No answer in time (the relay restarted, or the answer was lost): the read fails, and a sub-lease on its way is never taken up.",
      after: "the call's timeout",
      uses: [subsOut],
      run(s, _, ctx) {
        if (!s.reading) return;
        s.reading = false;
        if (s.sub === null)
          ctx.set(
            subsOut,
            ctx.get(subsOut).filter((r) => r !== ctx.id),
          );
      },
    },
    subEnds: {
      description: "Its sub-lease ends, by its clock: before the relay's lease.",
      after: "the sub-lease, less the margin",
      routine: true,
      uses: [subsOut],
      run(s, _, ctx) {
        s.sub = null;
        ctx.set(
          subsOut,
          ctx.get(subsOut).filter((r) => r !== ctx.id),
        );
      },
    },
    subLapses: {
      description: "A sub-lease no read took up (lost, or late) runs out: it was timed from before the ask.",
      uses: [subsOut],
      when: (s, m) => s.sub === null && m.get(subsOut).includes(s.me),
      run(_s, _, ctx) {
        ctx.set(
          subsOut,
          ctx.get(subsOut).filter((r) => r !== ctx.id),
        );
      },
    },
    answerLost: lose(Answer, { spend: faults }),
  },
});

/** A read finishes with `value`: checked against what was acknowledged, and seen, when it started. */
function finish(
  s: { reading: boolean; floorAcked: number; floorSeen: number; last: number | null; stale: boolean; wentBack: boolean },
  value: number,
  ctx: { get<T>(v: { name: string; initial: T }): T; set<T>(v: { name: string; initial: T }, value: T): void },
) {
  s.reading = false;
  s.last = value;
  if (value < s.floorAcked) s.stale = true;
  if (value < s.floorSeen) s.wentBack = true;
  ctx.set(seen, Math.max(ctx.get(seen), value));
}

const freshReads = defineProperty("freshReads", {
  description: "A read sees every write acknowledged before it started.",
  always: (w) => w.all(Reader).every((r) => !r.state.stale),
  needs: [holdWrites, joinLaterReads, ignoreRevokedLease, waitForSubs, matchLease, restartHold],
});

const neverGoBack = defineProperty("neverGoBack", {
  description: "A read sees at least what an earlier finished read saw, through any path.",
  always: (w) => w.all(Reader).every((r) => !r.state.wentBack),
  needs: [storeHeldKeys],
});

const leasesNest = defineProperty("leasesNest", {
  description: "While the relay answers from a lease, the source counts one (or, restarted, holds writes until leases from before are over).",
  always: (w) => w.the(Relay).state.lease === null || w.the(Source).state.leaseOut || w.the(Source).state.holdAll || !w.the(Source).state.started,
});

const writesShown = defineProperty("writesShown", {
  description: "A held write is eventually shown, and acknowledged.",
  leadsTo: { whenever: (w) => w.the(Source).state.held, then: (w) => !w.the(Source).state.held },
});

/** Isolate A reads through the relay, which takes a lease and gives A a sub-lease. */
function leaseToA(s: Parameters<Parameters<typeof defineScenario>[1]["run"]>[0]) {
  const a = s.spawn(Reader, "Isolate A");
  s.step(s.the(Source), "start");
  s.act(a, "read");
  s.deliver(s.the(Relay), Ask);
  s.deliver(s.the(Source), SourceRead);
  s.deliver(s.the(Relay), SourceAnswer);
  s.deliver(a, Answer);
  return a;
}

const hotRead = defineScenario("Reads from the lease", {
  description: "Isolate A's read reaches the relay, which reads the source and gets a lease; A gets a sub-lease, and its next read is answered from memory.",
  run(s) {
    const a = leaseToA(s);
    s.act(a, "read");
  },
});

const heldWrite = defineScenario("A write waits for the lease", {
  description: "The relay holds a lease and A a sub-lease. A write is held, and asks the relay for the lease back; the relay says a sub-lease is out, so the write is shown once A's sub-lease is over.",
  run(s) {
    const a = leaseToA(s);
    s.act(s.the(Source), "write");
    s.deliver(s.the(Relay), Revoke);
    s.deliver(s.the(Source), Returned);
    s.fire(a, "subEnds");
    s.step(s.the(Source), "subsEnd");
    s.step(s.the(Source), "show");
  },
});

export default defineSpec({
  name: "Leased reads",
  description: "How a hot key is read through a relay's lease and isolates' sub-leases, while every read stays fresh and never goes back, across revokes and restarts.",
  actors: [Source, Relay, Reader],
  properties: [freshReads, neverGoBack, leasesNest, writesShown],
  scenarios: [hotRead, heldWrite],
  finalState: { description: "Nothing is in flight, no read waits, and no write is held.", when: (w) => w.quiet && !w.the(Source).state.held && w.all(Reader).every((r) => !r.state.reading) },
});
