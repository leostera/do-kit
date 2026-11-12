// RFD0008: an all-or-nothing call over keys in several shards. The shard of the call's smallest
// key (the primary) records the outcome; every other shard prepares its share first and holds its
// keys, then applies or drops it once it hears the outcome, from the client or, past its hold's
// deadline, by asking the primary, which records an abort if it has no outcome yet.
//
// Specs have no clock: a hold's deadline may pass at any moment (the `recover` step), which covers
// a slow client as well as a dead one. Shards keep everything that matters in storage, so a crash
// keeps it; the client keeps nothing, so its crash is a caller that died mid-call. Every shard call
// the client makes is a DO RPC whose failure reaches it (a lost request or a lost answer), and the
// client handles each failure as atomicAcross does. A prepare or a write that finds the keys held
// stays in flight until the hold ends, as the RPC waiting on the hold does. A prepare that finds
// them held by an older call gives way instead (wait-die): it holds nothing and answers contended,
// and the client aborts and makes the call again. Here calls come one after another, so a hold
// another call left is always older; RFD0008 has why age keeps concurrent calls from waiting in a
// ring.
//
// Each rule is needed; `--without` shows how each property breaks:
// - youngerGivesWay: a second call's prepare overwrites the first's hold (needs two calls, repeats=2);
// - writesWait: a write lands on a held key;
// - commitHonoursOutcome: a commit after a recovery's abort applies at the primary only;
// - recoveryRecordsAbort: an answered abort the primary did not record lets the commit through;
// - forgetAfterAllResolved: forgetting with a share unresolved lets its recovery abort a commit.
import { type Ctx, atMost, budget, crash, defineActor, defineCall, defineLimit, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineVariable, defineZone, failed, input, lose, oneOf } from "@leostera/feynman";

type Outcome = "commit" | "abort";
/** Where the client's call is refused, if anywhere: by no shard, the primary, or the first other shard. */
type RefusedBy = "none" | "primary" | "other";
/** What the caller was told: committed, refused, or an error (the outcome unknown to it); or "retry", the call given way and made again. */
type Told = Outcome | "error" | "retry";

const faults = budget("faults", 1);
const repeats = defineVariable("repeats", 1);
const OutcomeSchema = oneOf("commit", "abort");

const Prepare = defineCall("Prepare", { tx: String, refuse: Boolean }, { name: "prepare", over: "DO RPC" });
const Prepared = defineMessage("Prepared", { tx: String, shard: Number }, { name: "prepared", over: "DO RPC", tone: "reply" });
const PrepareRefused = defineMessage("PrepareRefused", { tx: String, shard: Number }, { name: "refused", over: "DO RPC", tone: "refused" });
const Contended = defineMessage("Contended", { tx: String, shard: Number }, { name: "contended", over: "DO RPC", tone: "refused" });
const Commit = defineCall("Commit", { tx: String, refuse: Boolean }, { name: "commit", over: "DO RPC" });
const Committed = defineMessage("Committed", { tx: String }, { name: "committed", over: "DO RPC", tone: "reply" });
const NotCommitted = defineMessage("NotCommitted", { tx: String }, { name: "not committed", over: "DO RPC", tone: "refused" });
const Decide = defineMessage("Decide", { tx: String }, { name: "record abort", over: "DO RPC" });
const Resolve = defineCall("Resolve", { tx: String, outcome: OutcomeSchema }, { name: "resolve", over: "DO RPC" });
const Resolved = defineMessage("Resolved", { tx: String, shard: Number }, { name: "resolved", over: "DO RPC", tone: "reply" });
const Forget = defineMessage("Forget", { tx: String }, { name: "forget", over: "DO RPC" });
const AskOutcome = defineCall("AskOutcome", { tx: String }, { name: "outcome?", over: "DO RPC" });
const OutcomeIs = defineMessage("OutcomeIs", { tx: String, outcome: OutcomeSchema }, { name: "outcome", over: "DO RPC", tone: "reply" });
const Write = defineMessage("Write", {}, { name: "write", over: "DO RPC" });

const youngerGivesWay = defineRule("youngerGivesWay", { description: "A prepare that finds its keys held by an older call holds nothing and answers contended; the client aborts and calls again." });
const writesWait = defineRule("writesWait", { description: "A write to a held key waits until the hold ends." });
const commitHonoursOutcome = defineRule("commitHonoursOutcome", { description: "The primary's commit answers an outcome recorded already instead of deciding again." });
const recoveryRecordsAbort = defineRule("recoveryRecordsAbort", { description: "Asked for an outcome it has not recorded, the primary records an abort before it answers one." });
const forgetAfterAllResolved = defineRule("forgetAfterAllResolved", { description: "The client asks the primary to forget an outcome only once every other shard has answered its resolve." });

const workers = defineZone({ name: "Workers", description: "the caller, which keeps nothing" });
const objects = defineZone({ name: "Durable Objects", description: "shards, which keep their holds and outcomes in storage" });

const holdDeadline = defineLimit({ name: "Hold deadline", value: 5, unit: "seconds", sources: ["src/sharded/atomic.ts#ATOMIC_HOLD_MS"] });

type ClientState = {
  tx: string | null;
  refuse: RefusedBy;
  /** Shards (by index) that answered prepared, refused, or contended, and whether every prepare has answered. */
  prepared: number[];
  refusedBy: number[];
  contendedBy: number[];
  answered: number;
  /** The call is past its prepares: committing, resolving, or done. */
  decided: boolean;
  outcome: Outcome | null;
  resolved: number;
  resolveFailed: boolean;
  told: Told | null;
};

export const Client = defineActor({
  name: "Client",
  description: "The Worker making the atomic call (ShardedClient.atomically).",
  zone: workers,
  singleton: true,
  initialState: (): ClientState => ({ tx: null, refuse: "none", prepared: [], refusedBy: [], contendedBy: [], answered: 0, decided: false, outcome: null, resolved: 0, resolveFailed: false, told: null }),
  display: {
    icon: "code",
    status: (s) => (s.told ? `told: ${s.told}` : s.tx ? (s.decided ? "committing" : `${s.answered} prepare(s) answered`) : "idle"),
    mood: (s) => (s.told === "commit" ? "ok" : s.told ? "warn" : s.tx ? "busy" : "off"),
  },
  actions: {
    call: {
      with: [atMost(repeats)],
      label: "Atomic call",
      description: "Asks every other shard to prepare its share; one of them, or the primary, may refuse.",
      inputs: { refuse: input(String, { examples: ["none", "primary", "other"] }) },
      when: (s) => s.tx === null || s.told !== null,
      sources: ["src/sharded/client.ts#ShardedClient.atomically", "src/sharded/client.ts#ShardedClient.atomicAcross"],
      sends: [Prepare],
      run(s, { refuse }, ctx) {
        Object.assign(s, { tx: ctx.fresh("tx"), refuse: refuse as RefusedBy, prepared: [], refusedBy: [], contendedBy: [], answered: 0, decided: false, outcome: null, resolved: 0, resolveFailed: false, told: null });
        ctx.all(Other).forEach((o, i) => ctx.send(o, Prepare, { tx: s.tx!, refuse: s.refuse === "other" && i === 0 }));
      },
    },
  },
  transitions: {
    prepared: {
      on: Prepared,
      description: "Counts a prepared share; once every prepare has answered, commits or aborts.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross"],
      sends: [Commit, Decide, Resolve],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx || s.decided) return;
        s.prepared.push(msg.shard);
        s.answered++;
        afterPrepares(s, ctx);
      },
    },
    prepareRefused: {
      on: PrepareRefused,
      description: "Counts a refused share; once every prepare has answered, aborts.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross"],
      sends: [Commit, Decide, Resolve],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx || s.decided) return;
        s.refusedBy.push(msg.shard);
        s.answered++;
        afterPrepares(s, ctx);
      },
    },
    contended: {
      on: Contended,
      description: "Counts a share that gave way to an older call; once every prepare has answered, aborts and calls again.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross", "src/sharded/client.ts#ShardedClient.atomically"],
      sends: [Commit, Decide, Resolve],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx || s.decided) return;
        s.contendedBy.push(msg.shard);
        s.answered++;
        afterPrepares(s, ctx);
      },
    },
    prepareFailed: {
      on: failed(Prepare),
      description: "A prepare threw: records an abort at the primary and throws; shards that hold end at their deadline.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross", "src/sharded/client.ts#ShardedClient.decideAbort"],
      sends: [Decide],
      run(s, failure, ctx) {
        if (failure.payload.tx !== s.tx || s.decided) return;
        s.decided = true;
        ctx.send(ctx.the(Primary), Decide, { tx: failure.payload.tx });
        s.told = "error";
      },
    },
    committed: {
      on: Committed,
      description: "Committed: every other shard is told to apply its share.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross", "src/sharded/client.ts#ShardedClient.resolveAll"],
      sends: [Resolve, Forget],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx || s.outcome) return;
        resolveAll(s, "commit", ctx);
        s.told = "commit";
        if (!forgetAfterAllResolved.on) ctx.send(ctx.the(Primary), Forget, { tx: msg.tx });
      },
    },
    notCommitted: {
      on: NotCommitted,
      description: "The primary refused, or a recovery aborted first: the shards drop their holds.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross", "src/sharded/client.ts#ShardedClient.resolveAll"],
      sends: [Resolve],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx || s.outcome) return;
        resolveAll(s, "abort", ctx);
        s.told = "abort";
      },
    },
    commitFailed: {
      on: failed(Commit),
      description: "The commit's answer is lost: asks the primary for the outcome, as a shard would.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross", "src/sharded/client.ts#ShardedClient.atomicOutcome"],
      sends: [AskOutcome],
      run(s, failure, ctx) {
        if (failure.payload.tx !== s.tx) return;
        ctx.send(ctx.the(Primary), AskOutcome, { tx: failure.payload.tx });
      },
    },
    outcomeIs: {
      on: OutcomeIs,
      description: "The outcome after a lost commit answer: the shards are told it; the caller hears commit, or an error.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross", "src/sharded/client.ts#ShardedClient.resolveAll"],
      sends: [Resolve],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx || s.outcome) return;
        resolveAll(s, msg.outcome, ctx);
        s.resolveFailed = true; // this path never forgets
        s.told = msg.outcome === "commit" ? "commit" : "error";
      },
    },
    askOutcomeFailed: {
      on: failed(AskOutcome),
      description: "No outcome either: throws; the shards that hold end at their deadline.",
      sources: ["src/sharded/client.ts#ShardedClient.atomicAcross"],
      run(s, failure) {
        if (failure.payload.tx === s.tx && !s.outcome) s.told = "error";
      },
    },
    resolved: {
      on: Resolved,
      description: "A shard resolved its share; once all did, after a commit, the primary may forget the outcome.",
      sources: ["src/sharded/client.ts#ShardedClient.resolveAll", "src/sharded/client.ts#ShardedClient.atomicAcross"],
      sends: [Forget],
      run(s, msg, ctx) {
        if (msg.tx !== s.tx) return;
        s.resolved++;
        if (forgetAfterAllResolved.on && s.outcome === "commit" && !s.resolveFailed && s.resolved === s.prepared.length) ctx.send(ctx.the(Primary), Forget, { tx: msg.tx });
      },
    },
    resolveFailed: {
      on: failed(Resolve),
      description: "A resolve threw: that shard recovers at its deadline, and the outcome is kept for it.",
      sources: ["src/sharded/client.ts#ShardedClient.resolveAll"],
      run(s, failure) {
        if (failure.payload.tx === s.tx) s.resolveFailed = true;
      },
    },
    preparedLost: lose(Prepared, { fails: Prepare, spend: faults }),
    prepareRefusedLost: lose(PrepareRefused, { fails: Prepare, spend: faults }),
    contendedLost: lose(Contended, { fails: Prepare, spend: faults }),
    committedLost: lose(Committed, { fails: Commit, spend: faults }),
    notCommittedLost: lose(NotCommitted, { fails: Commit, spend: faults }),
    resolvedLost: lose(Resolved, { fails: Resolve, spend: faults }),
    outcomeLost: lose(OutcomeIs, { fails: AskOutcome, spend: faults }),
    crash: crash({ spend: faults }),
  },
});

type ClientCtx = Ctx<typeof Commit | typeof Decide | typeof Resolve>;

/** After the last prepare answers: refused or given way anywhere, abort (recorded at the primary first); else commit at the primary. */
function afterPrepares(s: ClientState, ctx: ClientCtx): void {
  if (s.answered < ctx.all(Other).length) return;
  s.decided = true;
  if (s.refusedBy.length > 0 || s.contendedBy.length > 0) {
    ctx.send(ctx.the(Primary), Decide, { tx: s.tx! });
    resolveAll(s, "abort", ctx);
    // Refused wins: the call would be refused again. Given way, the client calls again (the next `call`).
    s.told = s.refusedBy.length > 0 ? "abort" : "retry";
    return;
  }
  ctx.send(ctx.the(Primary), Commit, { tx: s.tx!, refuse: s.refuse === "primary" });
}

/** Tells every shard that prepared the outcome. */
function resolveAll(s: ClientState, outcome: Outcome, ctx: ClientCtx): void {
  s.outcome = outcome;
  for (const i of s.prepared) ctx.send(ctx.all(Other)[i]!, Resolve, { tx: s.tx!, outcome });
}

export const Primary = defineActor({
  name: "Primary",
  description: "The shard of the call's smallest key: it checks its own share and records the outcome.",
  zone: objects,
  singleton: true,
  initialState: () => ({ outcomes: {} as Record<string, Outcome>, applied: [] as string[] }),
  display: {
    icon: "database",
    status: (s) => `${Object.keys(s.outcomes).length} outcome(s)`,
    rows: (s) => Object.entries(s.outcomes).map(([tx, o]) => ({ label: tx, detail: o, mood: o === "commit" ? "ok" : "warn" })),
  },
  transitions: {
    commit: {
      on: Commit,
      description: "Checks its share; records commit and applies it in one transaction, unless an outcome is recorded already or it refuses.",
      sources: ["src/sharded/index.ts#ShardedObject.atomicCommit", "src/sharded/index.ts#ShardedObject.recordOutcome"],
      sends: [Committed, NotCommitted],
      run(s, msg, ctx) {
        const recorded = s.outcomes[msg.tx];
        if (recorded && commitHonoursOutcome.on) return recorded === "commit" ? ctx.reply(Committed, { tx: msg.tx }) : ctx.reply(NotCommitted, { tx: msg.tx });
        if (msg.refuse) {
          s.outcomes[msg.tx] ??= "abort";
          return ctx.reply(NotCommitted, { tx: msg.tx });
        }
        s.outcomes[msg.tx] = "commit";
        s.applied.push(msg.tx);
        ctx.reply(Committed, { tx: msg.tx });
      },
    },
    decide: {
      on: Decide,
      description: "Records an abort, unless an outcome is recorded already.",
      sources: ["src/sharded/index.ts#ShardedObject.atomicStep", "src/sharded/index.ts#ShardedObject.recordOutcome"],
      run(s, msg) {
        s.outcomes[msg.tx] ??= "abort";
      },
    },
    askOutcome: {
      on: AskOutcome,
      description: "Answers the recorded outcome; with none yet, records an abort and answers that.",
      sources: ["src/sharded/index.ts#ShardedObject.atomicStep", "src/sharded/index.ts#ShardedObject.recordOutcome"],
      sends: [OutcomeIs],
      run(s, msg, ctx) {
        const outcome = s.outcomes[msg.tx] ?? "abort";
        if (recoveryRecordsAbort.on) s.outcomes[msg.tx] = outcome;
        ctx.reply(OutcomeIs, { tx: msg.tx, outcome });
      },
    },
    forget: {
      on: Forget,
      description: "Deletes the outcome: no shard holds for the call any more.",
      sources: ["src/sharded/index.ts#ShardedObject.atomicStep"],
      run(s, msg) {
        delete s.outcomes[msg.tx];
      },
    },
    commitLost: lose(Commit, { spend: faults }),
    decideLost: lose(Decide, { spend: faults }),
    askOutcomeLost: lose(AskOutcome, { spend: faults }),
    forgetLost: lose(Forget, { spend: faults }),
    crash: crash({ keeps: ["outcomes", "applied"], spend: faults }),
  },
});

type OtherState = { value: number; hold: { tx: string } | null; prepared: string[]; asks: number; applied: string[]; dropped: string[] };

/** Ends a hold: the share is applied (the value moves) or dropped. Writes and prepares waiting on it can then run. */
function end(s: OtherState, outcome: Outcome): void {
  const tx = s.hold!.tx;
  if (outcome === "commit") {
    s.value += 10;
    s.applied.push(tx);
  } else s.dropped.push(tx);
  s.hold = null;
}

export const Other = defineActor({
  name: "Other",
  description: "Another shard the call touches: it holds its keys from its prepare until it hears the outcome.",
  zone: objects,
  initialState: (): OtherState => ({
    value: 0,
    hold: null,
    /** The calls this shard prepared (bookkeeping for the properties). */
    prepared: [],
    /** Times it has asked the primary about the current hold (in memory): the alarm retries for as long as it holds, and one more ask than the faults checked stands for that. */
    asks: 0,
    applied: [],
    dropped: [],
  }),
  display: {
    icon: "database",
    names: ["Shard 1", "Shard 2"],
    status: (s) => (s.hold ? `holding for ${s.hold.tx}` : `value ${s.value}`),
    mood: (s) => (s.hold ? "busy" : "ok"),
  },
  transitions: {
    prepare: {
      on: Prepare,
      description: "Keys held by an older call: gives way, holding nothing. Else checks its share and, if it would apply, keeps it and holds the keys (writes to them now wait).",
      sources: ["src/sharded/index.ts#ShardedObject.atomicPrepare", "src/sharded/index.ts#ShardedObject.whenFree", "src/sharded/index.ts#ShardedObject.giveWayToOlder"],
      sends: [Prepared, PrepareRefused, Contended],
      run(s, msg, ctx) {
        const shard = ctx.all(Other).findIndex((o) => o.id === ctx.id);
        if (youngerGivesWay.on && s.hold !== null && s.hold.tx !== msg.tx) return ctx.reply(Contended, { tx: msg.tx, shard });
        if (msg.refuse) return ctx.reply(PrepareRefused, { tx: msg.tx, shard });
        s.hold = { tx: msg.tx };
        s.prepared.push(msg.tx);
        s.asks = 0;
        ctx.reply(Prepared, { tx: msg.tx, shard });
      },
    },
    resolve: {
      on: Resolve,
      description: "Applies its share or drops it, and releases the keys; a resolve for a hold already ended does nothing.",
      sources: ["src/sharded/index.ts#ShardedObject.atomicResolve"],
      sends: [Resolved],
      run(s, msg, ctx) {
        if (s.hold?.tx === msg.tx) end(s, msg.outcome);
        ctx.reply(Resolved, { tx: msg.tx, shard: ctx.all(Other).findIndex((o) => o.id === ctx.id) });
      },
    },
    outcomeIs: {
      on: OutcomeIs,
      description: "The primary's outcome, asked for after the deadline: applied or dropped as from the client.",
      sources: ["src/sharded/index.ts#ShardedObject.recoverDue"],
      run(s, msg) {
        if (s.hold?.tx === msg.tx) end(s, msg.outcome);
      },
    },
    askOutcomeFailed: {
      on: failed(AskOutcome),
      description: "The primary did not answer: the hold stays, and the alarm asks again.",
      sources: ["src/sharded/index.ts#ShardedObject.recoverDue"],
      run() {},
    },
    write: {
      on: Write,
      when: (s) => !writesWait.on || s.hold === null,
      description: "Another write to the keys: it applies once nothing holds them.",
      sources: ["src/sharded/index.ts#ShardedObject.update", "src/sharded/index.ts#ShardedObject.waitForHold"],
      run(s) {
        s.value++;
      },
    },
    recover: {
      description: "The hold's deadline has passed: asks the primary for the outcome.",
      after: "5 seconds",
      when: (s) => s.hold !== null && s.asks < 3,
      sources: ["src/sharded/index.ts#ShardedObject.recoverHolds", "src/sharded/index.ts#ShardedObject.recoverDue"],
      sends: [AskOutcome],
      run(s, _, ctx) {
        s.asks++;
        ctx.send(ctx.the(Primary), AskOutcome, { tx: s.hold!.tx });
      },
    },
    prepareLost: lose(Prepare, { spend: faults }),
    resolveLost: lose(Resolve, { spend: faults }),
    outcomeLost: lose(OutcomeIs, { fails: AskOutcome, spend: faults }),
    crash: crash({ keeps: ["value", "hold", "prepared", "applied", "dropped"], spend: faults }),
  },
});

export const Writer = defineActor({
  name: "Writer",
  description: "Another caller writing the keys the call holds.",
  zone: workers,
  singleton: true,
  initialState: () => ({ sent: 0 }),
  display: { icon: "code", status: (s) => `${s.sent} write(s)` },
  actions: {
    write: {
      with: [atMost(repeats)],
      label: "Write a held key",
      description: "Writes one of the keys the first other shard holds.",
      sources: ["src/sharded/index.ts#ShardedObject.update"],
      sends: [Write],
      run(s, _i, ctx) {
        s.sent++;
        ctx.send(ctx.all(Other)[0]!, Write, {});
      },
    },
  },
});

type View = { the(a: typeof Primary): { state: { outcomes: Record<string, Outcome>; applied: string[] } } };
/** Whether the primary committed (and applied) call `tx`: it keeps that even once it forgets the outcome. */
const committed = (w: View, tx: string) => w.the(Primary).state.applied.includes(tx);

const allOrNothing = defineProperty("allOrNothing", {
  description: "A share is applied only if the primary committed, and dropped only if it did not; while a shard holds for a call, the primary's record of it is true.",
  always: (w) => {
    const p = w.the(Primary).state;
    if (Object.entries(p.outcomes).some(([tx, o]) => o === "commit" && !p.applied.includes(tx))) return false;
    const holding = new Set(w.all(Other).flatMap((o) => (o.state.hold ? [o.state.hold.tx] : [])));
    if (Object.entries(p.outcomes).some(([tx, o]) => o === "abort" && p.applied.includes(tx) && holding.has(tx))) return false;
    return w.all(Other).every((o) => o.state.applied.every((tx) => committed(w, tx)) && o.state.dropped.every((tx) => !committed(w, tx)));
  },
  needs: [commitHonoursOutcome, recoveryRecordsAbort, forgetAfterAllResolved],
});

const oneOutcome = defineProperty("oneOutcome", {
  description: "Once the primary records an outcome for a call, it never changes (it may be forgotten).",
  everyStep: (before, after) => Object.entries(before.the(Primary).state.outcomes).every(([tx, o]) => (after.the(Primary).state.outcomes[tx] ?? o) === o),
  needs: [commitHonoursOutcome],
});

const heldKeysAreFrozen = defineProperty("heldKeysAreFrozen", {
  description: "While a shard holds its keys, no other write to them applies.",
  everyStep: (before, after) =>
    before.all(Other).every((o, i) => {
      const next = after.all(Other)[i];
      return !next || o.state.hold === null || next.state.hold?.tx !== o.state.hold.tx || next.state.value === o.state.value;
    }),
  needs: [writesWait],
});

const callerToldTheTruth = defineProperty("callerToldTheTruth", {
  description: "The caller is told commit only if the primary committed, and abort only if it did not.",
  always: (w) => {
    const c = w.the(Client).state;
    if (c.tx === null || c.told === null || c.told === "error") return true;
    // "retry": the call is made again, so it must not have applied anywhere.
    return c.told === "commit" ? committed(w, c.tx) : !committed(w, c.tx);
  },
});

const holdsEndBeforeTheNext = defineProperty("holdsEndBeforeTheNext", {
  description: "A shard's hold for one call ends before it holds the keys for another.",
  everyStep: (before, after) => before.all(Other).every((o, i) => o.state.hold === null || (after.all(Other)[i]?.state.hold ?? null) === null || after.all(Other)[i]!.state.hold!.tx === o.state.hold.tx),
  needs: [youngerGivesWay],
});

const committedSharesApply = defineProperty("committedSharesApply", {
  description: "Once the primary commits, every shard that prepared the call applies its share.",
  leadsTo: {
    whenever: (w) => w.all(Other).some((o) => o.state.prepared.some((tx) => committed(w, tx) && !o.state.applied.includes(tx))),
    then: (w) => w.all(Other).every((o) => o.state.prepared.every((tx) => !committed(w, tx) || o.state.applied.includes(tx))),
  },
});

const everyHoldEnds = defineProperty("everyHoldEnds", {
  description: "A shard holding keys for a call ends the hold, whether or not the caller is still there.",
  leadsTo: { whenever: (w) => w.all(Other).some((o) => o.state.hold !== null), then: (w) => w.all(Other).every((o) => o.state.hold === null) },
});

const commitAcross = defineScenario("Commit across two shards", {
  description: "Every other shard prepares, the primary commits, the other shard applies, and the primary forgets the outcome.",
  run(s) {
    s.spawn(Other, "Shard 1");
    s.act(s.the(Client), "call", { refuse: "none" });
    s.settle();
  },
});

const refusedByAShard = defineScenario("Refused by another shard", {
  description: "The first other shard refuses; the primary records an abort and the shard that prepared drops its hold.",
  run(s) {
    s.spawn(Other, "Shard 1");
    s.spawn(Other, "Shard 2");
    s.act(s.the(Client), "call", { refuse: "other" });
    s.settle();
  },
});

const callerDies = defineScenario("The caller dies after the prepares", {
  description: "The other shard holds its keys until its deadline, then asks the primary, which records an abort.",
  run(s) {
    const shard = s.spawn(Other, "Shard 1");
    s.act(s.the(Client), "call", { refuse: "none" });
    s.settle({ until: (w) => w.all(Other).every((o) => o.state.hold !== null) });
    s.step(s.the(Client), "crash");
    s.step(shard, "recover");
    s.settle();
  },
});

const recoveryFirst = defineScenario("A recovery reaches the primary first", {
  description: "The hold's deadline passes before the commit: the recovery records the abort, and the late commit is refused.",
  run(s) {
    const shard = s.spawn(Other, "Shard 1");
    s.act(s.the(Client), "call", { refuse: "none" });
    s.settle({ until: (w) => w.all(Other).every((o) => o.state.hold !== null) });
    s.step(shard, "recover");
    s.deliver(s.the(Primary), AskOutcome);
    s.settle();
  },
});

const lateAsk = defineScenario("A recovery's ask arrives after the forget", {
  description:
    "Intended: the hold's deadline passes and it asks, but the commit and its resolve get there first and the primary forgets; the late ask then records an abort. No shard holds for the call any more, so nothing reads it, and it is purged with the outcomes past their TTL.",
  run(s) {
    const shard = s.spawn(Other, "Shard 1");
    s.act(s.the(Client), "call", { refuse: "none" });
    s.settle({ until: (w) => w.all(Other).every((o) => o.state.hold !== null) });
    s.step(shard, "recover");
    const [client, primary] = [s.the(Client), s.the(Primary)];
    s.deliver(client, Prepared);
    s.deliver(primary, Commit);
    s.deliver(client, Committed);
    s.deliver(shard, Resolve);
    s.deliver(client, Resolved);
    s.deliver(primary, Forget);
    s.settle();
  },
});

const givesWay = defineScenario("A call gives way to an older call's hold", {
  description: "A caller dies holding a shard's keys; the next call's prepare there gives way at once, holding nothing, and the call aborts to be made again. The old hold ends at its deadline, aborted.",
  run(s) {
    s.spawn(Other, "Shard 1");
    s.act(s.the(Client), "call", { refuse: "none" });
    s.settle({ until: (w) => w.all(Other).every((o) => o.state.hold !== null) });
    s.step(s.the(Client), "crash");
    s.act(s.the(Client), "call", { refuse: "none" });
    s.settle({ until: (w) => w.the(Client).state.told === "retry" });
    s.settle();
  },
});

export default defineSpec({
  name: "Atomic calls across shards",
  description: "How a call over keys in several shards applies in all of them or in none, whether its caller stays or goes.",
  actors: [Client, Primary, Other, Writer],
  limits: [holdDeadline],
  properties: [allOrNothing, oneOutcome, heldKeysAreFrozen, holdsEndBeforeTheNext, callerToldTheTruth, committedSharesApply, everyHoldEnds],
  scenarios: [commitAcross, refusedByAShard, callerDies, recoveryFirst, lateAsk, givesWay],
  finalState: {
    description: "No shard holds keys, nothing is in flight, and the caller has its answer or is gone.",
    when: (w) => w.quiet && w.all(Other).every((o) => o.state.hold === null) && (w.the(Client).state.tx === null || w.the(Client).state.told !== null),
  },
});
