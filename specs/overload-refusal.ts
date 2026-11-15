// Overload refusal (RFD0013): a shard that is overloaded refuses calls at once, with a typed
// answer the client throws as an Overloaded error, and the client refuses calls it knows would be
// refused, until the shard's retry-after passes. Nothing is queued or retried on the caller's
// behalf: the caller decides.
//
// One shard, two keys: "hot" and "cold". The shard is overloaded at most once, either as a whole
// ("shard": too many calls over many keys) or by one key ("key": the hot key takes most of its
// calls), and recovers at most once. The client sends up to three calls. One answer may be lost:
// the call is then "unknown" to the caller (OutcomeUnknown), never "overloaded".
//
//   - refuseBeforeWork: the shard decides to refuse before it runs the method. Without it the
//     method runs, and the caller is told "overloaded" of a call that was applied.
//   - keyScoped: an overload of one key refuses only that key's calls, in the shard and in the
//     client's memory of it. Without it a hot key takes down every key on its shard.
//   - clientForgets: the client remembers a refusal only until its retry-after, a timer. Without
//     it the client goes on refusing after the shard has recovered.
import { atMost, budget, defineActor, defineCall, defineMessage, defineProperty, defineRule, defineScenario, defineSpec, defineZone, failed, input, lose } from "@leostera/feynman";

const faults = budget("faults", 1);

const refuseBeforeWork = defineRule("refuseBeforeWork", { description: "The shard refuses an overloaded call before it runs the method." });
const keyScoped = defineRule("keyScoped", { description: "An overload of one key refuses only that key's calls, in the shard and in the client." });
const clientForgets = defineRule("clientForgets", { description: "The client remembers a refusal until its retry-after passes, then sends again." });

type Key = "hot" | "cold";
type Scope = "shard" | "key";

const Call = defineCall("Call", { id: Number, key: String }, { name: "call", over: "DO RPC" });
const Ok = defineMessage("Ok", { id: Number }, { name: "ok", over: "DO RPC", tone: "reply" });
const Refused = defineMessage("Refused", { id: Number, scope: String }, { name: "overloaded", over: "DO RPC", tone: "refused" });

const workers = defineZone({ name: "Worker isolate", place: { column: 0 } });
const objects = defineZone({ name: "Durable Objects", place: { column: 1 } });

/** Whether an overload of `scope` refuses a call to `key`: a key overload is of the hot key. */
function covers(scope: Scope | null, key: string): boolean {
  if (scope === null) return false;
  if (scope === "shard") return true;
  return keyScoped.on ? key === "hot" : true;
}

interface CallState {
  key: Key;
  /** null while in flight; "client" when the client refused it without sending. */
  report: null | "ok" | "refused" | "unknown";
  by: null | "shard" | "client";
  scope: Scope | null;
}

export const Client = defineActor({
  name: "Client",
  description: "Sends calls, throws Overloaded on a refusal, and refuses itself what it knows the shard would refuse.",
  zone: workers,
  singleton: true,
  initialState: () => ({ calls: [] as CallState[], remembered: null as Scope | null }),
  display: {
    icon: "code",
    status: (s) => (s.remembered ? `refusing (${s.remembered})` : "sending"),
    rows: (s) => s.calls.map((c, i) => ({ group: "calls", label: `${i} ${c.key}`, detail: c.report === null ? "in flight" : `${c.report}${c.by ? ` by ${c.by}` : ""}` })),
  },
  actions: {
    send: {
      with: [atMost(3)],
      label: "call a key",
      description: "A call to one key: refused at once if the client remembers an overload that covers it, else sent.",
      inputs: { key: input(String, { examples: () => ["hot", "cold"] }) },
      sends: [Call],
      run(s, { key }, ctx) {
        const id = s.calls.length;
        if (covers(s.remembered, key)) {
          s.calls.push({ key: key as Key, report: "refused", by: "client", scope: s.remembered });
          return;
        }
        s.calls.push({ key: key as Key, report: null, by: null, scope: null });
        ctx.send(ctx.the(Shard), Call, { id, key });
      },
    },
  },
  transitions: {
    ok: { on: Ok, description: "The call was applied.", run: (s, m) => void (s.calls[m.id]!.report = "ok") },
    refused: {
      on: Refused,
      description: "Throws Overloaded, and remembers the overload until its retry-after.",
      starts: ["forget"],
      run(s, m, ctx) {
        const call = s.calls[m.id]!;
        call.report = "refused";
        call.by = "shard";
        call.scope = m.scope as Scope;
        // A shard overload covers more than a key one: remember the wider.
        if (s.remembered !== "shard") s.remembered = m.scope as Scope;
        if (clientForgets.on) ctx.start("forget");
      },
    },
    failed: {
      on: failed(Call),
      description: "The answer is lost: the caller is told the outcome is unknown, not that the shard was overloaded.",
      run: (s, failure) => void (s.calls[failure.payload.id]!.report ??= "unknown"),
    },
    okLost: lose(Ok, { fails: Call, spend: faults }),
    refusedLost: lose(Refused, { fails: Call, spend: faults }),
    forget: {
      description: "The retry-after passes: the client sends calls to the shard again.",
      after: "retryAfterMs",
      run: (s) => void (s.remembered = null),
    },
  },
});

export const Shard = defineActor({
  name: "Shard",
  description: "Serves calls, and refuses them while overloaded: all of them, or the hot key's.",
  zone: objects,
  singleton: true,
  initialState: () => ({ scope: null as Scope | null, applied: [] as number[], everShard: false, overloaded: false, recovered: false }),
  display: { icon: "database", status: (s) => (s.scope ? `overloaded (${s.scope})` : s.recovered ? "recovered" : "serving") },
  actions: {
    overload: {
      with: [atMost(1)],
      label: "overload",
      description: "Load arrives: over the whole shard, or on the hot key.",
      inputs: { scope: input(String, { examples: () => ["shard", "key"] }) },
      when: (s) => !s.overloaded,
      run(s, { scope }) {
        s.scope = scope as Scope;
        s.overloaded = true;
        if (scope === "shard") s.everShard = true;
      },
    },
    recover: {
      with: [atMost(1)],
      label: "recover",
      description: "The load goes (or a split takes it): the shard serves every call again.",
      when: (s) => s.scope !== null,
      run(s) {
        s.scope = null;
        s.recovered = true;
      },
    },
  },
  transitions: {
    call: {
      on: Call,
      description: "Refuses the call if an overload covers it, before the method runs with the rule; else runs it.",
      sends: [Ok, Refused],
      run(s, m, ctx) {
        if (covers(s.scope, m.key)) {
          // Without the rule, the method runs before the shard looks at its load.
          if (!refuseBeforeWork.on) s.applied.push(m.id);
          return ctx.reply(Refused, { id: m.id, scope: s.scope! });
        }
        s.applied.push(m.id);
        ctx.reply(Ok, { id: m.id });
      },
    },
  },
});

const refusedNotApplied = defineProperty("refusedNotApplied", {
  description: "A call the caller was told is overloaded was not applied, whoever refused it.",
  always: (w) => {
    const applied = w.the(Shard).state.applied;
    return w.the(Client).state.calls.every((c, id) => c.report !== "refused" || !applied.includes(id));
  },
  needs: [refuseBeforeWork],
});
const okApplied = defineProperty("okApplied", {
  description: "A call the caller was told is ok was applied.",
  always: (w) => {
    const applied = w.the(Shard).state.applied;
    return w.the(Client).state.calls.every((c, id) => c.report !== "ok" || applied.includes(id));
  },
});
const othersServed = defineProperty("othersServed", {
  description: "While only the hot key is overloaded, no call to the cold key is refused, by the shard or the client.",
  always: (w) => w.the(Shard).state.everShard || w.the(Client).state.calls.every((c) => c.key !== "cold" || c.report !== "refused"),
  needs: [keyScoped],
});
const recovers = defineProperty("recovers", {
  description: "Once the shard has recovered, the client stops refusing calls itself.",
  leadsTo: {
    whenever: (w) => w.the(Shard).state.recovered && w.the(Client).state.remembered !== null,
    then: (w) => w.the(Client).state.remembered === null,
  },
  needs: [clientForgets],
});
const refusedByShard = defineProperty("refusedByShard", { description: "The shard can refuse a call.", reachable: (w) => w.the(Client).state.calls.some((c) => c.by === "shard") });
const refusedByClient = defineProperty("refusedByClient", { description: "The client can refuse a call without sending it.", reachable: (w) => w.the(Client).state.calls.some((c) => c.by === "client") });
const servedAgain = defineProperty("servedAgain", {
  description: "A hot-key call can be served after one was refused.",
  reachable: (w) => {
    const calls = w.the(Client).state.calls;
    const refused = calls.findIndex((c) => c.key === "hot" && c.report === "refused");
    return refused >= 0 && calls.slice(refused + 1).some((c) => c.key === "hot" && c.report === "ok");
  },
});

const hotKey = defineScenario("A hot key", {
  description: "The hot key is overloaded: its call is refused, the cold key's is served, and the client refuses the next hot call itself.",
  run(s) {
    s.act(s.the(Shard), "overload", { scope: "key" });
    s.act(s.the(Client), "send", { key: "hot" });
    s.act(s.the(Client), "send", { key: "cold" });
    s.settle({ until: (w) => w.the(Client).state.calls.every((c) => c.report !== null) });
    s.act(s.the(Client), "send", { key: "hot" });
  },
});

export default defineSpec({
  name: "Overload refusal",
  description: "What an overloaded shard refuses, what the caller is told, and when calls flow again.",
  actors: [Client, Shard],
  properties: [refusedNotApplied, okApplied, othersServed, recovers, refusedByShard, refusedByClient, servedAgain],
  scenarios: [hotKey],
  finalState: { description: "Every call has its answer.", when: (w) => w.quiet && w.the(Client).state.calls.every((c) => c.report !== null) },
});
