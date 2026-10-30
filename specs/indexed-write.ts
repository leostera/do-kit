// How an IndexedTable write keeps a unique global index true (RFD0003): the client reserves the
// value on its index shard and gets a token, the row shard writes the row and an outbox in one
// transaction, delivers the outbox to the index, and only then answers. A reservation left behind
// is resolved by asking the row shard, which fences the token so a late write cannot land.
//
// Two writers race for the email "b"; either may move to "c" or delete its row. Each row is on its
// own shard; one index shard holds both values. Calls may be lost and objects restarted.
//
// Checked with --repeats 1 (package.json specs:check): with more, the search does not finish. Each
// rule is needed: --without fences and --without waitForIndex break a property in that search, and
// the scenario "A delete overtakes the write before it" breaks converges --without tombstones.
import { call, defineSpec, message } from "@leostera/feynman";

type Key = "k" | "j";
type Value = "b" | "c";
const VALUES: Value[] = ["b", "c"];
/** How many times a write asks for its reservation before it fails as busy. */
const MAX_TRIES = 2;

/**
 * An index entry: whose value it is, the row version it reflects (0: only reserved), a pending
 * reservation's token, the newest token issued to the owner, whether the row let go of it, the
 * newest row version that let go of it, and whether the owner's tokens are fenced (so it can pass to
 * another row).
 */
interface Entry {
  owner: Key;
  version: number;
  pending: number | null;
  issued: number;
  removed: boolean;
  floor: number;
  fenced: boolean;
}

/** An index change a row shard has to deliver. */
interface Op {
  id: number;
  value: Value;
  kind: "put" | "remove";
  version: number;
  token: number | null;
}

export default defineSpec(
  "Indexed writes",
  {
    description: "How a write to an IndexedTable reserves a unique value, writes the row and its outbox, and returns only once the index has it.",
    messages: {
      Reserve: call<{ value: Value; owner: Key; attempt: number }>({ name: "reserve", over: "rpc" }),
      Reserved: message<{ value: Value; token: number; attempt: number }>({ name: "reserved", tone: "reply" }),
      Taken: message<{ value: Value; attempt: number }>({ name: "taken", tone: "refused" }),
      Contended: message<{ value: Value; attempt: number }>({ name: "contended", tone: "refused" }),
      Write: call<{ value: Value | null; token: number | null; attempt: number }>({ name: "write row", over: "rpc" }),
      Refused: message<{ value: Value; attempt: number }>({ name: "token fenced", tone: "refused" }),
      WriteDone: message<{ version: number; attempt: number }>({ name: "written and indexed", tone: "reply" }),
      Apply: call<{ owner: Key; op: Op }>({ name: "deliver", over: "rpc" }),
      Applied: message<{ id: number }>({ name: "delivered", tone: "reply" }),
      Resolve: call<{ owner: Key; value: Value; token: number }>({ name: "resolve reservation", over: "rpc" }),
      Committed: message<{ owner: Key; value: Value; token: number; version: number }>({ name: "committed", tone: "reply" }),
      Aborted: message<{ owner: Key; value: Value; token: number; version: number }>({ name: "aborted", tone: "refused" }),
    },
  },
  (spec) => {
    const { Reserve, Reserved, Taken, Contended, Write, Refused, WriteDone, Apply, Applied, Resolve, Committed, Aborted } = spec.messages;

    const workers = spec.defineZone({ name: "Workers", description: "the table's client", place: { column: 0 } });
    const rows = spec.defineZone({ name: "Row shards", description: "the source of truth", place: { column: 1, arrange: "stack" } });
    const indexes = spec.defineZone({ name: "Index shards", description: "derived, repairable", place: { column: 2, region: true } });

    const fences = spec.defineRule({ name: "fences", description: "A row shard that aborts a reservation fences its token, and refuses writes that carry it." });
    const waitForIndex = spec.defineRule({ name: "waitForIndex", description: "A row shard answers a write only once the index has acknowledged its changes." });
    const tombstones = spec.defineRule({ name: "tombstones", description: "An index entry a row let go of keeps the version it let go at, so an older change arriving late is ignored." });

    spec.defineLimit({ name: "Reservation age before the index resolves it", value: 5, unit: "seconds" });

    function makeWriter(name: string, key: Key) {
      return spec.defineActor({
        name,
        description: `The client writing row ${key}.`,
        zone: workers,
        singleton: true,
        initialState: () => ({ key, attempt: 0, wrote: 0, tries: 0, want: null as Value | null, phase: "idle" as "idle" | "reserving" | "writing" | "done", token: null as number | null, outcome: "" }),
        display: {
          icon: "code",
          status: (s) => (s.phase === "idle" ? "idle" : s.phase === "done" ? s.outcome : `${s.phase} ${s.want ?? "delete"}`),
          mood: (s) => (s.phase === "done" ? (s.outcome === "written" ? "ok" : "warn") : s.phase === "idle" ? "off" : "busy"),
        },
        actions: {
          write: {
            label: "Write the row",
            sources: ["src/indexed/client.ts#IndexedTableClient.putMany", "src/indexed/client.ts#IndexedTableClient.patch", "src/indexed/client.ts#IndexedTableClient.delete"],
            description: "Sets the row's email to a value, or deletes the row.",
            inputs: { value: spec.defineInput(String, { examples: ["b", "c", "delete"] }) },
            when: (s) => s.phase === "idle" || s.phase === "done",
            sends: [Reserve, Write],
            starts: ["gaveUp"],
            run(s, { value }, ctx) {
              s.want = value === "delete" ? null : (value as Value);
              s.token = null;
              s.tries = 1;
              if (s.want === null) {
                s.phase = "writing";
                s.attempt += 1;
                ctx.send(rowShard(ctx), Write, { value: null, token: null, attempt: s.attempt });
                ctx.start("gaveUp");
              } else {
                s.phase = "reserving";
                s.attempt += 1;
                ctx.send(ctx.the(Index), Reserve, { value: s.want, owner: s.key, attempt: s.attempt });
              }
            },
          },
        },
        transitions: {
          [Reserved]: {
            description: "Holds the reservation: writes the row with its token.",
            sources: ["src/indexed/client.ts#IndexedTableClient.putMany"],
            sends: [Write],
            starts: ["gaveUp"],
            run(s, msg, ctx) {
              if (s.phase !== "reserving" || msg.attempt !== s.attempt) return;
              s.phase = "writing";
              s.token = msg.token;
              s.attempt += 1;
              ctx.send(rowShard(ctx), Write, { value: msg.value, token: msg.token, attempt: s.attempt });
              ctx.start("gaveUp");
            },
          },
          [Taken]: {
            description: "Another row has the value: the write fails, and nothing was written.",
            sources: ["src/indexed/client.ts#IndexedTableClient.reserveAll"],
            run(s, msg) {
              if (s.phase !== "reserving" || msg.attempt !== s.attempt) return;
              s.phase = "done";
              s.outcome = "taken";
            },
          },
          [Contended]: {
            description: "Another write holds the value while the index resolves it: tries again shortly.",
            sources: ["src/indexed/client.ts#IndexedTableClient.reserveAll"],
            starts: ["retry"],
            run(s, msg, ctx) {
              if (s.phase === "reserving" && msg.attempt === s.attempt) ctx.start("retry");
            },
          },
          [Refused]: {
            description: "The reservation was given up before the row was written: reserves again.",
            sources: ["src/indexed/client.ts#IndexedTableClient.settle"],
            starts: ["retry"],
            run(s, msg, ctx) {
              if (s.phase !== "writing" || msg.attempt !== s.attempt) return;
              s.phase = "reserving";
              ctx.cancel("gaveUp");
              ctx.start("retry");
            },
          },
          retry: {
            description: "Asks for the reservation again, or fails as busy after the last try.",
            sources: ["src/indexed/client.ts#IndexedTableClient.reserveAll"],
            after: "a short backoff",
            sends: [Reserve],
            run(s, ctx) {
              if (s.phase !== "reserving" || s.want === null) return;
              if (s.tries >= MAX_TRIES) {
                s.phase = "done";
                s.outcome = "busy";
                return;
              }
              s.tries += 1;
              s.attempt += 1;
              ctx.send(ctx.the(Index), Reserve, { value: s.want, owner: s.key, attempt: s.attempt });
            },
          },
          [WriteDone]: {
            description: "The row is written and the index has it: the write returns.",
            sources: ["src/indexed/client.ts#IndexedTableClient.settle"],
            run(s, msg, ctx) {
              if (s.phase !== "writing" || msg.attempt !== s.attempt) return;
              s.phase = "done";
              s.outcome = "written";
              s.wrote = msg.version;
              ctx.cancel("gaveUp");
            },
          },
          gaveUp: {
            description: "No answer in time: the write may or may not be done (IndexPending, or a lost call).",
            sources: ["src/indexed/client.ts#IndexedTableClient.settle", "src/indexed/table.ts#IndexedTable.deliverWithin"],
            after: "the write's deadline",
            run(s) {
              if (s.phase !== "writing") return;
              s.phase = "done";
              s.outcome = "unknown";
            },
          },
          [spec.failed(Reserve)]: {
            description: "The reservation call threw: tries again.",
            starts: ["retry"],
            run(s, f, ctx) {
              if (s.phase === "reserving" && f.payload.attempt === s.attempt) ctx.start("retry");
            },
          },
          [spec.failed(Write)]: {
            description: "The write call threw: whether it landed is unknown.",
            run(s, f, ctx) {
              if (s.phase !== "writing" || f.payload.attempt !== s.attempt) return;
              s.phase = "done";
              s.outcome = "unknown";
              ctx.cancel("gaveUp");
            },
          },
        },
      });
    }

    function makeShard(name: string, key: Key) {
      return spec.defineActor({
        name,
        description: `The row shard of ${key}: its row, its fences and its outbox, in one transaction.`,
        zone: rows,
        singleton: true,
        initialState: () => ({
          key,
          value: null as Value | null,
          version: 0,
          fence: {} as Partial<Record<Value, number>>,
          outbox: [] as Op[],
          nextOp: 1,
          answering: null as { version: number; attempt: number } | null,
          sending: new Set<number>(),
        }),
        durable: ["value", "version", "fence", "outbox", "nextOp"],
        display: {
          icon: "database",
          status: (s) => `${s.key} = ${s.value ?? "none"} (v${s.version})${s.outbox.length ? `, ${s.outbox.length} to deliver` : ""}`,
          mood: (s) => (s.outbox.length ? "busy" : "ok"),
          rows: (s) => s.outbox.map((op) => ({ label: `${op.kind} ${op.value}`, detail: `v${op.version}`, mood: "info" as const })),
        },
        transitions: {
          [Write]: {
            description: "Refuses a fenced token; otherwise writes the row and its index changes to the outbox in one transaction, then delivers them.",
            sources: ["src/indexed/table.ts#IndexedTable.writeBatch", "src/indexed/table.ts#IndexedTable.refuseRowWrite", "src/indexed/table.ts#IndexedTable.rowWritten"],
            sends: [Refused, Apply, WriteDone],
            run(s, msg, ctx) {
              if (fences.on && msg.value !== null && msg.token !== null && msg.token <= (s.fence[msg.value] ?? 0)) return ctx.reply(Refused, { value: msg.value, attempt: msg.attempt });
              const before = s.value;
              s.version += 1;
              s.value = msg.value;
              if (before !== null && before !== msg.value) s.outbox.push({ id: s.nextOp++, value: before, kind: "remove", version: s.version, token: null });
              if (msg.value !== null) s.outbox.push({ id: s.nextOp++, value: msg.value, kind: "put", version: s.version, token: msg.token });
              if (!waitForIndex.on || s.outbox.length === 0) ctx.send(writer(ctx, s.key), WriteDone, { version: s.version, attempt: msg.attempt });
              else s.answering = { version: s.version, attempt: msg.attempt };
              for (const op of s.outbox) {
                if (s.sending.has(op.id)) continue;
                s.sending.add(op.id);
                ctx.send(ctx.the(Index), Apply, { owner: s.key, op });
              }
            },
          },
          [Applied]: {
            description: "The index has a change: drops it from the outbox, and answers the waiting write once all of its changes are in.",
            sources: ["src/indexed/table.ts#IndexedTable.deliver"],
            sends: [WriteDone],
            run(s, msg, ctx) {
              s.outbox = s.outbox.filter((op) => op.id !== msg.id);
              s.sending.delete(msg.id);
              if (s.answering !== null && !s.outbox.some((op) => op.version <= s.answering!.version)) {
                ctx.send(writer(ctx, s.key), WriteDone, s.answering);
                s.answering = null;
              }
            },
          },
          [spec.failed(Apply)]: {
            description: "A delivery threw: the alarm delivers it again.",
            sources: ["src/indexed/table.ts#IndexedTable.deliverWithin"],
            run(s, f) {
              s.sending.delete(f.payload.op.id);
            },
          },
          deliver: {
            description: "The alarm: delivers what is left in the outbox.",
            sources: ["src/indexed/table.ts#IndexedTable.onAlarm"],
            routine: true,
            when: (s) => s.outbox.some((op) => !s.sending.has(op.id)),
            sends: [Apply],
            run(s, ctx) {
              for (const op of s.outbox) {
                if (s.sending.has(op.id)) continue;
                s.sending.add(op.id);
                ctx.send(ctx.the(Index), Apply, { owner: s.key, op });
              }
            },
          },
          [Resolve]: {
            description: "Answers whether the row has the value; if not, fences the token so a write carrying it cannot land later.",
            sources: ["src/indexed/table.ts#IndexedTable.indexResolve"],
            sends: [Committed, Aborted],
            run(s, msg, ctx) {
              if (s.value === msg.value) return ctx.reply(Committed, { owner: s.key, value: msg.value, token: msg.token, version: s.version });
              if (fences.on) s.fence[msg.value] = Math.max(s.fence[msg.value] ?? 0, msg.token);
              ctx.reply(Aborted, { owner: s.key, value: msg.value, token: msg.token, version: s.version });
            },
          },
        },
      });
    }

    const Alice = makeWriter("Alice", "k");
    const Bob = makeWriter("Bob", "j");
    const ShardK = makeShard("Row shard k", "k");
    const ShardJ = makeShard("Row shard j", "j");
    type Ctx = { the: (a: typeof Alice | typeof ShardK) => unknown; label: string };
    function rowShard(ctx: { label: string; the: Ctx["the"] }) {
      return (ctx.label === "Alice" ? ctx.the(ShardK) : ctx.the(ShardJ)) as never;
    }
    function writer(ctx: { the: Ctx["the"] }, key: Key) {
      return (key === "k" ? ctx.the(Alice) : ctx.the(Bob)) as never;
    }
    function shardOf(ctx: { the: Ctx["the"] }, key: Key) {
      return (key === "k" ? ctx.the(ShardK) : ctx.the(ShardJ)) as never;
    }

    const Index = spec.defineActor({
      name: "Index shard",
      description: "Holds the unique index's entries for both values, and issues reservation tokens.",
      zone: indexes,
      singleton: true,
      initialState: () => ({ entries: {} as Partial<Record<Value, Entry>>, counter: { b: 0, c: 0 } as Record<Value, number>, resolving: new Set<Value>() }),
      durable: ["entries", "counter"],
      display: {
        icon: "storage",
        status: (s) => `${Object.values(s.entries).filter((e) => e && !e.removed && e.version > 0).length} live entr(ies)`,
        rows: (s) =>
          VALUES.flatMap((v) => {
            const e = s.entries[v];
            if (!e) return [];
            return [{ label: v, detail: `${e.removed ? "let go by" : "owned by"} ${e.owner}, v${e.version}${e.pending !== null ? `, reserved #${e.pending}` : ""}`, mood: e.removed ? ("off" as const) : e.pending !== null ? ("busy" as const) : ("ok" as const) }];
          }),
      },
      transitions: {
        [Reserve]: {
          description: "Reserves a free value, or one the same row holds; refuses one another row has; otherwise asks the owner's row shard first.",
          sources: ["src/indexed/global-index.ts#GlobalIndex.reserve"],
          sends: [Reserved, Taken, Contended, Resolve],
          run(s, msg, ctx) {
            const e = s.entries[msg.value];
            // Tokens count per value: a fence is per value too.
            const next = () => (s.counter[msg.value] += 1);
            const fresh = (token: number): Entry => ({ owner: msg.owner, version: 0, pending: token, issued: token, removed: false, floor: 0, fenced: false });
            if (!e) {
              const token = next();
              s.entries[msg.value] = fresh(token);
              return ctx.reply(Reserved, { value: msg.value, token, attempt: msg.attempt });
            }
            if (e.owner === msg.owner) {
              const token = next();
              e.pending = e.issued = token;
              e.fenced = false;
              return ctx.reply(Reserved, { value: msg.value, token, attempt: msg.attempt });
            }
            // Another row's. It passes on only once that row let go of it and fenced every token it
            // was issued for it: an old write still in flight must not land after the hand-over.
            if (e.removed && e.pending === null && (e.fenced || !fences.on)) {
              const token = next();
              s.entries[msg.value] = fresh(token);
              return ctx.reply(Reserved, { value: msg.value, token, attempt: msg.attempt });
            }
            if (!e.removed && e.pending === null && e.version > 0) return ctx.reply(Taken, { value: msg.value, attempt: msg.attempt });
            if (!s.resolving.has(msg.value)) {
              s.resolving.add(msg.value);
              ctx.send(shardOf(ctx, e.owner), Resolve, { owner: e.owner, value: msg.value, token: e.issued });
            }
            ctx.reply(Contended, { value: msg.value, attempt: msg.attempt });
          },
        },
        [Apply]: {
          description: "Applies a delivered change of the entry's owner, idempotently and by version.",
          sources: ["src/indexed/global-index.ts#GlobalIndex.apply"],
          sends: [Applied],
          run(s, msg, ctx) {
            const { op, owner } = msg;
            const e = s.entries[op.value];
            if (op.kind === "put") {
              if (!e) s.entries[op.value] = { owner, version: op.version, pending: null, issued: op.token ?? 0, removed: false, floor: 0, fenced: false };
              else if (e.owner === owner) {
                if (op.version > e.version && (!tombstones.on || op.version > e.floor)) {
                  e.version = op.version;
                  e.removed = false;
                }
                if (op.token !== null && e.pending !== null && op.token >= e.pending) e.pending = null;
              }
            } else if (e && e.owner === owner && op.version > e.version) {
              e.removed = true;
              if (tombstones.on) e.floor = Math.max(e.floor, op.version);
              else e.floor = 0;
            }
            ctx.reply(Applied, { id: op.id });
          },
        },
        resolveStale: {
          description: "The alarm: asks the row shard about a reservation that has waited too long.",
          sources: ["src/indexed/global-index.ts#GlobalIndex.onAlarm"],
          after: "5 seconds",
          when: (s) => VALUES.some((v) => s.entries[v]?.pending != null && !s.resolving.has(v)),
          sends: [Resolve],
          run(s, ctx) {
            for (const v of VALUES) {
              const e = s.entries[v];
              if (!e || e.pending === null || s.resolving.has(v)) continue;
              s.resolving.add(v);
              ctx.send(shardOf(ctx, e.owner), Resolve, { owner: e.owner, value: v, token: e.issued });
            }
          },
        },
        [Committed]: {
          description: "The row has the value: the reservation is confirmed.",
          sources: ["src/indexed/global-index.ts#GlobalIndex.settle"],
          run(s, msg) {
            s.resolving.delete(msg.value);
            const e = s.entries[msg.value];
            if (!e || e.owner !== msg.owner) return;
            if (e.pending !== null && e.pending <= msg.token) e.pending = null;
            if (msg.version > e.version && msg.version > e.floor) {
              e.version = msg.version;
              e.removed = false;
            }
          },
        },
        [Aborted]: {
          description: "The row does not have the value, and every token up to this one is fenced: the entry is let go of, and may pass to another row.",
          sources: ["src/indexed/global-index.ts#GlobalIndex.settle"],
          run(s, msg) {
            s.resolving.delete(msg.value);
            const e = s.entries[msg.value];
            if (!e || e.owner !== msg.owner || msg.token < e.issued) return;
            e.pending = null;
            e.removed = true;
            e.fenced = true;
            if (tombstones.on) e.floor = Math.max(e.floor, msg.version);
          },
        },
        [spec.failed(Resolve)]: {
          description: "Asking the row shard threw: the alarm asks again.",
          sources: ["src/indexed/global-index.ts#GlobalIndex.resolve"],
          run(s, f) {
            s.resolving.delete(f.payload.value);
          },
        },
      },
    });

    const shards = (w: { the: (a: typeof ShardK) => { state: { key: Key; value: Value | null; version: number; outbox: Op[] } } }) => [w.the(ShardK).state, w.the(ShardJ).state];

    spec.defineFinalState({
      description: "Both writers are done, nothing is in flight, and nothing is left to deliver or resolve.",
      when: (w) =>
        w.quiet &&
        [w.the(Alice).state, w.the(Bob).state].every((s) => s.phase === "idle" || s.phase === "done") &&
        shards(w as never).every((s) => s.outbox.length === 0) &&
        VALUES.every((v) => w.the(Index).state.entries[v]?.pending == null),
    });

    spec.defineProperties({
      unique: {
        description: "At most one row has a given value.",
        always: (w) => VALUES.every((v) => shards(w as never).filter((s) => s.value === v).length <= 1),
        needs: [fences],
      },
      readYourWrites: {
        description: "When a write returns, the index finds its row at that version, unless the row has moved on since.",
        everyStep: (before, after, e) => {
          for (const W of [Alice, Bob]) {
            if (!e.is(W, WriteDone) || before.the(W).state.phase !== "writing") continue;
            const s = after.the(W).state;
            if (s.phase !== "done" || s.want === null || s.outcome !== "written") continue;
            const entry = after.the(Index).state.entries[s.want];
            const row = shards(after as never).find((r) => r.key === s.key)!;
            // A later write of the same row (one that timed out and landed late) may have moved it on.
            if (row.version > s.wrote) continue;
            if (!entry || entry.removed || entry.owner !== s.key || entry.version < s.wrote) return false;
          }
          return true;
        },
        needs: [waitForIndex],
      },
      converges: {
        description: "Once nothing is in flight, delivered or reserved, every row with a value has exactly its entry, and no other entry is live.",
        always: (w) => {
          const idx = w.the(Index).state;
          const rs = shards(w as never);
          if (!w.quiet || rs.some((s) => s.outbox.length > 0) || VALUES.some((v) => idx.entries[v]?.pending != null)) return true;
          return VALUES.every((v) => {
            const holder = rs.find((s) => s.value === v);
            const e = idx.entries[v];
            const live = e && !e.removed && e.version > 0 ? e : null;
            if (!holder) return live === null;
            return live !== null && live.owner === holder.key && live.version === holder.version;
          });
        },
        needs: [tombstones],
      },
      reservationsEnd: {
        description: "A pending reservation is eventually confirmed or dropped.",
        leadsTo: {
          whenever: (w) => VALUES.some((v) => w.the(Index).state.entries[v]?.pending != null),
          then: (w) => VALUES.every((v) => w.the(Index).state.entries[v]?.pending == null),
        },
      },
    });

    spec.defineScenario({
      name: "Claim an email",
      description: "Alice reserves b, writes her row, the index gets the change, and the write returns.",
      run(s) {
        s.act(s.the(Alice), "write", { value: "b" });
        s.settle();
      },
    });

    spec.defineScenario({
      name: "Two sign-ups race for one email",
      description: "Alice and Bob both want b. Alice reserves it first; Bob is told it is contended, then taken once Alice's row has it.",
      run(s) {
        s.act(s.the(Alice), "write", { value: "b" });
        s.deliver(s.the(Index), "Reserve");
        s.act(s.the(Bob), "write", { value: "b" });
        s.settle();
      },
    });

    spec.defineScenario({
      name: "A writer that never writes",
      description: "Alice reserves b and her write call is lost. Bob asks for b: the index asks Alice's row shard, which fences her token, and Bob gets b.",
      run(s) {
        s.act(s.the(Alice), "write", { value: "b" });
        s.deliver(s.the(Index), "Reserve");
        s.deliver(s.the(Alice), "Reserved");
        s.drop(s.the(ShardK), "Write");
        s.settle({ until: (w) => w.the(Alice).state.phase === "done" });
        s.act(s.the(Bob), "write", { value: "b" });
        s.settle();
      },
    });

    spec.defineScenario({
      name: "Move to another email",
      description: "Alice has b and moves to c: the index gets c and lets go of b in one write, and Bob can then have b.",
      run(s) {
        s.act(s.the(Alice), "write", { value: "b" });
        s.settle();
        s.act(s.the(Alice), "write", { value: "c" });
        s.settle();
        s.act(s.the(Bob), "write", { value: "b" });
        s.settle();
      },
    });

    spec.defineScenario({
      name: "A delete overtakes the write before it",
      description: "Bob's write of b times out and he deletes his row. Both land, and the removal reaches the index before the put: the tombstone keeps the late put from bringing b back. Run it with --without tombstones to see b come back.",
      run(s) {
        s.act(s.the(Bob), "write", { value: "b" });
        s.deliver(s.the(Index), "Reserve");
        s.deliver(s.the(Bob), "Reserved");
        s.fire(s.the(Bob), "gaveUp");
        s.act(s.the(Bob), "write", { value: "delete" });
        s.deliver(s.the(ShardJ), "Write", { value: "b", token: 1, attempt: 2 });
        s.deliver(s.the(ShardJ), "Write");
        s.deliver(s.the(Index), "Apply", { owner: "j", op: { id: 2, value: "b", kind: "remove", version: 2, token: null } });
        s.deliver(s.the(Index), "Apply");
        s.deliver(s.the(ShardJ), "Applied", { id: 1 });
        s.deliver(s.the(ShardJ), "Applied");
        s.deliver(s.the(Bob), "WriteDone");
      },
    });

    return { Alice, Bob, ShardK, ShardJ, Index };
  },
);
