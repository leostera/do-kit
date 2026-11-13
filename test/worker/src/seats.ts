// A seat that can be held for a while, in the test Worker: a plain XState machine with a delayed
// transition, run by DurableStateMachine. The route runs in the Worker, so the class's client
// methods run here, as in any caller.
import { assign, setup } from "xstate";
import { CallError, type CallOptions } from "../../../src/client/index.ts";
import { DurableStateMachine, durableActions, reply, sendToObject } from "../../../src/behaviours/index.ts";
import type { BehavioursEnv } from "./behaviours.ts";

export interface SeatsEnv extends BehavioursEnv {
  readonly SEATS: DurableObjectNamespace<Seat>;
}

type SeatEvent =
  | { readonly type: "grab"; readonly who: string; readonly holdMs: number; readonly audit?: string }
  | { readonly type: "release" }
  | { readonly type: "status" }
  | { readonly type: "explode" };

const seat = setup({
  types: { context: {} as { holder: string | null; holdMs: number }, events: {} as SeatEvent },
  actions: {
    ...durableActions,
    boom: () => {
      throw new Error("explode");
    },
  },
  delays: { hold: ({ context }) => context.holdMs },
}).createMachine({
  id: "seat",
  context: { holder: null, holdMs: 60_000 },
  initial: "available",
  states: {
    available: {
      on: {
        grab: {
          target: "held",
          actions: [
            assign({ holder: ({ event }) => event.who, holdMs: ({ event }) => event.holdMs }),
            reply(() => true),
            sendToObject("AUDITS", ({ event }) => event.audit ?? "nobody", ({ event }) => ({ type: "held", lock: event.who })),
          ],
        },
        status: { actions: reply(({ context }) => ({ state: "available", holder: context.holder })) },
        // Changes the context, then throws: neither may stick.
        explode: { actions: [assign({ holder: "exploded" }), "boom"] },
      },
    },
    held: {
      after: { hold: { target: "available", actions: assign({ holder: null }) } },
      on: {
        release: { target: "available", actions: [assign({ holder: null }), reply(() => true)] },
        status: { actions: reply(({ context }) => ({ state: "held", holder: context.holder })) },
        // grab: no transition here, so it waits until the seat is available again.
      },
    },
  },
});

export class Seat extends DurableStateMachine<SeatsEnv, typeof seat> {
  protected readonly machine = seat;

  // ---- client API: runs in the caller ----
  grab(who: string, holdMs = 60_000, audit?: string): Promise<boolean> {
    if (!who) return Promise.reject(new CallError("INVALID_ARGUMENT", "who must be set"));
    return this.call(audit ? { type: "grab", who, holdMs, audit } : { type: "grab", who, holdMs }) as Promise<boolean>;
  }
  release(): Promise<boolean> {
    return this.call({ type: "release" }) as Promise<boolean>;
  }
  status(): Promise<{ state: string; holder: string | null }> {
    return this.call({ type: "status" }) as Promise<{ state: string; holder: string | null }>;
  }
  explode(): Promise<never> {
    return this.call({ type: "explode" }) as Promise<never>;
  }
  /** Test only: an event type no state names, as a typo in a client method would send. */
  misspelled(kind: "call" | "cast"): Promise<unknown> {
    const event = { type: "grabb", who: "ana", holdMs: 1 } as never;
    return kind === "call" ? this.call(event) : this.cast(event);
  }

  /** Test only, through a raw stub. */
  async restart(): Promise<void> {
    this.ctx.abort("test restart");
  }
  /** Test only: timers still set. */
  async timers(): Promise<number> {
    return this.ctx.storage.sql.exec<{ n: number }>("SELECT COUNT(*) AS n FROM behaviour_timers").one().n;
  }
}

export type SeatsRequest = { op: string; name: string; options?: CallOptions; args?: unknown[] };

async function settle(work: () => Promise<unknown>): Promise<unknown> {
  try {
    return { ok: true, value: (await work()) ?? null };
  } catch (error) {
    return { ok: false, code: error instanceof CallError ? error.code : "THROWN", message: error instanceof Error ? error.message : String(error) };
  }
}

export async function seatsRoute(env: SeatsEnv, request: SeatsRequest): Promise<unknown> {
  const { name, options = {} } = request;
  const args = request.args ?? [];
  const seats = Seat.client(env.SEATS, name, options);
  switch (request.op) {
    case "grab":
      return settle(() => seats.grab(String(args[0]), args[1] as number | undefined, args[2] as string | undefined));
    case "release":
      return settle(() => seats.release());
    case "status":
      return settle(() => seats.status());
    case "explode":
      return settle(() => seats.explode());
    case "misspelled":
      return settle(() => seats.misspelled(args[0] as "call" | "cast"));
    case "untyped":
      return settle(async () => {
        const outcome = (await env.SEATS.getByName(name).receive({ kind: "call", message: { who: "ana" } })) as { ok: true; value: unknown } | { ok: false; code: "INVALID_ARGUMENT"; message: string };
        if (!outcome.ok) throw new CallError(outcome.code, outcome.message);
        return outcome.value;
      });
    case "grabWhileHeld": {
      // Waits while held; answered once a release makes the seat available.
      const waiting = settle(() => seats.grab(String(args[0])));
      await scheduler.wait(50);
      await seats.release();
      return waiting;
    }
    case "restart":
      return settle(() => env.SEATS.getByName(name).restart());
    case "timers":
      return settle(() => env.SEATS.getByName(name).timers());
    default:
      throw new Error(`unknown op ${request.op}`);
  }
}
