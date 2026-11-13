// Durable XState on objects, under cf dev or deployed: a plain XState machine whose delayed
// transitions, answers and outgoing messages survive the object.
import { describe, expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";

useWorker();

const unique = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

type Settled = { ok: true; value: any } | { ok: false; code: string; message: string };
const op = (name: string, operation: string, args: unknown[] = []) => post<Settled>("/xstate", { op: operation, name, args });
const value = async (name: string, operation: string, args: unknown[] = []) => {
  const settled = await op(name, operation, args);
  if (!settled.ok) throw new Error(`${operation}: ${settled.code} ${settled.message}`);
  return settled.value;
};
const behaviours = (name: string, operation: string) => post<Settled>("/behaviours", { op: operation, name, args: [] });

describe("DurableXState", () => {
  test("answers calls from the machine's transitions", async () => {
    const seat = unique("seat");
    expect(await value(seat, "status")).toEqual({ state: "available", holder: null });
    expect(await value(seat, "grab", ["ana"])).toBe(true);
    expect(await value(seat, "status")).toEqual({ state: "held", holder: "ana" });
    expect(await value(seat, "release")).toBe(true);
    expect(await value(seat, "status")).toEqual({ state: "available", holder: null });
  });

  test("a delayed transition fires from the alarm, and survives a restart", async () => {
    const seat = unique("seat");
    await value(seat, "grab", ["ana", 400]);
    await op(seat, "restart");
    expect((await value(seat, "status")).state).toBe("held");
    await Bun.sleep(1_500);
    expect(await value(seat, "status")).toEqual({ state: "available", holder: null });
    expect(await value(seat, "timers")).toBe(0);
  });

  test("leaving the state cancels its delayed transition", async () => {
    const seat = unique("seat");
    await value(seat, "grab", ["ana", 60_000]);
    expect(await value(seat, "timers")).toBe(1);
    await value(seat, "release");
    expect(await value(seat, "timers")).toBe(0);
  });

  test("an event the state takes no transition for waits, and is answered after the next change", async () => {
    const seat = unique("seat");
    await value(seat, "grab", ["ana"]);
    expect(await value(seat, "grabWhileHeld", ["bo"])).toBe(true);
    expect(await value(seat, "status")).toEqual({ state: "held", holder: "bo" });
  });

  test("an action that throws commits nothing", async () => {
    const seat = unique("seat");
    expect(await op(seat, "explode")).toMatchObject({ ok: false, message: "explode" });
    expect(await value(seat, "status")).toEqual({ state: "available", holder: null });
  });

  test("a message the machine sends reaches another object", async () => {
    const seat = unique("seat");
    const audit = unique("audit");
    await value(seat, "grab", ["ana", 60_000, audit]);
    for (let i = 0; i < 50; i++) {
      const recent = await behaviours(audit, "auditRecent");
      if (recent.ok && recent.value.length > 0) {
        expect(recent.value).toEqual([{ type: "held", lock: "ana" }]);
        return;
      }
      await Bun.sleep(50);
    }
    throw new Error("the audit never got the seat's message");
  });
});
