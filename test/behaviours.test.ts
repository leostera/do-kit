// Behaviours and DOClient on objects (RFD0009), under cf dev or deployed: the classes' client
// methods run in the test Worker, and only messages reach the objects.
import { describe, expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";

useWorker();

const unique = (prefix: string) => `${prefix}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

type Settled = { ok: true; value: any } | { ok: false; code: string; message: string };
const op = (name: string, operation: string, args: unknown[] = [], options: Record<string, unknown> = {}) => post<Settled>("/behaviours", { op: operation, name, args, options });
const value = async (name: string, operation: string, args: unknown[] = [], options: Record<string, unknown> = {}) => {
  const settled = await op(name, operation, args, options);
  if (!settled.ok) throw new Error(`${operation}: ${settled.code} ${settled.message}`);
  return settled.value;
};

describe("GenServer", () => {
  test("serves calls and casts on its state", async () => {
    const pool = unique("pool");
    expect(await value(pool, "alloc")).toBe(0);
    expect(await value(pool, "alloc")).toBe(1);
    await value(pool, "free", [0]);
    expect((await value(pool, "poolState")).free).toEqual([2, 0]);
  });

  test("client methods run in the caller: a bad argument never reaches the object", async () => {
    const pool = unique("pool");
    expect(await op(pool, "free", [-1])).toMatchObject({ ok: false, code: "INVALID_ARGUMENT" });
  });

  test("only the message crosses the wire, as one call to receive, with metadata", async () => {
    const pool = unique("pool");
    const { value: channel, seen } = await post<{ value: number; seen: { method: string; args: any[] }[] }>("/behaviours", { op: "intercepted", name: pool });
    expect(channel).toBe(0);
    expect(seen).toHaveLength(1);
    expect(seen[0]!.method).toBe("receive");
    expect(seen[0]!.args[0]).toMatchObject({ kind: "call", message: { op: "alloc" }, metadata: { caller: "tests" } });
    expect((await value(pool, "poolState")).lastCaller).toBe("tests");
  });

  test("state survives a restart", async () => {
    const pool = unique("pool");
    await value(pool, "alloc");
    await op(pool, "restartPool");
    expect(await value(pool, "alloc")).toBe(1);
  });

  test("a callback that throws commits nothing: its rows, its state, even a state changed in place", async () => {
    const pool = unique("pool");
    expect(await op(pool, "explode")).toMatchObject({ ok: false, code: "INTERNAL", message: "explode" });
    expect(await value(pool, "poolRows")).toBe(0);
    expect((await value(pool, "poolState")).free).toEqual([0, 1, 2]);
  });

  test("an answer can come later, from another event", async () => {
    const pool = unique("pool");
    for (let i = 0; i < 3; i++) await value(pool, "alloc");
    expect(await value(pool, "allocWaitThenFree", [7])).toBe(7);
  });

  test("a timeout ends the wait with DEADLINE_EXCEEDED", async () => {
    const pool = unique("pool");
    const started = Date.now();
    expect(await op(pool, "hang", [], { timeoutMs: 100 })).toMatchObject({ ok: false, code: "DEADLINE_EXCEEDED" });
    expect(Date.now() - started).toBeLessThan(2_000);
  });

  test("a call that arrives after its deadline is not handled", async () => {
    const pool = unique("pool");
    expect(await post("/behaviours", { op: "pastDeadline", name: pool })).toMatchObject({ ok: false, code: "DEADLINE_EXCEEDED" });
    expect(await value(pool, "alloc")).toBe(0);
  });

  test("a repeated idempotency key gets the first answer, and runs once", async () => {
    const pool = unique("pool");
    expect(await value(pool, "allocTwiceWithKey", ["k1"])).toEqual([0, 0]);
    expect(await value(pool, "alloc")).toBe(1);
  });

  test("the same key in flight twice waits on one run", async () => {
    const pool = unique("pool");
    for (let i = 0; i < 3; i++) await value(pool, "alloc");
    const both = op(pool, "allocConcurrentWithKey", ["k2"]);
    await Bun.sleep(100);
    await value(pool, "free", [5]);
    expect(await both).toEqual({ ok: true, value: [5, 5] });
  });

  test("a caller waiting on an object that resets gets an error, not a hang", async () => {
    const pool = unique("pool");
    const settled = await op(pool, "hangThenRestart", [], { timeoutMs: 5_000 });
    expect(settled.ok).toBe(false);
    expect((settled as { code: string }).code).not.toBe("DEADLINE_EXCEEDED");
  });

  test("a raw stub can't run a callback outside an event", async () => {
    const reached = await post<{ handleCall: Settled; states: Settled }>("/behaviours", { op: "reachCallback", name: unique("pool") });
    // Protected methods are on the prototype, so RPC reaches them: the base class refuses them.
    expect(reached.handleCall).toMatchObject({ ok: false });
    expect((reached.handleCall as { message: string }).message).toContain("is a callback");
    // Instance fields (a state machine's handlers) are never exposed over RPC.
    expect(reached.states).toMatchObject({ ok: false });
  });
});

describe("GenStatem", () => {
  test("moves between states on events", async () => {
    const lock = unique("lock") + ":60000";
    expect(await value(lock, "lockStatus")).toEqual({ state: "locked", opened: 0 });
    await value(lock, "enter", ["9912"]);
    expect((await value(lock, "lockStatus")).state).toBe("locked");
    await value(lock, "enter", ["1234"]);
    expect(await value(lock, "lockStatus")).toEqual({ state: "open", opened: 1 });
    await value(lock, "lock");
    expect((await value(lock, "lockStatus")).state).toBe("locked");
  });

  test("a state timeout fires, and survives a restart", async () => {
    const lock = unique("lock") + ":300";
    await value(lock, "enter", ["1234"]);
    await op(lock, "restartLock");
    expect((await value(lock, "lockStatus")).state).toBe("open");
    await Bun.sleep(1_500);
    expect((await value(lock, "lockStatus")).state).toBe("locked");
  });

  test("leaving the state cancels its timeout", async () => {
    const lock = unique("lock") + ":400";
    await value(lock, "enter", ["1234"]);
    await value(lock, "lock");
    await value(lock, "enter", ["12"]); // half a code, kept in data while locked
    await Bun.sleep(1_000);
    await value(lock, "enter", ["34"]);
    expect(await value(lock, "lockStatus")).toEqual({ state: "open", opened: 2 });
  });

  test("postponed events run after the next state change, in order", async () => {
    const lock = unique("lock") + ":60000";
    await value(lock, "enter", ["1234"]);
    await value(lock, "enter", ["1234"]); // presses while open wait
    await value(lock, "lock"); // locked: the postponed presses run and open it again
    expect(await value(lock, "lockStatus")).toEqual({ state: "open", opened: 2 });
  });

  test("a postponed call is answered when the state lets it run", async () => {
    const lock = unique("lock") + ":60000";
    expect(await value(lock, "setCodeWhileLocked", ["0000", "1234"])).toBe(true);
    await value(lock, "lock");
    await value(lock, "enter", ["0000"]);
    expect((await value(lock, "lockStatus")).state).toBe("open");
  });
});

describe("GenEvent", () => {
  test("every handler sees every event, each with its own state", async () => {
    const audit = unique("audit");
    await value(audit, "addHandler", ["total", "counter"]);
    await value(audit, "addHandler", ["last", "recent"]);
    await value(audit, "notify", [{ type: "a" }]);
    await value(audit, "notify", [{ type: "b" }]);
    expect(await value(audit, "callHandler", ["total", "count"])).toBe(2);
    expect(await value(audit, "callHandler", ["last", "list"])).toEqual([{ type: "a" }, { type: "b" }]);
  });

  test("a handler that throws is removed, and the others still see the event", async () => {
    const audit = unique("audit");
    await value(audit, "addHandler", ["fragile", "fragile"]);
    await value(audit, "addHandler", ["total", "counter"]);
    await value(audit, "notify", [{ type: "boom" }]);
    expect(await value(audit, "whichHandlers")).toEqual(["total"]);
    expect(await value(audit, "callHandler", ["total", "count"])).toBe(1);
  });

  test("handlers can remove themselves, and be removed", async () => {
    const audit = unique("audit");
    await value(audit, "addHandler", ["once", "once"]);
    await value(audit, "addHandler", ["total", "counter"]);
    await value(audit, "notify", [{ type: "a" }]);
    expect(await value(audit, "whichHandlers")).toEqual(["total"]);
    expect(await value(audit, "deleteHandler", ["total"])).toBe(true);
    expect(await value(audit, "deleteHandler", ["total"])).toBe(false);
    expect(await op(audit, "addHandler", ["x", "nope"])).toMatchObject({ ok: false, code: "INVALID_ARGUMENT" });
  });

  test("an outbox message from a state machine reaches the event manager", async () => {
    const name = unique("door") + ":60000";
    await value(name, "addHandler", ["last", "recent"]);
    await value(name, "enter", ["1234"]);
    for (let i = 0; i < 50; i++) {
      const events = await value(name, "callHandler", ["last", "list"]);
      if (events.length > 0) {
        expect(events).toEqual([{ type: "opened", lock: name, n: 1 }]);
        return;
      }
      await Bun.sleep(50);
    }
    throw new Error(`the audit log never got the lock's message: ${JSON.stringify(await op(name, "lockOutbox"))}`);
  });
});

describe("DOClient", () => {
  test("calls any object's methods, method by method too", async () => {
    const plain = unique("plain");
    expect(await value(plain, "plain", ["echo", { a: 1 }])).toEqual({ a: 1 });
    expect(await value(plain, "plainProxy")).toEqual(["hi", 2]);
  });

  test("a timeout ends the wait", async () => {
    expect(await op(unique("plain"), "plain", ["sleep", 1_000], { timeoutMs: 50 })).toMatchObject({ ok: false, code: "DEADLINE_EXCEEDED" });
  });

  test("an abort ends the wait with CANCELLED", async () => {
    expect(await op(unique("plain"), "plainAbort")).toMatchObject({ ok: false, code: "CANCELLED" });
  });

  test("the object's errors are UNKNOWN, and not retried", async () => {
    const plain = unique("plain");
    expect(await op(plain, "plain", ["fail", "nope"], { retry: { maxAttempts: 3 } })).toMatchObject({ ok: false, code: "UNKNOWN", message: "nope" });
    expect(await value(plain, "plain", ["calls"])).toBe(1);
  });
});
