// DOClient's retries, deadlines and option merging (RFD0009), against a fake namespace: what the
// runtime throws is reproduced by hand, which cf dev can't do on demand.
import { expect, test } from "bun:test";
import { CallError, DOClient, type Attempt } from "../src/client/index.ts";

type Script = Array<() => Promise<unknown>>;

/** A namespace whose stub runs the next step of `script` on each call, and counts stubs made. */
function fakeNamespace(script: Script) {
  const made = { stubs: 0, calls: 0 };
  const namespace = {
    getByName() {
      made.stubs++;
      return { work: () => script[made.calls++]!() };
    },
  };
  return { namespace: namespace as unknown as DurableObjectNamespace<any>, made };
}

const transient = () => Promise.reject(Object.assign(new Error("Network connection lost"), { retryable: true }));
const overloaded = () => Promise.reject(Object.assign(new Error("Durable Object is overloaded"), { retryable: true, overloaded: true }));
const ok = (value: unknown) => () => Promise.resolve(value);

const work = (client: DOClient<any>) => (client.invoke as (m: string) => Promise<unknown>)("work");

test("retries transient failures, with a new stub each time", async () => {
  const { namespace, made } = fakeNamespace([transient, transient, ok("done")]);
  const client = new DOClient(namespace, `a-${Math.random()}`, { retry: { maxAttempts: 3, baseDelayMs: 1 } });
  expect(await work(client)).toBe("done");
  expect(made.calls).toBe(3);
  expect(made.stubs).toBe(3);
});

test("gives up after maxAttempts with UNAVAILABLE", async () => {
  const { namespace } = fakeNamespace([transient, transient, ok("late")]);
  const client = new DOClient(namespace, `b-${Math.random()}`, { retry: { maxAttempts: 2, baseDelayMs: 1 } });
  await expect(work(client)).rejects.toMatchObject({ code: "UNAVAILABLE" });
});

test("never retries an overloaded object", async () => {
  const { namespace, made } = fakeNamespace([overloaded, ok("no")]);
  const client = new DOClient(namespace, `c-${Math.random()}`, { retry: { maxAttempts: 5, baseDelayMs: 1 } });
  await expect(work(client)).rejects.toMatchObject({ code: "RESOURCE_EXHAUSTED" });
  expect(made.calls).toBe(1);
});

test("keeps one stub while calls succeed", async () => {
  const { namespace, made } = fakeNamespace([ok(1), ok(2), ok(3)]);
  const client = new DOClient(namespace, `d-${Math.random()}`);
  await work(client);
  await work(client.with({ timeoutMs: 1_000 }));
  await work(client);
  expect(made.stubs).toBe(1);
});

test("a retry never waits past the deadline", async () => {
  const { namespace } = fakeNamespace([transient, transient, transient, transient]);
  const client = new DOClient(namespace, `e-${Math.random()}`, { timeoutMs: 30, retry: { maxAttempts: 100, baseDelayMs: 20, maxDelayMs: 20 } });
  const started = Date.now();
  await expect(work(client)).rejects.toBeInstanceOf(CallError);
  expect(Date.now() - started).toBeLessThan(200);
});

test("interceptors wrap every attempt, outermost first, and add up across with()", async () => {
  const { namespace } = fakeNamespace([transient, ok("x")]);
  const seen: string[] = [];
  const tag = (label: string) => (attempt: Attempt, next: (a: Attempt) => Promise<unknown>) => {
    seen.push(`${label}${attempt.attempt}`);
    return next(attempt);
  };
  const client = new DOClient(namespace, `f-${Math.random()}`, { interceptors: [tag("a")] }).with({ interceptors: [tag("b")], retry: { maxAttempts: 2, baseDelayMs: 1 } });
  expect(await work(client)).toBe("x");
  expect(seen).toEqual(["a1", "b1", "a2", "b2"]);
});

test("with() narrows options: a per-call timeout replaces the client's deadline, metadata merges", () => {
  const { namespace } = fakeNamespace([]);
  const client = new DOClient(namespace, "g", { deadline: 1, metadata: { a: "1" } }).with({ timeoutMs: 5, metadata: { b: "2" } });
  expect(client.options).toEqual({ timeoutMs: 5, metadata: { a: "1", b: "2" } });
});
