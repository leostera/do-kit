// The client keeps one stub per shard. A kept stub made in another request cannot be used: the
// runtime refuses to send on it, so the call never ran and is made again on a new stub. Any
// other error may come after the method ran, so the call is not made again, whatever the error
// says: a method's own error can mention "a different request" too.
import { expect, mock, test } from "bun:test";

// The client imports the object's module for its helpers; outside workerd its base class is a stand-in.
mock.module("cloudflare:workers", () => ({ DurableObject: class {} }));
const { sharded } = await import("../src/sharded/client.ts");

/** Thrown by the runtime when a stub made in one request is used in another: nothing was sent. */
const refused = () => new Error("Cannot perform I/O on behalf of a different request. I/O objects (such as streams, request/response bodies, and others) created in the context of one request handler cannot be accessed from a different request's handler.");

/** A namespace of one shard, whose method counts each run and throws `fail(run)` on the runs it names. */
function fakeNamespace(fail: (run: number) => Error | null) {
  let runs = 0;
  let sends = 0;
  const stub = (): unknown => {
    let used = false;
    return {
      async shardedCall(_name: string, _bits: string, _method: string, keys: readonly string[]) {
        sends++;
        // The runtime refuses a stub made in an earlier request: the second use of a kept stub here.
        if (used && fail(-1)) throw fail(-1);
        used = true;
        runs++;
        const error = fail(runs);
        if (error) throw error;
        return { ok: keys.map(() => runs) };
      },
    };
  };
  const namespace = { idFromName: (name: string) => ({ toString: () => name, name }), get: () => stub() };
  return { namespace: namespace as unknown as DurableObjectNamespace<never>, runs: () => runs, sends: () => sends };
}

test("a shard method's own error that mentions a different request is not retried", async () => {
  const appError = Object.assign(new Error("app: Cannot perform I/O on behalf of a different request"), { remote: true });
  const fake = fakeNamespace((run) => (run === 2 ? appError : null));
  const client = sharded(fake.namespace, `retry-app-${Date.now()}`) as unknown as { call(keys: string[], method: string, payloads: null): Promise<unknown> };
  await client.call(["a"], "get", null); // keeps the stub
  await expect(client.call(["a"], "get", null)).rejects.toThrow("app: Cannot perform I/O");
  expect(fake.runs()).toBe(2);
});

test("a kept stub the runtime refuses to send on is replaced, and the call made once on the new one", async () => {
  const fake = fakeNamespace((run) => (run === -1 ? refused() : null));
  const client = sharded(fake.namespace, `retry-refused-${Date.now()}`) as unknown as { call(keys: string[], method: string, payloads: null): Promise<{ result: number[] }[]> };
  await client.call(["a"], "get", null);
  expect((await client.call(["a"], "get", null))[0]!.result).toEqual([2]);
  expect(fake.runs()).toBe(2);
});
