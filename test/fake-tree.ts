// A sharded object's tree, faked for unit tests of the client: its leaves are bit strings, and
// each answers what the test says. A node above a leaf answers "moved", as a shard that split
// does; a node below one answers "stale". Outside workerd, the client's module needs
// `cloudflare:workers` stood in for: call `loadClient()` first.
import { mock } from "bun:test";

export type Answer = { ok: unknown } | { moved: string } | { notReady: string; until?: number } | { stale: string } | Record<string, unknown>;
export type Kind = "call" | "scan";
/** What a leaf answers: an answer, or an error to throw. */
export type Leaf = (kind: Kind, method: string, keys: readonly string[]) => Answer | Error | Promise<Answer | Error>;

export interface FakeTree {
  readonly namespace: DurableObjectNamespace<never>;
  /** Calls made, by the bits they asked for, in order. */
  readonly calls: { bits: string; kind: Kind | "describe"; method: string }[];
  /** The stub options each stub was made with (null: none). */
  readonly stubOptions: (unknown | null)[];
  /** The most calls in flight at once. */
  maxInFlight: number;
}

export async function loadClient(): Promise<typeof import("../src/sharded/client.ts")> {
  mock.module("cloudflare:workers", () => ({ DurableObject: class {} }));
  return import("../src/sharded/client.ts");
}

/** A tree whose shards are `leaves` (bits; "" for one shard), each answering `leaf`. */
export function fakeTree(leaves: readonly string[], leaf: Leaf): FakeTree {
  const set = new Set(leaves);
  const answerFor = async (kind: Kind, bits: string, method: string, keys: readonly string[]): Promise<Answer> => {
    if (set.has(bits)) {
      const a = await leaf(kind, method, keys);
      if (a instanceof Error) throw a;
      return a;
    }
    if (leaves.some((l) => l.startsWith(bits))) return { moved: bits };
    return { stale: bits };
  };
  let inFlight = 0;
  const tree: FakeTree = {
    namespace: null as never,
    calls: [],
    stubOptions: [],
    maxInFlight: 0,
  };
  const track = async <A>(f: () => Promise<A>): Promise<A> => {
    inFlight++;
    tree.maxInFlight = Math.max(tree.maxInFlight, inFlight);
    try {
      await new Promise((resolve) => setTimeout(resolve, 1));
      return await f();
    } finally {
      inFlight--;
    }
  };
  const stub = (objectName: string) => ({
    shardedCall: (_name: string, bits: string, method: string, keys: readonly string[]) => {
      tree.calls.push({ bits, kind: "call", method });
      return track(() => answerFor("call", bits, method, keys));
    },
    shardedScan: (_name: string, bits: string, method: string) => {
      tree.calls.push({ bits, kind: "scan", method });
      return track(() => answerFor("scan", bits, method, []));
    },
    shardedDescribe: async () => {
      tree.calls.push({ bits: objectName, kind: "describe", method: "" });
      return { bits: leaves.find((l) => l.replace(/0+$/, "") === objectName.split("/").pop()) ?? "", presplit: 0, state: "shard", rows: 0, storedRows: 0, callsPerSecond: 0, lastSplit: null, lastSplitError: null, splits: [], counters: {} };
    },
  });
  (tree as { namespace: unknown }).namespace = {
    idFromName: (name: string) => ({ toString: () => name, name }),
    get: (id: { name: string }, options?: unknown) => {
      tree.stubOptions.push(options ?? null);
      return stub(id.name);
    },
  };
  return tree;
}

/** A stand-in for `caches.default`, shared by every client in the test: the colo's cache. */
export function fakeColoCache(): Map<string, string> {
  const store = new Map<string, string>();
  (globalThis as { caches?: unknown }).caches = {
    default: {
      async match(url: string) {
        const body = store.get(url);
        return body === undefined ? undefined : new Response(body);
      },
      async put(url: string, response: Response) {
        store.set(url, await response.text());
      },
    },
  };
  return store;
}
