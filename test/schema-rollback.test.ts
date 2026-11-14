// Rollbacks: during a deploy, and again on a rollback, older code runs against storage that newer
// code has changed. Older code must never drop schema it merely doesn't know about, or a rollback
// and the roll forward after it lose an index, or rebuild it, which blocks the shard.
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";

useWorker();

type Result<T> = { ok: T } | { error: string; message: string };
type WriteResult = { ok: boolean; version: number; conflict?: string };

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const ok = async <T>(r: Promise<Result<T>>): Promise<T> => {
  const v = await r;
  if (!("ok" in v)) throw new Error(`${v.error}: ${v.message}`);
  return v.ok;
};
async function eventually<T>(read: () => Promise<T>, check: (v: T) => boolean, ms = 30_000): Promise<T> {
  const until = Date.now() + ms;
  for (;;) {
    const v = await read();
    if (check(v) || Date.now() > until) return v;
    await sleep(200);
  }
}
const user = (id: string, email: string) => ({ id, email, name: id, team: "core", createdAt: 1, active: true });
type Status = { building: string[] };

/** The evolving IndexedTable: schema 1 has no byEmail; schema 2 adds it, unique. */
function evolvingUsers(name: string) {
  const call = <T>(operation: string, args: unknown[], version?: number) => post<T>("/indexed", { cls: "evolving", name, op: operation, args, version });
  return {
    call,
    deploy: (version: number) => call("upgrade", [""], version),
    put: (row: ReturnType<typeof user>) => ok(call<Result<WriteResult>>("put", [row, {}])),
    lookup: async (email: string) => (await ok(call<Result<{ row: { id: string } } | null>>("lookup", ["byEmail", [email]])))?.row.id ?? null,
    built: () => eventually(() => ok(call<Result<Status>>("indexStatus", [])), (s: Status) => s.building.length === 0),
    entries: async () => (await call<{ entries: number }[]>("indexStats", [])).reduce((n, s) => n + s.entries, 0),
  };
}

test("a global index finds every row after a rollback and the roll forward after it", async () => {
  const t = evolvingUsers(`rollback-global-${unique()}`);
  await t.deploy(2);
  await t.put(user("a", "a@example.com"));
  await t.built();
  expect(await t.lookup("a@example.com")).toBe("a");
  // The rollback: code that has never heard of byEmail serves for a while.
  await t.deploy(1);
  await t.put(user("b", "b@example.com"));
  await sleep(4_000);
  // The roll forward: byEmail is declared again, with the same definition.
  await t.deploy(2);
  await t.put(user("c", "c@example.com"));
  await t.built();
  expect(await t.lookup("a@example.com")).toBe("a");
  expect(await t.lookup("b@example.com")).toBe("b");
  expect(await t.lookup("c@example.com")).toBe("c");
}, 90_000);

test("a unique global index refuses a taken value again after a rollback and the roll forward", async () => {
  const t = evolvingUsers(`rollback-unique-${unique()}`);
  await t.deploy(2);
  await t.put(user("a", "a@example.com"));
  await t.built();
  await t.deploy(1);
  await t.put(user("b", "b@example.com"));
  await sleep(4_000);
  await t.deploy(2);
  await t.put(user("c", "c@example.com"));
  await t.built();
  expect(await t.put(user("other-b", "b@example.com"))).toMatchObject({ ok: false, conflict: "byEmail" });
  expect(await t.put(user("other-c", "c@example.com"))).toMatchObject({ ok: false, conflict: "byEmail" });
}, 90_000);

test("a global index holds one entry per row per index after a rollback and the roll forward", async () => {
  const t = evolvingUsers(`rollback-entries-${unique()}`);
  await t.deploy(2);
  await t.put(user("a", "a@example.com"));
  await t.built();
  expect(await t.entries()).toBe(2); // byEmail and byTeam
  await t.deploy(1);
  await t.put(user("b", "b@example.com"));
  await sleep(4_000);
  await t.deploy(2);
  await t.put(user("c", "c@example.com"));
  await t.built();
  expect(await eventually(t.entries, (n) => n === 2 * 3)).toBe(2 * 3);
}, 90_000);

/** The evolving ShardedTable: schema 1 indexes byEdit; schema 2 has a stars column and byStars instead. */
function evolvingDocs(name: string) {
  const call = <T>(operation: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op: operation, args });
  return { call, deploy: (version: number) => call("upgrade", version, "") };
}
const docs = (n: number) => Array.from({ length: n }, (_, i) => ({ name: `doc-${String(i).padStart(4, "0")}`, id: `id-${i}`, createdAt: i, lastEditAt: i, deleted: false, meta: null, stars: i % 5 }));

test("a rollback keeps the local index the newer schema declared", async () => {
  const t = evolvingDocs(`rollback-local-${unique()}`);
  await t.deploy(2);
  await t.call("putMany", docs(50).map((row) => ({ row })));
  expect((await t.call<string[]>("indexNames")).some((n) => n.includes("_byStars_"))).toBe(true);
  await t.deploy(1);
  await t.call("get", "doc-0000");
  expect((await t.call<string[]>("indexNames")).some((n) => n.includes("_byStars_"))).toBe(true);
}, 60_000);

test("a roll forward after a rollback serves at once: nothing is rebuilt", async () => {
  const name = `rollback-rebuild-${unique()}`;
  const t = evolvingDocs(name);
  await t.deploy(2);
  // More rows than one build call covers (EvolvingDocs builds 300 a call).
  const rows = docs(900);
  for (let i = 0; i < rows.length; i += 150) await t.call("putMany", rows.slice(i, i + 150).map((row) => ({ row })));
  await t.deploy(1);
  await t.call("get", "doc-0000");
  await t.deploy(2);
  await t.call("get", "doc-0000");
  // The shard restarted with the deploy: its counters count from it.
  const shard = await post<{ counters: { notReady: number } }>("/describe", { cls: "evolving", name, bits: "" });
  expect(shard.counters.notReady).toBe(0);
}, 120_000);
