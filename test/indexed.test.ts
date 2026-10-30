// IndexedTable under cf dev: unique global indexes (conflicts, races, moves, deletes), lookups and
// listings within a partition, reservations left behind by writers that never wrote, deliveries that
// fail and are retried by the alarm, and splits of both the row and the index shards while writing.
import { expect, test } from "bun:test";
import { entryOf, globalIndexes } from "../src/indexed/entries.ts";
import { defineTable } from "../src/table/schema.ts";
import { post, shards, useWorker } from "./harness.ts";

useWorker();

type User = { id: string; email: string | null; name: string; team: string; createdAt: number; active: boolean };
type Versioned = { row: User; version: number };
type Result<T> = { ok: T } | { error: string; message: string; version?: number; index?: string };
type WriteResult = { ok: boolean; version: number; conflict?: string; refused?: string; indexed?: boolean };

// The test Worker's schema, for computing entry addresses here.
const users = defineTable<User>({
  primaryKey: "id",
  columns: { id: "text", email: "text", name: "text", team: "text", createdAt: "integer", active: "boolean" },
  indexes: { byName: ["name"] },
  globalIndexes: { byEmail: { columns: ["email"], unique: true }, byTeam: { columns: ["team", "createdAt"], partition: 1 } },
});
const byEmail = globalIndexes(users).find((ix) => ix.name === "byEmail")!;

let n = 0;
const fresh = (what: string) => `${what}-${Date.now()}-${n++}`;
const op = <T>(name: string, operation: string, ...args: unknown[]) => post<T>("/indexed", { name, op: operation, args });
const ok = async <T>(r: Promise<Result<T>>): Promise<T> => {
  const v = await r;
  if (!("ok" in v)) throw new Error(`${v.error}: ${v.message}`);
  return v.ok;
};
const put = (name: string, row: User, options: { ifVersion?: number } = {}) => ok(op<Result<WriteResult>>(name, "put", row, options));
const lookup = (name: string, email: string) => ok(op<Result<Versioned | null>>(name, "lookup", "byEmail", [email]));
const user = (id: string, email: string | null, team = "core", createdAt = 1): User => ({ id, email, name: id, team, createdAt, active: true });
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function eventually<T>(read: () => Promise<T>, check: (v: T) => boolean, ms = 15_000): Promise<T> {
  const until = Date.now() + ms;
  for (;;) {
    const v = await read();
    if (check(v) || Date.now() > until) return v;
    await sleep(200);
  }
}

test("a write is found through its unique index as soon as it returns, and a second row with the value is refused", async () => {
  const t = fresh("users");
  expect(await put(t, user("u1", "ada@example.com"), { ifVersion: 0 })).toEqual({ ok: true, version: 1 });
  expect(await lookup(t, "ada@example.com")).toEqual({ row: user("u1", "ada@example.com"), version: 1 });
  expect(await put(t, user("u2", "ada@example.com"), { ifVersion: 0 })).toEqual({ ok: false, version: 0, conflict: "byEmail" });
  expect(await ok(op<Result<Versioned | null>>(t, "get", "u2"))).toBeNull();
  // The same row writing its own value again is no conflict.
  expect(await put(t, { ...user("u1", "ada@example.com"), name: "Ada" })).toEqual({ ok: true, version: 2 });
  expect((await lookup(t, "ada@example.com"))!.row.name).toBe("Ada");
  // Rows with no value are not indexed, and do not conflict.
  expect((await put(t, user("u3", null))).ok).toBe(true);
  expect((await put(t, user("u4", null))).ok).toBe(true);
});

test("moving to another value frees the old one, and deleting frees it too", async () => {
  const t = fresh("users");
  await put(t, user("u1", "a@example.com"));
  expect(await put(t, user("u1", "b@example.com"))).toEqual({ ok: true, version: 2 });
  expect(await lookup(t, "a@example.com")).toBeNull();
  expect((await lookup(t, "b@example.com"))!.row.id).toBe("u1");
  expect((await put(t, user("u2", "a@example.com"), { ifVersion: 0 })).ok).toBe(true);
  expect((await lookup(t, "a@example.com"))!.row.id).toBe("u2");

  expect(await ok(op<Result<WriteResult>>(t, "delete", "u1"))).toEqual({ ok: true, version: 0 });
  expect(await lookup(t, "b@example.com")).toBeNull();
  expect((await put(t, user("u3", "b@example.com"), { ifVersion: 0 })).ok).toBe(true);
  // Recreated, the row's new entry is not hidden by the old one's removal.
  expect((await put(t, user("u1", "c@example.com"), { ifVersion: 0 })).ok).toBe(true);
  expect((await lookup(t, "c@example.com"))!.row.id).toBe("u1");
});

test("patches reserve the unique values they change", async () => {
  const t = fresh("users");
  await put(t, user("u1", "a@example.com"));
  await put(t, user("u2", "b@example.com"));
  expect(await ok(op<Result<WriteResult>>(t, "patch", "u2", { email: "a@example.com" }))).toEqual({ ok: false, version: 0, conflict: "byEmail" });
  expect(await ok(op<Result<WriteResult>>(t, "patch", "u2", { email: "c@example.com" }))).toEqual({ ok: true, version: 2 });
  expect((await lookup(t, "c@example.com"))!.row.id).toBe("u2");
  expect(await lookup(t, "b@example.com")).toBeNull();
  // A patch of other columns updates the row copy in the index.
  await ok(op<Result<WriteResult>>(t, "patch", "u2", { name: "Bee" }));
  expect((await lookup(t, "c@example.com"))!).toMatchObject({ row: { name: "Bee" }, version: 3 });
});

test("many writers racing for one value: exactly one gets it", async () => {
  const t = fresh("users");
  const results = await Promise.all(Array.from({ length: 12 }, (_, i) => put(t, user(`racer-${i}`, "same@example.com"), { ifVersion: 0 })));
  const winners = results.filter((r) => r.ok);
  expect(winners.length).toBe(1);
  for (const r of results) if (!r.ok) expect(r.conflict).toBe("byEmail");
  const found = await lookup(t, "same@example.com");
  const rows = await ok(op<Result<(Versioned | null)[]>>(t, "getMany", Array.from({ length: 12 }, (_, i) => `racer-${i}`)));
  expect(rows.filter((r) => r !== null).map((r) => r!.row.id)).toEqual([found!.row.id]);
});

test("a reservation whose writer never wrote is resolved, and the writer's token is fenced", async () => {
  const t = fresh("users");
  const e = entryOf(users, byEmail, user("ghost", "taken@example.com"))!;
  // A writer reserves the value and dies before writing its row.
  const [reserved] = await op<{ reserved: number }[]>(t, "rawReserve", [e.route], [{ entry: e.entry, owner: "ghost" }]);
  expect(reserved!.reserved).toBeGreaterThan(0);
  // Another row asks: the index asks the ghost's row shard, which fences the token, and the value passes on.
  expect((await put(t, user("u1", "taken@example.com"), { ifVersion: 0 })).ok).toBe(true);
  // The ghost's write arrives late, with its token: refused, and nothing is written.
  const [late] = await op<WriteResult[]>(t, "rawPut", ["ghost"], [{ row: user("ghost", "taken@example.com"), tokens: { byEmail: reserved!.reserved } }]);
  expect(late).toEqual({ ok: false, version: 0, refused: "byEmail" });
  expect((await lookup(t, "taken@example.com"))!.row.id).toBe("u1");
});

test("a reservation left by a failed version check is resolved by the alarm", async () => {
  const t = fresh("users");
  await put(t, user("u1", "old@example.com"));
  // Reserves new@, then fails its version check: new@ stays reserved by u1, unwritten.
  expect(await put(t, user("u1", "new@example.com"), { ifVersion: 7 })).toEqual({ ok: false, version: 1 });
  const pending = (s: { pending: number }[]) => s.reduce((a, b) => a + b.pending, 0);
  expect(pending(await op<{ pending: number }[]>(t, "indexStats"))).toBe(1);
  // The alarm asks u1's shard once the reservation is old enough, and drops it.
  expect(pending(await eventually(() => op<{ pending: number }[]>(t, "indexStats"), (s) => pending(s) === 0))).toBe(0);
  expect(await lookup(t, "new@example.com")).toBeNull();
  expect((await put(t, user("u2", "new@example.com"), { ifVersion: 0 })).ok).toBe(true);
});

test("listing a partition of a global index reads one index shard, in index order, with filters", async () => {
  const t = fresh("users");
  const rows = Array.from({ length: 40 }, (_, i) => ({ ...user(`m-${String(i).padStart(2, "0")}`, `m${i}@example.com`, i % 3 === 0 ? "web" : "core", 1_000 + ((i * 7) % 13)), active: i % 4 !== 0 }));
  const written = await ok(op<Result<WriteResult[]>>(t, "putMany", rows.map((row) => ({ row }))));
  expect(written.every((w) => w.ok)).toBe(true);
  const core = rows.filter((r) => r.team === "core").sort((a, b) => b.createdAt - a.createdAt || (a.id < b.id ? 1 : -1));
  const listed = await op<Versioned[]>(t, "listAll", { index: "byTeam", partition: ["core"], direction: "desc", limit: 7 });
  expect(listed.map((r) => r.row.id)).toEqual(core.map((r) => r.id));
  const active = await op<Versioned[]>(t, "listAll", { index: "byTeam", partition: ["core"], where: [{ column: "active", op: "=", value: true }], limit: 5, budget: 5 });
  expect(active.map((r) => r.row.id).sort()).toEqual(core.filter((r) => r.active).map((r) => r.id).sort());
  // Moving a row to another team moves its entry.
  await put(t, { ...rows[1]!, team: "web" });
  const web = await op<Versioned[]>(t, "listAll", { index: "byTeam", partition: ["web"] });
  expect(web.map((r) => r.row.id)).toContain(rows[1]!.id);
  expect((await op<Versioned[]>(t, "listAll", { index: "byTeam", partition: ["core"] })).map((r) => r.row.id)).not.toContain(rows[1]!.id);
});

test("custom methods deliver their index changes, and cannot change a unique value without a reservation", async () => {
  const t = fresh("users");
  await put(t, user("u1", "a@example.com"));
  await op(t, "rawCall", "rename", ["u1"], [{ name: "Renamed" }]);
  expect((await lookup(t, "a@example.com"))!.row.name).toBe("Renamed");
  expect(await op(t, "rawCall", "setEmail", ["u1"], [{ email: "b@example.com" }])).toEqual(["byEmail"]);
  expect((await lookup(t, "a@example.com"))!.row.id).toBe("u1");
});

test("a write whose entries cannot be delivered throws IndexPending, and the alarm delivers them later", async () => {
  const t = fresh("users");
  await put(t, user("u0", "warm@example.com")); // the index shard exists
  await op(t, "failApply", true);
  const r = await op<Result<WriteResult>>(t, "put", user("u1", "late@example.com"));
  expect(r).toMatchObject({ error: "IndexPending", version: 1 });
  expect(await ok(op<Result<Versioned | null>>(t, "get", "u1"))).toMatchObject({ version: 1 });
  expect(await op<number>(t, "outbox")).toBeGreaterThan(0);
  await op(t, "failApply", false);
  expect(await eventually(() => op<number>(t, "outbox"), (left) => left === 0)).toBe(0);
  expect((await lookup(t, "late@example.com"))!.row.id).toBe("u1");
});

test("splits of the rows and of the index while writing: every row is found through every index, and no entry is left without its row", async () => {
  const t = fresh("users");
  const model = new Map<string, User>();
  const write = async (round: number) => {
    const batch = Array.from({ length: 30 }, (_, i) => {
      const id = `s-${(round * 17 + i * 7) % 60}`;
      // Emails move between rows: a row takes a value another row may have just let go of.
      return user(id, `e${(round * 5 + i * 3) % 45}@example.com`, i % 2 ? "core" : "web", round * 100 + i);
    });
    const results = await Promise.all(batch.map((row) => put(t, row)));
    results.forEach((r, i) => {
      if (r.ok) model.set(batch[i]!.id, batch[i]!);
    });
  };
  await write(0);
  const splits = [
    op(t, "split", "rows", ""),
    write(1),
    op(t, "split", "index", ""),
    write(2),
  ];
  const [rowsSplit, , indexSplit] = await Promise.all(splits);
  expect([rowsSplit, indexSplit]).toEqual([true, true]);
  expect(await op(t, "split", "rows", "0")).toBe(true);
  await write(3);
  expect(await op(t, "split", "index", "1")).toBe(true);
  await write(4);
  expect((await shards("users", t)).length).toBe(3);
  expect((await shards("userIndexes", t)).length).toBe(3);

  // The rows as stored are the truth; check every index against them.
  const stored = await ok(op<Result<(Versioned | null)[]>>(t, "getMany", [...model.keys()]));
  const rows = stored.filter((s): s is Versioned => s !== null).map((s) => s.row);
  const emails = new Map<string, string>();
  for (const r of rows) {
    if (r.email === null) continue;
    expect(emails.has(r.email)).toBe(false); // unique
    emails.set(r.email, r.id);
  }
  for (const [email, id] of emails) expect((await lookup(t, email))?.row.id).toBe(id);
  for (let i = 0; i < 45; i++) {
    const email = `e${i}@example.com`;
    if (!emails.has(email)) expect(await lookup(t, email)).toBeNull();
  }
  for (const team of ["core", "web"]) {
    const listed = await op<Versioned[]>(t, "listAll", { index: "byTeam", partition: [team] });
    const expected = rows.filter((r) => r.team === team).sort((a, b) => a.createdAt - b.createdAt || (a.id < b.id ? -1 : 1));
    expect(listed.map((r) => r.row.id)).toEqual(expected.map((r) => r.id));
  }
  expect(await op<number>(t, "outbox")).toBe(0);
}, 120_000);
