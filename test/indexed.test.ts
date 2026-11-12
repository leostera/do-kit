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
  // An update sent optimistically fails its version check before it reserves anything.
  expect(await put(t, user("u1", "new@example.com"), { ifVersion: 7 })).toEqual({ ok: false, version: 1 });
  const pendingNow = (s: { pending: number }[]) => s.reduce((a, b) => a + b.pending, 0);
  expect(pendingNow(await op<{ pending: number }[]>(t, "indexStats"))).toBe(0);
  // A create reserves first: here it fails its version check (u1 exists), and new@ stays reserved by u1, unwritten.
  expect(await put(t, user("u1", "new@example.com"), { ifVersion: 0 })).toEqual({ ok: false, version: 1 });
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

test("a batch larger than one SQL statement's parameters is written and delivered", async () => {
  const t = fresh("users");
  const rows = Array.from({ length: 120 }, (_, i) => ({ row: user(`b-${i}`, `b${i}@example.com`, `team-${i % 3}`, i) }));
  const written = await ok(op<Result<WriteResult[]>>(t, "putMany", rows));
  expect(written.every((w) => w.ok)).toBe(true);
  expect((await lookup(t, "b119@example.com"))!.row.id).toBe("b-119");
  expect(await op<number>(t, "outbox")).toBe(0);
});

test("custom methods deliver their index changes, and cannot change a unique value without a reservation", async () => {
  const t = fresh("users");
  await put(t, user("u1", "a@example.com"));
  await op(t, "rawCall", "rename", ["u1"], [{ name: "Renamed" }]);
  expect((await lookup(t, "a@example.com"))!.row.name).toBe("Renamed");
  expect(await op(t, "rawCall", "setEmail", ["u1"], [{ email: "b@example.com" }])).toEqual(["byEmail"]);
  expect((await lookup(t, "a@example.com"))!.row.id).toBe("u1");
});

test("a reservation whose alarm could not be set is still resolved, once a later write on its index shard comes", async () => {
  const t = fresh("users");
  await put(t, user("u0", "warm@example.com"));
  await sleep(2_500); // the index shard's alarm has run, and nothing is left for it
  // The reservation is stored, and its wake; setting the alarm for it fails.
  await op(t, "failSetAlarmOnce", "");
  expect(await op<Result<WriteResult>>(t, "put", user("u1", "a@example.com"))).toMatchObject({ error: "Error" });
  // Another write reserves on the same shard (reservations time out after 1 s in the test class).
  await put(t, user("u2", "b@example.com"));
  const pending = (s: { pending: number }[]) => s.reduce((a, x) => a + x.pending, 0);
  expect(pending(await eventually(() => op<{ pending: number }[]>(t, "indexStats"), (s) => pending(s) === 0, 8_000))).toBe(0);
}, 30_000);

test("a reservation whose wake could not be stored is still resolved by the next write's wake", async () => {
  const t = fresh("users");
  await put(t, user("u0", "warm@example.com"));
  await sleep(2_500); // the index shard's alarm has run, and nothing is left for it
  // The reservation is stored; storing the wake that would resolve it fails, so no alarm comes.
  await op(t, "failStatementOnce", "index", "", "INSERT INTO sharded_meta (key, value) VALUES ('wake'");
  expect(await op<Result<WriteResult>>(t, "put", user("u1", "a@example.com"))).toMatchObject({ error: "Error" });
  // The next write's reservation asks for a wake of its own: it must not be taken for done.
  await put(t, user("u2", "b@example.com"));
  const pending = (s: { pending: number }[]) => s.reduce((a, x) => a + x.pending, 0);
  expect(pending(await eventually(() => op<{ pending: number }[]>(t, "indexStats"), (s) => pending(s) === 0, 8_000))).toBe(0);
}, 30_000);

test("index changes whose alarm was lost are still delivered, once a call reaches their row shard", async () => {
  const t = fresh("users");
  await put(t, user("u0", "warm@example.com"));
  await op(t, "failApply", true);
  expect(await op<Result<WriteResult>>(t, "put", user("u1", "late@example.com"))).toMatchObject({ error: "IndexPending" });
  await op(t, "loseAlarm", "");
  await op(t, "failApply", false);
  // Past ALARM_CHECK_MS since the last call to the row shard; then a read reaches it.
  await sleep(5_500);
  await ok(op<Result<Versioned | null>>(t, "get", "u0"));
  expect(await eventually(() => op<number>(t, "outbox"), (left) => left === 0, 8_000)).toBe(0);
  expect((await lookup(t, "late@example.com"))!.row.id).toBe("u1");
}, 30_000);

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

test("a custom write whose entries cannot be delivered throws IndexPending, and the alarm delivers them soon", async () => {
  const t = fresh("users");
  await put(t, user("u1", "a@example.com"));
  await op(t, "failApply", true);
  expect(String(await op(t, "rawCall", "rename", ["u1"], [{ name: "Renamed" }]).catch((e: unknown) => e))).toContain("IndexPending");
  await op(t, "failApply", false);
  // Well before the 30 s a write asks for the alarm in case it never delivers.
  expect(await eventually(() => op<number>(t, "outbox"), (left) => left === 0, 10_000)).toBe(0);
  expect((await lookup(t, "a@example.com"))!.row.name).toBe("Renamed");
});

test("rows that move in a split while their entries are undelivered still reach the index", async () => {
  const t = fresh("users");
  await put(t, user("w0", "w0@example.com")); // the index shard exists
  await op(t, "failApply", true);
  const ids = Array.from({ length: 12 }, (_, i) => `m-${i}`);
  for (const id of ids) expect(await op<Result<WriteResult>>(t, "put", user(id, `${id}@example.com`))).toMatchObject({ error: "IndexPending" });
  // Half the rows move to a new shard, and their undelivered entries with them: each shard ends up
  // holding the entries of its own keys only, and sends them from its own rows.
  expect(await op(t, "split", "rows", "")).toBe(true);
  const settled = await eventually(() => op<{ key: string; owned: boolean }[][]>(t, "outboxKeys"), (shards) => shards.length === 2 && shards.flat().every((e) => e.owned), 30_000);
  expect(settled.map((s) => s.length).every((n) => n > 0)).toBe(true);
  expect(settled.flat().map((e) => e.key).sort()).toEqual([...ids].sort());
  await op(t, "failApply", false);
  expect(await eventually(() => op<number>(t, "outbox"), (left) => left === 0, 30_000)).toBe(0);
  for (const id of ids) expect((await lookup(t, `${id}@example.com`))?.row.id).toBe(id);
}, 60_000);

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

test("a unique index added to a table that has rows is built over them, and reports the rows that share a value", async () => {
  const t = fresh("evolving-users");
  const evolve = <T>(operation: string, ...args: unknown[]) => post<T>("/indexed", { cls: "evolving", name: t, op: operation, args });
  // 450 users under the first schema (no index on email), two of them with the same email. Split
  // once, so the index is built by two shards, each over its own rows.
  const people = Array.from({ length: 450 }, (_, i) => user(`p-${i}`, i === 7 ? "p-3@example.com" : `p-${i}@example.com`));
  for (let i = 0; i < people.length; i += 90) await evolve("putMany", people.slice(i, i + 90).map((row) => ({ row })));
  expect(await evolve("split", "rows", "")).toBe(true);
  await evolve("upgrade", "", "1");
  type Status = { building: string[]; duplicates: { index: string; key: string; owner: string }[] };
  const status = await eventually(() => ok(evolve<Result<Status>>("indexStatus")), (s) => s.building.length === 0, 60_000);
  // One of the two rows with the email has it; the other is reported, with the row that has it.
  expect(status.duplicates.length).toBe(1);
  const [dup] = status.duplicates;
  expect(dup!.index).toBe("byEmail");
  expect([dup!.key, dup!.owner].sort()).toEqual(["p-3", "p-7"]);
  const find = (email: string) => ok(evolve<Result<Versioned | null>>("lookup", "byEmail", [email]));
  expect((await find("p-3@example.com"))?.row.id).toBe(dup!.owner);
  // Every other row is found by its email, at the version it has.
  for (const i of [0, 1, 100, 249, 449]) expect((await find(`p-${i}@example.com`))?.row.id).toBe(`p-${i}`);
  expect((await evolve<{ entries: number }[]>("indexStats")).reduce((n, s) => n + s.entries, 0)).toBe(2 * people.length - 1);
  // The unique index is enforced from here on, and writing the reported row with a free value settles it.
  expect(await ok(evolve<Result<WriteResult>>("put", user("new", "p-100@example.com"), { ifVersion: 0 }))).toMatchObject({ ok: false, conflict: "byEmail" });
  expect(await ok(evolve<Result<WriteResult>>("patch", dup!.key, { email: "free@example.com" }))).toMatchObject({ ok: true });
  expect((await find("free@example.com"))?.row.id).toBe(dup!.key);
  expect((await ok(evolve<Result<Status>>("indexStatus"))).duplicates).toEqual([]);
}, 120_000);

test("an index the schema drops has its entries dropped, and one redefined under the same name is built again apart", async () => {
  const t = fresh("evolving-users");
  const evolve = <T>(operation: string, args: unknown[], version?: number) => post<T>("/indexed", { cls: "evolving", name: t, op: operation, args, version });
  const entries = async () => (await evolve<{ entries: number }[]>("indexStats", [])).reduce((n, s) => n + s.entries, 0);
  // 300 users on two row shards, under the schema with byEmail and byTeam (team, createdAt).
  await evolve("upgrade", ["", "0", "1"], 2);
  const people = Array.from({ length: 300 }, (_, i) => ({ ...user(`p-${i}`, `p-${i}@example.com`, i % 2 ? "web" : "core", 300 - i), name: `n-${String(i).padStart(3, "0")}` }));
  for (let i = 0; i < people.length; i += 100) await evolve("putMany", [people.slice(i, i + 100).map((row) => ({ row }))]);
  expect(await evolve("split", ["rows", ""])).toBe(true);
  expect(await entries()).toBe(2 * people.length);
  // The next deploy drops byEmail, and orders byTeam by name instead.
  await evolve("upgrade", ["0", "1"], 3);
  type Status = { building: string[] };
  await eventually(() => ok(evolve<Result<Status>>("indexStatus", [], 3)), (s) => s.building.length === 0, 60_000);
  // Only the new byTeam's entries are left: byEmail's, and the old byTeam's, are gone.
  expect(await eventually(entries, (n) => n === people.length, 30_000)).toBe(people.length);
  for (const team of ["core", "web"]) {
    const listed = await evolve<Versioned[]>("listAll", [{ index: "byTeam", partition: [team] }], 3);
    expect(listed.map((r) => r.row.id)).toEqual(people.filter((p) => p.team === team).sort((a, b) => (a.name < b.name ? -1 : 1)).map((p) => p.id));
  }
  // Writes from here on change the new index only.
  expect(await ok(evolve<Result<WriteResult>>("patch", ["p-0", { name: "a-first" }], 3))).toMatchObject({ ok: true });
  expect((await evolve<Versioned[]>("listAll", [{ index: "byTeam", partition: ["core"], limit: 1 }], 3))[0]!.row.id).toBe("p-0");
  expect(await entries()).toBe(people.length);
  // A put of the dropped index that comes late (from a row shard of the earlier deploy) is ignored.
  await evolve("lateOldPut", [people[1]], 3);
  expect(await entries()).toBe(people.length);
}, 180_000);
