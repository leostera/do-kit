// Accounts keyed by id, found by email and listed by team: two things a sharded table cannot do
// without asking every shard, because neither is the primary key. An IndexedTable keeps a global
// index for each, on shards of their own:
//   byEmail: unique, so no two accounts share an email, and a lookup is one call to one shard;
//   byTeam:  partitioned by team and sorted by when the account was made, so a team's page is one
//            call to one shard.
// A write returns once both indexes have it.
//
//   POST   /api/accounts {"email", "name", "team"}  -> 201 the account; 409 if the email is taken
//   GET    /api/accounts/by-email/:email            -> the account; 404
//   PATCH  /api/accounts/:id {"email"?, "team"?}    -> the account; 409 if the email is taken; 404
//   DELETE /api/accounts/:id                        -> 204
//   GET    /api/teams/:team?cursor=...              -> {"accounts": [...], "cursor": ...}, newest first
//   GET    /api/shards, /api/index-shards           -> the row shards and the index shards, for the page
import { defineTable, GlobalIndex, IndexedTable, indexedTable } from "@leostera/do-kit";
import { Hono } from "hono";
import { describeShards, tableRows } from "../../../_shared/shards.ts";

interface Env {
  readonly ACCOUNTS: DurableObjectNamespace<Accounts>;
  readonly ACCOUNT_INDEXES: DurableObjectNamespace<AccountIndexes>;
}

export type Account = { id: string; email: string; name: string; team: string; createdAt: number };

const accounts = defineTable<Account>({
  primaryKey: "id",
  columns: { id: "text", email: "text", name: "text", team: "text", createdAt: "integer" },
  globalIndexes: {
    byEmail: { columns: ["email"], unique: true },
    // The first column picks the index shard: a team's accounts are together, sorted by createdAt.
    byTeam: { columns: ["team", "createdAt"], partition: 1 },
  },
});

/** The rows, sharded by id. */
export class Accounts extends IndexedTable<Env, Account> {
  protected table = accounts;
  protected nodes() {
    return this.env.ACCOUNTS;
  }
  protected indexNodes() {
    return this.env.ACCOUNT_INDEXES;
  }
}

/** The entries of both indexes, sharded by index and partition. */
export class AccountIndexes extends GlobalIndex<Env> {
  protected nodes() {
    return this.env.ACCOUNT_INDEXES;
  }
  protected rowNodes() {
    return this.env.ACCOUNTS;
  }
}

const NAME = "accounts";
const app = new Hono<{ Bindings: Env }>();
const table = (env: Env) => indexedTable(env.ACCOUNTS, env.ACCOUNT_INDEXES, NAME, accounts);

app.post("/api/accounts", async (c) => {
  const { email, name, team } = await c.req.json<{ email: string; name: string; team: string }>();
  const account: Account = { id: crypto.randomUUID(), email: email.toLowerCase(), name, team, createdAt: Date.now() };
  // Only if new, and only if no other account has the email: the index decides, across every shard.
  const created = await table(c.env).put(account, { ifVersion: 0 });
  return created.ok ? c.json(account, 201) : c.text("email taken\n", 409);
});

app.get("/api/accounts/by-email/:email", async (c) => {
  const found = await table(c.env).lookup("byEmail", [c.req.param("email").toLowerCase()]);
  return found ? c.json(found.row) : c.text("no such account\n", 404);
});

app.patch("/api/accounts/:id", async (c) => {
  const changes = await c.req.json<{ email?: string; team?: string }>();
  const t = table(c.env);
  const written = await t.patch(c.req.param("id"), { ...(changes.email ? { email: changes.email.toLowerCase() } : {}), ...(changes.team ? { team: changes.team } : {}) });
  if ("conflict" in written && written.conflict) return c.text("email taken\n", 409);
  if (!written.ok) return c.text("no such account\n", 404);
  return c.json((await t.get(c.req.param("id")))!.row);
});

app.delete("/api/accounts/:id", async (c) => {
  await table(c.env).delete(c.req.param("id"));
  return c.body(null, 204);
});

app.get("/api/teams/:team", async (c) => {
  const page = await table(c.env).list({ index: "byTeam", partition: [c.req.param("team")], direction: "desc", limit: 20, cursor: c.req.query("cursor") ?? null });
  return c.json({ accounts: page.rows.map((r) => r.row), cursor: page.cursor });
});

app.get("/api/shards", async (c) => c.json(await describeShards(c.env.ACCOUNTS, NAME, tableRows)));
app.get("/api/index-shards", async (c) => c.json(await describeShards(c.env.ACCOUNT_INDEXES, NAME, async (shard) => (await shard.indexStats()).entries)));

export default app;
