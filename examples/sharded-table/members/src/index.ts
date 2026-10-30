// The members of a workspace: handles are unique per workspace (ignoring case), members go through a
// lifecycle (invited, active, removed), and the workspace lists them newest first with how many there
// are in each state.
//
// Every route takes the workspace in an `x-workspace` header, or `?workspace=` ("demo" when neither).
//
//   POST   /api/members {"handle": "Ada"}                 -> 201 {"handle": "ada", "id": "...", "status": "invited"}; 409 if taken
//   POST   /api/members/:handle/activate {"id": "..."}    -> the member, now "active"
//   DELETE /api/members/:handle {"id": "..."}             -> 204 (marked "removed")
//   GET    /api/members?cursor=...                        -> {"members": [...], "cursor": ..., "total": 3, "byStatus": {...}}
//   GET    /api/shards                                    -> the workspace's shards, for the page
import { defineTable, sharded, ShardedTable, table, type WriteResult } from "@leostera/do-kit";
import { Hono } from "hono";
import { describeShards, tableRows } from "../../../_shared/shards.ts";

interface Env {
  readonly MEMBERS: DurableObjectNamespace<Members>;
}

export type Status = "invited" | "active" | "removed";
export type Member = { handle: string; displayName: string; id: string; status: Status; createdAt: number; updatedAt: number };

const members = defineTable<Member>({
  // The lowercased handle: unique per workspace, and what routes a member to its shard.
  primaryKey: "handle",
  columns: { handle: "text", displayName: "text", id: "text", status: "text", createdAt: "integer", updatedAt: "integer" },
  indexes: { byCreated: ["createdAt"] },
  counts: { byStatus: "status" },
});

/** Which status changes the lifecycle allows. */
const NEXT: Record<Status, readonly Status[]> = { invited: ["active", "removed"], active: ["removed"], removed: [] };

export class Members extends ShardedTable<Env, Member> {
  protected table = members;

  protected nodes() {
    return this.env.MEMBERS;
  }

  /**
   * Moves members along their lifecycle, each only if it is still the member with that id (a handle
   * can be removed and taken again) and the change is allowed. One transaction per shard.
   */
  async setStatus(keys: readonly string[], changes: readonly { id: string; to: Status }[]): Promise<(WriteResult & { member: Member | null })[]> {
    return this.transact(keys, () =>
      keys.map((handle, i) => {
        const current = this.getRow(handle);
        const { id, to } = changes[i]!;
        if (!current || current.row.id !== id || !NEXT[current.row.status].includes(to)) return { ok: false, version: current?.version ?? 0, member: current?.row ?? null };
        const written = this.patchRow(handle, { status: to, updatedAt: Date.now() }, { ifVersion: current.version });
        return { ...written, member: this.getRow(handle)!.row };
      }),
    );
  }
}

const app = new Hono<{ Bindings: Env; Variables: { workspace: string } }>();
app.use("/api/*", async (c, next) => {
  c.set("workspace", c.req.header("x-workspace") ?? c.req.query("workspace") ?? "demo");
  await next();
});
// One sharded table per workspace; `sharded` reaches the same shards, for custom methods.
const tableName = (workspace: string) => `members:${workspace}`;

app.post("/api/members", async (c) => {
  const { handle: displayName } = await c.req.json<{ handle: string }>();
  const now = Date.now();
  const member: Member = { handle: displayName.toLowerCase(), displayName, id: crypto.randomUUID(), status: "invited", createdAt: now, updatedAt: now };
  // Only if no member has this handle: the shard of the handle decides, in one transaction.
  const created = await table(c.env.MEMBERS, tableName(c.get("workspace")), members).put(member, { ifVersion: 0 });
  return created.ok ? c.json(member, 201) : c.text("handle taken\n", 409);
});

/** Moves a member along its lifecycle, in one call to the shard of its handle. */
async function move(env: Env, workspace: string, handle: string, id: string, to: Status) {
  const [result] = await sharded(env.MEMBERS, tableName(workspace)).callOneShard([handle], "setStatus", [{ id, to }]);
  return result!;
}

app.post("/api/members/:handle/activate", async (c) => {
  const result = await move(c.env, c.get("workspace"), c.req.param("handle"), (await c.req.json<{ id: string }>()).id, "active");
  return result.ok ? c.json(result.member!) : c.text("no such member, or not allowed now\n", 409);
});

app.delete("/api/members/:handle", async (c) => {
  const result = await move(c.env, c.get("workspace"), c.req.param("handle"), (await c.req.json<{ id: string }>()).id, "removed");
  return result.ok ? c.body(null, 204) : c.text("no such member, or not allowed now\n", 409);
});

app.get("/api/members", async (c) => {
  // Newest first, hiding removed ones, with the counts per status from the same calls.
  const page = await table(c.env.MEMBERS, tableName(c.get("workspace")), members).list({ index: "byCreated", direction: "desc", where: [{ column: "status", op: "!=", value: "removed" }], limit: 20, cursor: c.req.query("cursor") ?? null, withCount: "byStatus" });
  const byStatus = Object.fromEntries((page.counts ?? []).map((n) => [String(n.value), n.count]));
  return c.json({ members: page.rows.map((r) => r.row), cursor: page.cursor, total: page.total, byStatus });
});

app.get("/api/shards", async (c) => c.json(await describeShards(c.env.MEMBERS, tableName(c.get("workspace")), tableRows)));

export default app;
