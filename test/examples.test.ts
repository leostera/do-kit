// Every example under examples/ boots with `bun dev` (vite), serves its page, and its API does what
// its README says.
import { afterAll, expect, test } from "bun:test";
import { join } from "node:path";

const running: (() => Promise<void>)[] = [];
afterAll(async () => {
  await Promise.all(running.map((stop) => stop()));
});

/** Starts an example's Worker under cf dev and returns its base URL. */
async function start(example: string): Promise<string> {
  const probe = Bun.serve({ port: 0, fetch: () => new Response() });
  const port = probe.port!;
  probe.stop(true);
  const dir = join(import.meta.dir, "../examples", example);
  const child = Bun.spawn([join(import.meta.dir, "../node_modules/.bin/vite"), "--port", String(port), "--strictPort"], { cwd: dir, stdout: "ignore", stderr: "ignore", env: { ...process.env, NO_COLOR: "1" } });
  running.push(async () => {
    child.kill();
    await child.exited;
  });
  const base = `http://localhost:${port}`;
  for (let i = 0; ; i++) {
    try {
      const page = await fetch(base);
      if (page.ok && (await page.text()).includes('id="root"')) return base;
    } catch {
      // not up yet
    }
    if (i > 300) throw new Error(`${example} did not start`);
    await Bun.sleep(100);
  }
}

/** Local state outlives a run: names unique to this run keep runs apart. */
const run = Date.now().toString(36);
const json = (body: unknown, headers: Record<string, string> = {}) => ({ body: JSON.stringify(body), headers: { "content-type": "application/json", ...headers } });

test("counters: hits add up per page", async () => {
  const base = await start("sharded-object/counters");
  const [home, pricing, docs] = [`/${run}`, `/${run}/pricing`, `/${run}/docs`];
  expect(await (await fetch(`${base}/api/hits`, { method: "POST", ...json({ pages: [home, pricing, home] }) })).json()).toEqual({ [home]: 2, [pricing]: 1 });
  expect(await (await fetch(`${base}/api/counts?page=${home}&page=${pricing}&page=${docs}`)).json()).toEqual({ [home]: 2, [pricing]: 1, [docs]: 0 });
}, 60_000);

test("feature-flags: changes apply only to the version they were made from", async () => {
  const base = await start("sharded-object/feature-flags");
  const [flag, other] = [`checkout-${run}`, `dark-mode-${run}`];
  const put = (body: unknown) => fetch(`${base}/api/flags/${flag}`, { method: "PUT", ...json(body) });
  const created = await put({ enabled: true, rollout: 100, ifVersion: 0 });
  expect(created.status).toBe(200);
  expect(await created.json()).toEqual({ ok: true, version: 1 });
  expect((await put({ enabled: false, rollout: 0, ifVersion: 0 })).status).toBe(409); // someone else's change came first
  expect(await (await fetch(`${base}/api/flags/${flag}`)).json()).toEqual({ enabled: true, rollout: 100, version: 1 });
  expect(await (await fetch(`${base}/api/flags?name=${flag}&name=${other}&user=alice`)).json()).toEqual({ [flag]: true, [other]: false });
}, 60_000);

test("sessions: a session is found until it ends or expires", async () => {
  const base = await start("sharded-object/sessions");
  const start1 = (ttlSeconds: number) => fetch(`${base}/api/sessions`, { method: "POST", ...json({ user: "alice", ttlSeconds }) }).then((r) => r.json() as Promise<{ id: string }>);
  const { id } = await start1(3600);
  expect(((await (await fetch(`${base}/api/sessions/${id}`)).json()) as { user: string }).user).toBe("alice");
  expect((await fetch(`${base}/api/sessions/${id}`, { method: "DELETE" })).status).toBe(204);
  expect((await fetch(`${base}/api/sessions/${id}`)).status).toBe(404);
  const short = await start1(1);
  expect((await fetch(`${base}/api/sessions/${short.id}`)).status).toBe(200);
  await Bun.sleep(1_500);
  expect((await fetch(`${base}/api/sessions/${short.id}`)).status).toBe(404);
}, 60_000);

test("kv: ETags make writes conditional, and listings page by prefix", async () => {
  const base = await start("sharded-table/kv");
  const put = (key: string, value: unknown, headers: Record<string, string>) => fetch(`${base}/api/kv/${key}`, { method: "PUT", ...json(value, headers) });
  const created = await put(`${run}-user:alice`, { plan: "free" }, { "if-none-match": "*" });
  expect(created.status).toBe(201);
  expect(created.headers.get("etag")).toBe('"1"');
  expect((await put(`${run}-user:alice`, { plan: "pro" }, { "if-none-match": "*" })).status).toBe(412);
  const updated = await put(`${run}-user:alice`, { plan: "pro" }, { "if-match": '"1"' });
  expect(updated.status).toBe(200);
  expect(updated.headers.get("etag")).toBe('"2"');
  const read = await fetch(`${base}/api/kv/${run}-user:alice`);
  expect(await read.json()).toEqual({ plan: "pro" });
  expect(read.headers.get("etag")).toBe('"2"');
  await put(`${run}-user:bob`, { plan: "free" }, {});
  await put(`${run}-team:core`, { size: 3 }, {});
  expect(await (await fetch(`${base}/api/kv?prefix=${run}-user:`)).json()).toEqual({ keys: [`${run}-user:alice`, `${run}-user:bob`], cursor: null });
  expect((await fetch(`${base}/api/kv/${run}-user:alice`, { method: "DELETE", headers: { "if-match": '"1"' } })).status).toBe(412);
  expect((await fetch(`${base}/api/kv/${run}-user:alice`, { method: "DELETE", headers: { "if-match": '"2"' } })).status).toBe(204);
  expect((await fetch(`${base}/api/kv/${run}-user:alice`)).status).toBe(404);
}, 60_000);

test("members: unique handles, a lifecycle, and listings with counts", async () => {
  const base = await start("sharded-table/members");
  const workspace = { "x-workspace": run };
  const invite = (handle: string) => fetch(`${base}/api/members`, { method: "POST", ...json({ handle }, workspace) });
  const ada = await invite("Ada");
  expect(ada.status).toBe(201);
  const member = (await ada.json()) as { handle: string; id: string; status: string };
  expect(member).toMatchObject({ handle: "ada", status: "invited" });
  expect((await invite("ADA")).status).toBe(409); // handles are unique ignoring case
  await invite("Grace");
  const active = await fetch(`${base}/api/members/ada/activate`, { method: "POST", ...json({ id: member.id }, workspace) });
  expect(((await active.json()) as { status: string }).status).toBe("active");
  expect((await fetch(`${base}/api/members/ada/activate`, { method: "POST", ...json({ id: "someone-else" }, workspace) })).status).toBe(409);
  type Listing = { members: { handle: string }[]; total: number; byStatus: Record<string, number> };
  const before = (await (await fetch(`${base}/api/members`, { headers: workspace })).json()) as Listing;
  expect(before.members.map((m) => m.handle)).toEqual(["grace", "ada"]); // newest first
  expect(before).toMatchObject({ total: 2, byStatus: { active: 1, invited: 1 } });
  expect((await fetch(`${base}/api/members/ada`, { method: "DELETE", ...json({ id: member.id }, workspace) })).status).toBe(204);
  const after = (await (await fetch(`${base}/api/members`, { headers: workspace })).json()) as Listing;
  expect(after.members.map((m) => m.handle)).toEqual(["grace"]);
  expect(after.byStatus).toEqual({ removed: 1, invited: 1 });
}, 60_000);
