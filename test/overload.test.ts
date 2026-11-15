// Overloaded across real RPC (RFD0013): a shard method throws an app's kind, and the caller gets
// that class, with its fields, from a plain answer; nothing the refused call did is kept.
import { expect, test } from "bun:test";
import { call, describe, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
type Got = {
  ok?: number[];
  name: string | null;
  message: string;
  overloaded: boolean;
  shard: boolean;
  tenant: string | null;
  kind: string | null;
  source: string | null;
  retryAfterMs: number | null;
  scope: unknown;
  detail: unknown;
  refused: { keys: string[] }[] | null;
  settled: { keys: string[] }[] | null;
  unknown: unknown[] | null;
};
const overload = (cls: string, name: string, keys: string[], register = true) => post<Got>("/overload", { cls, name, keys, payloads: keys.map(() => 1), register });

test("an app's kind thrown in a shard reaches the caller as its class, with its retry-after, scope and detail, and writes nothing", async () => {
  const name = `quota-${unique()}`;
  const got = await overload("counters", name, ["over-acme"]);
  expect(got).toMatchObject({ name: "TenantOverQuota", overloaded: true, shard: false, tenant: "over-acme", kind: "tenantQuota", source: "object", retryAfterMs: 60_000, scope: { key: "over-acme" }, detail: { tenant: "over-acme", spent: 12 } });
  expect(await call<(number | null)[]>("counters", name, "get", ["over-acme"])).toEqual([null]);
  expect((await describe("counters", name, "")).counters.overloaded).toBe(1);
}, 30_000);

test("a kind the client does not register is a plain Overloaded that keeps its kind", async () => {
  const name = `unregistered-${unique()}`;
  expect(await overload("counters", name, ["over-acme"], false)).toMatchObject({ name: "Overloaded", overloaded: true, tenant: null, kind: "tenantQuota", retryAfterMs: 60_000 });
  expect(await overload("counters", name, ["mystery-1"])).toMatchObject({ name: "Overloaded", overloaded: true, kind: "mystery", retryAfterMs: 5, scope: null, detail: {} });
}, 30_000);

test("an overload thrown inside the update, after it wrote, is rolled back with it", async () => {
  const name = `late-${unique()}`;
  expect(await overload("counters", name, ["late-acme"])).toMatchObject({ name: "TenantOverQuota", tenant: "late-acme" });
  const [rows] = await post<unknown[][]>("/object", { cls: "counters", name, bits: "", method: "sqlRun", args: [[{ query: "SELECT key FROM history WHERE key = 'late-acme'" }]] });
  expect(rows).toEqual([]);
  expect(await call<(number | null)[]>("counters", name, "get", ["late-acme"])).toEqual([null]);
}, 30_000);

test("a method's own error that reads like the runtime's overload is not taken for a refusal: it may have run", async () => {
  const got = await overload("counters", `says-${unique()}`, ["says-1"]);
  expect(got.overloaded).toBe(false);
  expect(got.message).toMatch(/overloaded/);
}, 30_000);

test("a write over several shards, one refusing, applies the others and lists the refused keys as refused, not unknown", async () => {
  const name = `spread-${unique()}`;
  const keys = ["over-acme", ...Array.from({ length: 12 }, (_, i) => `fine-${i}`)];
  const got = await overload("presplit", name, keys);
  expect(got).toMatchObject({ name: "TenantOverQuota", overloaded: true });
  const refused = got.refused!.flatMap((r) => r.keys);
  const settled = got.settled!.flatMap((s) => s.keys);
  expect(refused).toContain("over-acme");
  expect(got.unknown).toEqual([]);
  expect([...refused, ...settled].sort()).toEqual([...keys].sort());
  // What was refused was not written; what settled was.
  const values = await call<(number | null)[]>("presplit", name, "get", keys);
  keys.forEach((k, i) => expect({ k, v: values[i] }).toEqual({ k, v: refused.includes(k) ? null : 1 }));
  expect(settled.length).toBeGreaterThan(0);
}, 30_000);

test("a scan method's overload reaches the caller as its class too", async () => {
  expect(await post("/overloadScan", { cls: "counters", name: `scan-${unique()}` })).toEqual({ name: "TenantOverQuota", tenant: "all", retryAfterMs: 7 });
}, 30_000);
