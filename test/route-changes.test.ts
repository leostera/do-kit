// routeOf decides which shard owns a row, so it is part of a tree's identity, like its name: a
// tree is readable only with the routeOf it was written with. A deploy that changes it, on the
// objects and their clients together, must be refused on an existing tree, not read through:
// rows stay where the old routes put them, and reads routed by the new ones answer null.
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const doc = (name: string, i: number) => ({ name, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null });
const KEYS = Array.from({ length: 24 }, (_, i) => `refs/heads/b${i}/x`);

test("a tree whose routeOf changed in a deploy refuses calls, and answers no row as missing", async () => {
  const name = `route-change-${unique()}`;
  for (const [i, key] of KEYS.entries()) await post("/table", { cls: "routedTable", name, op: "put", args: [doc(key, i)] });
  expect(await post<boolean>("/split", { cls: "routedTable", name, bits: "" })).toBe(true);
  // The deploy: every shard restarts on the new routeOf, and clients route by it.
  for (const bits of ["", "1"]) await post("/table", { cls: "routedTable", name, op: "deployRoute", args: [2, bits] });
  const answers = await Promise.all(KEYS.map((key) => post<{ ok?: { row: { name: string } } | null; threw?: string }>("/table", { cls: "routedTable", name, op: "try:get", args: [key], route: 2 })));
  // Every stored row is found, or the call is refused; none reads as missing.
  expect(answers.filter((a) => "ok" in a && a.ok === null).length).toBe(0);
  expect(answers.some((a) => /route/i.test(a.threw ?? ""))).toBe(true);
}, 60_000);

test("a tree whose routeOf changed in a deploy refuses writes, so no row lands under a second route", async () => {
  const name = `route-change-write-${unique()}`;
  for (const [i, key] of KEYS.entries()) await post("/table", { cls: "routedTable", name, op: "put", args: [doc(key, i)] });
  expect(await post<boolean>("/split", { cls: "routedTable", name, bits: "" })).toBe(true);
  for (const bits of ["", "1"]) await post("/table", { cls: "routedTable", name, op: "deployRoute", args: [2, bits] });
  const writes = await Promise.all(KEYS.map((key, i) => post<{ ok?: unknown; threw?: string }>("/table", { cls: "routedTable", name, op: "try:put", args: [doc(key, 100 + i)], route: 2 })));
  expect(writes.every((w) => /route/i.test(w.threw ?? ""))).toBe(true);
}, 60_000);
