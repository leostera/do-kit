// Atomic holds and splits: a split copies the whole shard, holds included. The child owns only
// the moving half; every hold it was copied is a hold on the half it doesn't own.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
type Tables = { holds: { tx: string; keys: string }[]; outcomes: { tx: string; outcome: string }[] };
const half = (key: string) => hashBit(keyHash(key), 0);

test("a split's child drops the atomic holds of the half it does not own", async () => {
  const name = `split-child-holds-${unique()}`;
  const left = Array.from({ length: 50 }, (_, i) => `k${i}`).find((k) => half(k) === "0")!;
  const right = Array.from({ length: 50 }, (_, i) => `k${i}`).find((k) => half(k) === "1")!;
  await call("counters", name, "add", [left, right], [10, 10]);
  // The split waits before it copies; a prepare on a left-half key comes in meanwhile and holds it.
  await post("/slowHandOut", { cls: "counters", name, bits: "", ms: 1_500 });
  const split = post<boolean>("/split", { cls: "counters", name, bits: "" });
  await Bun.sleep(300);
  const tx = `tx-${unique()}`;
  expect((await post<{ ok?: unknown }>("/atomicStep", { cls: "counters", name, bits: "", step: { step: "prepare", tx, method: "transfer", primaryKey: left, deadline: Date.now() + 60_000 }, keys: [left], payloads: [-1] })).ok).toEqual({ prepared: true });
  expect(await split).toBe(true);
  // The original keeps its hold; the child, which owns only the right half, keeps none.
  expect((await post<Tables>("/atomicTables", { cls: "counters", name, bits: "0" })).holds.map((h) => h.tx)).toEqual([tx]);
  expect((await post<Tables>("/atomicTables", { cls: "counters", name, bits: "1" })).holds).toEqual([]);
}, 60_000);
