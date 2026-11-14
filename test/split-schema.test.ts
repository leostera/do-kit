// A split copies a shard's whole schema, as the shard holds it: a subclass's triggers and views
// as well as its tables. A snapshot does; a row copy must do the same, or the moved half stops
// keeping what its triggers keep, with no error.
import { expect, test } from "bun:test";
import { hashBit, keyHash } from "../src/sharded/hash.ts";
import { call, post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const keyOn = (half: "0" | "1", from = 0) => Array.from({ length: 200 }, (_, i) => `n${from + i}`).find((k) => hashBit(keyHash(k), 0) === half)!;

test("a subclass's trigger fires on both halves after a split", async () => {
  const name = `split-trigger-${unique()}`;
  await call("m", name, "keepCounted", ["a"]);
  await call("m", name, "addCounted", [keyOn("0"), keyOn("1")]);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  const [left] = await call<number[]>("m", name, "addCounted", [keyOn("0", 50)]);
  const [right] = await call<number[]>("m", name, "addCounted", [keyOn("1", 50)]);
  // Both halves had the count at 2; each note since bumped its own half's.
  expect(left).toBe(3);
  expect(right).toBe(3);
}, 30_000);

test("a subclass's view is on both halves after a split", async () => {
  const name = `split-view-${unique()}`;
  await call("m", name, "keepCounted", ["a"]);
  expect(await post<boolean>("/split", { cls: "m", name, bits: "" })).toBe(true);
  expect(await call<boolean[]>("m", name, "viewed", [keyOn("0"), keyOn("1")])).toEqual([true, true]);
}, 30_000);
