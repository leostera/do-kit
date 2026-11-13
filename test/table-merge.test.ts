// Merging shard pages (pure): positions are strings, and the empty string is one.
import { expect, test } from "bun:test";
import { mergePages, nextShard } from "../src/table/schema.ts";

const row = (key: string) => ({ key, row: { name: key }, version: 1, position: key });

test("a page that stops at a row whose key is the empty string has more to come", () => {
  // One shard, limit 1, primary-key order: its first row has the key "", and it has more rows.
  const merged = mergePages([{ rows: [row("")], frontier: "" }], { limit: 1, direction: "asc" });
  expect(merged).toEqual({ rows: [row("")], next: "" });
  // With another shard: the smallest frontier is still "".
  expect(mergePages([{ rows: [row("")], frontier: "" }, { rows: [row("a")], frontier: "a" }], { limit: 2, direction: "asc" })).toEqual({ rows: [row("")], next: "" });
});

test("a walk in shard order goes from a shard to the node right after it, by its bits alone, and wraps around to its start", () => {
  type At = Parameters<typeof nextShard>[0];
  const at = (leaf: string, rest: Partial<At> = {}): At => ({ leaf, after: "p", floor: null, start: "", wrapped: false, ...rest });
  // The sibling of the nearest left half: whatever the client's map knows of the tree.
  expect(nextShard(at("0101", { start: "0100" }))).toEqual(at("011", { after: null, start: "0100" }));
  expect(nextShard(at("0111", { start: "0100" }))).toEqual(at("1", { after: null, start: "0100" }));
  // After the last shard, the root (its first shard, once asked), and done back under the start.
  expect(nextShard(at("111", { start: "01" }))).toEqual(at("", { after: null, start: "01", wrapped: true }));
  expect(nextShard(at("00", { start: "01", wrapped: true }))).toBeNull();
  expect(nextShard(at("001", { start: "0110", wrapped: true }))).toEqual(at("01", { after: null, start: "0110", wrapped: true }));
  // A walk that started at the first shard ends at the last; the only shard is the whole walk.
  expect(nextShard(at("1", { start: "0" }))).toEqual(at("", { after: null, start: "0", wrapped: true }));
  expect(nextShard(at("", { start: "" }))).toBeNull();
  // A shard that split partway: its other shards go on from where it was; past it, from the start.
  const floor = { bits: "01", after: "p" };
  expect(nextShard(at("010", { after: "q", floor, start: "01" }))).toEqual(at("011", { after: "p", floor, start: "01" }));
  expect(nextShard(at("011", { after: "q", floor, start: "01" }))).toEqual(at("1", { after: null, start: "01" }));
});
