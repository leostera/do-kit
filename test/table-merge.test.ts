// Merging shard pages (pure): positions are strings, and the empty string is one.
import { expect, test } from "bun:test";
import { mergePages } from "../src/table/schema.ts";

const row = (key: string) => ({ key, row: { name: key }, version: 1, position: key });

test("a page that stops at a row whose key is the empty string has more to come", () => {
  // One shard, limit 1, primary-key order: its first row has the key "", and it has more rows.
  const merged = mergePages([{ rows: [row("")], frontier: "" }], { limit: 1, direction: "asc" });
  expect(merged).toEqual({ rows: [row("")], next: "" });
  // With another shard: the smallest frontier is still "".
  expect(mergePages([{ rows: [row("")], frontier: "" }, { rows: [row("a")], frontier: "a" }], { limit: 2, direction: "asc" })).toEqual({ rows: [row("")], next: "" });
});
