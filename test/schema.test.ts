// A sharded table's schema helpers: column values checked against their type (never silently
// changed into something else), and what a listing cursor belongs to.
import { expect, test } from "bun:test";
import { defineTable, encodeValue, queryFingerprint } from "../src/table/schema.ts";

const schema = defineTable<{ id: string; n: number; x: number; ok: boolean; data: unknown }>({
  primaryKey: "id",
  columns: { id: "text", n: "integer", x: "real", ok: "boolean", data: "json" },
});

test("values are stored as their column's type says", () => {
  expect(encodeValue(schema, "id", "a")).toBe("a");
  expect(encodeValue(schema, "n", 3)).toBe(3);
  expect(encodeValue(schema, "x", 1.5)).toBe(1.5);
  expect(encodeValue(schema, "x", Infinity)).toBe(Infinity);
  expect(encodeValue(schema, "ok", true)).toBe(1);
  expect(encodeValue(schema, "data", { a: [1] })).toBe('{"a":[1]}');
  expect(encodeValue(schema, "n", null)).toBeNull();
});

test("a value SQLite would store as something else is refused", () => {
  expect(() => encodeValue(schema, "x", Number.NaN)).toThrow("NaN"); // SQLite stores NaN as null
  expect(() => encodeValue(schema, "n", 1.5)).toThrow("integer");
  expect(() => encodeValue(schema, "data", () => 1)).toThrow("JSON"); // JSON.stringify gives undefined
  expect(() => encodeValue(schema, "id", null)).toThrow("primary key");
  expect(() => encodeValue(schema, "nope", 1)).toThrow("no column");
});

test("a cursor's fingerprint changes when its index is redefined under the same name", () => {
  const request = { index: "byCreated", direction: "asc" as const, where: [] };
  expect(queryFingerprint(request, ["createdAt"])).toBe(queryFingerprint(request, ["createdAt"]));
  expect(queryFingerprint(request, ["createdAt"])).not.toBe(queryFingerprint(request, ["createdAt", "id"]));
});
