// A sharded table's schema helpers: column values checked against their type (never silently
// changed into something else), and what a listing cursor belongs to.
import { expect, test } from "bun:test";
import { assertRowFits, defineTable, encodeValue, queryFingerprint, readToEnd, rowBytes } from "../src/table/schema.ts";

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

// ---- Row size ----

const text = defineTable<{ id: string; body: string }>({ primaryKey: "id", columns: { id: "text", body: "text" }, maxRowBytes: 1_000 });
const fits = (row: { id: string; body: string }) => {
  try {
    assertRowFits(text, row);
    return true;
  } catch (error) {
    if ((error as Error).name === "RowTooLarge") return false;
    throw error;
  }
};

test("a row's size counts text as UTF-8, and maxRowBytes is the most it may be", () => {
  // 1 + 999 bytes of 1, 2, 3 and 4 byte characters: in UTF-16 units, 1 + 999, 500, 333 and 500.
  for (const [char, bytes] of [["a", 1], ["é", 2], ["€", 3], ["😀", 4]] as const) {
    const body = char.repeat(Math.floor(999 / bytes)) + "a".repeat(999 % bytes);
    expect(rowBytes(text, { id: "k", body })).toBe(1_000);
    expect(fits({ id: "k", body }), `${char} at the limit`).toBe(true);
    expect(fits({ id: "k", body: body + "a" }), `${char} one byte past it`).toBe(false);
  }
});

test("a row well under maxRowBytes is checked without encoding its text", () => {
  // Every put checks its row in the caller: copying each value to UTF-8 to count it costs as much
  // CPU as the rest of the client's work for a write. Text takes at most 3 bytes per UTF-16 unit.
  const encode = TextEncoder.prototype.encode;
  let encoded = 0;
  TextEncoder.prototype.encode = function (this: TextEncoder, input?: string) {
    encoded++;
    return encode.call(this, input);
  };
  try {
    expect(fits({ id: "k", body: "€".repeat(300) })).toBe(true);
    expect(encoded).toBe(0);
    // Past the bound, it counts exactly: 900 bytes of a 450-unit string can't be told from 1,350 without.
    expect(fits({ id: "k", body: "é".repeat(450) })).toBe(true);
    expect(encoded).toBeGreaterThan(0);
  } finally {
    TextEncoder.prototype.encode = encode;
  }
});

// ---- Reading to the end ----

test("a read starts with a statement of the rows asked for, then doubles them, and reads each to its end", () => {
  const sizes: number[] = [];
  const rows = (n: number) => {
    sizes.push(n);
    return { toArray: () => Array.from({ length: n }, (_, i) => ({ i })) } as unknown as SqlStorageCursor<{ i: number }>;
  };
  expect([...readToEnd(1_000, rows, 4)].length).toBe(1_000);
  expect(sizes.slice(0, 6)).toEqual([4, 8, 16, 32, 64, 64]);
  sizes.length = 0;
  // No first size: statements of the most rows one reads.
  [...readToEnd(100, rows)];
  expect(sizes).toEqual([64, 36]);
});
