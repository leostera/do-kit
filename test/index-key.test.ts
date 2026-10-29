// The index key encoding: for any two tuples, comparing their keys as strings orders them as
// SQLite orders the values (null, then numbers, then text by code point), column by column.
import { expect, test } from "bun:test";
import fc from "fast-check";
import { compareText, encodeIndexKey, type SqlValue } from "../src/table/schema.ts";

function sqlite(a: SqlValue, b: SqlValue): number {
  const rank = (v: SqlValue) => (v === null ? 0 : typeof v === "number" ? 1 : 2);
  if (rank(a) !== rank(b)) return Math.sign(rank(a) - rank(b));
  if (a === null) return 0;
  if (typeof a === "number") return a < (b as number) ? -1 : a > (b as number) ? 1 : 0;
  return Math.sign(compareText(a, b as string));
}
const tuple = (a: SqlValue[], b: SqlValue[]) => {
  for (let i = 0; i < a.length; i++) {
    const d = sqlite(a[i]!, b[i]!);
    if (d !== 0) return d;
  }
  return 0;
};

const value = fc.oneof(
  fc.constant(null),
  fc.double({ noNaN: true }),
  fc.integer(),
  fc.constantFrom(0, -0, 1, -1, Number.MAX_VALUE, -Number.MAX_VALUE, Number.MIN_VALUE, Infinity, -Infinity),
  fc.string({ unit: "binary" }),
  fc.constantFrom("", "\u0000", "a\u0000", "a", "ab", "\uFFFF", "😀", "é"),
);

test("index keys order tuples as SQLite orders their values", () => {
  fc.assert(
    fc.property(fc.integer({ min: 1, max: 3 }).chain((n) => fc.tuple(fc.array(value, { minLength: n, maxLength: n }), fc.array(value, { minLength: n, maxLength: n }))), ([a, b]) => {
      expect(Math.sign(compareText(encodeIndexKey(a), encodeIndexKey(b)))).toBe(tuple(a, b));
    }),
    { numRuns: 20_000 },
  );
});
