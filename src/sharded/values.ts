// Values on their way into storage: each comes back as it went in, or is refused here, by name.
// JSON.stringify changes values silently (NaN and Infinity become null, undefined and functions
// disappear, a Date becomes a string), and SQLite stores a string that is not well-formed UTF-16
// as U+FFFD, so a key read back from SQL is another key.

/** A lone surrogate: text that SQLite would store as U+FFFD. */
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/;

/** Whether `text` is well-formed UTF-16 (no lone surrogate): SQLite stores it as it is. */
export function isWellFormed(text: string): boolean {
  return !LONE_SURROGATE.test(text);
}

/** Throws unless `key` is text SQLite stores as it is. */
export function assertKey(key: unknown): asserts key is string {
  if (typeof key !== "string") throw new TypeError(`a key is text, not ${typeof key}`);
  if (!isWellFormed(key)) throw new TypeError(`key ${JSON.stringify(key)} is not well-formed UTF-16 (a lone surrogate): SQLite would store another key`);
}

/**
 * `value` as JSON text, refusing anything JSON would change on the way: non-finite numbers,
 * undefined, functions, symbols, bigints, text that is not well-formed, and objects that are not
 * plain (a Date, a Map, a class instance). `path` names the value in the error, as in `meta.a`.
 */
export function storedJson(value: unknown, path: string): string {
  checkJson(value, path, new Set());
  return JSON.stringify(value);
}

function checkJson(value: unknown, path: string, seen: Set<object>): void {
  switch (typeof value) {
    case "string":
      if (!isWellFormed(value)) throw new TypeError(`${path}: text that is not well-formed UTF-16 is not JSON`);
      return;
    case "boolean":
      return;
    case "number":
      if (!Number.isFinite(value)) throw new TypeError(`${path}: ${value} is not JSON`);
      return;
    case "object": {
      if (value === null) return;
      if (seen.has(value)) throw new TypeError(`${path}: a cycle is not JSON`);
      seen.add(value);
      if (Array.isArray(value)) {
        for (let i = 0; i < value.length; i++) {
          if (!(i in value)) throw new TypeError(`${path}[${i}]: a hole is not JSON`);
          checkJson(value[i], `${path}[${i}]`, seen);
        }
      } else {
        const proto = Object.getPrototypeOf(value);
        if (proto !== null && proto !== Object.prototype) throw new TypeError(`${path}: a ${(value as object).constructor?.name ?? "non-plain object"} is not JSON`);
        for (const [k, v] of Object.entries(value)) checkJson(v, `${path}.${k}`, seen);
      }
      seen.delete(value);
      return;
    }
    default:
      throw new TypeError(`${path}: ${typeof value === "undefined" ? "undefined" : `a ${typeof value}`} is not JSON`);
  }
}

/** Code point order: the order of UTF-8 bytes, which is how SQLite compares text. */
export function compareText(a: string, b: string): number {
  if (a === b) return 0;
  const ia = a[Symbol.iterator]();
  const ib = b[Symbol.iterator]();
  for (;;) {
    const ca = ia.next();
    const cb = ib.next();
    if (ca.done || cb.done) return ca.done ? (cb.done ? 0 : -1) : 1;
    const d = ca.value.codePointAt(0)! - cb.value.codePointAt(0)!;
    if (d !== 0) return d;
  }
}
