// How results and values are kept in the results store (RFD0005): JSON, with Uint8Arrays kept as
// bytes, under a key per unit hash.
import { Buffer } from "node:buffer";

/** The largest result or value kept here, encoded: bulk data belongs in the app's storage. */
export const MAX_STORED_BYTES = 1024 * 1024;

/** The store's key of a unit's result. */
export const resultKey = (hash: string): string => `r:${hash}`;
/** The store's key of a unit's exported value. */
export const valueKey = (hash: string): string => `v:${hash}`;

/** JSON, with Uint8Arrays kept as bytes. */
export function encodeStored(value: unknown, what: string): string {
  const text = JSON.stringify(value === undefined ? null : value, (_k, v: unknown) => (v instanceof Uint8Array ? { $bytes: Buffer.from(v.buffer, v.byteOffset, v.byteLength).toString("base64") } : v));
  if (text.length > MAX_STORED_BYTES) throw new Error(`${what} is ${text.length} bytes encoded, over ${MAX_STORED_BYTES}: keep bulk data in your own storage, and a pointer to it in the result`);
  return text;
}

export function decodeStored<T>(text: string): T {
  return JSON.parse(text, (_k, v: unknown) => {
    if (v && typeof v === "object" && "$bytes" in v && Object.keys(v).length === 1) return new Uint8Array(Buffer.from((v as { $bytes: string }).$bytes, "base64"));
    return v;
  }) as T;
}
