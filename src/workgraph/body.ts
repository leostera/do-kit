// A chunk's body as bytes (RFD0005): its units and exports as JSON, with every Uint8Array in them
// (units' inputs) kept as raw bytes after the JSON, not as text. The coordinator stores it in parts
// that fit a SQLite row, and hands it to runners as bytes.
//
// Layout: [u32 JSON length][JSON, UTF-8][u32 count] then, per Uint8Array, [u32 length][bytes]. In
// the JSON, the i-th Uint8Array is { "\u0000bin": i }.
import type { ChunkBody } from "./execute.js";

/** The key that stands for a Uint8Array in a body's JSON (and in a unit's hashed input). */
export const BIN = "\u0000bin";

/** The largest body a chunk may have, encoded: it travels in one RPC call to its runner. */
export const MAX_BODY_BYTES = 16 * 1024 * 1024;

/** `value` as JSON, with each Uint8Array replaced by its index in `binaries`. */
export function jsonWithBinaries(value: unknown, binaries: Uint8Array[]): string {
  return JSON.stringify(value, (_k, v: unknown) => {
    if (v instanceof Uint8Array) {
      binaries.push(v);
      return { [BIN]: binaries.length - 1 };
    }
    return v;
  });
}

export function encodeBody(body: ChunkBody): Uint8Array {
  const binaries: Uint8Array[] = [];
  const json = new TextEncoder().encode(jsonWithBinaries({ units: body.units, exports: body.exports }, binaries));
  const size = 8 + json.length + binaries.reduce((sum, b) => sum + 4 + b.length, 0);
  const out = new Uint8Array(size);
  const view = new DataView(out.buffer);
  view.setUint32(0, json.length);
  out.set(json, 4);
  let at = 4 + json.length;
  view.setUint32(at, binaries.length);
  at += 4;
  for (const b of binaries) {
    view.setUint32(at, b.length);
    out.set(b, at + 4);
    at += 4 + b.length;
  }
  return out;
}

export function decodeBody(bytes: Uint8Array): ChunkBody {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const jsonLength = view.getUint32(0);
  const json = new TextDecoder().decode(bytes.subarray(4, 4 + jsonLength));
  let at = 4 + jsonLength;
  const count = view.getUint32(at);
  at += 4;
  const binaries: Uint8Array[] = [];
  for (let i = 0; i < count; i++) {
    const length = view.getUint32(at);
    // A copy: the unit owns its input, apart from the body's buffer.
    binaries.push(bytes.slice(at + 4, at + 4 + length));
    at += 4 + length;
  }
  return JSON.parse(json, (_k, v: unknown) => {
    if (v && typeof v === "object" && BIN in v && Object.keys(v).length === 1) return binaries[(v as Record<string, number>)[BIN]!];
    return v;
  }) as ChunkBody;
}
