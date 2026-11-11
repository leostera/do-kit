import type { ChunkBody } from "./execute.js";
/** The key that stands for a Uint8Array in a body's JSON (and in a unit's hashed input). */
export declare const BIN = "\0bin";
/** The largest body a chunk may have, encoded: it travels in one RPC call to its runner. */
export declare const MAX_BODY_BYTES: number;
/** `value` as JSON, with each Uint8Array replaced by its index in `binaries`. */
export declare function jsonWithBinaries(value: unknown, binaries: Uint8Array[]): string;
export declare function encodeBody(body: ChunkBody): Uint8Array;
export declare function decodeBody(bytes: Uint8Array): ChunkBody;
//# sourceMappingURL=body.d.ts.map