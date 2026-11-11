/** The first 64 bits of the key's SHA-256, as 16 hex digits: what ownership is decided on. */
export declare function keyHash(key: string): string;
/** Bit `i` (0-based, most significant first) of a 16-hex-digit hash. */
export declare function hashBit(hash: string, i: number): "0" | "1";
//# sourceMappingURL=hash.d.ts.map