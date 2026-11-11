// How keys map to shards: a shard owns the keys whose hash starts with its bits. Pure, with no
// runtime imports, so tests and callers outside a Worker compute the same bits.
import { createHash } from "node:crypto";
/** The first 64 bits of the key's SHA-256, as 16 hex digits: what ownership is decided on. */
export function keyHash(key) {
    return createHash("sha256").update(key).digest("hex").slice(0, 16);
}
/** Bit `i` (0-based, most significant first) of a 16-hex-digit hash. */
export function hashBit(hash, i) {
    const nibble = Number.parseInt(hash[i >> 2], 16);
    return (nibble >> (3 - (i & 3))) & 1 ? "1" : "0";
}
//# sourceMappingURL=hash.js.map