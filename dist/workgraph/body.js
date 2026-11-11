/** The key that stands for a Uint8Array in a body's JSON (and in a unit's hashed input). */
export const BIN = "\u0000bin";
/** The largest body a chunk may have, encoded: it travels in one RPC call to its runner. */
export const MAX_BODY_BYTES = 16 * 1024 * 1024;
/** `value` as JSON, with each Uint8Array replaced by its index in `binaries`. */
export function jsonWithBinaries(value, binaries) {
    return JSON.stringify(value, (_k, v) => {
        if (v instanceof Uint8Array) {
            binaries.push(v);
            return { [BIN]: binaries.length - 1 };
        }
        return v;
    });
}
export function encodeBody(body) {
    const binaries = [];
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
export function decodeBody(bytes) {
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const jsonLength = view.getUint32(0);
    const json = new TextDecoder().decode(bytes.subarray(4, 4 + jsonLength));
    let at = 4 + jsonLength;
    const count = view.getUint32(at);
    at += 4;
    const binaries = [];
    for (let i = 0; i < count; i++) {
        const length = view.getUint32(at);
        // A copy: the unit owns its input, apart from the body's buffer.
        binaries.push(bytes.slice(at + 4, at + 4 + length));
        at += 4 + length;
    }
    return JSON.parse(json, (_k, v) => {
        if (v && typeof v === "object" && BIN in v && Object.keys(v).length === 1)
            return binaries[v[BIN]];
        return v;
    });
}
//# sourceMappingURL=body.js.map