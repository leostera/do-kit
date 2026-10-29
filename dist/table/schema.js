// The schema of a sharded table, the queries a listing takes, and the sort order shared by the
// shards (SQLite) and the client that merges their pages (here): they must agree exactly.
const IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
/** Checks a schema and returns it: names are plain identifiers, the key is text, indexes name columns. */
export function defineTable(schema) {
    const columns = Object.entries(schema.columns);
    for (const [name, type] of columns) {
        if (!IDENTIFIER.test(name) || name.startsWith("_"))
            throw new Error(`column ${JSON.stringify(name)}: a plain identifier not starting with _`);
        if (!["text", "integer", "real", "boolean", "json"].includes(type))
            throw new Error(`column ${name}: unknown type ${type}`);
    }
    if (schema.columns[schema.primaryKey] !== "text")
        throw new Error(`primary key ${schema.primaryKey} must be a text column`);
    for (const [name, cols] of Object.entries(schema.indexes ?? {})) {
        if (!IDENTIFIER.test(name))
            throw new Error(`index ${JSON.stringify(name)}: a plain identifier`);
        if (cols.length === 0)
            throw new Error(`index ${name}: no columns`);
        for (const col of cols) {
            const type = schema.columns[col];
            if (!type)
                throw new Error(`index ${name}: no column ${col}`);
            if (type === "json")
                throw new Error(`index ${name}: json column ${col} cannot be indexed`);
        }
    }
    return schema;
}
export const MAX_LIMIT = 1_000;
export const MAX_BUDGET = 10_000;
/** The request for a query, with defaults and bounds applied. */
export function pageRequest(query, after) {
    const limit = query.limit ?? 50;
    if (!Number.isInteger(limit) || limit < 1 || limit > MAX_LIMIT)
        throw new Error(`limit must be an integer from 1 to ${MAX_LIMIT}`);
    const budget = query.budget ?? Math.max(1_000, limit * 10);
    if (!Number.isInteger(budget) || budget < limit || budget > MAX_BUDGET)
        throw new Error(`budget must be an integer from limit to ${MAX_BUDGET}`);
    const direction = query.direction ?? "asc";
    if (direction !== "asc" && direction !== "desc")
        throw new Error("direction must be asc or desc");
    return { index: query.index ?? null, direction, where: query.where ?? [], limit, budget, after };
}
/** SQLite's order: null, then numbers, then text by code point (the order of its UTF-8 bytes). */
export function compareValues(a, b) {
    const rank = (v) => (v === null ? 0 : typeof v === "number" ? 1 : 2);
    const ra = rank(a);
    const rb = rank(b);
    if (ra !== rb)
        return ra - rb;
    if (a === null)
        return 0;
    if (typeof a === "number")
        return a - b;
    return compareText(a, b);
}
function compareText(a, b) {
    if (a === b)
        return 0;
    const ia = a[Symbol.iterator]();
    const ib = b[Symbol.iterator]();
    for (;;) {
        const ca = ia.next();
        const cb = ib.next();
        if (ca.done || cb.done)
            return ca.done ? (cb.done ? 0 : -1) : 1;
        const d = ca.value.codePointAt(0) - cb.value.codePointAt(0);
        if (d !== 0)
            return d;
    }
}
/** Compares two positions in listing order: `desc` reverses every column, the key included. */
export function comparePositions(a, b, direction) {
    for (let i = 0; i < a.length; i++) {
        const d = compareValues(a[i], b[i]);
        if (d !== 0)
            return direction === "asc" ? d : -d;
    }
    return 0;
}
/** A short fingerprint of what a cursor belongs to: the index, direction and conditions. */
export function queryFingerprint(request) {
    const text = JSON.stringify([request.index, request.direction, request.where]);
    let h = 0x811c9dc5;
    for (let i = 0; i < text.length; i++)
        h = Math.imul(h ^ text.charCodeAt(i), 0x01000193);
    return (h >>> 0).toString(36);
}
export function encodeCursor(fingerprint, position) {
    return btoa(unescape(encodeURIComponent(JSON.stringify({ f: fingerprint, p: position })))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
export function decodeCursor(cursor, fingerprint) {
    let parsed;
    try {
        parsed = JSON.parse(decodeURIComponent(escape(atob(cursor.replace(/-/g, "+").replace(/_/g, "/")))));
    }
    catch {
        throw new Error("not a cursor of this table");
    }
    const { f, p } = (parsed ?? {});
    if (f !== fingerprint)
        throw new Error("the cursor belongs to another query (index, direction or conditions)");
    if (!Array.isArray(p) || !p.every((v) => v === null || typeof v === "string" || typeof v === "number"))
        throw new Error("not a cursor of this table");
    return p;
}
//# sourceMappingURL=schema.js.map