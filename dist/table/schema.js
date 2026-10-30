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
    for (const [name, def] of Object.entries(schema.globalIndexes ?? {})) {
        if (!IDENTIFIER.test(name))
            throw new Error(`global index ${JSON.stringify(name)}: a plain identifier`);
        if (schema.indexes?.[name])
            throw new Error(`global index ${name}: an index has that name`);
        if (def.columns.length === 0)
            throw new Error(`global index ${name}: no columns`);
        for (const col of def.columns) {
            const type = schema.columns[col];
            if (!type)
                throw new Error(`global index ${name}: no column ${col}`);
            if (type === "json")
                throw new Error(`global index ${name}: json column ${col} cannot be indexed`);
        }
        const partition = def.partition ?? def.columns.length;
        if (partition === 0)
            throw new Error(`global index ${name}: partition 0 (by range) is not built yet`);
        if (!Number.isInteger(partition) || partition < 1 || partition > def.columns.length)
            throw new Error(`global index ${name}: partition must be from 1 to ${def.columns.length}`);
        if (def.unique && partition !== def.columns.length)
            throw new Error(`global index ${name}: a unique index is partitioned by all its columns`);
    }
    for (const [name, col] of Object.entries(schema.counts ?? {})) {
        if (!IDENTIFIER.test(name))
            throw new Error(`count group ${JSON.stringify(name)}: a plain identifier`);
        const type = schema.columns[col];
        if (!type)
            throw new Error(`count group ${name}: no column ${col}`);
        if (type === "json")
            throw new Error(`count group ${name}: json column ${col} cannot be counted by value`);
    }
    return schema;
}
/**
 * A column's value as SQLite stores it, checked against the column's type: `boolean` as 0/1,
 * `json` as text. NaN is refused: SQLite would store it as null.
 */
export function encodeValue(schema, column, value) {
    const type = schema.columns[column];
    if (!type)
        throw new Error(`no column ${column}`);
    if (value === null || value === undefined) {
        if (column === schema.primaryKey)
            throw new Error("the primary key cannot be null");
        return null;
    }
    switch (type) {
        case "text":
            if (typeof value !== "string")
                throw new Error(`${column}: expected text`);
            return value;
        case "integer":
            if (typeof value !== "number" || !Number.isInteger(value))
                throw new Error(`${column}: expected an integer`);
            return value;
        case "real":
            if (typeof value !== "number" || Number.isNaN(value))
                throw new Error(`${column}: expected a number (not NaN)`);
            return value;
        case "boolean":
            if (typeof value !== "boolean")
                throw new Error(`${column}: expected a boolean`);
            return value ? 1 : 0;
        case "json": {
            const text = JSON.stringify(value);
            if (text === undefined)
                throw new Error(`${column}: not representable as JSON`);
            return text;
        }
    }
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
/** Compares two positions in listing order: code point order, reversed for `desc`. */
export function comparePositions(a, b, direction) {
    const d = compareText(a, b);
    return direction === "asc" ? d : -d;
}
/** Code point order: the order of UTF-8 bytes, which is how SQLite compares text. */
export function compareText(a, b) {
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
/**
 * The index key of these values, as hex: byte order is the values' order. Null (0x00) sorts
 * before numbers (0x01, then an order-preserving float64) and text (0x02, then its UTF-8 with
 * 0x00 escaped as 0x00 0xFF, then 0x00): nulls first ascending, last descending.
 */
export function encodeIndexKey(values) {
    const bytes = [];
    for (const v of values) {
        if (v === null)
            bytes.push(0x00);
        else if (typeof v === "number") {
            if (Number.isNaN(v))
                throw new Error("NaN cannot be indexed");
            const view = new DataView(new ArrayBuffer(8));
            view.setFloat64(0, v === 0 ? 0 : v);
            const b = new Uint8Array(view.buffer);
            if (b[0] & 0x80)
                for (let i = 0; i < 8; i++)
                    b[i] = ~b[i] & 0xff;
            else
                b[0] = b[0] | 0x80;
            bytes.push(0x01, ...b);
        }
        else {
            bytes.push(0x02);
            for (const byte of new TextEncoder().encode(v)) {
                bytes.push(byte);
                if (byte === 0x00)
                    bytes.push(0xff);
            }
            bytes.push(0x00);
        }
    }
    let hex = "";
    for (const byte of bytes)
        hex += byte.toString(16).padStart(2, "0");
    return hex;
}
/**
 * A short fingerprint of what a cursor belongs to: the index (its name and columns, so a cursor
 * from before an index was redefined is refused, not resumed at a meaningless position), the
 * direction and the conditions.
 */
export function queryFingerprint(request, columns) {
    const text = JSON.stringify([request.index, columns, request.direction, request.where]);
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
    if (typeof p !== "string")
        throw new Error("not a cursor of this table");
    return p;
}
//# sourceMappingURL=schema.js.map