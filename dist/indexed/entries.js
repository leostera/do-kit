// What a row's global index entries are, and the messages between row shards, index shards and the
// client (RFD0003). Row shards and the client compute entries the same way, here.
import { encodeIndexKey, encodeValue } from "../table/schema.js";
export function globalIndexes(schema) {
    return Object.entries(schema.globalIndexes ?? {}).map(([name, def]) => {
        if (name.includes(":"))
            throw new Error(`global index ${name}: a name cannot have a ":"`);
        const columns = def.columns;
        const unique = def.unique === true;
        const partition = def.partition ?? def.columns.length;
        return { name, id: `${name}~${fingerprint(JSON.stringify([columns, unique, partition]))}`, columns, unique, partition };
    });
}
/** The routing key of an index's partition (`index` is the index's id). */
export function routeOf(index, partitionValues) {
    return `${index}:${encodeIndexKey(partitionValues)}`;
}
/** The id of the index a route is of. */
export function indexIdOfRoute(route) {
    return route.slice(0, route.indexOf(":"));
}
/** The name of the index an id is of (an id from before ids had a fingerprint is the name). */
export function indexNameOfId(id) {
    return /~[0-9a-f]{8}$/.test(id) ? id.slice(0, -9) : id;
}
/** Every route of the index with this id is in [from, to): its entries, as one range. */
export function routeRangeOf(id) {
    return [`${id}:`, `${id};`];
}
/** 32-bit FNV-1a, as 8 hex digits: the same on every runtime. */
function fingerprint(text) {
    let h = 0x811c9dc5;
    for (let i = 0; i < text.length; i++)
        h = Math.imul(h ^ text.charCodeAt(i), 0x01000193) >>> 0;
    return h.toString(16).padStart(8, "0");
}
/** The entry a row has in an index, or null (a unique index skips rows with a null in its columns). */
export function entryOf(schema, ix, row) {
    const values = ix.columns.map((c) => encodeValue(schema, c, row[c]));
    if (ix.unique && values.some((v) => v === null))
        return null;
    const key = encodeValue(schema, schema.primaryKey, row[schema.primaryKey]);
    return { index: ix.name, route: routeOf(ix.id, values.slice(0, ix.partition)), entry: encodeIndexKey(ix.unique ? values : [...values, key]) };
}
/** Every entry a row has, keyed by `route entry`. */
export function entriesOf(schema, row) {
    const out = new Map();
    if (!row)
        return out;
    for (const ix of globalIndexes(schema)) {
        const e = entryOf(schema, ix, row);
        if (e)
            out.set(addressKey(e), e);
    }
    return out;
}
export function addressKey(e) {
    return `${e.route} ${e.entry}`;
}
//# sourceMappingURL=entries.js.map