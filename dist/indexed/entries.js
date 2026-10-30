// What a row's global index entries are, and the messages between row shards, index shards and the
// client (RFD0003). Row shards and the client compute entries the same way, here.
import { encodeIndexKey, encodeValue } from "../table/schema.js";
export function globalIndexes(schema) {
    return Object.entries(schema.globalIndexes ?? {}).map(([name, def]) => ({ name, columns: def.columns, unique: def.unique === true, partition: def.partition ?? def.columns.length }));
}
/** The routing key of an index's partition. */
export function routeOf(index, partitionValues) {
    return `${index}:${encodeIndexKey(partitionValues)}`;
}
/** The entry a row has in an index, or null (a unique index skips rows with a null in its columns). */
export function entryOf(schema, ix, row) {
    const values = ix.columns.map((c) => encodeValue(schema, c, row[c]));
    if (ix.unique && values.some((v) => v === null))
        return null;
    const key = encodeValue(schema, schema.primaryKey, row[schema.primaryKey]);
    return { index: ix.name, route: routeOf(ix.name, values.slice(0, ix.partition)), entry: encodeIndexKey(ix.unique ? values : [...values, key]) };
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