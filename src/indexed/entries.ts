// What a row's global index entries are, and the messages between row shards, index shards and the
// client (RFD0003). Row shards and the client compute entries the same way, here.
import { encodeIndexKey, encodeValue, type Condition, type RowShape, type SqlValue, type TableSchema } from "../table/schema.js";

/** A global index, resolved from the schema. */
export interface GlobalIndexInfo {
  readonly name: string;
  /**
   * The index's name and a fingerprint of its definition: what its routes start with. An index
   * redefined under the same name gets another id, so its entries never mix with the old ones.
   */
  readonly id: string;
  readonly columns: readonly string[];
  readonly unique: boolean;
  readonly partition: number;
}

/**
 * Where an entry lives: `route` is its index shard's routing key (the index's name and the
 * partition values), `entry` its key there (the values, and the row's key unless unique), both
 * in the order-preserving encoding of RFD0002.
 */
export interface EntryAddress {
  readonly index: string;
  readonly route: string;
  readonly entry: string;
}

export function globalIndexes<Row extends RowShape>(schema: TableSchema<Row>): GlobalIndexInfo[] {
  return Object.entries(schema.globalIndexes ?? {}).map(([name, def]) => {
    if (name.includes(":")) throw new Error(`global index ${name}: a name cannot have a ":"`);
    const columns = def.columns;
    const unique = def.unique === true;
    const partition = def.partition ?? def.columns.length;
    return { name, id: `${name}~${fingerprint(JSON.stringify([columns, unique, partition]))}`, columns, unique, partition };
  });
}

/** The routing key of an index's partition (`index` is the index's id). */
export function routeOf(index: string, partitionValues: readonly SqlValue[]): string {
  return `${index}:${encodeIndexKey(partitionValues)}`;
}

/** The id of the index a route is of. */
export function indexIdOfRoute(route: string): string {
  return route.slice(0, route.indexOf(":"));
}

/** The name of the index an id is of (an id from before ids had a fingerprint is the name). */
export function indexNameOfId(id: string): string {
  return /~[0-9a-f]{8}$/.test(id) ? id.slice(0, -9) : id;
}

/** Every route of the index with this id is in [from, to): its entries, as one range. */
export function routeRangeOf(id: string): readonly [from: string, to: string] {
  return [`${id}:`, `${id};`];
}

/** 32-bit FNV-1a, as 8 hex digits: the same on every runtime. */
function fingerprint(text: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i++) h = Math.imul(h ^ text.charCodeAt(i), 0x01000193) >>> 0;
  return h.toString(16).padStart(8, "0");
}

/** The entry a row has in an index, or null (a unique index skips rows with a null in its columns). */
export function entryOf<Row extends RowShape>(schema: TableSchema<Row>, ix: GlobalIndexInfo, row: RowShape): EntryAddress | null {
  const values = ix.columns.map((c) => encodeValue(schema, c, row[c]));
  if (ix.unique && values.some((v) => v === null)) return null;
  const key = encodeValue(schema, schema.primaryKey, row[schema.primaryKey]);
  return { index: ix.name, route: routeOf(ix.id, values.slice(0, ix.partition)), entry: encodeIndexKey(ix.unique ? values : [...values, key]) };
}

/** Every entry a row has, keyed by `route entry`. */
export function entriesOf<Row extends RowShape>(schema: TableSchema<Row>, row: RowShape | null): Map<string, EntryAddress> {
  const out = new Map<string, EntryAddress>();
  if (!row) return out;
  for (const ix of globalIndexes(schema)) {
    const e = entryOf(schema, ix, row);
    if (e) out.set(addressKey(e), e);
  }
  return out;
}

export function addressKey(e: Pick<EntryAddress, "route" | "entry">): string {
  return `${e.route} ${e.entry}`;
}

/**
 * A change a row shard delivers to an index shard: a put (the row's entry at `version`, with a
 * copy of the row; with the reservation token it was written with, for a unique index), a remove
 * (the row no longer has the entry since `version`), or a confirm (a put a later write superseded:
 * only its token, which clears the reservation).
 */
export type IndexOp =
  | { readonly kind: "put"; readonly route: string; readonly entry: string; readonly owner: string; readonly version: number; readonly row: RowShape; readonly token: number | null; readonly backfill?: true }
  | { readonly kind: "remove"; readonly route: string; readonly entry: string; readonly owner: string; readonly version: number }
  | { readonly kind: "confirm"; readonly route: string; readonly entry: string; readonly owner: string; readonly version: number; readonly token: number };

/** An index change as the outbox keeps it: a put without the row, which is read when it is sent. */
export type StoredOp = Exclude<IndexOp, { kind: "put" } | { kind: "confirm" }> | { readonly kind: "put"; readonly route: string; readonly entry: string; readonly owner: string; readonly version: number; readonly token: number | null; readonly backfill?: true };

/**
 * What an index shard answers a delivered change: null, or for a put of a backfill (a row that was
 * there before its unique index), the row that has the value already.
 */
export type ApplyAnswer = null | { readonly taken: string };

/** How far a table's global indexes are built, and the rows a unique one could not take. */
export interface IndexStatus {
  /** Indexes still being built over rows that were there before them: lookups and pages may miss rows. */
  readonly building: readonly string[];
  /** Rows whose value of a unique index another row has (at most 100 a shard): they are not in it. */
  readonly duplicates: readonly { readonly index: string; readonly key: string; readonly owner: string }[];
}

/** What reserving a unique value answered. */
export type ReserveAnswer = { readonly reserved: number } | { readonly taken: string } | { readonly contended: true };

/** What a row shard answers about a reservation: the row has the entry, or not (and its token is fenced). */
export type ResolveAnswer = { readonly committed: true; readonly version: number; readonly row: RowShape } | { readonly committed: false; readonly version: number };

/** One index shard's part of a listing within a partition. */
export interface IndexPageRequest {
  readonly direction: "asc" | "desc";
  /** Conditions on the row copies, their values encoded as the rows store them. */
  readonly where: readonly Condition[];
  readonly limit: number;
  readonly budget: number;
  /** The entry key to start after. */
  readonly after: string | null;
}

export interface IndexPage {
  readonly rows: readonly { readonly row: RowShape; readonly version: number; readonly entry: string }[];
  /** Up to where every match was reported; null: the end. */
  readonly frontier: string | null;
}
