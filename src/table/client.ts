// The client of a sharded table: routes rows by key and merges listing pages from every shard.
import { sharded, type ShardedClient, type ShardedClientOptions } from "../sharded/client.js";
import {
  comparePositions,
  decodeCursor,
  encodeCursor,
  pageRequest,
  queryFingerprint,
  type ListPage,
  type ListQuery,
  type Position,
  type RowShape,
  type ShardPage,
  type TableSchema,
  type ShardPageRequest,
  type Versioned,
  type WriteResult,
} from "./schema.js";

/** The `table*` methods, as the client calls them through the sharded client. */
interface TableMethods {
  tableGet(keys: readonly string[], payloads: null): Promise<(Versioned<RowShape> | null)[]>;
  tablePut(keys: readonly string[], payloads: readonly { row: RowShape; ifVersion?: number }[]): Promise<WriteResult[]>;
  tablePatch(keys: readonly string[], payloads: readonly { changes: RowShape; ifVersion?: number }[]): Promise<WriteResult[]>;
  tableDelete(keys: readonly string[], payloads: readonly { ifVersion?: number }[] | null): Promise<WriteResult[]>;
  tablePage(request: ShardPageRequest): Promise<ShardPage>;
  tableCount(): Promise<number>;
}

type Untyped = {
  call(keys: readonly string[], method: string, payloads: readonly unknown[] | null): Promise<{ positions: readonly number[]; result: readonly unknown[] }[]>;
  scan(method: string, ...args: unknown[]): Promise<unknown[]>;
  knownShardCount(): Promise<number>;
};

/** Rounds of shard calls one page may take to fill up, when shards returned less than needed. */
const MAX_ROUNDS = 3;

/**
 * Rows to ask each of `shards` shards for, to fill `wanted`: keys spread over shards by hash, so
 * each holds about wanted/shards of a page; twice that, plus two, rarely comes up short.
 */
export function perShardLimit(wanted: number, shards: number): number {
  return Math.min(wanted, Math.ceil(wanted / shards) * 2 + 2);
}

export class TableClient<Row extends RowShape> {
  private readonly client: Untyped;
  private readonly primaryKey: keyof Row & string;
  private readonly indexes: Readonly<Record<string, readonly string[]>>;

  constructor(
    namespace: DurableObjectNamespace<Rpc.DurableObjectBranded>,
    readonly name: string,
    schema: TableSchema<Row>,
    options: ShardedClientOptions = {},
  ) {
    this.primaryKey = schema.primaryKey;
    this.indexes = schema.indexes ?? {};
    this.client = sharded(namespace as never, name, options) as ShardedClient<never> as unknown as Untyped;
  }

  async get(key: string): Promise<Versioned<Row> | null> {
    return (await this.getMany([key]))[0]!;
  }

  /** Rows of these keys, in order (null: absent). */
  async getMany(keys: readonly string[]): Promise<(Versioned<Row> | null)[]> {
    return this.byPosition<Versioned<Row> | null>(keys, "tableGet", null);
  }

  /** Writes a whole row; `ifVersion: 0` only if absent, `ifVersion: n` only if at version n. */
  async put(row: Row, options: { ifVersion?: number } = {}): Promise<WriteResult> {
    return (await this.putMany([{ row, ...options }]))[0]!;
  }

  async putMany(writes: readonly { row: Row; ifVersion?: number }[]): Promise<WriteResult[]> {
    return this.byPosition<WriteResult>(writes.map((w) => this.keyOf(w.row)), "tablePut", writes);
  }

  /** Changes some columns of an existing row (`ok: false` if absent or at another version). */
  async patch(key: string, changes: Partial<Row>, options: { ifVersion?: number } = {}): Promise<WriteResult> {
    return (await this.byPosition<WriteResult>([key], "tablePatch", [{ changes, ...options }]))[0]!;
  }

  async delete(key: string, options: { ifVersion?: number } = {}): Promise<WriteResult> {
    return (await this.byPosition<WriteResult>([key], "tableDelete", [options]))[0]!;
  }

  /** Rows in the whole table: exact unless a split commits during the count. */
  async count(): Promise<number> {
    return ((await this.client.scan("tableCount")) as number[]).reduce((a, b) => a + b, 0);
  }

  /**
   * One page of a listing across every shard. Pass the page's cursor back for the next one;
   * a null cursor means the listing is done. A page may hold fewer than `limit` rows (a sparse
   * filter ran out of budget) and still have a cursor.
   */
  async list(query: ListQuery = {}): Promise<ListPage<Row>> {
    const request0 = pageRequest(query, null);
    const fingerprint = queryFingerprint(request0, request0.index === null ? [] : (this.indexes[request0.index] ?? []));
    const after = query.cursor ? decodeCursor(query.cursor, fingerprint) : null;
    const request = pageRequest(query, after);
    const rows: ShardPage["rows"][number][] = [];
    let next: Position | null = after;
    for (let round = 0; round < MAX_ROUNDS; round++) {
      const wanted = request.limit - rows.length;
      const limit = perShardLimit(wanted, await this.client.knownShardCount());
      const pages = (await this.client.scan("tablePage", { ...request, limit, after: next })) as ShardPage[];
      const merged = mergePages(pages, { limit: wanted, direction: request.direction });
      rows.push(...merged.rows);
      next = merged.next;
      if (next === null || rows.length === request.limit) break;
    }
    return {
      rows: rows.map(({ row, version }) => ({ row: row as Row, version })),
      cursor: next === null ? null : encodeCursor(fingerprint, next),
    };
  }

  /** Every row of a query, page after page. */
  async *all(query: Omit<ListQuery, "cursor"> = {}): AsyncGenerator<Versioned<Row>> {
    let cursor: string | null = null;
    do {
      const page: ListPage<Row> = await this.list({ ...query, cursor });
      yield* page.rows;
      cursor = page.cursor;
    } while (cursor !== null);
  }

  private keyOf(row: Row): string {
    const key = row[this.primaryKey];
    if (typeof key !== "string") throw new Error(`the row has no ${this.primaryKey}`);
    return key;
  }

  private async byPosition<R>(keys: readonly string[], method: keyof TableMethods, payloads: readonly unknown[] | null): Promise<R[]> {
    const out = new Array<R>(keys.length);
    for (const group of await this.client.call(keys, method, payloads)) group.positions.forEach((p, i) => (out[p] = group.result[i] as R));
    return out;
  }
}

/**
 * Merges one page from every shard: every shard reported every match up to the smallest frontier,
 * so rows up to it are complete. Returns them in order, up to `limit`, and where the next page
 * starts (null: done).
 */
export function mergePages(pages: readonly ShardPage[], request: Pick<ShardPageRequest, "limit" | "direction">): { rows: ShardPage["rows"][number][]; next: Position | null } {
  const cmp = (a: Position, b: Position) => comparePositions(a, b, request.direction);
  let frontier: Position | null = null;
  for (const page of pages) if (page.frontier && (!frontier || cmp(page.frontier, frontier) < 0)) frontier = page.frontier;
  const seen = new Set<string>();
  const complete = pages
    .flatMap((p) => p.rows)
    .filter((r) => frontier === null || cmp(r.position, frontier) <= 0)
    .filter((r) => {
      // A key reported by two shards in one page (one before a split committed, one after).
      if (seen.has(r.key)) return false;
      seen.add(r.key);
      return true;
    })
    .sort((a, b) => cmp(a.position, b.position));
  const rows = complete.slice(0, request.limit);
  if (rows.length === request.limit && (complete.length > request.limit || frontier !== null)) return { rows, next: rows.at(-1)!.position };
  return { rows, next: frontier };
}

/**
 * The client of the sharded table `name`: `namespace` is the binding of a `ShardedTable` class,
 * `schema` the table it declares (the client routes rows by its primary key).
 */
export function table<T extends Rpc.DurableObjectBranded, Row extends RowShape>(namespace: DurableObjectNamespace<T>, name: string, schema: TableSchema<Row>, options: ShardedClientOptions = {}): TableClient<Row> {
  return new TableClient<Row>(namespace as unknown as DurableObjectNamespace<Rpc.DurableObjectBranded>, name, schema, options);
}
