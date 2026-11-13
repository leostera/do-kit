// The client of a sharded table: routes rows by key and merges listing pages from every shard.
import { sharded, type ShardedClient, type ShardedClientOptions } from "../sharded/client.js";
import {
  mergePages,
  decodeCursor,
  decodeShardCursor,
  nextShard,
  encodeCursor,
  encodeShardCursor,
  pageRequest,
  queryFingerprint,
  type GroupCount,
  type ListPage,
  type ListQuery,
  type Position,
  type RowShape,
  type ShardCursor,
  type ApplyOutcome,
  type ApplyRequest,
  type CommandArgs,
  type CommandResult,
  type Commands,
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
  tableCountBy(group: string): Promise<GroupCount[]>;
}

export type Untyped = {
  call(keys: readonly string[], method: string, payloads: readonly unknown[] | null): Promise<{ positions: readonly number[]; result: readonly unknown[] }[]>;
  scan(method: string, ...args: unknown[]): Promise<unknown[]>;
  scanShard(bits: string, method: string, ...args: unknown[]): Promise<{ ok: unknown } | { split: string[] }>;
  knownLeaves(): Promise<string[]>;
  knownShardCount(): Promise<number>;
};

/** Rounds of shard calls one page may take to fill up, when shards returned less than needed. */
const MAX_ROUNDS = 3;
/** Shards one page in shard order may ask, when those before it run out partway. */
const SHARD_CALLS_PER_PAGE = 3;

/** A walk in shard order that has not begun, from one of these shards picked at random. */
function fromAnyShard(leaves: readonly string[]): ShardCursor {
  const start = leaves[Math.floor(Math.random() * leaves.length)]!;
  return { leaf: start, after: null, floor: null, start, wrapped: false };
}


/**
 * Rows to ask each of `shards` shards for, to fill `wanted`: keys spread over shards by hash, so
 * each holds about wanted/shards of a page; twice that, plus two, rarely comes up short.
 */
export function perShardLimit(wanted: number, shards: number): number {
  return Math.min(wanted, Math.ceil(wanted / shards) * 2 + 2);
}

/** A table client's options: the sharded client's, and the table's commands (`defineCommands`), for `apply`. */
export interface TableClientOptions<C> extends ShardedClientOptions {
  readonly commands?: C;
}

export class TableClient<Row extends RowShape, C extends Commands<Row> = {}> {
  protected readonly client: Untyped;
  protected readonly primaryKey: keyof Row & string;
  private readonly indexes: Readonly<Record<string, readonly string[]>>;
  private readonly commands: C | null;

  constructor(
    namespace: DurableObjectNamespace<Rpc.DurableObjectBranded>,
    readonly name: string,
    schema: TableSchema<Row>,
    options: TableClientOptions<C> = {},
  ) {
    this.primaryKey = schema.primaryKey;
    this.indexes = schema.indexes ?? {};
    const { commands, ...sharding } = options;
    this.commands = commands ?? null;
    this.client = sharded(namespace as never, name, sharding) as ShardedClient<never> as unknown as Untyped;
  }

  /**
   * Runs the table's command `command` on the row of `key`, inside its shard, in one transaction:
   * it reads the row, decides what to write and what to answer, and writes. One call, with no
   * version retries, however many callers update the row at once. A call answered "moved" or "not
   * ready" did not run, and is routed again; a call whose answer is lost may have committed. The
   * shard class must register the same commands (`protected commands`).
   */
  async apply<K extends keyof C & string>(key: string, command: K, args: CommandArgs<C[K]>): Promise<ApplyOutcome<CommandResult<C[K]>>> {
    if (this.commands !== null && !Object.hasOwn(this.commands, command)) throw new Error(`no command ${JSON.stringify(command)} in this client's commands`);
    const request: ApplyRequest = { command, args };
    const [group] = await this.client.call([key], "tableApply", [request]);
    return group!.result[0] as ApplyOutcome<CommandResult<C[K]>>;
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
   * Rows of the whole table per value of a count group's column (values with no rows left out),
   * kept with every write: as exact as `count()`.
   */
  async countBy(group: string): Promise<GroupCount[]> {
    return sumGroups((await this.client.scan("tableCountBy", group)) as GroupCount[][]);
  }

  /**
   * One page of a listing across every shard. Pass the page's cursor back for the next one;
   * a null cursor means the listing is done. A page may hold fewer than `limit` rows (a sparse
   * filter ran out of budget) and still have a cursor.
   */
  async list(query: ListQuery = {}): Promise<ListPage<Row>> {
    if (query.order === "shard") return this.listByShard(query);
    if (query.order !== undefined && query.order !== "query") throw new Error('order must be "query" or "shard"');
    const request0 = pageRequest(query, null);
    const fingerprint = queryFingerprint(request0, request0.index === null ? [] : (this.indexes[request0.index] ?? []));
    const after = query.cursor ? decodeCursor(query.cursor, fingerprint) : null;
    const request = pageRequest(query, after);
    const rows: ShardPage["rows"][number][] = [];
    let next: Position | null = after;
    let counted: { total?: number; counts?: GroupCount[] } = {};
    for (let round = 0; round < MAX_ROUNDS; round++) {
      const wanted = request.limit - rows.length;
      const limit = perShardLimit(wanted, await this.client.knownShardCount());
      // Counts come with the first round's pages: the same calls to the same shards.
      const withCount = round === 0 && query.withCount ? { withCount: query.withCount } : {};
      const pages = (await this.client.scan("tablePage", { ...request, limit, after: next, ...withCount })) as ShardPage[];
      if (round === 0 && query.withCount) {
        counted = { total: pages.reduce((n, p) => n + (p.total ?? 0), 0) };
        if (typeof query.withCount === "string") counted.counts = sumGroups(pages.map((p) => p.counts ?? []));
      }
      const merged = mergePages(pages, { limit: wanted, direction: request.direction });
      rows.push(...merged.rows);
      next = merged.next;
      if (next === null || rows.length === request.limit) break;
    }
    return {
      rows: rows.map(({ row, version }) => ({ row: row as Row, version })),
      cursor: next === null ? null : encodeCursor(fingerprint, next),
      ...counted,
    };
  }

  /**
   * One page of a listing in shard order: from the cursor's shard on, one shard at a time, until
   * the page is full or SHARD_CALLS_PER_PAGE shards were asked.
   */
  private async listByShard(query: ListQuery): Promise<ListPage<Row>> {
    if (query.withCount) throw new Error('withCount is not supported in order "shard"');
    const request = pageRequest(query, null);
    const fingerprint = `${queryFingerprint(request, request.index === null ? [] : (this.indexes[request.index] ?? []))}:shard`;
    let at: ShardCursor | null = query.cursor ? decodeShardCursor(query.cursor, fingerprint) : fromAnyShard(await this.client.knownLeaves());
    const rows: ShardPage["rows"][number][] = [];
    for (let calls = 0; at !== null && rows.length < request.limit && calls < SHARD_CALLS_PER_PAGE; ) {
      const answer = await this.client.scanShard(at.leaf, "tablePage", { ...request, limit: request.limit - rows.length, after: at.after });
      if ("split" in answer) {
        // A walk not yet begun starts at any of its shards.
        if (!at.wrapped && at.leaf === at.start && at.after === null) at = fromAnyShard(answer.split);
        // Otherwise the shard split since: its shards, in order, each from where it was.
        else at = { ...at, leaf: answer.split[0]!, floor: at.floor ?? (at.after === null ? null : { bits: at.leaf, after: at.after }) };
        // Wrapped around to the root, which has split down to the start: done.
        if (at.wrapped && at.leaf.startsWith(at.start)) at = null;
        continue;
      }
      calls++;
      const page = answer.ok as ShardPage;
      rows.push(...page.rows);
      at = page.frontier !== null ? { ...at, after: page.frontier } : nextShard(at);
    }
    return { rows: rows.map(({ row, version }) => ({ row: row as Row, version })), cursor: at === null ? null : encodeShardCursor(fingerprint, at) };
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

  protected keyOf(row: Row): string {
    const key = row[this.primaryKey];
    if (typeof key !== "string") throw new Error(`the row has no ${this.primaryKey}`);
    return key;
  }

  protected async byPosition<R>(keys: readonly string[], method: keyof TableMethods, payloads: readonly unknown[] | null): Promise<R[]> {
    const out = new Array<R>(keys.length);
    for (const group of await this.client.call(keys, method, payloads)) group.positions.forEach((p, i) => (out[p] = group.result[i] as R));
    return out;
  }
}

/** Sums counts per value over shards, leaving out values with none. */
function sumGroups(perShard: readonly (readonly GroupCount[])[]): GroupCount[] {
  const sums = new Map<string, GroupCount>();
  for (const counts of perShard) {
    for (const { value, count } of counts) {
      const key = JSON.stringify(value);
      sums.set(key, { value, count: (sums.get(key)?.count ?? 0) + count });
    }
  }
  return [...sums.values()].filter((c) => c.count !== 0);
}

/**
 * The client of the sharded table `name`: `namespace` is the binding of a `ShardedTable` class,
 * `schema` the table it declares (the client routes rows by its primary key).
 */
export function table<T extends Rpc.DurableObjectBranded, Row extends RowShape, C extends Commands<Row> = {}>(namespace: DurableObjectNamespace<T>, name: string, schema: TableSchema<Row>, options: TableClientOptions<C> = {}): TableClient<Row, C> {
  return new TableClient<Row, C>(namespace as unknown as DurableObjectNamespace<Rpc.DurableObjectBranded>, name, schema, options);
}

export { mergePages } from "./schema.js";
