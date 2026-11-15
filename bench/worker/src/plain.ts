// The baselines for the table targets: the same rows, columns and indexes in plain SQLite
// Durable Objects, with none of do-kit. "plain-1" is one object; "plain-N" is N, the client hashing
// each key to one and merging their pages itself, as an app sharding by hand would. A table-N
// point less the plain-N point at the same load is what do-kit costs.
import { DurableObject } from "cloudflare:workers";

/** The bench's row, as the table targets store it. */
export interface PlainRow {
  readonly name: string;
  readonly createdAt: number;
  readonly owner: string;
  readonly size: number;
  readonly payload: string;
  readonly email: string;
  readonly team: string;
}

type Stored = PlainRow & { readonly version: number };
/** Where a page in createdAt order stops: the last row's (createdAt, name), compared as a pair. */
type After = readonly [number, string];

const COLUMNS = ["name", "createdAt", "owner", "size", "payload", "email", "team"] as const;

/** One plain object: the rows, an index on createdAt and one on (owner, createdAt), and a version a row. */
export class PlainRows<Env = unknown> extends DurableObject<Env> {
  private readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS plain_rows (name TEXT PRIMARY KEY, createdAt INTEGER NOT NULL, owner TEXT NOT NULL, size INTEGER NOT NULL, payload TEXT NOT NULL, email TEXT NOT NULL, team TEXT NOT NULL, version INTEGER NOT NULL)`);
    this.sql.exec("CREATE INDEX IF NOT EXISTS plain_by_created ON plain_rows (createdAt, name)");
    this.sql.exec("CREATE INDEX IF NOT EXISTS plain_by_owner ON plain_rows (owner, createdAt)");
  }

  /** Puts each row, unless `ifVersion` is given and is not the row's version (0: absent). */
  async put(writes: readonly { row: PlainRow; ifVersion?: number }[]): Promise<{ ok: boolean; version: number }[]> {
    return this.ctx.storage.transactionSync(() =>
      writes.map(({ row, ifVersion }) => {
        const current = this.sql.exec<{ version: number }>("SELECT version FROM plain_rows WHERE name = ?", row.name).toArray()[0]?.version ?? 0;
        if (ifVersion !== undefined && ifVersion !== current) return { ok: false, version: current };
        this.sql.exec(
          `INSERT INTO plain_rows (${COLUMNS.join(", ")}, version) VALUES (?, ?, ?, ?, ?, ?, ?, 1)
           ON CONFLICT (name) DO UPDATE SET createdAt = excluded.createdAt, owner = excluded.owner, size = excluded.size, payload = excluded.payload, email = excluded.email, team = excluded.team, version = version + 1`,
          ...COLUMNS.map((c) => row[c]),
        );
        return { ok: true, version: current + 1 };
      }),
    );
  }

  async get(keys: readonly string[]): Promise<(Stored | null)[]> {
    return keys.map((key) => this.sql.exec<Stored & Record<string, SqlStorageValue>>("SELECT * FROM plain_rows WHERE name = ?", key).toArray()[0] ?? null);
  }

  /** Newest first: up to `limit` rows before `after` in (createdAt, name) order. */
  async page(limit: number, after: After | null): Promise<Stored[]> {
    const rows = after
      ? this.sql.exec<Stored & Record<string, SqlStorageValue>>("SELECT * FROM plain_rows WHERE (createdAt, name) < (?, ?) ORDER BY createdAt DESC, name DESC LIMIT ?", after[0], after[1], limit)
      : this.sql.exec<Stored & Record<string, SqlStorageValue>>("SELECT * FROM plain_rows ORDER BY createdAt DESC, name DESC LIMIT ?", limit);
    return rows.toArray();
  }

  async count(): Promise<number> {
    return this.sql.exec<{ n: number }>("SELECT count(*) AS n FROM plain_rows").one().n;
  }
}

/** FNV-1a of a key: which of the plain objects holds it. */
function shardOf(key: string, shards: number): number {
  let h = 0x811c9dc5;
  for (let i = 0; i < key.length; i++) h = Math.imul(h ^ key.charCodeAt(i), 0x01000193);
  return (h >>> 0) % shards;
}

const versioned = (row: Stored) => {
  const { version, ...rest } = row;
  return { row: rest as PlainRow, version };
};

/**
 * A client of `shards` plain objects, with the calls the bench makes of a table: get, put, a page
 * newest first in each object, one object after another, and count.
 */
export function plainTable(ns: DurableObjectNamespace<PlainRows>, name: string, shards: number) {
  const stub = (i: number) => ns.get(ns.idFromName(`${name}/${shards}/${i}`));
  const byShard = <T extends { key: string }>(items: readonly T[]) => {
    const groups = new Map<number, { item: T; at: number }[]>();
    items.forEach((item, at) => {
      const s = shardOf(item.key, shards);
      groups.set(s, [...(groups.get(s) ?? []), { item, at }]);
    });
    return groups;
  };
  const client = {
    async getMany(keys: readonly string[]) {
      const out = new Array<{ row: PlainRow; version: number } | null>(keys.length).fill(null);
      await Promise.all(
        [...byShard(keys.map((key) => ({ key })))].map(async ([s, group]) => {
          const rows = await stub(s).get(group.map((g) => g.item.key));
          group.forEach((g, i) => (out[g.at] = rows[i] ? versioned(rows[i]!) : null));
        }),
      );
      return out;
    },
    async get(key: string) {
      return (await client.getMany([key]))[0]!;
    },
    async putMany(writes: readonly { row: PlainRow; ifVersion?: number }[]) {
      const out = new Array<{ ok: boolean; version: number }>(writes.length);
      await Promise.all(
        [...byShard(writes.map((w) => ({ ...w, key: w.row.name })))].map(async ([s, group]) => {
          const answers = await stub(s).put(group.map(({ item }) => ({ row: item.row, ...(item.ifVersion !== undefined ? { ifVersion: item.ifVersion } : {}) })));
          group.forEach((g, i) => (out[g.at] = answers[i]!));
        }),
      );
      return out;
    },
    async put(row: PlainRow, options: { ifVersion?: number } = {}) {
      return (await client.putMany([{ row, ...options }]))[0]!;
    },
    async list(query: { limit?: number; cursor?: string | null } = {}) {
      const limit = query.limit ?? 50;
      // A cursor of (object, after): the rest of one object, then the next.
      let [at, after] = query.cursor ? (JSON.parse(query.cursor) as [number, After | null]) : [0, null];
      const rows: Stored[] = [];
      while (at < shards && rows.length < limit) {
        const want = limit - rows.length;
        const page = await stub(at).page(want, after);
        rows.push(...page);
        // A short page: this object is done.
        if (page.length < want) [at, after] = [at + 1, null];
        else after = [page.at(-1)!.createdAt, page.at(-1)!.name];
      }
      return { rows: rows.map(versioned), cursor: at < shards ? JSON.stringify([at, after]) : null };
    },
    async count() {
      return (await Promise.all(Array.from({ length: shards }, (_, s) => stub(s).count()))).reduce((a, b) => a + b, 0);
    },
  };
  return client;
}
