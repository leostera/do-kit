// Leased reads of one hot key, wired as an app would: a source object holding the key, relays it
// leases the key to, and the readers' isolates, which answer from their sub-lease while it lasts.
// The plain baseline is the same readers reading one Durable Object.
import { DurableObject } from "cloudflare:workers";
import { HOT_READS, READ_LEASE_MARGIN_MS, ReadLeases, RelayPlan, RelayReads, relayIndex, relayParent, type RelayAnswer, type SourceAnswer } from "../../../src/leased/index.ts";

export interface LeasedEnv {
  readonly SINGLE: DurableObjectNamespace<PlainObject>;
  readonly LEASED_SOURCE: DurableObjectNamespace<LeasedSource>;
  readonly LEASED_RELAY: DurableObjectNamespace<LeasedRelay>;
}

/** The plain object's interface (SingleObject, index.ts). */
interface PlainObject extends Rpc.DurableObjectBranded {
  put(keys: readonly string[], values: readonly string[]): Promise<number>;
  get(keys: readonly string[], payloads: null): Promise<(string | null)[]>;
}

/** "reader-single": readers read one plain object; "reader-leased": with leases and relays. */
export type ReaderTarget = "reader-single" | "reader-leased";

type Value = string | null;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** The object holding the key: leases it out, holds writes until the leases are over or given back. */
export class LeasedSource extends DurableObject<LeasedEnv> {
  private readonly sql: SqlStorage;
  private readonly leases: ReadLeases<Value>;
  private readonly plan: RelayPlan;
  private name = "";
  /** Reads and writes this source served, since it started (in memory): what a benchmark point reports. */
  private readonly served = { direct: 0, relay: 0, writes: 0, relays: 0 };

  constructor(ctx: DurableObjectState, env: LeasedEnv) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL)");
    this.sql.exec("CREATE TABLE IF NOT EXISTS held (key TEXT PRIMARY KEY, until INTEGER NOT NULL)");
    const horizon = this.sql.exec<{ v: number }>("SELECT v FROM meta WHERE k = 'horizon'").toArray()[0]?.v ?? 0;
    this.leases = new ReadLeases<Value>(horizon, (h) => void this.sql.exec("INSERT INTO meta (k, v) VALUES ('horizon', ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v", h), {
      load: () => this.sql.exec<{ key: string; until: number }>("SELECT key, until FROM held").toArray(),
      put: ({ key, until }) => void this.sql.exec("INSERT INTO held (key, until) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET until = excluded.until", key, until),
      delete: (key) => void this.sql.exec("DELETE FROM held WHERE key = ?", key),
    });
    // A key being written gets no relay lease, so it isn't relayed: its readers read here.
    this.plan = new RelayPlan(HOT_READS, (key) => this.leases.written(key));
  }

  private committed(key: string): Value | undefined {
    const row = this.sql.exec<{ value: string | null }>("SELECT value FROM kv WHERE key = ?", key).toArray()[0];
    return row ? row.value : undefined;
  }

  private async readable(key: string): Promise<void> {
    const wait = this.leases.readableAt([key]) - Date.now();
    if (wait > 0) await sleep(wait);
  }

  /** An isolate's read: a lease that can't be asked back, and the key's relay count. */
  async read(name: string, key: string): Promise<SourceAnswer<Value>> {
    this.name = name;
    await this.readable(key);
    const [relays] = this.plan.direct([key]);
    this.served.direct++;
    this.served.relays = relays!;
    return { value: this.leases.visible(key, this.committed(key)) ?? null, leaseMs: this.leases.grant([key]), relays: relays! };
  }

  /** A relay's read, with its asks in the last second: a lease it gives back when asked (none if `fresh`). */
  async relayRead(name: string, key: string, relay: number, asks: number, fresh: boolean): Promise<SourceAnswer<Value>> {
    this.name = name;
    await this.readable(key);
    const [relays] = this.plan.reported([key], relay, asks);
    this.served.relay++;
    this.served.relays = relays!;
    return { value: this.leases.visible(key, this.committed(key)) ?? null, leaseMs: fresh ? 0 : this.leases.grant([key], relay), relays: relays! };
  }

  /** What this source served since it started, and the hot key's relay count last answered. */
  async stats(): Promise<{ direct: number; relay: number; writes: number; relays: number }> {
    return { ...this.served };
  }

  /** Commits at once; acknowledged once every lease on the key is over or given back. */
  async write(name: string, key: string, value: string): Promise<void> {
    this.name = name;
    this.served.writes++;
    const before = this.committed(key);
    this.ctx.storage.transactionSync(() => {
      this.sql.exec("INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", key, value);
      this.leases.holdUntil([{ key, before }]);
    });
    await Promise.all(
      this.leases.toRevoke(key).map(async (revoked) => {
        const inMs = await relayStub(this.env, this.name, revoked.holder).revoke(key).catch(() => null);
        if (inMs !== null) this.leases.returned(key, revoked, inMs);
      }),
    );
    const wait = this.leases.shownAt(key) - Date.now();
    if (wait > 0) await sleep(wait);
  }
}

function relayStub(env: LeasedEnv, name: string, index: number): DurableObjectStub<LeasedRelay> {
  return env.LEASED_RELAY.get(env.LEASED_RELAY.idFromName(`${name}#relay-${index}`));
}

function sourceStub(env: LeasedEnv, name: string): DurableObjectStub<LeasedSource> {
  return env.LEASED_SOURCE.get(env.LEASED_SOURCE.idFromName(name));
}

/** A relay: answers isolates from its lease, renewed ahead; reads the source, or its parent relay. */
export class LeasedRelay extends DurableObject<LeasedEnv> {
  private readonly keys = new Map<string, { reads: RelayReads<Value>; reported: { at: number; asks: number } }>();

  async read(name: string, key: string, index: number): Promise<RelayAnswer<Value>> {
    return this.relay(name, key, index).reads.read();
  }

  async revoke(key: string): Promise<number> {
    return this.keys.get(key)?.reads.revoke() ?? 0;
  }

  private relay(name: string, key: string, index: number): { reads: RelayReads<Value>; reported: { at: number; asks: number } } {
    let r = this.keys.get(key);
    if (r) return r;
    const source = (fresh: boolean) => async (): Promise<SourceAnswer<Value>> => {
      const parent = relayParent(index);
      // Reads of a parent relay are an isolate's: its sub-lease is this relay's lease.
      if (parent !== null) return relayStub(this.env, name, parent).read(name, key, parent);
      const now = Date.now();
      const asks = r!.reads.asks;
      const perSecond = Math.round(((asks - r!.reported.asks) * 1000) / Math.max(1, now - r!.reported.at));
      r!.reported = { at: now, asks };
      return sourceStub(this.env, name).relayRead(name, key, index, perSecond, fresh);
    };
    r = { reads: new RelayReads<Value>(source(false), source(true)), reported: { at: Date.now(), asks: 0 } };
    this.keys.set(key, r);
    return r;
  }
}

/**
 * This isolate's sub-leases, by object and key. A miss reads on its own: a read shared with
 * another request would hang if that request went away first (relays share their reads anyway).
 */
const cache = new Map<string, { value: Value; until: number; relays: number }>();
/** Which of a key's relays this isolate reads through. */
const DRAW = Math.random();

/**
 * A reader's read, in the isolate it runs in (a driver's, here; an app's Worker, in production):
 * from the sub-lease while it lasts, else through a relay once the key has them, else the source.
 */
export async function readHot(env: LeasedEnv, target: ReaderTarget, name: string, key: string): Promise<Value> {
  if (target === "reader-single") return (await env.SINGLE.get(env.SINGLE.idFromName(name)).get([key], null))[0] ?? null;
  const id = `${name}/${key}`;
  const held = cache.get(id);
  if (held && held.until > Date.now()) return held.value;
  const asked = Date.now();
  const relays = held?.relays ?? 0;
  const index = relayIndex(DRAW, relays);
  const a = await (relays > 0 ? relayStub(env, name, index).read(name, key, index) : sourceStub(env, name).read(name, key));
  const until = asked + a.leaseMs - READ_LEASE_MARGIN_MS;
  if (until > (cache.get(id)?.until ?? 0) || a.relays !== held?.relays) cache.set(id, { value: a.value, until, relays: a.relays });
  return a.value;
}

export async function writeHot(env: LeasedEnv, target: ReaderTarget, name: string, key: string, value: string): Promise<void> {
  if (target === "reader-single") await env.SINGLE.get(env.SINGLE.idFromName(name)).put([key], [value]);
  else await sourceStub(env, name).write(name, key, value);
}
