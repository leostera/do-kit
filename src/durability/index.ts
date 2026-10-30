// SPIKE: durability's helpers (https://github.com/avenceslau/durability) inside a sharded object.
//
// Three seams make them fit:
//   1. The alarm. A sharded object's alarm belongs to the base class (splits run on it), and
//      durability's scheduler wants to own the alarm too: it sets, moves later and deletes it on
//      every transaction. So the scheduler gets a storage whose alarm is virtual: a row in
//      `dokit_durability_alarm`, mapped onto `scheduleAlarm`, and run from `onAlarm`.
//   2. Staging. `update`/`transact` run one synchronous transaction behind the split fence;
//      `DurabilityOutbox.stage` is async (it opens its own transaction). So `stage` here writes the
//      outbox row synchronously, inside the fenced transaction, the way the outbox's own (private)
//      `#stageIn` does.
//   3. Splits. A split copies every table, durability's too. Outbox rows are keyed by `entry_key`,
//      which here is always the routing key, so ownership of a pending entry follows its key:
//      `afterClone` and `cleanupForeign` delete the entries of keys a shard no longer owns.
import { DurabilityScheduler, type DurabilityContext, type DurabilitySchedulerOptions } from "durability";
import { DurabilityOutbox, type DurabilityOutboxConfig } from "durability/experimental";

const ALARM_TABLE = "dokit_durability_alarm";

export interface ShardedDurabilityConfig extends DurabilitySchedulerOptions {
  readonly ctx: DurableObjectState;
  /** The sharded object's `scheduleAlarm`: `(at) => this.scheduleAlarm(at)`. */
  readonly scheduleAlarm: (at: number) => Promise<void>;
}

/** Outbox options, less the scheduler (the sharded object's) and the channel (the map key). */
export type ShardedOutboxConfig<Value> = Omit<DurabilityOutboxConfig<Value>, "scheduler" | "context" | "channel">;

/**
 * Durability's scheduler and outboxes for one sharded object. Construct once, in the constructor,
 * after `super`. Wire `onAlarm`, `afterClone` and `cleanupForeign` to the methods of the same names.
 */
export class ShardedDurability<Channels extends Record<string, unknown> = Record<string, unknown>> {
  readonly scheduler: DurabilityScheduler;
  private readonly ctx: DurableObjectState;
  private readonly sql: SqlStorage;
  private readonly scheduleAlarm: (at: number) => Promise<void>;
  private readonly outboxes = new Map<string, DurabilityOutbox<unknown>>();
  /** SPIKE instrumentation: rows durability's own statements wrote, and alarm traffic. */
  readonly stats = { statements: 0, rowsWritten: 0, stageRowsWritten: 0, virtualSets: 0, scheduleAlarmCalls: 0, schedulerRuns: 0 };

  constructor(config: ShardedDurabilityConfig) {
    const { ctx, scheduleAlarm, ...options } = config;
    this.ctx = ctx;
    this.sql = ctx.storage.sql;
    this.scheduleAlarm = scheduleAlarm;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS ${ALARM_TABLE} (id INTEGER PRIMARY KEY CHECK (id = 1), at INTEGER NOT NULL)`);
    this.scheduler = new DurabilityScheduler({ ...options, context: this.virtualContext() });
  }

  /** Adds an outbox on `channel`. Its `publish` runs on whichever shard owns the entry's key. */
  outbox<C extends keyof Channels & string>(channel: C, config: ShardedOutboxConfig<Channels[C]>): void {
    if (this.outboxes.has(channel)) throw new Error(`outbox ${channel} already exists`);
    this.outboxes.set(channel, new DurabilityOutbox<Channels[C]>({ ...config, channel, scheduler: this.scheduler }) as DurabilityOutbox<unknown>);
  }

  /**
   * Stages the newest value of `key` on `channel`: call it inside `update` or `transact`, with
   * `key` among their keys, so it commits with the write it describes and behind the split fence.
   * Returns the entry's version. Publication starts after the transaction (call `kick`) or on the
   * next alarm.
   *
   * Mirrors DurabilityOutbox#stageIn (durability 5.1.0): the upstream ask is a public `stageSync`.
   */
  stage<C extends keyof Channels & string>(channel: C, key: string, value: Channels[C]): number {
    if (!this.outboxes.has(channel)) throw new Error(`no outbox ${channel}`);
    if (typeof key !== "string" || key.length === 0) throw new TypeError("outbox keys must be non-empty strings");
    const payload = JSON.stringify(value === undefined ? { kind: "undefined" } : { kind: "value", value });
    const seq = this.sql.exec<{ version: number }>("UPDATE durability_outbox_seq SET next_version = next_version + 1 WHERE id = 1 RETURNING next_version - 1 AS version");
    const { version } = seq.one();
    const now = Date.now();
    // Replacing the generation fences an in-flight publication of an older version.
    const insert = this.sql.exec(
      `INSERT OR REPLACE INTO durability_outbox (id, channel, entry_key, version, payload, status, attempt, next_attempt_at, last_error, last_error_name, created_at, generation_id)
       VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, NULL, NULL, ?, ?)`,
      JSON.stringify([channel, key]),
      channel,
      key,
      version,
      payload,
      now,
      now,
      String(version),
    );
    this.stats.stageRowsWritten += seq.rowsWritten + insert.rowsWritten;
    return version;
  }

  /** Publishes what is due now, in the background: after a transaction that staged entries. */
  kick(): void {
    this.ctx.waitUntil(this.scheduler.alarm().catch((error) => console.error(JSON.stringify({ event: "durability-kick-failed", error: String(error) }))));
  }

  /** From the sharded object's `onAlarm`: runs the scheduler if its (virtual) alarm is due. */
  async onAlarm(): Promise<void> {
    const at = this.virtualAlarm();
    if (at === null) return;
    if (at > Date.now()) return this.scheduleAlarm(at); // woken for something else: keep our time asked for
    this.setVirtualAlarm(null);
    this.stats.schedulerRuns++;
    // The scheduler's alarm never throws while it can re-arm (virtually) itself.
    await this.scheduler.alarm();
  }

  /** From `afterClone`: the copy drops pending entries of keys that stayed with the original. */
  afterClone(owns: (key: string) => boolean): void {
    this.dropForeign(owns, Number.MAX_SAFE_INTEGER);
  }

  /** From `cleanupForeign`: each side drops pending entries of keys it gave away. */
  cleanupForeign(owns: (key: string) => boolean, limit: number): number {
    return this.dropForeign(owns, limit);
  }

  private dropForeign(owns: (key: string) => boolean, limit: number): number {
    if (!this.hasOutboxTable()) return 0;
    let deleted = 0;
    // A full scan: entries are few (one per key with an unpublished version).
    for (const { id, entry_key } of this.sql.exec<{ id: string; entry_key: string }>("SELECT id, entry_key FROM durability_outbox").toArray()) {
      if (deleted >= limit) break;
      if (owns(entry_key)) continue;
      this.sql.exec("DELETE FROM durability_outbox WHERE id = ?", id);
      deleted++;
    }
    return deleted;
  }

  private hasOutboxTable(): boolean {
    return this.sql.exec("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'durability_outbox'").toArray().length > 0;
  }

  /** Pending outbox entries here (for tests and operators). */
  pending(): { channel: string; key: string; version: number; attempt: number }[] {
    if (!this.hasOutboxTable()) return [];
    return this.sql.exec<{ channel: string; entry_key: string; version: number; attempt: number }>("SELECT channel, entry_key, version, attempt FROM durability_outbox ORDER BY version").toArray().map((r) => ({ channel: r.channel, key: r.entry_key, version: r.version, attempt: r.attempt }));
  }

  // ---- The virtual alarm ----

  private virtualAlarm(): number | null {
    return this.sql.exec<{ at: number }>(`SELECT at FROM ${ALARM_TABLE} WHERE id = 1`).toArray()[0]?.at ?? null;
  }

  private setVirtualAlarm(at: number | null): void {
    if (at === null) this.sql.exec(`DELETE FROM ${ALARM_TABLE}`);
    else this.sql.exec(`INSERT INTO ${ALARM_TABLE} (id, at) VALUES (1, ?) ON CONFLICT (id) DO UPDATE SET at = excluded.at`, at);
  }

  /**
   * The storage durability sees: the real one, but its alarm is the row above. Setting it also
   * asks the sharded object for an alarm at that time (earlier wins there), so `onAlarm` comes.
   * Moving it later or deleting it touches only the row: an early wake-up finds nothing due.
   */
  private virtualContext(): DurabilityContext {
    const storage = this.ctx.storage;
    const alarm = {
      getAlarm: async () => this.virtualAlarm(),
      setAlarm: async (at: number | Date) => {
        const time = typeof at === "number" ? at : at.getTime();
        this.setVirtualAlarm(time);
        this.stats.virtualSets++;
        this.stats.scheduleAlarmCalls++;
        await this.scheduleAlarm(time);
      },
      deleteAlarm: async () => this.setVirtualAlarm(null),
    };
    const withAlarm = <T extends object>(target: T): T =>
      new Proxy(target, {
        get: (t, prop) => {
          if (prop in alarm) return alarm[prop as keyof typeof alarm];
          const value = Reflect.get(t, prop, t);
          return typeof value === "function" ? value.bind(t) : value;
        },
      });
    const stats = this.stats;
    const sql = new Proxy(storage.sql, {
      get: (t, prop) => {
        if (prop === "exec") {
          return (query: string, ...bindings: unknown[]) => {
            const cursor = t.exec(query, ...bindings);
            stats.statements++;
            // rowsWritten is final once the cursor is drained; durability drains with toArray().
            const toArray = cursor.toArray.bind(cursor);
            cursor.toArray = () => {
              const rows = toArray();
              stats.rowsWritten += cursor.rowsWritten;
              return rows;
            };
            return cursor;
          };
        }
        const value = Reflect.get(t, prop, t);
        return typeof value === "function" ? value.bind(t) : value;
      },
    });
    const virtual = new Proxy(storage, {
      get: (t, prop) => {
        if (prop === "sql") return sql;
        if (prop in alarm) return alarm[prop as keyof typeof alarm];
        if (prop === "transaction") return <R>(fn: (txn: DurableObjectTransaction) => Promise<R>) => t.transaction((txn) => fn(withAlarm(txn)));
        const value = Reflect.get(t, prop, t);
        return typeof value === "function" ? value.bind(t) : value;
      },
    });
    return { storage: virtual, waitUntil: (p: Promise<unknown>) => this.ctx.waitUntil(p) };
  }
}
