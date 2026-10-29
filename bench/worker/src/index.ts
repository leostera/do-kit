import { DurableObject } from "cloudflare:workers";
import { nodeName, sharded, ShardedObject, shardObjectName, type SplitPolicy, type UpdateResult } from "../../../src/sharded/index.ts";
import { defineTable, ShardedTable, table, type TableClient } from "../../../src/table/index.ts";
import { splittable as twoCopySplittable, SplittableObject as TwoCopySplittableObject } from "./two-copy/index.ts";
import { splittable as legacySplittable, SplittableObject as LegacySplittableObject } from "./legacy/index.ts";

const LOCATIONS = ["wnam", "enam", "weur", "eeur", "apac"] as const;
/** "split": snapshot splits (this package); "legacy": the previous row-by-row handoff. */
type Target = "single" | "split" | "legacy" | "presplit" | "presplit-4096" | TableTarget;
const TARGETS: readonly Target[] = ["single", "split", "legacy", "presplit", "presplit-4096", "table-1", "table-4", "table-16", "table-64"];
/** A ShardedTable that starts as this many shards and never splits by load: "table-1" is one object. */
type TableTarget = "table-1" | "table-4" | "table-16" | "table-64";
const isTable = (t: Target): t is TableTarget => t.startsWith("table-");
/** Drivers per fleet: one fleet object starts and gathers this many drivers, as one Worker did before. */
const DRIVERS_PER_FLEET = 256;
/** The most drivers one measurement starts (32 fleets). */
const MAX_DRIVERS = 8192;
/** Calls each driver keeps in flight: one driver completes only about 90 calls/s, however many it keeps. */
const CALLS_PER_DRIVER = 16;
/** "list": one page of 50 rows, newest first, each worker walking its own cursor (tables only). */
type Operation = "read" | "write" | "list";
/**
 * How keys are chosen: "uniform" over `keyCount` keys, one "hot" key, or "control-plane": a handful
 * (`keyCount`) of popular objects, mostly read. Key shape does not matter beyond that: keys are
 * routed by their SHA-256.
 */
type Distribution = "uniform" | "hot" | "control-plane";
const DISTRIBUTIONS: readonly Distribution[] = ["uniform", "hot", "control-plane"];

/** Control plane: `keyCount` objects (a handful), popularity Zipf(s), and this share of calls are writes. */
const CONTROL_PLANE = { s: 1.1, writeShare: 0.05 };
const zipfCdfs = new Map<number, number[]>();
function zipfObject(objects: number): string {
  let cdf = zipfCdfs.get(objects);
  if (!cdf) {
    const weights = Array.from({ length: objects }, (_, i) => 1 / (i + 1) ** CONTROL_PLANE.s);
    const total = weights.reduce((a, b) => a + b, 0);
    let sum = 0;
    zipfCdfs.set(objects, (cdf = weights.map((w) => (sum += w / total))));
  }
  const r = Math.random();
  let lo = 0;
  let hi = cdf.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (cdf[mid]! < r) lo = mid + 1;
    else hi = mid;
  }
  return `config-${lo}`;
}

interface Env {
  readonly BENCH_DRIVERS: DurableObjectNamespace<BenchDriver>;
  readonly SINGLE: DurableObjectNamespace<SingleObject>;
  readonly SPLIT: DurableObjectNamespace<SplitObject>;
  readonly LEGACY: DurableObjectNamespace<LegacySplitObject>;
  readonly SPLIT_MANUAL: DurableObjectNamespace<ManualSplitObject>;
  readonly LEGACY_MANUAL: DurableObjectNamespace<ManualLegacySplitObject>;
  readonly TWO_COPY_MANUAL: DurableObjectNamespace<ManualTwoCopySplitObject>;
  readonly BENCH_TOKEN: string;
  readonly SPLIT_JOBS: DurableObjectNamespace<SplitBenchJob>;
  readonly PRESPLIT: DurableObjectNamespace<PresplitObject>;
  readonly PRESPLIT_4096: DurableObjectNamespace<WidePresplitObject>;
  readonly BENCH_FLEETS: DurableObjectNamespace<BenchFleet>;
  readonly TABLE_1: DurableObjectNamespace<Table1>;
  readonly TABLE_4: DurableObjectNamespace<Table4>;
  readonly TABLE_16: DurableObjectNamespace<Table16>;
  readonly TABLE_64: DurableObjectNamespace<Table64>;
}

// ---- Sharded tables: the same rows, indexes and queries on 1, 4, 16 and 64 shards ----

type BenchRow = { name: string; createdAt: number; owner: string; size: number; payload: string };
const benchRows = defineTable<BenchRow>({
  primaryKey: "name",
  columns: { name: "text", createdAt: "integer", owner: "text", size: "integer", payload: "text" },
  indexes: { byCreated: ["createdAt"], byOwner: ["owner", "createdAt"] },
});
function benchRow(key: string, payloadBytes: number): BenchRow {
  return { name: key, createdAt: Date.now(), owner: `owner-${key.length % 16}`, size: payloadBytes, payload: "x".repeat(payloadBytes) };
}

abstract class TableBench extends ShardedTable<Env, BenchRow> {
  protected table = benchRows;
}
const tablePolicy = (depth: number): SplitPolicy => ({ callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12, initialDepth: depth, copy: "native" });
export class Table1 extends TableBench {
  protected override splitPolicy = tablePolicy(0);
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.TABLE_1 as never;
  }
}
export class Table4 extends TableBench {
  protected override splitPolicy = tablePolicy(2);
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.TABLE_4 as never;
  }
}
export class Table16 extends TableBench {
  protected override splitPolicy = tablePolicy(4);
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.TABLE_16 as never;
  }
}
export class Table64 extends TableBench {
  protected override splitPolicy = tablePolicy(6);
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, never>> {
    return this.env.TABLE_64 as never;
  }
}
function tableClient(env: Env, target: TableTarget, name: string): TableClient<BenchRow> {
  const ns = { "table-1": env.TABLE_1, "table-4": env.TABLE_4, "table-16": env.TABLE_16, "table-64": env.TABLE_64 }[target];
  return table(ns as never, name, benchRows);
}

interface MeasureConfig {
  readonly target: Target;
  readonly operation: Operation;
  readonly distribution: Distribution;
  readonly objectName: string;
  readonly keyCount: number;
  readonly payloadBytes: number;
  readonly driverIndex: number;
  readonly driverCount: number;
  readonly concurrency: number;
  readonly durationMs: number;
  readonly startAt: number;
}

interface DriverResult {
  readonly successes: number;
  readonly errors: number;
  readonly bytes: number;
  readonly elapsedMs: number;
  /** One-millisecond buckets, with the last bucket holding 120 seconds and above. */
  /** Non-empty buckets only, as [bucket, count]: a dense histogram per driver is too big to gather from many. */
  readonly latencyMs: readonly (readonly number[])[];
  readonly maxLatencyMs: number;
  /** Up to five distinct messages of the calls that failed. */
  readonly errorSamples?: readonly string[];
}

abstract class KeyValueBase<EnvT> extends DurableObject<EnvT> {
  protected readonly sql: SqlStorage;

  constructor(ctx: DurableObjectState, env: EnvT) {
    super(ctx, env);
    this.sql = ctx.storage.sql;
    this.sql.exec("CREATE TABLE IF NOT EXISTS bench_values (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID");
  }

  async put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.ctx.storage.transactionSync(() => {
      for (let i = 0; i < keys.length; i++) {
        this.sql.exec("INSERT INTO bench_values (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value", keys[i]!, values[i]!);
      }
      return keys.length;
    });
  }

  async get(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    return keys.map((key) => this.sql.exec<{ value: string }>("SELECT value FROM bench_values WHERE key = ?", key).toArray()[0]?.value ?? null);
  }
}

/** Ordinary single-instance Durable Object baseline. */
export class SingleObject extends KeyValueBase<Env> {}

type Seeded = { readonly rows: number };

/** Writes `count` rows of `bytes` bytes, keys `row-${start}`…, in transactions of 500 rows. */
async function seedRows(update: (keys: readonly string[], values: readonly string[]) => Promise<number>, start: number, count: number, bytes: number): Promise<Seeded> {
  const value = "x".repeat(bytes);
  for (let i = start; i < start + count; i += 500) {
    const keys = Array.from({ length: Math.min(500, start + count - i) }, (_, j) => `row-${i + j}`);
    await update(keys, keys.map(() => value));
  }
  return { rows: count };
}

/** The identical key/value interface, distributed over hash-prefix leaves split with snapshots. */
export class SplitObject extends ShardedObject<Env, string> {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25, copy: "native" };
  protected nodes(): DurableObjectNamespace<ShardedObject<Env, string>> {
    return this.env.SPLIT as DurableObjectNamespace<ShardedObject<Env, string>>;
  }

  async put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.update(keys, (): UpdateResult<string, number> => ({ writes: new Map(keys.map((key, i) => [key, values[i]!])), result: keys.length }));
  }

  async get(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    const current = await this.read(keys);
    return keys.map((key) => current.get(key) ?? null);
  }

  async seed(_keys: readonly string[], _payloads: null, start: number, count: number, bytes: number): Promise<Seeded> {
    return seedRows((keys, values) => this.put(keys, values), start, count, bytes);
  }
}

/** Starts as 16 leaves, before its first call: steady-state capacity, with no split while measuring. */
export class PresplitObject extends SplitObject {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12, initialDepth: 4, copy: "native" };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, string>> {
    return this.env.PRESPLIT as never;
  }
}

/** Starts as 4,096 shards (depth 12) before its first call: the widest object the default depth allows. */
export class WidePresplitObject extends SplitObject {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12, initialDepth: 12, copy: "native" };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, string>> {
    return this.env.PRESPLIT_4096 as never;
  }
}

/** The namespace of a sharded target. */
function shardedNamespace(env: Env, target: Target): DurableObjectNamespace<SplitObject> {
  if (target === "presplit") return env.PRESPLIT;
  if (target === "presplit-4096") return env.PRESPLIT_4096;
  return env.SPLIT;
}

/** Splits only when told to: for timing one split. */
export class ManualSplitObject extends SplitObject {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12, copy: "native", splitTimeoutMs: 300_000 };
  protected override nodes(): DurableObjectNamespace<ShardedObject<Env, string>> {
    return this.env.SPLIT_MANUAL as never;
  }
}

/** The same interface on the previous implementation (children copy rows from the parent in batches). */
export class LegacySplitObject extends LegacySplittableObject<Env, string> {
  protected nodes(): DurableObjectNamespace<LegacySplittableObject<Env, string>> {
    return this.env.LEGACY as never;
  }

  async put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.update(keys, () => ({ writes: new Map(keys.map((key, i) => [key, values[i]!])), result: keys.length }));
  }

  async get(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    const current = await this.read(keys);
    return keys.map((key) => current.get(key) ?? null);
  }

  async seed(_keys: readonly string[], _payloads: null, start: number, count: number, bytes: number): Promise<Seeded> {
    return seedRows((keys, values) => this.put(keys, values), start, count, bytes);
  }
}

export class ManualLegacySplitObject extends LegacySplitObject {
  protected override splitPolicy = { callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12 };
  protected override nodes(): DurableObjectNamespace<LegacySplittableObject<Env, string>> {
    return this.env.LEGACY_MANUAL as never;
  }
}

/** The alternative snapshot design: both halves restore into new children, the parent routes. */
export class ManualTwoCopySplitObject extends TwoCopySplittableObject<Env, string> {
  protected override splitPolicy = { callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12 };
  protected nodes(): DurableObjectNamespace<TwoCopySplittableObject<Env, string>> {
    return this.env.TWO_COPY_MANUAL as never;
  }

  async put(keys: readonly string[], values: readonly string[]): Promise<number> {
    return this.update(keys, () => ({ writes: new Map(keys.map((key, i) => [key, values[i]!])), result: keys.length }));
  }

  async seed(_keys: readonly string[], _payloads: null, start: number, count: number, bytes: number): Promise<Seeded> {
    return seedRows((keys, values) => this.put(keys, values), start, count, bytes);
  }
}

/**
 * Runs one split benchmark in the background and answers polls about it: a split of a large
 * object takes minutes, longer than one request should be kept open.
 */
export class SplitBenchJob extends DurableObject<Env> {
  private progress: unknown = null;
  private outcome: { result?: unknown; error?: string } | null = null;
  private running = false;

  async start(input: SplitBenchInput): Promise<void> {
    if (this.running || this.outcome) return;
    this.running = true;
    void splitBench(this.env, input, async (line) => void (this.progress = line))
      .then((result) => (this.outcome = { result }))
      .catch((error: unknown) => (this.outcome = { error: error instanceof Error ? error.message : String(error) }))
      .finally(() => (this.running = false));
  }

  async poll(): Promise<{ progress: unknown; result?: unknown; error?: string; running: boolean }> {
    return { progress: this.progress, running: this.running, ...(this.outcome ?? {}) };
  }
}

/** The key of call `seq` of this driver. */
function keyFor(config: MeasureConfig, seq: number): string {
  switch (config.distribution) {
    case "hot":
      return "hot-key";
    case "uniform":
      return `key-${seq % config.keyCount}`;
    case "control-plane":
      return zipfObject(config.keyCount);
  }
}

/** Driver results merged: a fleet's drivers, or every fleet's. */
interface MergedResult {
  readonly successes: number;
  readonly errors: number;
  readonly bytes: number;
  readonly elapsedMs: number;
  readonly latencyMs: readonly (readonly number[])[];
  readonly maxLatencyMs: number;
  readonly failedDrivers: number;
  readonly errorSamples: readonly string[];
}

function merge(results: readonly (DriverResult & { readonly failedDrivers?: number })[], failedDrivers: number, extraSamples: readonly string[] = []): MergedResult {
  const buckets = new Map<number, number>();
  const samples = new Set<string>(extraSamples);
  let successes = 0;
  let errors = 0;
  let bytes = 0;
  let elapsedMs = 0;
  let maxLatencyMs = 0;
  for (const result of results) {
    successes += result.successes;
    errors += result.errors;
    bytes += result.bytes;
    elapsedMs = Math.max(elapsedMs, result.elapsedMs);
    maxLatencyMs = Math.max(maxLatencyMs, result.maxLatencyMs);
    failedDrivers += result.failedDrivers ?? 0;
    for (const sample of result.errorSamples ?? []) if (samples.size < 10) samples.add(sample);
    for (const [bucket, count] of result.latencyMs) buckets.set(bucket!, (buckets.get(bucket!) ?? 0) + count!);
  }
  return { successes, errors, bytes, elapsedMs, maxLatencyMs, failedDrivers, latencyMs: [...buckets], errorSamples: [...samples] };
}

function driverStub(env: Env, config: { readonly objectName: string; readonly driverIndex: number }): DurableObjectStub<BenchDriver> {
  const id = env.BENCH_DRIVERS.idFromName(`${config.objectName}-driver-${config.driverIndex}`);
  return env.BENCH_DRIVERS.get(id, { locationHint: LOCATIONS[config.driverIndex % LOCATIONS.length] });
}

/**
 * Starts and gathers up to DRIVERS_PER_FLEET drivers, so that one measurement can start thousands of
 * them: the Worker starts the fleets, each fleet starts its drivers.
 */
export class BenchFleet extends DurableObject<Env> {
  async prepare(configs: readonly Omit<MeasureConfig, "startAt">[]): Promise<void> {
    await Promise.all(configs.map((config) => driverStub(this.env, config).prepare(config)));
  }

  async measure(configs: readonly MeasureConfig[]): Promise<MergedResult> {
    // A driver that fails outright (rather than counting its failed calls) is reported, not fatal.
    const settled = await Promise.allSettled(configs.map((config) => driverStub(this.env, config).measure(config)));
    const results = settled.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
    const reasons = settled.flatMap((r) => (r.status === "rejected" ? [`driver: ${String(r.reason).slice(0, 200)}`] : [])).slice(0, 3);
    return merge(results, settled.length - results.length, reasons);
  }
}

/** Distributed load generator. locationHint is set only when each fresh driver is first created. */
export class BenchDriver extends DurableObject<Env> {
  async prepare(config: Omit<MeasureConfig, "startAt">): Promise<void> {
    const value = "x".repeat(config.payloadBytes);
    if (config.distribution === "control-plane") {
      if (config.driverIndex !== 0) return;
      const keys = Array.from({ length: config.keyCount }, (_, i) => `config-${i}`);
      await this.write(config.target, config.objectName, keys, keys.map(() => value));
      return;
    }
    if (isTable(config.target)) {
      if (config.operation === "write" || config.driverIndex !== 0) return;
      const t = tableClient(this.env, config.target, config.objectName);
      for (let i = 0; i < config.keyCount; i += 100) {
        const rows = Array.from({ length: Math.min(100, config.keyCount - i) }, (_, j) => ({ row: benchRow(`key-${i + j}`, config.payloadBytes) }));
        for (let attempt = 0; ; attempt++) {
          try {
            await t.putMany(rows);
            break;
          } catch (error) {
            if (attempt >= 5) throw error;
            await new Promise((resolve) => setTimeout(resolve, 200 * 2 ** attempt));
          }
        }
      }
      return;
    }
    if (config.operation !== "read" || config.distribution !== "uniform" && config.distribution !== "hot") return;
    const keys: string[] = [];
    const values: string[] = [];
    if (config.distribution === "hot") {
      if (config.driverIndex === 0) await this.write(config.target, config.objectName, ["hot-key"], [value]);
      return;
    }
    // Thousands of shards take a seed from every driver at once: each seeds its own stripe of keys.
    if (config.target === "presplit-4096") {
      for (let i = config.driverIndex; i < config.keyCount; i += config.driverCount) {
        keys.push(`key-${i}`);
        values.push(value);
      }
      for (let i = 0; i < keys.length; i += 100) {
        // Retried: under heavy load a call can fail with a transient internal error.
        for (let attempt = 0; ; attempt++) {
          try {
            await this.write(config.target, config.objectName, keys.slice(i, i + 100), values.slice(i, i + 100));
            break;
          } catch (error) {
            if (attempt >= 5) throw error;
            await new Promise((resolve) => setTimeout(resolve, 200 * 2 ** attempt));
          }
        }
      }
      return;
    }
    // One driver seeds every key, in batches, retrying: a hundred drivers seeding at once would
    // overload the very object about to be measured.
    if (config.driverIndex !== 0) return;
    const flush = async () => {
      for (let attempt = 0; ; attempt++) {
        try {
          await this.write(config.target, config.objectName, keys, values);
          break;
        } catch (error) {
          if (attempt >= 5) throw error;
          await new Promise((resolve) => setTimeout(resolve, 200 * 2 ** attempt));
        }
      }
      keys.length = 0;
      values.length = 0;
    };
    for (let i = 0; i < config.keyCount; i++) {
      keys.push(`key-${i}`);
      values.push(value);
      if (keys.length === 100) await flush();
    }
    if (keys.length) await flush();
  }

  async measure(config: MeasureConfig): Promise<DriverResult> {
    const waitMs = config.startAt - Date.now();
    if (waitMs > 0) await new Promise((resolve) => setTimeout(resolve, waitMs));
    const startedAt = Date.now();
    const stopAt = startedAt + config.durationMs;
    const latencyMs = new Array<number>(120_001).fill(0);
    let successes = 0;
    let errors = 0;
    let bytes = 0;
    let maxLatencyMs = 0;
    let sequence = config.driverIndex;
    const errorSamples = new Set<string>();

    const tableWorker = async (): Promise<void> => {
      const t = tableClient(this.env, config.target as TableTarget, config.objectName);
      let cursor: string | null = null;
      while (Date.now() < stopAt) {
        const key = keyFor(config, sequence++);
        const before = performance.now();
        try {
          if (config.operation === "write") {
            await t.put(benchRow(key, config.payloadBytes));
            bytes += config.payloadBytes;
          } else if (config.operation === "read") {
            const found = await t.get(key);
            bytes += found ? config.payloadBytes : 0;
          } else {
            const page: Awaited<ReturnType<typeof t.list>> = await t.list({ index: "byCreated", direction: "desc", limit: 50, cursor });
            cursor = page.cursor;
            bytes += page.rows.length * config.payloadBytes;
          }
          successes++;
        } catch (error) {
          errors++;
          if (errorSamples.size < 5) errorSamples.add(String(error instanceof Error ? error.message : error).slice(0, 200));
        } finally {
          const elapsed = performance.now() - before;
          maxLatencyMs = Math.max(maxLatencyMs, elapsed);
          latencyMs[Math.min(120_000, Math.floor(elapsed))]++;
        }
      }
    };

    const worker = async (): Promise<void> => {
      if (isTable(config.target)) return tableWorker();
      while (Date.now() < stopAt) {
        const seq = sequence++;
        const key = keyFor(config, seq);
        const write = config.distribution === "control-plane" ? Math.random() < CONTROL_PLANE.writeShare : config.operation === "write";
        const before = performance.now();
        try {
          if (write) {
            await this.write(config.target, config.objectName, [key], ["x".repeat(config.payloadBytes)]);
            bytes += config.payloadBytes;
          } else {
            const value = await this.read(config.target, config.objectName, key);
            bytes += value?.length ?? 0;
          }
          successes++;
        } catch (error) {
          errors++;
          if (errorSamples.size < 5) errorSamples.add(String(error instanceof Error ? error.message : error).slice(0, 200));
        } finally {
          const elapsed = performance.now() - before;
          maxLatencyMs = Math.max(maxLatencyMs, elapsed);
          const bucket = Math.min(120_000, Math.floor(elapsed));
          latencyMs[bucket]++;
        }
      }
    };

    await Promise.all(Array.from({ length: config.concurrency }, worker));
    const sparse: [number, number][] = [];
    latencyMs.forEach((count, bucket) => count && sparse.push([bucket, count]));
    return { successes, errors, bytes, elapsedMs: Date.now() - startedAt, latencyMs: sparse, maxLatencyMs, errorSamples: [...errorSamples] };
  }

  private async write(target: Target, name: string, keys: readonly string[], values: readonly string[]): Promise<void> {
    if (target === "single") {
      const stub = this.env.SINGLE.get(this.env.SINGLE.idFromName(name));
      await stub.put(keys, values);
      return;
    }
    if (target === "legacy") await legacySplittable(this.env.LEGACY, name).call(keys, "put", values);
    else await sharded(shardedNamespace(this.env, target), name).call(keys, "put", values);
  }

  private async read(target: Target, name: string, key: string): Promise<string | null> {
    if (target === "single") {
      const stub = this.env.SINGLE.get(this.env.SINGLE.idFromName(name));
      return (await stub.get([key], null))[0] ?? null;
    }
    const [leaf] = target === "legacy" ? await legacySplittable(this.env.LEGACY, name).call([key], "get", null) : await sharded(shardedNamespace(this.env, target), name).call([key], "get", null);
    return leaf?.result[0] ?? null;
  }
}

function validate(input: unknown): Omit<MeasureConfig, "driverIndex" | "driverCount" | "startAt"> {
  if (!input || typeof input !== "object") throw new Error("body must be an object");
  const body = input as Record<string, unknown>;
  const target = body.target;
  const operation = body.operation;
  const distribution = body.distribution ?? "uniform";
  const concurrency = body.concurrency;
  const durationMs = body.durationMs;
  const keyCount = body.keyCount ?? 10_000;
  const payloadBytes = body.payloadBytes ?? 1024;
  if (!TARGETS.includes(target as Target)) throw new Error(`target must be one of ${TARGETS.join(", ")}`);
  if (operation !== "read" && operation !== "write" && operation !== "list") throw new Error("operation must be read, write or list");
  if (operation === "list" && !isTable(target as Target)) throw new Error("list is for table targets");
  if (!DISTRIBUTIONS.includes(distribution as Distribution)) throw new Error(`distribution must be one of ${DISTRIBUTIONS.join(", ")}`);
  if (!Number.isSafeInteger(concurrency) || (concurrency as number) < 1 || (concurrency as number) > MAX_DRIVERS * CALLS_PER_DRIVER) throw new Error(`concurrency must be an integer from 1 to ${MAX_DRIVERS * CALLS_PER_DRIVER}`);
  if (!Number.isSafeInteger(durationMs) || (durationMs as number) < 5_000 || (durationMs as number) > 120_000) throw new Error("durationMs must be from 5000 to 120000");
  if (!Number.isSafeInteger(keyCount) || (keyCount as number) < 1 || (keyCount as number) > 100_000) throw new Error("keyCount must be from 1 to 100000");
  if (!Number.isSafeInteger(payloadBytes) || (payloadBytes as number) < 16 || (payloadBytes as number) > 64 * 1024) throw new Error("payloadBytes must be from 16 to 65536");
  const objectName = body.objectName;
  if (typeof objectName !== "string" || !/^[a-zA-Z0-9_-]{1,80}$/.test(objectName)) throw new Error("objectName must be 1-80 safe characters");
  return { target: target as Target, operation, distribution: distribution as Distribution, concurrency: concurrency as number, durationMs: durationMs as number, keyCount: keyCount as number, payloadBytes: payloadBytes as number, objectName };
}

function percentile(histogram: readonly number[], p: number): number {
  const total = histogram.reduce((sum, n) => sum + n, 0);
  if (total === 0) return 0;
  const target = Math.ceil(total * p);
  let seen = 0;
  for (let i = 0; i < histogram.length; i++) {
    seen += histogram[i]!;
    if (seen >= target) return i === 120_000 ? 120_000 : i + 1;
  }
  return 120_000;
}

async function describeTree(env: Env, name: string, bits = "", ns: DurableObjectNamespace<ShardedObject<Env, string>> = env.SPLIT as never): Promise<{ leaves: number; rows: number; depth: number }> {
  const namespace = ns;
  const description = await namespace.get(namespace.idFromName(shardObjectName(name, bits))).shardedDescribe();
  // The object hosting `bits` hosts a deeper leaf once `bits` has split.
  if (description.state === "new" || description.bits === bits) return { leaves: 1, rows: description.rows, depth: bits.length };
  const [zero, one] = await Promise.all([describeTree(env, name, `${bits}0`, ns), describeTree(env, name, `${bits}1`, ns)]);
  return { leaves: zero.leaves + one.leaves, rows: zero.rows + one.rows, depth: Math.max(zero.depth, one.depth) };
}

async function describeLegacyTree(env: Env, name: string, bits = ""): Promise<{ leaves: number; rows: number; depth: number }> {
  const namespace = env.LEGACY as DurableObjectNamespace<LegacySplittableObject<Env, string>>;
  const description = await namespace.get(namespace.idFromName(nodeName(name, bits))).splittableDescribe();
  if (description.state !== "split") return { leaves: 1, rows: description.rows, depth: bits.length };
  const [zero, one] = await Promise.all([describeLegacyTree(env, name, `${bits}0`), describeLegacyTree(env, name, `${bits}1`)]);
  return { leaves: zero.leaves + one.leaves, rows: description.rows + zero.rows + one.rows, depth: Math.max(zero.depth, one.depth) };
}

interface SplitBenchInput {
  readonly target: "split" | "legacy" | "two-copy";
  readonly objectName: string;
  readonly rows: number;
  readonly payloadBytes: number;
  readonly writers: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/** Seeds `input.rows` rows, then splits the root once while `writers` write random seeded keys. */
async function splitBench(env: Env, input: SplitBenchInput, progress: (line: unknown) => Promise<void>): Promise<unknown> {
  const name = input.objectName;
  const split = env.SPLIT_MANUAL as unknown as DurableObjectNamespace<ShardedObject<Env, string>>;
  const legacy = env.LEGACY_MANUAL as unknown as DurableObjectNamespace<LegacySplittableObject<Env, string>>;
  const rootSplit = split.get(split.idFromName(shardObjectName(name, "")));
  const rootLegacy = legacy.get(legacy.idFromName(nodeName(name, "")));
  const twoCopy = env.TWO_COPY_MANUAL as unknown as DurableObjectNamespace<TwoCopySplittableObject<Env, string>>;
  const rootTwoCopy = twoCopy.get(twoCopy.idFromName(nodeName(name, "")));
  const seedChunk = Math.max(1, Math.floor((16 * 1024 * 1024) / input.payloadBytes));
  const seedStarted = Date.now();
  for (let start = 0; start < input.rows; start += seedChunk) {
    const count = Math.min(seedChunk, input.rows - start);
    const args = [start, count, input.payloadBytes];
    const answer = input.target === "split" ? await rootSplit.shardedCall(name, "", "seed", [], null, args) : input.target === "two-copy" ? await rootTwoCopy.splittableCall(name, "", "seed", [], null, args) : await rootLegacy.splittableCall(name, "", "seed", [], null, args);
    if (!("ok" in answer)) throw new Error(`seeding: ${JSON.stringify(answer)}`);
    await progress({ seeded: start + count });
  }
  const seedMs = Date.now() - seedStarted;

  // Writers on random seeded keys (both halves) through the client, before, during and after.
  const histogram = new Array<number>(120_001).fill(0);
  let writes = 0;
  let errors = 0;
  let maxMs = 0;
  let stop = false;
  const value = "y".repeat(input.payloadBytes);
  const write = (key: string) =>
    input.target === "split" ? sharded(env.SPLIT_MANUAL, name).call([key], "put", [value]) : input.target === "two-copy" ? twoCopySplittable(env.TWO_COPY_MANUAL, name).call([key], "put", [value]) : legacySplittable(env.LEGACY_MANUAL, name).call([key], "put", [value]);
  const writer = async (w: number) => {
    for (let i = w; !stop; i += input.writers) {
      const key = `row-${Math.floor(Math.random() * input.rows)}`;
      const before = performance.now();
      try {
        await write(key);
        writes++;
      } catch {
        errors++;
      }
      const ms = performance.now() - before;
      maxMs = Math.max(maxMs, ms);
      histogram[Math.min(120_000, Math.floor(ms))]++;
    }
  };
  const writing = Promise.all(Array.from({ length: input.writers }, (_, w) => writer(w)));
  await sleep(1000);

  const splitStarted = Date.now();
  let servingMs = 0;
  let copyMs = 0;
  let doneMs = 0;
  let copy: string | null = null;
  if (input.target === "split") {
    if (!(await rootSplit.shardedSplit(name, ""))) throw new Error(`split abandoned: ${(await rootSplit.shardedDescribe()).lastSplitError}`);
    copyMs = Date.now() - splitStarted;
    const right = split.get(split.idFromName(shardObjectName(name, "1")));
    for (;;) {
      const [l, r] = await Promise.all([rootSplit.shardedDescribe(), right.shardedDescribe()]);
      if (r.state === "shard" && !servingMs) servingMs = Date.now() - splitStarted;
      copy = l.lastSplit?.copy ?? null;
      if (servingMs && l.storedRows === l.rows && r.storedRows === r.rows) break;
      await sleep(100);
    }
  } else if (input.target === "two-copy") {
    // Returns once both children started on their copy and dropped what they do not own.
    if (!(await rootTwoCopy.splittableSplit(name, ""))) throw new Error("split refused or failed");
    servingMs = copyMs = Date.now() - splitStarted;
    copy = "native";
  } else {
    if (!(await rootLegacy.splittableSplit(name, ""))) throw new Error("split refused");
    servingMs = copyMs = Date.now() - splitStarted; // children serve at once, pulling rows on demand
    const zero = legacy.get(legacy.idFromName(nodeName(name, "0")));
    const one = legacy.get(legacy.idFromName(nodeName(name, "1")));
    for (;;) {
      const [p, a, b] = await Promise.all([rootLegacy.splittableDescribe(), zero.splittableDescribe(), one.splittableDescribe()]);
      if (a.inherited && b.inherited && p.rows === 0) break;
      await sleep(100);
    }
    copy = "rows";
  }
  doneMs = Date.now() - splitStarted;
  await progress({ split: doneMs });
  await sleep(1000);
  stop = true;
  await writing;
  return {
    ...input,
    bytes: input.rows * input.payloadBytes,
    seedMs,
    copy,
    /** Until the copy was claimed (snapshot) or the parent froze (rows). */
    copyMs,
    /** Until the right half served. */
    servingMs,
    /** Until every row lived only at its owner (copies done, given-away rows deleted). */
    doneMs,
    writes,
    errors,
    writeLatencyMs: { p50: percentile(histogram, 0.5), p95: percentile(histogram, 0.95), p99: percentile(histogram, 0.99), max: Math.round(maxMs) },
  };
}

function validateSplitBench(input: unknown): SplitBenchInput {
  const body = (input ?? {}) as Record<string, unknown>;
  const { target, objectName, rows, payloadBytes = 1024, writers = 8 } = body;
  if (target !== "split" && target !== "legacy" && target !== "two-copy") throw new Error("target must be split, legacy or twoCopy");
  if (typeof objectName !== "string" || !/^[a-zA-Z0-9_-]{1,80}$/.test(objectName)) throw new Error("objectName must be 1-80 safe characters");
  if (!Number.isSafeInteger(rows) || (rows as number) < 1 || (rows as number) > 20_000_000) throw new Error("rows must be from 1 to 20000000");
  if (!Number.isSafeInteger(payloadBytes) || (payloadBytes as number) < 16 || (payloadBytes as number) > 1024 * 1024) throw new Error("payloadBytes must be from 16 to 1 MiB");
  if (!Number.isSafeInteger(writers) || (writers as number) < 0 || (writers as number) > 64) throw new Error("writers must be from 0 to 64");
  return { target, objectName, rows: rows as number, payloadBytes: payloadBytes as number, writers: writers as number };
}

/** Streams NDJSON: progress lines (which also keep the connection alive), then the result. */
function streamJob(job: (progress: (line: unknown) => Promise<void>) => Promise<unknown>): Response {
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  const writer = writable.getWriter();
  const encoder = new TextEncoder();
  const send = (line: unknown) => writer.write(encoder.encode(`${JSON.stringify(line)}\n`));
  const heartbeat = setInterval(() => void send({ heartbeat: Date.now() }), 10_000);
  void job(send)
    .then((result) => send({ result }))
    .catch((error) => send({ error: error instanceof Error ? error.message : String(error) }))
    .finally(() => {
      clearInterval(heartbeat);
      void writer.close();
    });
  return new Response(readable, { headers: { "content-type": "application/x-ndjson" } });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    try {
      return await handle(request, env);
    } catch (error) {
      return Response.json({ error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) }, { status: 500 });
    }
  },
};

async function handle(request: Request, env: Env): Promise<Response> {
  {
    if (request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`) return new Response("Unauthorized", { status: 401 });
    const path = new URL(request.url).pathname;
    if (request.method === "POST" && path === "/split-bench") {
      let input: SplitBenchInput;
      try {
        input = validateSplitBench(await request.json());
      } catch (error) {
        return Response.json({ error: error instanceof Error ? error.message : "invalid request" }, { status: 400 });
      }
      return streamJob((progress) => splitBench(env, input, progress));
    }
    // The same, as a background job: POST starts it (idempotent per objectName), GET polls it.
    if (path === "/split-job") {
      if (request.method === "POST") {
        let input: SplitBenchInput;
        try {
          input = validateSplitBench(await request.json());
        } catch (error) {
          return Response.json({ error: error instanceof Error ? error.message : "invalid request" }, { status: 400 });
        }
        await env.SPLIT_JOBS.get(env.SPLIT_JOBS.idFromName(input.objectName)).start(input);
        return Response.json({ started: input.objectName });
      }
      const name = new URL(request.url).searchParams.get("objectName") ?? "";
      if (!/^[a-zA-Z0-9_-]{1,80}$/.test(name)) return Response.json({ error: "objectName" }, { status: 400 });
      return Response.json(await env.SPLIT_JOBS.get(env.SPLIT_JOBS.idFromName(name)).poll());
    }
    // Forced native splits, a whole level at once, optionally under write load: where each split's time goes.
    if (request.method === "POST" && path === "/split-steps") {
      const body = (await request.json()) as { rows?: number; levels?: number; load?: number };
      const rows = Math.min(Math.max(Math.floor(body.rows ?? 2000), 100), 20_000);
      const levels = Math.min(Math.max(Math.floor(body.levels ?? 3), 1), 4);
      const load = Math.min(Math.max(Math.floor(body.load ?? 0), 0), 256);
      const name = `steps-${crypto.randomUUID()}`;
      const ns = env.SPLIT as DurableObjectNamespace<SplitObject>;
      const client = sharded(ns, name);
      const value = "x".repeat(200);
      for (let i = 0; i < rows; i += 100) {
        const keys = Array.from({ length: Math.min(100, rows - i) }, (_, j) => `r-${i + j}`);
        await client.call(keys, "put", keys.map(() => value));
      }
      let stop = false;
      let writes = 0;
      let failures = 0;
      const loaders = Array.from({ length: load }, async (_, w) => {
        for (let i = 0; !stop; i++) {
          try {
            await client.call([`r-${(w * 7919 + i * 104729) % rows}`], "put", [value]);
            writes++;
          } catch {
            failures++;
          }
        }
      });
      const records: unknown[] = [];
      let level = [""];
      for (let depth = 0; depth < levels; depth++) {
        const started = Date.now();
        const results = await Promise.all(
          level.map(async (bits) => {
            const stub = ns.get(ns.idFromName(shardObjectName(name, bits)));
            const ok = await stub.shardedSplit(name, bits);
            const split = (await stub.shardedDescribe()).splits.at(-1) ?? null;
            return { bits, ok, split };
          }),
        );
        records.push({ depth, wallMs: Date.now() - started, results });
        level = level.flatMap((bits) => [`${bits}0`, `${bits}1`]);
      }
      stop = true;
      await Promise.all(loaders);
      return Response.json({ rows, load, writes, failures, records });
    }
    // The first calls to a new presplit object: each waits for the presplit, so this times it.
    if (request.method === "POST" && path === "/first-calls") {
      const body = (await request.json()) as { target?: string; calls?: number };
      if (body.target !== "presplit" && body.target !== "presplit-4096") return Response.json({ error: "target" }, { status: 400 });
      const calls = Math.min(Math.max(Math.floor(body.calls ?? 64), 1), 256);
      const client = sharded(shardedNamespace(env, body.target), `first-${crypto.randomUUID()}`);
      const started = Date.now();
      const latencyMs = await Promise.all(
        Array.from({ length: calls }, async (_, i) => {
          const t = Date.now();
          await client.call([`k-${i}`], "get", null);
          return Date.now() - t;
        }),
      );
      return Response.json({ calls, wallMs: Date.now() - started, latencyMs: latencyMs.sort((x, y) => x - y) });
    }
    if (request.method !== "POST" || path !== "/measure") return new Response("Not found", { status: 404 });
    let input: ReturnType<typeof validate>;
    try {
      input = validate(await request.json());
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : "invalid request" }, { status: 400 });
    }

    // One driver tops out at about 90 calls/s, whatever its concurrency: 16 in-flight calls each, so
    // the drivers are not what is measured. Fleets of 256 drivers start them.
    // A listing calls every shard for each page: drivers scale with the calls, not the pages.
    const fanOut = input.operation === "list" && isTable(input.target) ? Number(input.target.slice("table-".length)) : 1;
    const driverCount = Math.min(MAX_DRIVERS, input.concurrency, Math.max(Math.min(16, input.concurrency), Math.ceil((input.concurrency * fanOut) / CALLS_PER_DRIVER)));
    const perDriver = Math.ceil(input.concurrency / driverCount);
    const configs = Array.from({ length: driverCount }, (_, driverIndex) => ({ ...input, driverIndex, driverCount, concurrency: perDriver }));
    const fleets: { stub: DurableObjectStub<BenchFleet>; configs: typeof configs }[] = [];
    for (let i = 0; i < configs.length; i += DRIVERS_PER_FLEET) {
      const stub = env.BENCH_FLEETS.get(env.BENCH_FLEETS.idFromName(`${input.objectName}-fleet-${i / DRIVERS_PER_FLEET}`));
      fleets.push({ stub, configs: configs.slice(i, i + DRIVERS_PER_FLEET) });
    }
    await Promise.all(fleets.map((fleet) => fleet.stub.prepare(fleet.configs)));
    // Later with more fleets: every driver must have its start time before it comes.
    const startAt = Date.now() + 750 + 250 * fleets.length;
    const settled = await Promise.allSettled(fleets.map((fleet) => fleet.stub.measure(fleet.configs.map((config) => ({ ...config, startAt })))));
    const fleetResults = settled.flatMap((r) => (r.status === "fulfilled" ? [r.value] : []));
    const lostDrivers = settled.reduce((n, r, i) => n + (r.status === "rejected" ? fleets[i]!.configs.length : 0), 0);
    const fleetReasons = settled.flatMap((r) => (r.status === "rejected" ? [`fleet: ${String(r.reason).slice(0, 200)}`] : [])).slice(0, 3);
    const { successes, errors, bytes, elapsedMs, maxLatencyMs, failedDrivers, latencyMs, errorSamples } = merge(fleetResults, lostDrivers, fleetReasons);
    const histogram = new Array<number>(120_001).fill(0);
    for (const [bucket, count] of latencyMs) histogram[bucket!] = count!;

    let topology: Awaited<ReturnType<typeof describeTree>> | null = null;
    if (input.target === "split") topology = await describeTree(env, input.objectName);
    if (input.target === "legacy") topology = await describeLegacyTree(env, input.objectName);
    if (input.target === "presplit") topology = await describeTree(env, input.objectName, "", env.PRESPLIT as never);
    if (isTable(input.target)) {
      const leaves = Number(input.target.slice("table-".length));
      // Counting asks every shard: one still overloaded from the run answers nothing.
      const rows = await tableClient(env, input.target, input.objectName).count().catch(() => -1);
      topology = { leaves, rows, depth: Math.log2(leaves) };
    }
    // Walking 4,096 shards is 8,191 calls. A presplit tree is complete, so the depth of the shards on
    // its first and last paths gives it: the root's object and the object of "1" (bits "1000…").
    if (input.target === "presplit-4096") {
      const ns = env.PRESPLIT_4096 as unknown as DurableObjectNamespace<ShardedObject<Env, string>>;
      const sampled = await Promise.all(["", "1"].map((bits) => ns.get(ns.idFromName(shardObjectName(input.objectName, bits))).shardedDescribe()));
      const depth = Math.min(...sampled.map((d) => d.bits.length));
      topology = { leaves: 2 ** depth, rows: sampled.reduce((n, d) => n + d.rows, 0), depth };
    }
    return Response.json({
      ...input,
      concurrency: Math.ceil(input.concurrency / driverCount) * driverCount,
      driverCount,
      failedDrivers,
      errorSamples,
      successes,
      errors,
      elapsedMs,
      operationsPerSecond: successes / (elapsedMs / 1000),
      mebibytesPerSecond: bytes / (1024 * 1024) / (elapsedMs / 1000),
      latencyMs: { p50: percentile(histogram, 0.5), p95: percentile(histogram, 0.95), p99: percentile(histogram, 0.99), max: Math.round(maxLatencyMs * 100) / 100 },
      topology,
      // The first sample, under the name earlier results use.
      errorSample: errorSamples[0] ?? null,
    });
  }
}
