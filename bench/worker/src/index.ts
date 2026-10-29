import { DurableObject } from "cloudflare:workers";
import { leafObjectName, nodeName, splittable, SplittableObject, type SplitPolicy, type UpdateResult } from "../../../src/index.ts";
import { splittable as twoCopySplittable, SplittableObject as TwoCopySplittableObject } from "./two-copy/index.ts";
import { splittable as legacySplittable, SplittableObject as LegacySplittableObject } from "./legacy/index.ts";

const LOCATIONS = ["wnam", "enam", "weur", "eeur", "apac"] as const;
/** "split": snapshot splits (this package); "legacy": the previous row-by-row handoff. */
type Target = "single" | "split" | "legacy" | "presplit";
type Operation = "read" | "write";
type Distribution = "uniform" | "hot";

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
  readonly latencyMs: readonly number[];
  readonly maxLatencyMs: number;
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
export class SplitObject extends SplittableObject<Env, string> {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 400, windowSeconds: 5, maxDepth: 12, keysPerCall: 25, copy: "native" };
  protected nodes(): DurableObjectNamespace<SplittableObject<Env, string>> {
    return this.env.SPLIT as DurableObjectNamespace<SplittableObject<Env, string>>;
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
  protected override nodes(): DurableObjectNamespace<SplittableObject<Env, string>> {
    return this.env.PRESPLIT as never;
  }
}

/** Splits only when told to: for timing one split. */
export class ManualSplitObject extends SplitObject {
  protected override splitPolicy: SplitPolicy = { callsPerSecond: 1e12, windowSeconds: 5, maxDepth: 12, copy: "native", splitTimeoutMs: 300_000 };
  protected override nodes(): DurableObjectNamespace<SplittableObject<Env, string>> {
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

/** Distributed load generator. locationHint is set only when each fresh driver is first created. */
export class BenchDriver extends DurableObject<Env> {
  async prepare(config: Omit<MeasureConfig, "startAt">): Promise<void> {
    if (config.operation !== "read") return;
    const value = "x".repeat(config.payloadBytes);
    const keys: string[] = [];
    const values: string[] = [];
    if (config.distribution === "hot") {
      if (config.driverIndex === 0) await this.write(config.target, config.objectName, ["hot-key"], [value]);
      return;
    }
    for (let i = config.driverIndex; i < config.keyCount; i += config.driverCount) {
      keys.push(`key-${i}`);
      values.push(value);
      if (keys.length === 100) {
        await this.write(config.target, config.objectName, keys, values);
        keys.length = 0;
        values.length = 0;
      }
    }
    if (keys.length) await this.write(config.target, config.objectName, keys, values);
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

    const worker = async (): Promise<void> => {
      while (Date.now() < stopAt) {
        const key = config.distribution === "hot" ? "hot-key" : `key-${sequence++ % config.keyCount}`;
        const before = performance.now();
        try {
          if (config.operation === "write") {
            await this.write(config.target, config.objectName, [key], ["x".repeat(config.payloadBytes)]);
            bytes += config.payloadBytes;
          } else {
            const value = await this.read(config.target, config.objectName, key);
            bytes += value?.length ?? 0;
          }
          successes++;
        } catch {
          errors++;
        } finally {
          const elapsed = performance.now() - before;
          maxLatencyMs = Math.max(maxLatencyMs, elapsed);
          const bucket = Math.min(120_000, Math.floor(elapsed));
          latencyMs[bucket]++;
        }
      }
    };

    await Promise.all(Array.from({ length: config.concurrency }, worker));
    return { successes, errors, bytes, elapsedMs: Date.now() - startedAt, latencyMs, maxLatencyMs };
  }

  private async write(target: Target, name: string, keys: readonly string[], values: readonly string[]): Promise<void> {
    if (target === "single") {
      const stub = this.env.SINGLE.get(this.env.SINGLE.idFromName(name));
      await stub.put(keys, values);
      return;
    }
    if (target === "legacy") await legacySplittable(this.env.LEGACY, name).call(keys, "put", values);
    else await splittable(target === "presplit" ? this.env.PRESPLIT : this.env.SPLIT, name).call(keys, "put", values);
  }

  private async read(target: Target, name: string, key: string): Promise<string | null> {
    if (target === "single") {
      const stub = this.env.SINGLE.get(this.env.SINGLE.idFromName(name));
      return (await stub.get([key], null))[0] ?? null;
    }
    const [leaf] = target === "legacy" ? await legacySplittable(this.env.LEGACY, name).call([key], "get", null) : await splittable(target === "presplit" ? this.env.PRESPLIT : this.env.SPLIT, name).call([key], "get", null);
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
  if (target !== "single" && target !== "split" && target !== "legacy" && target !== "presplit") throw new Error("target must be single, split, presplit or legacy");
  if (operation !== "read" && operation !== "write") throw new Error("operation must be read or write");
  if (distribution !== "uniform" && distribution !== "hot") throw new Error("distribution must be uniform or hot");
  if (!Number.isSafeInteger(concurrency) || (concurrency as number) < 1 || (concurrency as number) > 4096) throw new Error("concurrency must be an integer from 1 to 4096");
  if (!Number.isSafeInteger(durationMs) || (durationMs as number) < 5_000 || (durationMs as number) > 120_000) throw new Error("durationMs must be from 5000 to 120000");
  if (!Number.isSafeInteger(keyCount) || (keyCount as number) < 1 || (keyCount as number) > 100_000) throw new Error("keyCount must be from 1 to 100000");
  if (!Number.isSafeInteger(payloadBytes) || (payloadBytes as number) < 16 || (payloadBytes as number) > 64 * 1024) throw new Error("payloadBytes must be from 16 to 65536");
  const objectName = body.objectName;
  if (typeof objectName !== "string" || !/^[a-zA-Z0-9_-]{1,80}$/.test(objectName)) throw new Error("objectName must be 1-80 safe characters");
  return { target, operation, distribution, concurrency: concurrency as number, durationMs: durationMs as number, keyCount: keyCount as number, payloadBytes: payloadBytes as number, objectName };
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

async function describeTree(env: Env, name: string, bits = "", ns: DurableObjectNamespace<SplittableObject<Env, string>> = env.SPLIT as never): Promise<{ leaves: number; rows: number; depth: number }> {
  const namespace = ns;
  const description = await namespace.get(namespace.idFromName(leafObjectName(name, bits))).splittableDescribe();
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
  const split = env.SPLIT_MANUAL as unknown as DurableObjectNamespace<SplittableObject<Env, string>>;
  const legacy = env.LEGACY_MANUAL as unknown as DurableObjectNamespace<LegacySplittableObject<Env, string>>;
  const rootSplit = split.get(split.idFromName(leafObjectName(name, "")));
  const rootLegacy = legacy.get(legacy.idFromName(nodeName(name, "")));
  const twoCopy = env.TWO_COPY_MANUAL as unknown as DurableObjectNamespace<TwoCopySplittableObject<Env, string>>;
  const rootTwoCopy = twoCopy.get(twoCopy.idFromName(nodeName(name, "")));
  const seedChunk = Math.max(1, Math.floor((16 * 1024 * 1024) / input.payloadBytes));
  const seedStarted = Date.now();
  for (let start = 0; start < input.rows; start += seedChunk) {
    const count = Math.min(seedChunk, input.rows - start);
    const args = [start, count, input.payloadBytes];
    const answer = input.target === "split" ? await rootSplit.splittableCall(name, "", "seed", [], null, args) : input.target === "two-copy" ? await rootTwoCopy.splittableCall(name, "", "seed", [], null, args) : await rootLegacy.splittableCall(name, "", "seed", [], null, args);
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
    input.target === "split" ? splittable(env.SPLIT_MANUAL, name).call([key], "put", [value]) : input.target === "two-copy" ? twoCopySplittable(env.TWO_COPY_MANUAL, name).call([key], "put", [value]) : legacySplittable(env.LEGACY_MANUAL, name).call([key], "put", [value]);
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
    if (!(await rootSplit.splittableSplit(name, ""))) throw new Error(`split abandoned: ${(await rootSplit.splittableDescribe()).lastSplitError}`);
    copyMs = Date.now() - splitStarted;
    const right = split.get(split.idFromName(leafObjectName(name, "1")));
    for (;;) {
      const [l, r] = await Promise.all([rootSplit.splittableDescribe(), right.splittableDescribe()]);
      if (r.state === "leaf" && !servingMs) servingMs = Date.now() - splitStarted;
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
    if (request.method !== "POST" || path !== "/measure") return new Response("Not found", { status: 404 });
    let input: ReturnType<typeof validate>;
    try {
      input = validate(await request.json());
    } catch (error) {
      return Response.json({ error: error instanceof Error ? error.message : "invalid request" }, { status: 400 });
    }

    // About 64 in-flight calls per driver at most, up to 64 drivers.
    const driverCount = Math.min(64, Math.max(Math.min(16, input.concurrency), Math.ceil(input.concurrency / 64)));
    const configs = Array.from({ length: driverCount }, (_, driverIndex) => ({ ...input, driverIndex, driverCount }));
    const drivers = configs.map((config, i) => {
      const id = env.BENCH_DRIVERS.idFromName(`${input.objectName}-driver-${i}`);
      return env.BENCH_DRIVERS.get(id, { locationHint: LOCATIONS[i % LOCATIONS.length] });
    });
    await Promise.all(drivers.map((driver, i) => driver.prepare(configs[i]!)));
    const startAt = Date.now() + 750;
    const results = await Promise.all(drivers.map((driver, i) => driver.measure({ ...configs[i]!, startAt, concurrency: Math.ceil(input.concurrency / driverCount) })));

    const histogram = new Array<number>(120_001).fill(0);
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
      result.latencyMs.forEach((count, i) => histogram[i] += count);
    }
    let topology: Awaited<ReturnType<typeof describeTree>> | null = null;
    if (input.target === "split") topology = await describeTree(env, input.objectName);
    if (input.target === "legacy") topology = await describeLegacyTree(env, input.objectName);
    if (input.target === "presplit") topology = await describeTree(env, input.objectName, "", env.PRESPLIT as never);
    return Response.json({
      ...input,
      concurrency: Math.ceil(input.concurrency / driverCount) * driverCount,
      driverCount,
      successes,
      errors,
      elapsedMs,
      operationsPerSecond: successes / (elapsedMs / 1000),
      mebibytesPerSecond: bytes / (1024 * 1024) / (elapsedMs / 1000),
      latencyMs: { p50: percentile(histogram, 0.5), p95: percentile(histogram, 0.95), p99: percentile(histogram, 0.99), max: Math.round(maxLatencyMs * 100) / 100 },
      topology,
    });
  },
};
