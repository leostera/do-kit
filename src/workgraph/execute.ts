// Computing one chunk (RFD0005): which of its units are needed, given the results already stored,
// and computing them parents first, each with its parent's value and the results it reads. A
// replayed unit is computed only for a needed unit below it; a value is kept in memory only until
// its last child needing it is computed. The runner calls this; tests call it directly.
import type { AnyTask } from "./graph.js";
import type { PlannedUnit } from "./plan.js";
import { decodeStored, encodeStored, resultKey, valueKey } from "./stored.js";

/** A chunk as the coordinator hands it out. */
export interface ChunkBody {
  readonly units: readonly PlannedUnit[];
  readonly exports: readonly string[];
}

/**
 * Why a unit cannot be computed, the same way on every attempt: its compute threw (a pure unit
 * throws again), its task is not the runner's, or what it returned cannot be stored. The chunk
 * fails with it, and after maxAttempts so does the run.
 */
export class ComputeFailed extends Error {
  constructor(
    readonly unit: string,
    cause: unknown,
  ) {
    super(`unit ${unit}: ${cause instanceof Error ? cause.message : String(cause)}`);
  }
}

/** A chunk computed: the store's writes, and counts. */
export interface Executed {
  readonly writes: Map<string, string>;
  /** Units of this chunk computed for a result that was not stored. */
  readonly computed: number;
  /** Units of this chunk whose results were already stored. */
  readonly reused: number;
  /** Units computed only for their values: replayed ones, and stored ones above a unit computed. */
  readonly replayed: number;
}

/**
 * The store keys a chunk reads before computing: its results, its exports, its loads and after
 * results. `inputsOnly`: only what it takes from other chunks (its loads and after results), for a
 * chunk that cannot have anything of its own stored yet.
 */
export function keysToRead(body: ChunkBody, inputsOnly = false): string[] {
  const keys = new Set<string>();
  for (const u of body.units) {
    if (!u.replay && !inputsOnly) keys.add(resultKey(u.hash));
    if (u.load) keys.add(valueKey(u.load));
    for (const a of u.after) keys.add(resultKey(a.hash));
  }
  if (!inputsOnly) for (const hash of body.exports) keys.add(valueKey(hash));
  return [...keys];
}

export async function executeChunk<Env>(body: ChunkBody, tasks: Readonly<Record<string, AnyTask<Env>>>, stored: ReadonlyMap<string, string>, env: Env): Promise<Executed> {
  const exports = new Set(body.exports);
  // Needed: a unit of this chunk without its result or exported value, and every unit above one.
  const needed = new Set<string>();
  for (const u of body.units) if (!u.replay && (!stored.has(resultKey(u.hash)) || (exports.has(u.hash) && !stored.has(valueKey(u.hash))))) needed.add(u.key);
  for (let i = body.units.length - 1; i >= 0; i--) {
    const u = body.units[i]!;
    if (needed.has(u.key) && u.parent !== null) needed.add(u.parent);
  }
  // How many needed children each value still has to serve.
  const waiting = new Map<string, number>();
  for (const u of body.units) if (needed.has(u.key) && u.parent !== null) waiting.set(u.parent, (waiting.get(u.parent) ?? 0) + 1);

  const values = new Map<string, unknown>();
  const writes = new Map<string, string>();
  let computed = 0;
  let replayed = 0;
  for (const u of body.units) {
    if (!needed.has(u.key)) continue;
    const task = tasks[u.task];
    if (!task) throw new ComputeFailed(u.key, `its task ${u.task} is not one of this runner's tasks`);
    const parent = u.parent !== null ? values.get(u.parent) : u.load !== null ? decodeStored(required(stored, valueKey(u.load), `the value of ${u.key}'s parent`)) : null;
    const after: Record<string, unknown> = {};
    for (const a of u.after) after[a.name] = decodeStored(required(stored, resultKey(a.hash), `the result of ${a.key}, which ${u.key} runs after`));
    let out: { result: unknown; value: unknown };
    try {
      out = await task.compute(u.input, parent, after, { env });
    } catch (error) {
      throw new ComputeFailed(u.key, error);
    }
    const fresh = !u.replay && !stored.has(resultKey(u.hash));
    if (fresh) {
      computed++;
      writes.set(resultKey(u.hash), storable(u.key, out.result, `the result of ${u.key}`));
    } else replayed++;
    if (!u.replay && exports.has(u.hash) && !stored.has(valueKey(u.hash))) writes.set(valueKey(u.hash), storable(u.key, out.value, `the value of ${u.key}`));
    if ((waiting.get(u.key) ?? 0) > 0) values.set(u.key, out.value);
    if (u.parent !== null) {
      const left = waiting.get(u.parent)! - 1;
      waiting.set(u.parent, left);
      if (left === 0) values.delete(u.parent);
    }
  }
  const reused = body.units.filter((u) => !u.replay && stored.has(resultKey(u.hash))).length;
  return { writes, computed, reused, replayed };
}

/** `value` encoded, or the unit's failure: too big, or not JSON, it is so on every attempt. */
function storable(unit: string, value: unknown, what: string): string {
  try {
    return encodeStored(value, what);
  } catch (error) {
    throw new ComputeFailed(unit, error);
  }
}

/** A stored input a chunk needs: missing, it was not done first, which the coordinator prevents. */
function required(stored: ReadonlyMap<string, string>, key: string, what: string): string {
  const text = stored.get(key);
  if (text === undefined) throw new Error(`${what} is not stored: a chunk ran before a chunk it needs`);
  return text;
}
