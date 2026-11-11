// Where a work graph's results go (RFD0005): a sharded object keyed by unit hash, so a result is
// written once and found by any run of any graph with the same inputs. Values a chunk exports for
// chunks cut below it are kept here too, under their own keys.
import { ShardedObject } from "../sharded/index.js";

/** A store of results and values by unit hash. A subclass gives `nodes()`, its own binding. */
export abstract class WorkGraphResults<Env = unknown> extends ShardedObject<Env, string> {
  /** The stored text of each key, or null. */
  async graphGet(keys: readonly string[], _payloads: null): Promise<(string | null)[]> {
    const found = await this.read(keys);
    return keys.map((k) => found.get(k) ?? null);
  }

  /** Stores each key's text unless it is there: a key's text is a function of its hash. */
  async graphPut(keys: readonly string[], texts: readonly string[]): Promise<number> {
    return this.update(keys, (current) => {
      const writes = new Map<string, string>();
      keys.forEach((k, i) => {
        if (!current.has(k)) writes.set(k, texts[i]!);
      });
      return { writes, result: writes.size };
    });
  }
}
