import { ShardedObject } from "../sharded/index.js";
/** A store of results and values by unit hash. A subclass gives `nodes()`, its own binding. */
export declare abstract class WorkGraphResults<Env = unknown> extends ShardedObject<Env, string> {
    /** The stored text of each key, or null. */
    graphGet(keys: readonly string[], _payloads: null): Promise<(string | null)[]>;
    /** Stores each key's text unless it is there: a key's text is a function of its hash. */
    graphPut(keys: readonly string[], texts: readonly string[]): Promise<number>;
}
//# sourceMappingURL=results.d.ts.map