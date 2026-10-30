// Worker side, shared by the examples: walks an object's shards and describes each one, for the
// shard panel. Asks the objects themselves, not a client's map, so it shows splits as they happen.
import { shardObjectName, type NodeDescription, type ShardedObject } from "@leostera/do-kit";

export interface ShardInfo {
  readonly bits: string;
  readonly state: NodeDescription["state"];
  readonly rows: number;
  readonly storedRows: number;
  readonly callsPerSecond: number;
  readonly splits: number;
  readonly lastSplitMs: number | null;
}

/**
 * Rows a shard holds in tables of its own, which `shardedDescribe` does not count: a ShardedTable
 * passes `tableRows`, other subclasses a method of their own.
 */
export type CountRows = (shard: any) => Promise<number>;

export const tableRows: CountRows = (shard) => shard.tableCount();

export async function describeShards<T extends ShardedObject<any, any>>(ns: DurableObjectNamespace<T>, name: string, countRows?: CountRows, bits = ""): Promise<ShardInfo[]> {
  const node = ns.get(ns.idFromName(shardObjectName(name, bits))) as unknown as { shardedDescribe(): Promise<NodeDescription> };
  const d = await node.shardedDescribe();
  // The object hosting `bits` hosts a deeper shard once `bits` has split.
  if (d.state === "new" || d.bits === bits) {
    const own = countRows && d.state === "shard" ? await countRows(node) : null;
    const [rows, storedRows] = own === null ? [d.rows, d.storedRows] : [own, own];
    return [{ bits: bits || "(root)", state: d.state, rows, storedRows, callsPerSecond: Math.round(d.callsPerSecond), splits: d.splits.length, lastSplitMs: d.lastSplit?.ms ?? null }];
  }
  const [left, right] = await Promise.all([describeShards(ns, name, countRows, `${bits}0`), describeShards(ns, name, countRows, `${bits}1`)]);
  return [...left, ...right];
}
