// do-kit: building blocks for Durable Objects. Every layer is exported from here, and also from its own path:
//   @leostera/do-kit/sharded  ShardedObject: one object that splits itself into shards as it grows.
//   @leostera/do-kit/table    ShardedTable: typed rows with indexes and listings across shards.
//   @leostera/do-kit/indexed  IndexedTable: a ShardedTable with global indexes (lookups, uniqueness).
//   @leostera/do-kit/workgraph  Work graphs: incremental, parallel execution of a graph of pure units.
//   @leostera/do-kit/leased   ReadLeases, RelayReads, RelayPlan: one object's hot keys, read through relays.
//   @leostera/do-kit/client   Client: calls to any object with deadlines, retries and interceptors.
//   @leostera/do-kit/behaviours  DurableStateMachine: an XState machine as a Durable Object that survives eviction.
//                                Only from its own path, and only with xstate installed: it is not re-exported here,
//                                so this package needs nothing beyond itself.
export * from "./sharded/index.js";
export * from "./table/index.js";
export * from "./indexed/index.js";
export * from "./workgraph/index.js";
export * from "./leased/index.js";
export * from "./client/index.js";
