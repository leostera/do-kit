// IndexedTable: a sharded table with global indexes on shards of their own. See docs/rfds/RFD0003-indexed-table.md.
export { IndexedTable, type IndexedShardResult, type WriteTokens } from "./table.js";
export { GlobalIndex, type GlobalIndexStats } from "./global-index.js";
export { indexedTable, IndexedTableClient, IndexContended, IndexPending, type IndexedListQuery, type IndexedWriteResult } from "./client.js";
export type { GlobalIndexInfo } from "./entries.js";
