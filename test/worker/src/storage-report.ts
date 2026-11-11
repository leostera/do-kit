// What an object holds, for tests that check something left nothing behind: its SQLite tables
// with their row counts (the runtime's own tables aside: _cf_*, and cf dev's __miniflare_*), its
// key-value keys, and its alarm.
export interface StorageReport {
  readonly tables: { readonly name: string; readonly rows: number }[];
  readonly keys: number;
  readonly alarm: number | null;
}

export async function storageReport(storage: DurableObjectStorage): Promise<StorageReport> {
  const names = storage.sql
    .exec<{ name: string }>("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT GLOB '_cf_*' AND name NOT GLOB 'sqlite_*' AND name NOT GLOB '__miniflare_*'")
    .toArray()
    .map((r) => r.name);
  const tables = names.map((name) => ({ name, rows: storage.sql.exec<{ n: number }>(`SELECT COUNT(*) AS n FROM "${name.replaceAll('"', '""')}"`).one().n }));
  return { tables, keys: (await storage.list()).size, alarm: await storage.getAlarm() };
}
