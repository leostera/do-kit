// After a split, each index's key table is copied in key order into a dense new table, which then
// takes its place (see startCompactions). The copy takes many steps, and writes go on meanwhile: a
// write to a key the copy has passed must reach the copy too, or the swap loses it. Here the
// steps are taken one a call (the alarm's are paused), between writes that add rows, move index
// keys and delete rows, and every index then lists exactly the rows a model of them says.
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";
import type { StorageReport } from "./worker/src/storage-report.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown; stars: number | null };
type Versioned = { row: Doc; version: number };

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const table = <T = unknown>(name: string, op: string, ...args: unknown[]) => post<T>("/table", { cls: "evolving", name, op, args });
const object = <T = unknown>(name: string, bits: string, method: string, ...args: unknown[]) => post<T>("/object", { cls: "evolving", name, bits, method, args });
const doc = (i: number, createdAt: number, stars: number | null): Doc => ({ name: `d${i}`, id: `${i}`, createdAt, lastEditAt: null, deleted: i % 5 === 0, meta: null, stars });
const tables = async (name: string, bits: string) => (await object<StorageReport>(name, bits, "storage")).tables.map((t) => t.name);

test("writes while a split's key tables are compacted reach the compacted tables: every index lists the rows there are", async () => {
  const name = `compaction-${unique()}`;
  await table(name, "put", doc(0, 0, 0));
  await table(name, "upgrade", 2, "");
  for (let i = 0; i < 50 && (await object<number>(name, "", "buildsLeft")) > 0; i++) await table(name, "get", "d0");
  const model = new Map<string, Doc>();
  const rows = Array.from({ length: 1_200 }, (_, i) => doc(i, (i * 7919) % 1_000, i % 3 === 0 ? null : i % 50));
  for (let i = 0; i < rows.length; i += 200) await table(name, "putMany", rows.slice(i, i + 200).map((row) => ({ row })));
  for (const row of rows) model.set(row.name, row);
  // Steps from calls only, a few hundred keys each: the copy spans the writes below.
  await object(name, "", "pauseAlarmBuilds", true);
  expect(await post<boolean>("/split", { cls: "evolving", name, bits: "" })).toBe(true);
  let sawCompaction = false;
  for (let w = 0; w < 120; w++) {
    const i = (w * 131) % 1_400;
    const key = `d${i}`;
    if (w % 4 === 3 && model.has(key)) {
      await table(name, "delete", key);
      model.delete(key);
    } else {
      const row = doc(i, (w * 37) % 1_000, w % 2 === 0 ? null : w);
      await table(name, "put", row);
      model.set(key, row);
    }
    const now = [...(await tables(name, "0")), ...(await tables(name, "1"))];
    if (now.some((t) => t.startsWith("sharded_table_compact_"))) sawCompaction = true;
  }
  // The writes above ran while a compaction was under way.
  expect(sawCompaction).toBe(true);
  for (const bits of ["0", "1"]) await object(name, bits, "pauseAlarmBuilds", false);
  const settled = async () => {
    const all = [...(await tables(name, "0")), ...(await tables(name, "1"))];
    return !all.some((t) => t.startsWith("sharded_table_compact_") || t.startsWith("sharded_table_dropped_"));
  };
  // The paused alarms were not asked again: calls to every shard (a walk's pages) take the steps left.
  for (let i = 0; i < 200 && !(await settled()); i++) await table(name, "all", {});
  expect(await settled()).toBe(true);
  const expected = [...model.values()];
  const byIndex: Record<string, (d: Doc) => unknown[]> = {
    byCreated: (d) => [d.createdAt],
    byStars: (d) => [d.stars],
    byDeletedCreated: (d) => [d.deleted, d.createdAt],
  };
  for (const [index, columns] of Object.entries(byIndex)) {
    const listed = await table<Versioned[]>(name, "all", { index });
    const names = listed.map((r) => r.row.name);
    // Each row once (a key left behind by a move would list it twice), and every row.
    expect(new Set(names).size).toBe(names.length);
    expect([...names].sort()).toEqual(expected.map((d) => d.name).sort());
    // Each where its key is now.
    for (const r of listed) expect(columns(r.row)).toEqual(columns(model.get(r.row.name)!));
  }
}, 180_000);
