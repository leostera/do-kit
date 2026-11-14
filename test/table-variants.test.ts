// The table's core behaviour on every variant of it: the docs table (a held write refused at
// once), the default hold wait (a held write waits for the split), presplit into 4 shards, and
// routed by a route of the key. Versions, listings against a reference sort through a split
// mid-walk, counts, and writes during a split.
import { describe, expect, test } from "bun:test";
import { post, shards, useWorker } from "./harness.ts";

useWorker();

type Doc = { name: string; id: string; createdAt: number; lastEditAt: number | null; deleted: boolean; meta: unknown };
type Versioned = { row: Doc; version: number };
type Page = { rows: Versioned[]; cursor: string | null };
type Query = { index?: string; direction?: "asc" | "desc"; where?: { column: string; op: string; value?: unknown }[]; limit?: number };
const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;

function makeDocs(n: number): Doc[] {
  const special = ["x\uFFFD", "x😀", "é-doc", "Zeta", "alpha"];
  return Array.from({ length: n }, (_, i) => ({
    name: i < special.length ? special[i]! : `doc-${String(i).padStart(4, "0")}`,
    id: `id-${i}`,
    createdAt: 1_000 + ((i * 7919) % 97),
    lastEditAt: i % 5 === 0 ? null : 5_000 + ((i * 104729) % 211),
    deleted: i % 7 === 0,
    meta: { i },
  }));
}
function sqliteCompare(a: unknown, b: unknown): number {
  const rank = (v: unknown) => (v === null ? 0 : typeof v === "number" || typeof v === "boolean" ? 1 : 2);
  if (rank(a) !== rank(b)) return rank(a) - rank(b);
  if (a === null) return 0;
  if (rank(a) === 1) return Number(a) - Number(b);
  const ca = [...(a as string)].map((c) => c.codePointAt(0)!);
  const cb = [...(b as string)].map((c) => c.codePointAt(0)!);
  for (let i = 0; i < Math.min(ca.length, cb.length); i++) if (ca[i] !== cb[i]) return ca[i]! - cb[i]!;
  return ca.length - cb.length;
}
const INDEXES: Record<string, (keyof Doc)[]> = { primary: [], byCreated: ["createdAt"], byEdit: ["lastEditAt"], byDeletedCreated: ["deleted", "createdAt"] };
function expected(rows: Doc[], query: Query): string[] {
  const cols = [...INDEXES[query.index ?? "primary"]!, "name"] as (keyof Doc)[];
  const sign = query.direction === "desc" ? -1 : 1;
  return rows
    .filter((r) => (query.where ?? []).every((c) => r[c.column as keyof Doc] === c.value))
    .sort((a, b) => {
      for (const c of cols) {
        const d = sqliteCompare(a[c], b[c]);
        if (d !== 0) return sign * d;
      }
      return 0;
    })
    .map((r) => r.name);
}
const QUERIES: Query[] = [
  { limit: 7 },
  { index: "byCreated", direction: "desc", limit: 7 },
  { index: "byEdit", limit: 9 },
  { index: "byDeletedCreated", direction: "desc", where: [{ column: "deleted", op: "=", value: false }], limit: 11 },
];

describe.each(["table", "tableDefaultHold", "presplitTable", "routedTable"])("%s", (cls) => {
  const op = <T>(name: string, operation: string, ...args: unknown[]) => post<T>("/table", { cls, name, op: operation, args });
  const splitFirst = async (name: string) => post<boolean>("/split", { cls, name, bits: (await shards(cls, name))[0]! });

  test("puts, patches and deletes are conditional on the row's version", async () => {
    const name = `variants-versions-${unique()}`;
    const doc: Doc = { name: "foo", id: "r1", createdAt: 1, lastEditAt: null, deleted: false, meta: null };
    expect(await op(name, "put", doc, { ifVersion: 0 })).toEqual({ ok: true, version: 1 });
    expect(await op(name, "put", doc, { ifVersion: 0 })).toEqual({ ok: false, version: 1 });
    expect(await op(name, "patch", "foo", { lastEditAt: 9 }, { ifVersion: 1 })).toEqual({ ok: true, version: 2 });
    expect(await op(name, "delete", "foo", { ifVersion: 1 })).toEqual({ ok: false, version: 2 });
    expect((await op<{ ok: boolean }>(name, "delete", "foo", { ifVersion: 2 })).ok).toBe(true);
    expect(await op(name, "get", "foo")).toBeNull();
  }, 30_000);

  test("listings match the reference sort through a split mid-walk, and count every row", async () => {
    const name = `variants-list-${unique()}`;
    const rows = makeDocs(60);
    await op(name, "putMany", rows.map((row) => ({ row })));
    for (const [q, query] of QUERIES.entries()) {
      const names: string[] = [];
      let cursor: string | null = null;
      for (let page = 0; page < 100; page++) {
        const result: Page = await op<Page>(name, "list", { ...query, cursor });
        names.push(...result.rows.map((r) => r.row.name));
        cursor = result.cursor;
        if (cursor === null) break;
        if (page === 1 && q < 2) await splitFirst(name);
      }
      expect(names).toEqual(expected(rows, query));
    }
    expect(await op(name, "count")).toBe(rows.length);
  }, 120_000);

  test("every write acknowledged during a split is read back after it", async () => {
    const name = `variants-held-${unique()}`;
    await op(name, "putMany", makeDocs(40).map((row) => ({ row })));
    const leaf = (await shards(cls, name))[0]!;
    // The split is slow to hand out: writes to its moving half meet the hold.
    await post("/object", { cls, name, bits: leaf, method: "slowHandOut", args: [400] });
    const splitting = post<boolean>("/split", { cls, name, bits: leaf });
    await Bun.sleep(50);
    const writes = await Promise.all(Array.from({ length: 30 }, (_, i) => op<{ ok?: unknown; threw?: string }>(name, "try:put", { ...makeDocs(1)[0]!, name: `held-${i}`, lastEditAt: i })));
    await splitting;
    // The race happened: writes met the hold. With the default hold wait, they waited for the
    // split rather than being refused.
    const { counters } = await post<{ counters: { held: number; notReady: number } }>("/describe", { cls, name, bits: leaf });
    expect(counters.held).toBeGreaterThan(0);
    if (cls === "tableDefaultHold") expect(counters.notReady).toBe(0);
    const acked = writes.flatMap((w, i) => (w.threw === undefined ? [{ key: `held-${i}`, version: (w.ok as { version: number }).version }] : []));
    expect(acked.length).toBeGreaterThan(0);
    for (const { key, version } of acked) expect({ key, version: (await op<Versioned | null>(name, "get", key))?.version ?? 0 }).toEqual({ key, version });
  }, 120_000);
});
