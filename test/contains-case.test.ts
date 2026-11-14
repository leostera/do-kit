// `contains` folds ASCII case only: SQLite's lower() leaves every other letter as it is. Folding
// more would need a stored folded column; until then the docs say ASCII, and the tests'
// reference folds as SQLite does.
import { readFileSync } from "node:fs";
import { expect, test } from "bun:test";
import { post, useWorker } from "./harness.ts";

useWorker();

const unique = () => `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
const NAMES = ["École", "école", "ÉCOLE", "Straße", "STRASSE", "Ａ", "Doc-A", "doc-a"];

test("contains folds ASCII case only", async () => {
  const name = `contains-case-${unique()}`;
  for (const [i, n] of NAMES.entries()) await post("/table", { cls: "table", name, op: "put", args: [{ name: n, id: `id${i}`, createdAt: i, lastEditAt: null, deleted: false, meta: null }] });
  const search = async (value: string) => (await post<{ rows: { row: { name: string } }[] }>("/table", { cls: "table", name, op: "list", args: [{ index: "byCreated", where: [{ column: "name", op: "contains", value }], limit: 50 }] })).rows.map((r) => r.row.name).sort();
  expect(await search("école")).toEqual(["école"]);
  expect(await search("ÉCOLE")).toEqual(["ÉCOLE", "École"].sort());
  expect(await search("ａ")).toEqual([]);
  expect(await search("strasse")).toEqual(["STRASSE"]);
  expect(await search("DOC-A")).toEqual(["Doc-A", "doc-a"].sort());
}, 30_000);

test("the docs say contains ignores ASCII case, and the tests' reference folds only ASCII", () => {
  const says = (path: string) => readFileSync(new URL(`../${path}`, import.meta.url), "utf8");
  expect({
    readme: /`contains` \(ignoring ASCII case\)/.test(says("README.md")),
    schema: /`contains` ignores ASCII case/.test(says("src/table/schema.ts")),
    rfd: /`contains` \(substring, ignoring ASCII case\)/.test(says("docs/rfds/RFD0002-sharded-table.md")),
    reference: !says("test/table.test.ts").includes("v.toLowerCase().includes"),
  }).toEqual({ readme: true, schema: true, rfd: true, reference: true });
});
