import { Badge, Button, Empty, Input, InputArea, Table, Text } from "@cloudflare/kumo";
import { DatabaseIcon, FloppyDiskIcon, MagnifyingGlassIcon, TrashIcon } from "@phosphor-icons/react";
import { useEffect, useState } from "react";
import { api, ErrorBanner, Page, Section, ShardPanel } from "../../../_shared/ui.tsx";

/** The key open in the editor, and the version (ETag) it was loaded at: 0 for a new key. */
interface Open {
  key: string;
  version: number;
}

const SAMPLES: [string, unknown][] = [
  ["user:alice", { plan: "pro", seats: 5 }],
  ["user:bob", { plan: "free" }],
  ["user:carol", { plan: "team", seats: 12 }],
  ["team:core", { members: ["alice", "carol"] }],
  ["team:infra", { members: ["bob"] }],
];

export function App() {
  const [prefix, setPrefix] = useState("");
  const [keys, setKeys] = useState<string[]>([]);
  const [cursor, setCursor] = useState<string | null>(null);
  const [open, setOpen] = useState<Open>({ key: "user:alice", version: 0 });
  const [value, setValue] = useState('{\n  "plan": "pro"\n}');
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const list = async (more = false) => {
    const query = new URLSearchParams(prefix ? { prefix } : {});
    if (more && cursor) query.set("cursor", cursor);
    const page = await api<{ keys: string[]; cursor: string | null }>(`/api/kv?${query}`);
    setKeys(more ? [...keys, ...page.keys] : page.keys);
    setCursor(page.cursor);
  };
  useEffect(() => {
    void list().catch((e) => setError(String(e)));
    void load(open.key);
  }, []);

  const load = async (key: string) => {
    setError(null);
    setNotice(null);
    const response = await fetch(`/api/kv/${encodeURIComponent(key)}`);
    if (!response.ok) return setOpen({ key, version: 0 });
    setOpen({ key, version: Number(response.headers.get("etag")?.replaceAll('"', "")) });
    setValue(JSON.stringify(await response.json(), null, 2));
  };

  // Writes only if the key is still at the version it was loaded at: "If-None-Match: *" for a
  // new key, "If-Match" with its ETag otherwise.
  const save = async () => {
    setError(null);
    setNotice(null);
    let body: unknown;
    try {
      body = JSON.parse(value);
    } catch {
      return setError("The value is not valid JSON.");
    }
    const condition: Record<string, string> = open.version === 0 ? { "if-none-match": "*" } : { "if-match": `"${open.version}"` };
    const response = await fetch(`/api/kv/${encodeURIComponent(open.key)}`, { method: "PUT", headers: { "content-type": "application/json", ...condition }, body: JSON.stringify(body) });
    if (response.status === 412) return setNotice(`Someone changed ${open.key} since you loaded it (now at ${response.headers.get("etag")}). Load it again before saving.`);
    if (!response.ok) return setError(await response.text());
    setOpen({ ...open, version: Number(response.headers.get("etag")?.replaceAll('"', "")) });
    await list();
  };

  const remove = async () => {
    const response = await fetch(`/api/kv/${encodeURIComponent(open.key)}`, { method: "DELETE", headers: { "if-match": `"${open.version}"` } });
    if (response.status === 412) return setNotice(`${open.key} changed since you loaded it: load it again before deleting.`);
    setOpen({ ...open, version: 0 });
    await list();
  };

  const seed = async () => {
    await Promise.all(SAMPLES.map(([key, v]) => api(`/api/kv/${key}`, { method: "PUT", body: JSON.stringify(v) })));
    await list();
  };

  return (
    <Page
      title="Key-value store"
      layer="ShardedTable"
      description="A JSON key-value API on a table spread over 16 shards. Versions are ETags, so writes can be conditional; listings come back in key order across every shard."
    >
      <ErrorBanner error={error} />
      <div className="grid gap-6 md:grid-cols-2">
        <Section title="Keys" aside={<Button size="sm" onClick={seed}>Add sample keys</Button>}>
          <form
            className="flex items-end gap-2"
            onSubmit={(e) => {
              e.preventDefault();
              void list();
            }}
          >
            <div className="grow">
              <Input size="sm" placeholder="Prefix, like user:" value={prefix} onChange={(e) => setPrefix(e.target.value)} />
            </div>
            <Button size="sm" type="submit" icon={<MagnifyingGlassIcon />}>
              List
            </Button>
          </form>
          {keys.length === 0 ? (
            <Empty icon={<DatabaseIcon size={40} />} title="No keys" description="Save one, or add the sample keys." size="sm" />
          ) : (
            <Table>
              <Table.Body>
                {keys.map((key) => (
                  <Table.Row key={key} variant={key === open.key ? "selected" : "default"} onClick={() => load(key)} className="cursor-pointer">
                    <Table.Cell>
                      <Text variant="mono">{key}</Text>
                    </Table.Cell>
                  </Table.Row>
                ))}
              </Table.Body>
            </Table>
          )}
          {cursor && (
            <Button size="sm" variant="ghost" onClick={() => list(true)}>
              More
            </Button>
          )}
        </Section>
        <Section title="Value" aside={<Badge variant={open.version ? "secondary" : "neutral"}>{open.version ? `ETag "${open.version}"` : "new key"}</Badge>}>
          <Input size="sm" label="Key" value={open.key} onChange={(e) => setOpen({ key: e.target.value, version: 0 })} onBlur={() => load(open.key)} />
          <InputArea label="JSON" rows={8} className="font-mono" value={value} onChange={(e) => setValue(e.target.value)} />
          {notice && (
            <Text variant="error" size="sm">
              {notice}
            </Text>
          )}
          <div className="flex gap-2">
            <Button variant="primary" icon={<FloppyDiskIcon />} onClick={save}>
              {open.version ? "Save if unchanged" : "Create if new"}
            </Button>
            {open.version > 0 && (
              <Button variant="secondary-destructive" icon={<TrashIcon />} onClick={remove}>
                Delete
              </Button>
            )}
          </div>
        </Section>
      </div>
      <ShardPanel />
    </Page>
  );
}
