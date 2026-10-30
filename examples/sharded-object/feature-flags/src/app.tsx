import { Badge, Button, Input, Slider, Switch, Table, Text } from "@cloudflare/kumo";
import { ArrowsClockwiseIcon, FloppyDiskIcon } from "@phosphor-icons/react";
import { useEffect, useState } from "react";
import { api, ErrorBanner, Page, Section, ShardPanel } from "../../../_shared/ui.tsx";

interface Flag {
  enabled: boolean;
  rollout: number;
  version: number;
}

const NAMES = ["new-checkout", "dark-mode", "beta-search", "faster-builds"];
const USERS = ["alice", "bob", "carol", "dave", "erin", "frank", "grace", "heidi"];

/** One flag's editor. It keeps the version it loaded, so saving over someone else's change fails. */
function FlagEditor(props: { name: string; onSaved: () => void; onError: (e: string | null) => void }) {
  const [loaded, setLoaded] = useState<Flag | null>(null);
  const [enabled, setEnabled] = useState(false);
  const [rollout, setRollout] = useState(0);
  const [conflict, setConflict] = useState(false);

  const load = async () => {
    const flag = await api<Flag>(`/api/flags/${props.name}`).catch(() => null);
    setLoaded(flag);
    setEnabled(flag?.enabled ?? false);
    setRollout(flag?.rollout ?? 0);
    setConflict(false);
  };
  useEffect(() => void load(), [props.name]);

  const save = async () => {
    props.onError(null);
    const response = await fetch(`/api/flags/${props.name}`, { method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify({ enabled, rollout, ifVersion: loaded?.version ?? 0 }) });
    if (response.status === 409) return setConflict(true);
    if (!response.ok) return props.onError(await response.text());
    await load();
    props.onSaved();
  };

  return (
    <Table.Row>
      <Table.Cell>
        <Text variant="mono">{props.name}</Text>
      </Table.Cell>
      <Table.Cell>
        <Switch checked={enabled} onClick={() => setEnabled(!enabled)} aria-label={`${props.name} enabled`} />
      </Table.Cell>
      <Table.Cell className="w-64">
        <Slider value={rollout} onValueChange={(v) => setRollout(v as number)} getAriaLabel={() => `${props.name} rollout`} format={{ style: "unit", unit: "percent" }} />
      </Table.Cell>
      <Table.Cell>{loaded ? `v${loaded.version}` : <Badge variant="neutral">new</Badge>}</Table.Cell>
      <Table.Cell>
        {conflict ? (
          <Button size="sm" variant="secondary-destructive" icon={<ArrowsClockwiseIcon />} onClick={load}>
            Changed elsewhere: reload
          </Button>
        ) : (
          <Button size="sm" icon={<FloppyDiskIcon />} onClick={save}>
            Save
          </Button>
        )}
      </Table.Cell>
    </Table.Row>
  );
}

export function App() {
  const [error, setError] = useState<string | null>(null);
  const [extra, setExtra] = useState("");
  const [names, setNames] = useState(NAMES);
  const [answers, setAnswers] = useState<Record<string, Record<string, boolean>>>({});

  // What each user gets: one call per shard holding the flags, as a service would make.
  const refresh = async () => {
    const query = names.map((n) => `name=${encodeURIComponent(n)}`).join("&");
    const entries = await Promise.all(USERS.map(async (user) => [user, await api<Record<string, boolean>>(`/api/flags?${query}&user=${user}`)] as const));
    setAnswers(Object.fromEntries(entries));
  };
  useEffect(() => void refresh().catch((e) => setError(String(e))), [names]);

  return (
    <Page
      title="Feature flags"
      layer="ShardedObject"
      description="Flags for every service, in one logical object starting on 4 shards. Each save is a compare-and-set on the flag's version: open this page in two tabs and save the same flag in both."
    >
      <ErrorBanner error={error} />
      <Section title="Flags">
        <Table>
          <Table.Header>
            <Table.Row>
              <Table.Head>Flag</Table.Head>
              <Table.Head>Enabled</Table.Head>
              <Table.Head>Rollout</Table.Head>
              <Table.Head>Version</Table.Head>
              <Table.Head />
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {names.map((name) => (
              <FlagEditor key={name} name={name} onSaved={refresh} onError={setError} />
            ))}
          </Table.Body>
        </Table>
        <form
          className="flex items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            if (/^[\w-]+$/.test(extra) && !names.includes(extra)) setNames([...names, extra]);
            setExtra("");
          }}
        >
          <Input size="sm" placeholder="another-flag" value={extra} onChange={(e) => setExtra(e.target.value)} />
          <Button size="sm" type="submit">
            Add flag
          </Button>
        </form>
      </Section>
      <Section title="What users get">
        <Text variant="secondary" size="sm">
          A user is in a rollout when a hash of the flag and the user falls under the percentage, so each user gets the same answer every time.
        </Text>
        <Table>
          <Table.Header>
            <Table.Row>
              <Table.Head>User</Table.Head>
              {names.map((n) => (
                <Table.Head key={n}>{n}</Table.Head>
              ))}
            </Table.Row>
          </Table.Header>
          <Table.Body>
            {USERS.map((user) => (
              <Table.Row key={user}>
                <Table.Cell>{user}</Table.Cell>
                {names.map((n) => (
                  <Table.Cell key={n}>{answers[user]?.[n] ? <Badge variant="success">on</Badge> : <Badge variant="neutral">off</Badge>}</Table.Cell>
                ))}
              </Table.Row>
            ))}
          </Table.Body>
        </Table>
      </Section>
      <ShardPanel />
    </Page>
  );
}
