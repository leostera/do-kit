// Browser side, shared by the examples: the page frame, a JSON fetch helper, and the shard panel.
import { Badge, Banner, Surface, Table, Text } from "@cloudflare/kumo";
import { useEffect, useState, type ReactNode } from "react";
import type { ShardInfo } from "./shards.ts";

/** Fetches JSON from the example's API; throws with the server's message on an error status. */
export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(path, { ...init, headers: { "content-type": "application/json", ...init.headers } });
  const text = await response.text();
  if (!response.ok) throw new Error(text || `${response.status}`);
  return (text ? JSON.parse(text) : null) as T;
}

export function Page(props: { title: string; layer: string; children: ReactNode; description: ReactNode }) {
  return (
    <main className="mx-auto flex max-w-5xl flex-col gap-6 p-8">
      <header className="flex flex-col gap-2">
        <div className="flex items-center gap-3">
          <Text variant="heading" size="lg" as="h1">
            {props.title}
          </Text>
          <Badge variant="outline">{props.layer}</Badge>
        </div>
        <Text variant="secondary">{props.description}</Text>
      </header>
      {props.children}
    </main>
  );
}

export function Section(props: { title: string; children: ReactNode; aside?: ReactNode }) {
  return (
    <Surface className="flex flex-col gap-4 rounded-lg p-5">
      <div className="flex items-center justify-between gap-4">
        <Text variant="heading" as="h2">
          {props.title}
        </Text>
        {props.aside}
      </div>
      {props.children}
    </Surface>
  );
}

export function ErrorBanner(props: { error: string | null }) {
  return props.error ? <Banner variant="error" title="Request failed" description={props.error} /> : null;
}

/** The object's shards, polled every second: watch them split under load. */
export function ShardPanel(props: { path?: string }) {
  const [shards, setShards] = useState<ShardInfo[]>([]);
  useEffect(() => {
    let live = true;
    const poll = async () => {
      try {
        const next = await api<ShardInfo[]>(props.path ?? "/api/shards");
        if (live) setShards(next);
      } catch {
        // the next poll tries again
      }
    };
    void poll();
    const timer = setInterval(poll, 1000);
    return () => {
      live = false;
      clearInterval(timer);
    };
  }, [props.path]);
  return (
    <Section title="Shards" aside={<Badge variant="secondary">{shards.length === 1 ? "1 shard" : `${shards.length} shards`}</Badge>}>
      <Text variant="secondary" size="sm">
        Each shard is one Durable Object owning a prefix of the keys' hashes. A shard splits when it stays busy; locally, splits copy rows, on Cloudflare they use storage snapshots.
      </Text>
      <Table>
        <Table.Header>
          <Table.Row>
            <Table.Head>Shard</Table.Head>
            <Table.Head>State</Table.Head>
            <Table.Head>Rows</Table.Head>
            <Table.Head>Calls/s</Table.Head>
            <Table.Head>Last split</Table.Head>
          </Table.Row>
        </Table.Header>
        <Table.Body>
          {shards.map((s) => (
            <Table.Row key={s.bits}>
              <Table.Cell>
                <Text variant="mono">{s.bits}</Text>
              </Table.Cell>
              <Table.Cell>
                <Badge variant={s.state === "shard" ? "success" : s.state === "new" ? "neutral" : "warning"} appearance="dot">
                  {s.state === "new" ? "unused" : s.state}
                </Badge>
              </Table.Cell>
              <Table.Cell>{s.rows === s.storedRows ? s.rows : `${s.rows} (+${s.storedRows - s.rows} being cleaned up)`}</Table.Cell>
              <Table.Cell>{s.callsPerSecond}</Table.Cell>
              <Table.Cell>{s.lastSplitMs === null ? "—" : `${s.lastSplitMs} ms`}</Table.Cell>
            </Table.Row>
          ))}
        </Table.Body>
      </Table>
    </Section>
  );
}
