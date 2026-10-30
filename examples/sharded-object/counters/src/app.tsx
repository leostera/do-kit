import { Button, Input, Table, Text } from "@cloudflare/kumo";
import { LightningIcon, PlusIcon, StopIcon } from "@phosphor-icons/react";
import { useEffect, useRef, useState } from "react";
import { api, ErrorBanner, Page, Section, ShardPanel } from "../../../_shared/ui.tsx";

const LOAD_PAGES = Array.from({ length: 64 }, (_, i) => `/page/${i}`);

export function App() {
  const [pages, setPages] = useState("/, /pricing, /");
  const [totals, setTotals] = useState<Record<string, number>>({});
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [rate, setRate] = useState(0);
  const [top, setTop] = useState<[string, number][]>([]);
  const running = useRef(false);

  const hit = async () => {
    setError(null);
    try {
      setTotals(await api("/api/hits", { method: "POST", body: JSON.stringify({ pages: pages.split(",").map((p) => p.trim()).filter(Boolean) }) }));
    } catch (e) {
      setError(String(e));
    }
  };

  // While on, four requests at a time, each about a second of hits.
  const toggleLoad = () => {
    if (running.current) {
      running.current = false;
      setLoading(false);
      return;
    }
    running.current = true;
    setLoading(true);
    for (let i = 0; i < 4; i++) {
      void (async () => {
        while (running.current) {
          const started = Date.now();
          try {
            const { calls } = await api<{ calls: number }>("/api/load", { method: "POST" });
            setRate((r) => Math.round(r * 0.75 + ((calls * 1000) / (Date.now() - started)) * 4 * 0.25));
          } catch (e) {
            setError(String(e));
          }
        }
      })();
    }
  };

  useEffect(() => {
    const refresh = async () => {
      const query = LOAD_PAGES.map((p) => `page=${encodeURIComponent(p)}`).join("&");
      const counts = await api<Record<string, number>>(`/api/counts?${query}`).catch(() => ({}));
      setTop(Object.entries(counts).filter(([, n]) => n > 0).sort((a, b) => b[1] - a[1]).slice(0, 8));
    };
    const timer = setInterval(refresh, 1500);
    return () => clearInterval(timer);
  }, []);

  return (
    <Page
      title="Counters"
      layer="ShardedObject"
      description="Page views for a whole site, in one logical Durable Object. Every page is a key; the object splits itself into shards when it gets busy, and the Worker follows the splits."
    >
      <ErrorBanner error={error} />
      <Section title="Count some hits">
        <div className="flex items-end gap-2">
          <div className="grow">
            <Input label="Pages, comma-separated" value={pages} onChange={(e) => setPages(e.target.value)} />
          </div>
          <Button variant="primary" icon={<PlusIcon />} onClick={hit}>
            Hit
          </Button>
        </div>
        {Object.keys(totals).length > 0 && (
          <Text variant="mono">
            {Object.entries(totals)
              .map(([page, n]) => `${page}: ${n}`)
              .join("   ")}
          </Text>
        )}
      </Section>
      <Section
        title="Load"
        aside={
          <Button variant={loading ? "destructive" : "primary"} icon={loading ? <StopIcon /> : <LightningIcon />} onClick={toggleLoad}>
            {loading ? "Stop" : "Send load"}
          </Button>
        }
      >
        <Text variant="secondary">
          Sends hits to 64 pages as fast as it can. A shard splits once it serves over 400 calls a second for five seconds: keep it running and watch the shards below.
          {loading ? ` About ${rate} hits/s.` : ""}
        </Text>
        {top.length > 0 && (
          <Table>
            <Table.Header>
              <Table.Row>
                <Table.Head>Busiest pages</Table.Head>
                <Table.Head>Hits</Table.Head>
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {top.map(([page, n]) => (
                <Table.Row key={page}>
                  <Table.Cell>
                    <Text variant="mono">{page}</Text>
                  </Table.Cell>
                  <Table.Cell>{n}</Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        )}
      </Section>
      <ShardPanel />
    </Page>
  );
}
