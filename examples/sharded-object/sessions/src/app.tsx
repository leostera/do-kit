import { Badge, Button, Empty, Input, Table, Text } from "@cloudflare/kumo";
import { KeyIcon, SignInIcon, SignOutIcon } from "@phosphor-icons/react";
import { useEffect, useState } from "react";
import { api, ErrorBanner, Page, Section, ShardPanel } from "../../../_shared/ui.tsx";

interface Started {
  id: string;
  user: string;
  expiresAt: number;
  /** What the server last said: still there, or gone (ended or expired). */
  found: boolean;
}

export function App() {
  const [user, setUser] = useState("alice");
  const [ttl, setTtl] = useState("10");
  const [sessions, setSessions] = useState<Started[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [now, setNow] = useState(Date.now());

  const login = async () => {
    setError(null);
    try {
      const { id, expiresAt } = await api<{ id: string; expiresAt: number }>("/api/sessions", { method: "POST", body: JSON.stringify({ user, ttlSeconds: Number(ttl) }) });
      setSessions((s) => [{ id, user, expiresAt, found: true }, ...s]);
    } catch (e) {
      setError(String(e));
    }
  };

  const logout = async (id: string) => {
    await fetch(`/api/sessions/${id}`, { method: "DELETE" });
  };

  // Asks the server about every session each second: the alarm deletes them as they expire.
  useEffect(() => {
    const timer = setInterval(async () => {
      setNow(Date.now());
      const checked = await Promise.all(sessions.map(async (s) => ({ ...s, found: (await fetch(`/api/sessions/${s.id}`)).ok })));
      setSessions((current) => current.map((s) => checked.find((c) => c.id === s.id) ?? s));
    }, 1000);
    return () => clearInterval(timer);
  }, [sessions]);

  return (
    <Page
      title="Sessions"
      layer="ShardedObject"
      description="Login sessions in a table of the object's own. Each shard sets an alarm for its next expiry and deletes expired sessions when it fires."
    >
      <ErrorBanner error={error} />
      <Section title="Log in">
        <form
          className="flex items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            void login();
          }}
        >
          <Input label="User" value={user} onChange={(e) => setUser(e.target.value)} />
          <Input label="Expires after (seconds)" type="number" min={1} value={ttl} onChange={(e) => setTtl(e.target.value)} />
          <Button variant="primary" type="submit" icon={<SignInIcon />}>
            Start session
          </Button>
        </form>
      </Section>
      <Section title="Sessions started here">
        {sessions.length === 0 ? (
          <Empty icon={<KeyIcon size={40} />} title="No sessions yet" description="Start one above with a short expiry, and watch it go." size="sm" />
        ) : (
          <Table>
            <Table.Header>
              <Table.Row>
                <Table.Head>Session</Table.Head>
                <Table.Head>User</Table.Head>
                <Table.Head>Expires</Table.Head>
                <Table.Head>Server says</Table.Head>
                <Table.Head />
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {sessions.map((s) => (
                <Table.Row key={s.id}>
                  <Table.Cell>
                    <Text variant="mono-secondary">{s.id.slice(0, 8)}</Text>
                  </Table.Cell>
                  <Table.Cell>{s.user}</Table.Cell>
                  <Table.Cell>{s.expiresAt > now ? `in ${Math.ceil((s.expiresAt - now) / 1000)} s` : "now"}</Table.Cell>
                  <Table.Cell>
                    <Badge variant={s.found ? "success" : "neutral"} appearance="dot">
                      {s.found ? "active" : "gone"}
                    </Badge>
                  </Table.Cell>
                  <Table.Cell>
                    {s.found && (
                      <Button size="sm" variant="ghost" icon={<SignOutIcon />} onClick={() => logout(s.id)}>
                        Log out
                      </Button>
                    )}
                  </Table.Cell>
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
