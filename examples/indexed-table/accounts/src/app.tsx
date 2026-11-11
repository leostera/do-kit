import { Badge, Button, Empty, Input, Select, Table, Text } from "@cloudflare/kumo";
import { MagnifyingGlassIcon, PlusIcon, TrashIcon, UsersIcon } from "@phosphor-icons/react";
import { useEffect, useState } from "react";
import { api, ErrorBanner, Page, Section, ShardPanel } from "../../../_shared/ui.tsx";

interface Account {
  id: string;
  email: string;
  name: string;
  team: string;
  createdAt: number;
}
interface Listing {
  accounts: Account[];
  cursor: string | null;
}

const TEAMS = ["core", "growth", "support"];

export function App() {
  const [team, setTeam] = useState(TEAMS[0]!);
  const [email, setEmail] = useState("ada@example.com");
  const [name, setName] = useState("Ada");
  const [listing, setListing] = useState<Listing | null>(null);
  const [found, setFound] = useState<Account | "none" | null>(null);
  const [error, setError] = useState<string | null>(null);

  const refresh = async (more = false) => {
    const cursor = more && listing?.cursor ? `?cursor=${encodeURIComponent(listing.cursor)}` : "";
    const page = await api<Listing>(`/api/teams/${team}${cursor}`);
    setListing(more && listing ? { ...page, accounts: [...listing.accounts, ...page.accounts] } : page);
  };
  useEffect(() => void refresh().catch((e) => setError(String(e))), [team]);

  const run = async (request: () => Promise<unknown>) => {
    setError(null);
    try {
      await request();
    } catch (e) {
      setError(String(e).replace(/^Error: /, ""));
    }
    await refresh();
  };
  const create = () => run(() => api("/api/accounts", { method: "POST", body: JSON.stringify({ email, name, team }) }));
  const remove = (account: Account) => run(() => api(`/api/accounts/${account.id}`, { method: "DELETE" }));
  const move = (account: Account, to: string) => run(() => api(`/api/accounts/${account.id}`, { method: "PATCH", body: JSON.stringify({ team: to }) }));
  const find = async () => {
    setError(null);
    setFound(await api<Account>(`/api/accounts/by-email/${encodeURIComponent(email)}`).catch(() => "none" as const));
  };

  return (
    <Page
      title="Accounts"
      layer="IndexedTable"
      description="Rows are keyed by id. Two global indexes live on shards of their own: a unique one on email, so no two accounts share one and a lookup is one call, and one partitioned by team, so a team's page is one call. A write returns once both have it."
    >
      <ErrorBanner error={error} />
      <Section title="Team" aside={<Select aria-label="Team" value={team} onValueChange={(v) => setTeam(v as string)} items={TEAMS.map((t) => ({ value: t, label: t }))} />}>
        <form
          className="flex items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            void create();
          }}
        >
          <div className="grow">
            <Input label="Email" description="Unique across every team: try the same one twice." value={email} onChange={(e) => setEmail(e.target.value)} />
          </div>
          <Input label="Name" value={name} onChange={(e) => setName(e.target.value)} />
          <Button variant="primary" type="submit" icon={<PlusIcon />}>
            Create
          </Button>
          <Button type="button" icon={<MagnifyingGlassIcon />} onClick={find}>
            Find by email
          </Button>
        </form>
        {found && (
          <Text variant="secondary" size="sm">
            {found === "none" ? "No account has that email." : `${found.name} (${found.id}), in ${found.team}.`}
          </Text>
        )}
      </Section>
      <Section title={`Accounts in ${team}`} aside={listing && <Badge variant="outline">{listing.accounts.length} shown</Badge>}>
        {!listing || listing.accounts.length === 0 ? (
          <Empty icon={<UsersIcon size={40} />} title="No accounts" description={`Create the first one in ${team}.`} size="sm" />
        ) : (
          <Table>
            <Table.Header>
              <Table.Row>
                <Table.Head>Account</Table.Head>
                <Table.Head>Created</Table.Head>
                <Table.Head />
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {listing.accounts.map((account) => (
                <Table.Row key={account.id}>
                  <Table.Cell>
                    <Text bold>{account.name}</Text>
                    <Text variant="mono-secondary">{account.email}</Text>
                  </Table.Cell>
                  <Table.Cell>{new Date(account.createdAt).toLocaleTimeString()}</Table.Cell>
                  <Table.Cell>
                    <div className="flex justify-end gap-2">
                      {TEAMS.filter((t) => t !== team).map((t) => (
                        <Button key={t} size="sm" onClick={() => move(account, t)}>
                          Move to {t}
                        </Button>
                      ))}
                      <Button size="sm" variant="ghost" icon={<TrashIcon />} onClick={() => remove(account)}>
                        Delete
                      </Button>
                    </div>
                  </Table.Cell>
                </Table.Row>
              ))}
            </Table.Body>
          </Table>
        )}
        {listing?.cursor && (
          <Button size="sm" variant="ghost" onClick={() => refresh(true)}>
            More
          </Button>
        )}
      </Section>
      <ShardPanel title="Row shards" />
      <ShardPanel title="Index shards" path="/api/index-shards" />
    </Page>
  );
}
