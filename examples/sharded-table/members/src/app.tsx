import { Badge, Button, Empty, Input, Select, Table, Text } from "@cloudflare/kumo";
import { CheckIcon, PlusIcon, TrashIcon, UsersIcon } from "@phosphor-icons/react";
import { useEffect, useState } from "react";
import { api, ErrorBanner, Page, Section, ShardPanel } from "../../../_shared/ui.tsx";

type Status = "invited" | "active" | "removed";
interface Member {
  handle: string;
  displayName: string;
  id: string;
  status: Status;
  createdAt: number;
}
interface Listing {
  members: Member[];
  cursor: string | null;
  total: number;
  byStatus: Record<string, number>;
}

const WORKSPACES = ["acme", "globex", "initech"];
const STATUS_BADGE: Record<Status, "warning" | "success" | "neutral"> = { invited: "warning", active: "success", removed: "neutral" };

export function App() {
  const [workspace, setWorkspace] = useState(WORKSPACES[0]!);
  const [handle, setHandle] = useState("Ada");
  const [listing, setListing] = useState<Listing | null>(null);
  const [error, setError] = useState<string | null>(null);
  const headers = { "x-workspace": workspace };

  const refresh = async (more = false) => {
    const cursor = more && listing?.cursor ? `?cursor=${encodeURIComponent(listing.cursor)}` : "";
    const page = await api<Listing>(`/api/members${cursor}`, { headers });
    setListing(more && listing ? { ...page, members: [...listing.members, ...page.members] } : page);
  };
  useEffect(() => void refresh().catch((e) => setError(String(e))), [workspace]);

  const run = async (request: () => Promise<unknown>) => {
    setError(null);
    try {
      await request();
    } catch (e) {
      setError(String(e).replace(/^Error: /, ""));
    }
    await refresh();
  };
  const invite = () => run(() => api("/api/members", { method: "POST", headers, body: JSON.stringify({ handle }) }));
  const activate = (member: Member) => run(() => api(`/api/members/${member.handle}/activate`, { method: "POST", headers, body: JSON.stringify({ id: member.id }) }));
  const remove = (member: Member) => run(() => api(`/api/members/${member.handle}`, { method: "DELETE", headers, body: JSON.stringify({ id: member.id }) }));

  return (
    <Page
      title="Members"
      layer="ShardedTable"
      description="One sharded table per workspace. Handles are unique ignoring case, because the lowercased handle is the primary key and its shard decides; the listing is newest first, with counts per status from the same calls."
    >
      <ErrorBanner error={error} />
      <Section
        title="Workspace"
        aside={<Select aria-label="Workspace" value={workspace} onValueChange={(v) => setWorkspace(v as string)} items={WORKSPACES.map((w) => ({ value: w, label: w }))} />}
      >
        <form
          className="flex items-end gap-2"
          onSubmit={(e) => {
            e.preventDefault();
            void invite();
          }}
        >
          <div className="grow">
            <Input label="Invite a member" description="Try the same handle twice, in another case." value={handle} onChange={(e) => setHandle(e.target.value)} />
          </div>
          <Button variant="primary" type="submit" icon={<PlusIcon />}>
            Invite
          </Button>
        </form>
      </Section>
      <Section
        title="Members"
        aside={
          listing && (
            <div className="flex gap-2">
              <Badge variant="outline">{listing.total} total</Badge>
              {Object.entries(listing.byStatus).map(([status, count]) => (
                <Badge key={status} variant={STATUS_BADGE[status as Status] ?? "neutral"} appearance="dot">
                  {count} {status}
                </Badge>
              ))}
            </div>
          )
        }
      >
        {!listing || listing.members.length === 0 ? (
          <Empty icon={<UsersIcon size={40} />} title="No members" description={`Invite the first one to ${workspace}.`} size="sm" />
        ) : (
          <Table>
            <Table.Header>
              <Table.Row>
                <Table.Head>Handle</Table.Head>
                <Table.Head>Status</Table.Head>
                <Table.Head>Invited</Table.Head>
                <Table.Head />
              </Table.Row>
            </Table.Header>
            <Table.Body>
              {listing.members.map((member) => (
                <Table.Row key={member.id}>
                  <Table.Cell>
                    <Text bold>{member.displayName}</Text>
                    <Text variant="mono-secondary">{member.handle}</Text>
                  </Table.Cell>
                  <Table.Cell>
                    <Badge variant={STATUS_BADGE[member.status]} appearance="dot">
                      {member.status}
                    </Badge>
                  </Table.Cell>
                  <Table.Cell>{new Date(member.createdAt).toLocaleTimeString()}</Table.Cell>
                  <Table.Cell>
                    <div className="flex justify-end gap-2">
                      {member.status === "invited" && (
                        <Button size="sm" icon={<CheckIcon />} onClick={() => activate(member)}>
                          Activate
                        </Button>
                      )}
                      <Button size="sm" variant="ghost" icon={<TrashIcon />} onClick={() => remove(member)}>
                        Remove
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
      <ShardPanel key={workspace} path={`/api/shards?workspace=${workspace}`} />
    </Page>
  );
}
