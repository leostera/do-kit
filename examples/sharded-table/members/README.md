# Members

The members of a workspace: handles are unique per workspace ignoring case, members go through a
lifecycle (`invited`, `active`, `removed`), and the workspace lists them a shard at a time, newest
first within each.

```sh
bun dev        # or: npm run dev
```

Invite a few members, then try a handle again in another case: it is taken. Activate one, remove
another, and watch the list. Switch workspaces: each has its own table.

## API

Every request takes the workspace in an `x-workspace` header, or `?workspace=` (`demo` when neither).

| Request | Answer |
|---|---|
| `POST /api/members {"handle": "Ada"}` | 201 `{"handle": "ada", "id": "...", "status": "invited", ...}`; 409 if taken |
| `POST /api/members/:handle/activate {"id": "..."}` | The member, now `active`; 409 if it is not that member or not allowed |
| `DELETE /api/members/:handle {"id": "..."}` | 204, marked `removed` |
| `GET /api/members?cursor=...` | `{"members": [...], "cursor": ...}` |
| `GET /api/shards` | The workspace's shards, for the page |

## What to look at

- The primary key is the lowercased handle, so uniqueness is a conditional write on one shard:
  `put(member, { ifVersion: 0 })`.
- `Members.setStatus` is a method of the table's own: it checks the id and the lifecycle and
  patches the row in one `transact`, and is reached with `sharded(...)` on the same shards.
- The listing walks the shards one at a time, each through its `byCreated` index newest first, and
  filters out removed members: a page is one call however many shards the workspace has.
- Listing every member newest first, counting them per status, or finding a member by its id, not
  its handle, would ask every shard: that is what
  [IndexedTable](../../../docs/rfds/RFD0003-indexed-table.md) is for ([RFD0012](../../../docs/rfds/RFD0012-shard-local-tables.md)).
