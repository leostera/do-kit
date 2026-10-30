# Members

The members of a workspace: handles are unique per workspace ignoring case, members go through a
lifecycle (`invited`, `active`, `removed`), and the workspace lists them newest first with how
many there are in each state.

```sh
bun dev        # or: npm run dev
```

Invite a few members, then try a handle again in another case: it is taken. Activate one, remove
another, and watch the counts. Switch workspaces: each has its own table.

## API

Every request takes the workspace in an `x-workspace` header, or `?workspace=` (`demo` when neither).

| Request | Answer |
|---|---|
| `POST /api/members {"handle": "Ada"}` | 201 `{"handle": "ada", "id": "...", "status": "invited", ...}`; 409 if taken |
| `POST /api/members/:handle/activate {"id": "..."}` | The member, now `active`; 409 if it is not that member or not allowed |
| `DELETE /api/members/:handle {"id": "..."}` | 204, marked `removed` |
| `GET /api/members?cursor=...` | `{"members": [...], "cursor": ..., "total": 2, "byStatus": {"active": 1, "invited": 1}}` |
| `GET /api/shards` | The workspace's shards, for the page |

## What to look at

- The primary key is the lowercased handle, so uniqueness is a conditional write on one shard:
  `put(member, { ifVersion: 0 })`.
- `Members.setStatus` is a method of the table's own: it checks the id and the lifecycle and
  patches the row in one `transact`, and is reached with `sharded(...)` on the same shards.
- The listing walks the `byCreated` index newest first, filters out removed members, and gets the
  counts of the `byStatus` group from the same calls (`withCount`).
- Finding a member by its id, not its handle, would ask every shard: that is what
  [IndexedTable](../../../docs/rfds/RFD0003-indexed-table.md) is for.
