# Sessions

Login sessions that expire, kept in a table of the object's own rather than its key-value rows.
It shows the three things a subclass must do with its own tables:

- write them only inside `transact`, which waits while a split copies those keys;
- delete, after a split, the rows of keys a shard gave away (`cleanupForeign`);
- run scheduled work through `onAlarm` and `scheduleAlarm`, and act only on the rows it owns.

```sh
bun dev        # or: npm run dev
```

Start a session with a short expiry and watch the page: each shard sets an alarm for its next
expiry, and the session is gone when it fires.

## API

| Request | Answer |
|---|---|
| `POST /api/sessions {"user": "alice", "ttlSeconds": 3600}` | 201 `{"id": "...", "expiresAt": ...}` |
| `GET /api/sessions/:id` | `{"user": "alice", "expiresAt": ...}`, or 404 once ended or expired |
| `DELETE /api/sessions/:id` | 204 |
| `GET /api/shards` | The object's shards, for the page |

## What to look at

- Each row stores the routing hash of its id (`keyHash`), so a shard can find the rows it no
  longer owns with a range query (`ownedHashRange`).
- After a split, both halves hold every row until cleanup: `onAlarm` filters with `owns`, so a
  session is expired by one shard, not two.
- `sessionCount` counts only owned rows, for the shard panel.
