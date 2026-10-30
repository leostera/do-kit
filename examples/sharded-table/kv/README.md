# Key-value store

A key-value HTTP API on a `ShardedTable` with no indexes. Each value has a version, served as an
ETag, so clients can write "only if new" (`If-None-Match: *`) or "only if unchanged" (`If-Match`).

```sh
bun dev        # or: npm run dev
```

Add the sample keys, open one, edit it and save. Edit the same key with `curl` in between and the
page's save is refused: it was made from an older version.

## API

| Request | Answer |
|---|---|
| `PUT /api/kv/:key` with any JSON, and `If-None-Match: *` or `If-Match: "3"` | 201 (new) or 200, with the ETag; 412 on a version mismatch |
| `GET /api/kv/:key` | 200 with the ETag, or 404 |
| `DELETE /api/kv/:key` with `If-Match: "3"` | 204; 412 on a version mismatch |
| `GET /api/kv?prefix=user:&cursor=...` | `{"keys": [...], "cursor": ...}`, in key order |
| `GET /api/shards` | The table's shards, for the page |

## What to look at

- `defineTable` declares the columns; `ShardedTable` does the rest. There is no method to write.
- The split policy starts on 16 shards and never splits further: each takes 1/16 of the load,
  and a listing asks 16 shards for their rows in key order and merges them.
- The listing's cursor carries on even if the shards split between pages.
