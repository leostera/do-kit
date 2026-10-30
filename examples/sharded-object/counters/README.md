# Counters

Page views for a whole site, in one logical object: every page is a key. The object starts as one
Durable Object and splits itself when it stays busy, so no single object takes all the traffic.

```sh
bun dev        # or: npm run dev
```

Open the page, press **Send load**, and watch the shard panel. The default split policy splits a
shard after it serves more than 400 calls a second for 5 seconds; locally the page sends about
1,500 hits a second, so the first split comes within seconds, and the halves split again.

## API

| Request | Answer |
|---|---|
| `POST /api/hits {"pages": ["/", "/pricing", "/"]}` | `{"/": 2, "/pricing": 1}`: the new totals |
| `GET /api/counts?page=/&page=/pricing` | `{"/": 2, "/pricing": 1}` |
| `POST /api/load` | About a second of hits on 64 pages: `{"calls": 900}` |
| `GET /api/shards` | The object's shards, for the page |

## What to look at

- `Counters.add` adds in one transaction per shard with `update`: a call for several pages goes
  to each shard that owns some of them, once.
- `sharded(env.COUNTERS, "site").call(pages, "add", ...)` groups the pages by shard and follows
  splits; the Worker never needs to know how many shards there are.
