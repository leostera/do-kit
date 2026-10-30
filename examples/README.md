# Examples

Small apps, each a Worker with a Hono API under `/api` and a React page made with
[Kumo](https://github.com/cloudflare/kumo), grouped by the layer of DO Kit they use. Every page has
a shard panel that shows the object's shards, live, as they split.

| Layer | Example | Shows |
|---|---|---|
| [ShardedObject](../docs/rfds/RFD0001-sharded-object.md) | [counters](sharded-object/counters) | One logical object that splits itself under load: press **Send load** and watch it. |
| | [feature-flags](sharded-object/feature-flags) | A read-heavy control plane; compare-and-set on a version; starting on 4 shards. |
| | [sessions](sharded-object/sessions) | A table of the object's own: `transact`, `cleanupForeign`, alarms that expire rows. |
| [ShardedTable](../docs/rfds/RFD0002-sharded-table.md) | [kv](sharded-table/kv) | A JSON key-value API: ETags for conditional writes, listings by prefix over 16 shards. |
| | [members](sharded-table/members) | Unique handles, a lifecycle in one shard transaction, newest-first listings with counts per status. |
| IndexedTable ([RFD0003](../docs/rfds/RFD0003-indexed-table.md)) | — | Not built yet. |

## Run one

From the repository root, install once, then start an example:

```sh
bun install
cd examples/sharded-object/counters
bun dev                        # or: npm run dev
```

An example's folder also installs on its own (`npm install` there, then `npm run dev`); each has
an `.npmrc` that gets `@cloudflare` packages from the public registry.

`bun dev` runs Vite with the Cloudflare plugin: the Worker and its Durable Objects run locally in
workerd, the page reloads as you edit it, and state lives in the example's `.cloudflare/`. Delete
that folder to start over.

Locally, splits copy rows; on Cloudflare they restore a storage snapshot, which is faster. To try
an example on Cloudflare, run `bun run deploy` in its folder (it needs `cf auth login` first).

## How they are put together

```
examples/
  _shared/           the page frame, a fetch helper, and the shard panel (ui.tsx), and the
                     Worker side of the shard panel (shards.ts)
  <layer>/<example>/
    src/index.ts     the Durable Object class, and the Hono app: the API under /api
    src/app.tsx      the page
    index.html, src/main.tsx, src/app.css
    cloudflare.config.ts   the Worker: static assets for the page, /api to the Worker
    vite.config.ts
```

In this repository the examples import the library's source, through an alias in each
`vite.config.ts`. To start your own project from one, copy its folder and `examples/_shared`, fix
the `_shared` imports, delete the alias, and install the dependencies in its `package.json`.

The tests in [`test/examples.test.ts`](../test/examples.test.ts) start each example and check that
its API does what its README says.
