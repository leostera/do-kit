# RFD0006 - Co-location: keys that share a shard

- Feature Name: `co-location`
- Status: Implemented
- Author: `leostera`
- Start Date: `2026-11-12`
- Updated: `2026-11-12`
- Discussion PR: none
- Tracking Issue: none

## Summary

A sharded object can declare a **route** for its keys, `routeOf(key) => string`. Keys with the
same route always live in the same shard, through any number of splits, so one transaction
(`update`, `transact`, `callOneShard`) can read and write all of them. Shards own routes by the
hash of the route, just as they own keys by the hash of the key today, and the default route is the
key itself. Rows stay keyed by their full key.

## Motivation

A shard is the unit of a transaction ([RFD0001](RFD0001-sharded-object.md)). Keys route by
`keyHash(key)`, so two related keys almost always land in different shards once a tree splits, and
no transaction sees both. Some rules need exactly that:
- **Git refs.** `refs/heads/a` and `refs/heads/a/b` can't both exist: a ref is a file or a folder,
  never both. Any two refs that can conflict share their first three path components, so routing
  by that prefix puts every ref that can conflict with a given one in its shard. The check is then
  a read of the ref, its folders, and what is below it, in one transaction.
- **Per-tenant invariants**, such as "at most N active sessions per account", with one key per
  session.
- **A parent and its children** written together: an order and its lines, a document and its blocks.

Without routes, an app has to put the group in one key (one big value), or coordinate across
shards, which this layer doesn't do.

## Goals

- Keys with one route live in one shard, before and after any number of splits.
- A transaction over keys of one route never spans shards.
- With the default route, behaviour and stored hashes are unchanged: existing trees stay valid.
- A client and an object that route differently are refused loudly, on the first call.

## Non-goals

- Transactions across routes: two routes are as unrelated as two keys are today.
- Splitting a route. A route that is too hot, or too big, for one shard is one key's worth of shard.
- Changing a tree's route function in place. A tree routed one way is read the same way; a new
  function needs a new tree name.
- Routes on `IndexedTable` and `GlobalIndex` ([RFD0003](RFD0003-indexed-table.md)): their index
  shards call back into the row shards with a client of their own, which has no way to know the
  app's route. Both classes refuse a custom route, and `indexedTable(...)` refuses a `routeOf`.

## Guide-level explanation

Declare the route on the class, and the same function on the client:

```ts
const refRoute = (ref: string) => ref.split("/").slice(0, 3).join("/"); // refs/heads/a/b → refs/heads/a

export class Refs extends ShardedObject<Env, string> {
  protected override routeOf(key: string) { return refRoute(key); }
  protected nodes() { return this.env.REFS; }

  /** Creates a ref, unless git would refuse it: an existing ref is a folder of it, or a file under it. */
  async create(keys: readonly string[], payloads: readonly string[]) {
    const [ref] = keys;
    const parts = ref!.split("/");
    const folders = parts.slice(3).map((_, j) => parts.slice(0, 3 + j).join("/"));
    return this.update([ref!, ...folders], (current) => {
      const below = this.ctx.storage.sql.exec("SELECT key FROM sharded_rows WHERE key > ? AND key < ? LIMIT 1", `${ref}/`, `${ref}0`).toArray();
      if (current.size > 0 || below.length > 0) return { writes: new Map(), result: "conflict" };
      return { writes: new Map([[ref!, payloads[0]!]]), result: "created" };
    });
  }
}

const refs = sharded(env.REFS, "repo-1234", { routeOf: refRoute });
await refs.call(["refs/heads/a/b"], "create", ["c0ffee"]); // "conflict" if refs/heads/a exists
```

`read`, `update`, `transact` and the table client still take full keys. Only where a key lives
changes. In the example the folders and the rows below the ref are in its route, so they are in its
shard. The look below the ref can't see another route's rows, either: a split's leftover rows of
another route sort under other prefixes.

## Reference-level explanation

`ShardedObject.routeOf(key)` defaults to the key. Every place that decides who owns a key uses
`keyHash(routeOf(key))`, the **route hash**, where it used `keyHash(key)` before:
- the client's routing of each key to a shard;
- the `hash` column written by `update` (and `_hash`, and the per-prefix count buckets, of
  `ShardedTable`), which is what splits, cleanup, `ownedRowsPage` and listings filter on;
- `owns`, `assertOwns` and the ownership check of every routed call;
- the hold a split puts on writes to the half that moves;
- the `owns` given to `afterClone` and `cleanupForeign`.

The client takes `routeOf` as an option to `sharded(ns, name, { routeOf })` (and so `table(...)`).
It must be the class's function. A function can't be compared across an RPC, so the client sends
the route hash it computed for each key with the call, and the shard compares it with its own. On
any difference it answers `routeMismatch` without running anything, and the client throws
`RouteMismatch`, naming the key. The check runs before anything else, so a mismatched first call to a
new tree leaves the root new. This matters at the root: the root owns every hash, so without the
check it would accept a key routed the wrong way, and the mismatch would only show (as missing
keys) after the first split.

The default route costs nothing new: `routeHash` is `keyHash(key)`, and the stored hashes are those
of every tree written before. The hashes sent with each call cost 16 bytes a key.

A split is unchanged: it moves a half of the hash space, and with routes that half holds whole
routes. A shard whose load is one route splits like a shard whose load is one key: the half without
it is idle. The split policy can tell, as for one key, with `splittable()`.

### Invariants

1. **One route, one shard:** every key with the same route is owned by the same shard, at every
   moment, through any number of splits.
2. **One transaction per route:** `update`, `transact` and `callOneShard` over keys of one route
   never span shards.
3. **Default unchanged:** without a `routeOf`, every stored hash is `keyHash(key)`.
4. **No silent misroutes:** a call routed by another function than the shard's runs nothing.

Routing is the same function the specs already model: they decide ownership by hash, and a route
hash is a hash. The specs' sources (`route`, `shardedCall`, `holding`, `shardedClaim`, `owns`) were
re-checked and re-locked, not changed. The invariants above are tested under `cf dev`,
`test/colocation.test.ts`:
- keys of 40 routes, five each, written while the tree splits to six shards: each route in one
  shard, every acknowledged write counted once, `callOneShard` over each route, the subclass's own
  tables (cleaned by `cleanupForeign`, filtered by `afterClone`) following routes, and the same keys
  without routes spanning shards;
- git's file/folder conflict before and after splits, and both ends of 16 families raced at once,
  exactly one of each created;
- the stored hashes, with and without a route;
- a mismatch both ways, refused on the first call, the root left new;
- a routed `ShardedTable` listing and counting every row once through a split, before and after
  cleanup, and counting rows written after it;
- a split holding a write whose route moves though its own hash would stay, and not holding one
  whose route stays though its hash would move.

Each place listed above was replaced in turn by `keyHash(key)`, and each replacement fails at
least one of these tests.

## Drawbacks

- A route is a unit of load and size: a route as hot as a shard can take, or as big as 10 GB,
  can't be helped by splitting. Apps choose routes as narrow as their transactions allow.
- The route is declared twice, on the class and on the client. The mismatch check makes a
  disagreement loud, but only per key: a key whose route is itself (or happens to hash the same)
  passes either way, as it should.

## Rationale and alternatives

- **Route by a key prefix the layer picks** (the first N bytes, or up to a separator): simpler to
  declare, but apps' groups aren't always a prefix (an order's lines named by line id). A function
  covers prefixes and the rest.
- **A static on the class instead of a method.** The client can't read a class's statics through a
  namespace binding, so it would still need the function passed in. A method lets a subclass
  compute routes from its own state.
- **Send a fingerprint of the function** instead of per-key hashes: a fingerprint of code is not
  a fingerprint of behaviour, and a missing check is worse than 16 bytes a key.
- **Transactions across shards** would make routes unnecessary, at the cost of a commit protocol
  in every write. Routes keep every transaction in one shard, which is the point of the layer.

## Prior art

- **Spanner's interleaved tables and Cloud Bigtable's row-key design**: rows that are read and
  written together share a key prefix so they share a split.
- **Redis Cluster hash tags**: `{user1}.followers` and `{user1}.following` hash by `user1` only,
  so multi-key commands work on them.
- **CockroachDB and Vitess** shard keys: the first columns of a primary key decide placement.

## Unresolved questions

### Out of scope

- Routes on `IndexedTable`: the index shards' client would need the table's route, which means
  passing it through `GlobalIndex` and `rowNodes()`.

## Future possibilities

- A split policy that recognises a shard hot because of one route, as one hot key.
