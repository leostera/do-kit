# Feature flags

Feature flags for every service of a company: a read-heavy control plane in one logical object.
Each flag has a version, and a change applies only to the version it was made from, so two admins
editing the same flag cannot overwrite each other.

```sh
bun dev        # or: npm run dev
```

Turn a flag on, set its rollout, and save: the table below shows which users get it. Open the page
in a second tab, save the same flag in both, and the second save is refused until you reload.

## API

| Request | Answer |
|---|---|
| `GET /api/flags?name=new-checkout&name=dark-mode&user=alice` | `{"new-checkout": true, "dark-mode": false}` |
| `PUT /api/flags/new-checkout {"enabled": true, "rollout": 25, "ifVersion": 0}` | `{"ok": true, "version": 1}`, or 409 with the current version |
| `GET /api/flags/new-checkout` | `{"enabled": true, "rollout": 25, "version": 1}` |
| `GET /api/shards` | The object's shards, for the page |

## What to look at

- `splitPolicy` starts the object on 4 shards (`initialDepth: 2`) and lets load split it up to 16
  (`maxDepth: 4`): every service reads flags on every request, so reads are the load.
- `Flags.set` is a compare-and-set inside `update`: it reads the current version and writes only if
  it matches, in the shard's transaction.
- A user is in a rollout when a hash of the flag and the user falls under the percentage, so the
  answer is the same every time.
