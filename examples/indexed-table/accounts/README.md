# Accounts

Accounts keyed by id, found by email and listed by team. Neither is the primary key, so a sharded
table would have to ask every shard. An `IndexedTable` keeps a global index for each, on shards of
their own, and a write returns once both have it.

```sh
bun dev        # or: npm run dev
```

Create a few accounts, then try an email again: it is taken, whichever team it is in. Find one by
its email. Move one to another team, and it leaves this team's list for that one.

## API

| Request | Answer |
|---|---|
| `POST /api/accounts {"email": "ada@example.com", "name": "Ada", "team": "core"}` | 201 the account; 409 if the email is taken |
| `GET /api/accounts/by-email/:email` | The account; 404 |
| `PATCH /api/accounts/:id {"email": "...", "team": "..."}` | The account; 409 if the email is taken; 404 |
| `DELETE /api/accounts/:id` | 204 |
| `GET /api/teams/:team?cursor=...` | `{"accounts": [...], "cursor": ...}`, newest first |
| `GET /api/shards`, `GET /api/index-shards` | The row shards and the index shards, for the page |

## What to look at

- `byEmail` is a **unique** global index: `put(account, { ifVersion: 0 })` fails with `conflict`
  if another account has the email, on any shard. `lookup("byEmail", [email])` is one call to one
  index shard.
- `byTeam` is **partitioned** by its first column: a team's entries live on one index shard,
  sorted by `createdAt`, so `list({ index: "byTeam", partition: [team] })` reads one shard a page.
- Two classes: `Accounts` (the rows) and `AccountIndexes` (the entries of both indexes). Each
  names the other's binding, and both split by load on their own.
- Changing an account's email or team is one `patch`: the row shard writes the row and the index
  changes together, and sends them before it answers.
