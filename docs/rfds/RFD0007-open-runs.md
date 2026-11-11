# RFD0007 - Open runs: adding to a work graph while it runs

- Feature Name: `open-runs`
- Status: Draft
- Author: `leostera`
- Start Date: `2026-11-11`
- Updated: `2026-11-11`
- Discussion PR: none
- Tracking Issue: none

## Summary

A work graph run (RFD0005) starts only once its whole plan is submitted and sealed. This RFD lets
a run start **open**: the app adds the graph in parts (`run.add(part)`), from one request or many,
runners execute each part's chunks as soon as they are ready, and `run.seal()` says no more parts
will come. A run is done only once it is sealed and every chunk added is done. A part's units may
read the results of units in earlier parts, but a tree stays within one part.

## Motivation

Today the app builds the whole graph in one Worker, plans it, submits it, seals it, and only then
starts the runners. For big graphs this costs three ways:

- **Memory.** A graph of a few million units, with their inputs, does not fit a 128 MB isolate
  while it is being planned. Example: indexing every file of 10,000 repositories, one unit per
  file, where listing the files is itself work.
- **Time to first result.** Runners sit idle while the graph is listed, planned and submitted.
  Listing 10,000 repositories takes minutes; the first repository could be done in seconds.
- **Work that discovers work.** RFD0005 leaves dynamic graphs out: a run whose next steps depend
  on results is a sequence of runs. When the next steps only add work (list a directory, then
  index its files), a sequence of runs serializes what could overlap, and each run pays its own
  start and end.

## Goals

- Start running before the graph is complete, and finish in about the time of the last part plus
  its span.
- Add parts from several requests or Workers, each idempotent, so a retried add is harmless.
- Keep RFD0005's guarantees: each chunk's results are durable before it is done, a run ends with
  every chunk done or the first error, and an ephemeral run leaves nothing behind.
- Bound memory: no Worker holds more than one part.

## Non-goals

- Units that spawn units. A task stays a pure function from its input to its value; it does not
  add to the graph. The app adds parts, from outside the run or from a task's results it reads.
- Removing or changing what was added. A part, once added, is part of the run.
- Trees across parts. A unit's parent is in the same part (see Rationale).
- Ordering between parts beyond their edges: a later part may finish first.

## Guide-level explanation

```ts
const run = workGraphRun(env.COORDINATORS, "index-2026-11-11");
await run.open({ runners: 64 });                 // starts runners; nothing to do yet

for await (const repos of listRepositories()) {  // pages of repositories
  const g = workGraphOf({ index, summarize });
  for (const repo of repos) {
    const files = repo.files.map((f) => g.add(index, `${repo.id}/${f.path}`, f, { cost: f.size / 1e5 }));
    g.add(summarize, `${repo.id}/summary`, { repo: repo.id }, { cost: 5, after: Object.fromEntries(files.map((f, i) => [`f${i}`, f])) });
  }
  await run.add(planWorkGraph(g, { earlier: run.index() })); // runners pick these up at once
}

await run.seal();                                // no more parts: the run ends when they are done
```

A part reads an earlier part's results by naming its units: `g.earlier(task, key)` returns a handle
to a unit already added, usable in `after`. `run.index()` is what the planner needs about earlier
units (their hashes and chunks); it is fetched from the coordinator for the keys a part names, so
a part added by another request works the same.

While open, a runner with nothing ready waits (`{ wait }`) instead of finishing. `status()` shows
`open`, the parts and chunks added, and how many are done. An open run that is never sealed ends
at its deadline, as any run does, with the error `not sealed`.

## Reference-level explanation

### States

`submitting → sealed → running → done | failed` becomes:

- `open`: started, accepting parts. Runners lease what is ready and wait otherwise.
- `sealed`: accepting no parts. The run is done when every chunk is done.
- `done`, `failed`, and the tombstone, as in RFD0005.

`submit` + `seal` + `start` stays as it is: a run started sealed is the special case of an open run
sealed before its first lease.

### Adding a part

`graphAdd(name, part, chunks)`, in batches as `graphSubmit` is:

- `part` is the app's name for it, and the add is idempotent per (part, chunk): a retried add
  writes nothing new. The last batch of a part carries `last: true`; a part counts once its last
  batch is in.
- Chunk ids are the coordinator's: it assigns each part a range when the part's first batch
  arrives, and the batch's chunk ids are offsets in it. Needs on earlier parts name chunk ids the
  index returned.
- A chunk may need only chunks already added (in this part, or an earlier one). The coordinator
  refuses a batch that needs an unknown chunk, so a run's graph stays acyclic and every need
  exists by the time the chunk is stored.
- After `seal`, an add is refused.

### The index of earlier units

The planner needs, for each earlier unit a part reads: its hash (the unit's hash covers its
inputs' hashes) and the chunk that stores its result. The coordinator keeps
`graph_units(key, hash, chunk)` only for units that a part marks as readable by later parts
(`g.add(..., { exported: true })`, or every unit with `exportAll`), so a run of millions of units
stores rows only for the few that are read across parts. `graphIndex(name, keys)` answers them.

### Runners

`graphNext` answers `{ wait }` when nothing is ready and the run is open, as it does today while
needed chunks run. An add that stores ready chunks wakes that many waiting runners, as a finished
chunk that readies others does today (`wakeWaiting`, then `graphWake`), so runners start on a part
within a call of it being added.

### Spec

`specs/work-graph-run.ts` gains `add` and `seal` on the client, and these properties:

- **Not done while open:** a run is never done before it is sealed.
- **Everything added runs:** once sealed, every chunk added is done before the run is.
- **Needs exist:** no chunk is stored with a need on a chunk not yet added.

A rule `doneOnlyWhenSealed` should break the first without it.

### Invariants

- A run in `open` or `sealed` with chunks not done is never reported done.
- An add after seal changes nothing.
- A retried add writes nothing new; a chunk is never added twice under two ids.
- Each chunk's results are durable before it is done (unchanged).

## Drawbacks

- **Planning sees one part at a time.** Packing and cuts are per part, so chunks are less well
  packed at part boundaries, and a part's span adds to the run's. Large parts help.
- **More states.** An open run that the app forgets to seal holds its runners until the deadline.
- **A new table** for exported units, and an index round trip per part that reads earlier ones.

## Rationale and alternatives

### Proposed design

Parts added from outside the run keep tasks pure and the run's graph known to the coordinator at
every moment, so the proofs of RFD0005 carry over: the graph only grows, by whole parts, and only
with edges to what exists.

### Trees within one part

A child whose parent is in another chunk loads the parent's value, which that chunk stores only
if the planner knew to export it. A tree split across parts would need the earlier part's chunks
to store values for children not yet known: every value, or a re-run. Results, which every unit
stores, are what crosses parts.

### Alternatives considered

- **Tasks that return new units.** Expressive, but a task is then not a pure function of its
  input, retries may add different work, and a run's end depends on what tasks decide. Rejected:
  the app can add parts from results it reads, at the same cost.
- **A sequence of runs** (today's answer). No overlap between steps, and each run pays its start
  and end. Kept for work whose next steps depend on all of the previous step's results.
- **One coordinator per part, and a parent run.** Splits the coordinator's load, but needs cross
  coordinator needs. Possible later if one coordinator is the bottleneck.

## Prior art

- Bazel and Buck build the action graph lazily as targets are analyzed, and execute actions as
  soon as their inputs exist.
- Dask's `client.submit` adds futures to a running graph; Ray's tasks may submit tasks.
- Spark's structured streaming plans micro-batches against a running job.

## Unresolved questions

- Whether `graph_units` is opt-in per unit (`exported`), or every unit's key is kept with a
  compact encoding.
- Whether a part may be added twice under the same name with different chunks (refused today by
  the idempotency rule), for an app that re-plans a part after a crash.
- How many runners an open run starts with, when the first parts are small: all at `open`, or
  more as parts add work.

### Out of scope

- Units that spawn units (see Non-goals).
- Cancelling one part.

## Future possibilities

- Feeding parts from a Queue consumer, so listing the work is itself spread out.
- Seal on a condition: a run that seals itself once a named unit is done.
