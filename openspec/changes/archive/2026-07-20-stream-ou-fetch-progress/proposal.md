## Why

The OU fetcher's progress bar measures the wrong thing. It calls
`fetch_organizational_units`, which walks the whole tree and fetches every OU's
tags, and only *then* creates a bar sized by `ous.len()` and increments it while
serializing rows. Every network call has already returned by the time the
bar appears, so it reads `0/N` for the entire fetch and snaps to `N/N` in
milliseconds. The bar's need for a total up front is what forces the collect.

AWS Config never hits this: `build.rs` gets a real total from a cheap
`get_resource_counts` API before fetching starts. AWS Organizations has no count
API, and neither does IAM for the managed-policy fetcher that is coming next — so
fetchers over count-less APIs need a shape of their own, and the OU fetcher is
where it gets established.

## What Changes

- `org_client` gains a client handle type, so a caller can hold a client and make
  more than one call against it. Today's free functions each build their own
  client from a profile and cannot support a pipeline.
- OUs are exposed as an **eager** stream: a producer task walks the tree and
  pushes OUs through a bounded channel, rather than a lazy stream a consumer
  pulls. Eagerness is the point — a lazy stream's discovery is gated by its
  consumer's demand, so its length could never lead its position.
- The stream **increments the progress bar's length as the BFS enqueues** each
  discovered OU, and its position as each tagged OU is emitted. The total grows
  during the walk rather than being known up front.
- Tag fetching moves inside the stream, so each OU carries its own tags instead
  of being zipped back against a positionally-ordered tag pass. Concurrent
  discovery already landed (see below); moving enrichment into the stream is
  what lets discovery outrun it, which is what keeps the bar off 100%.
- The OU fetcher is reduced to mapping `OrganizationalUnit` values onto
  `resources` rows and emitting them; it no longer sizes or drives a bar itself.
- No new dependencies. `futures` is already in both crates, and `indicatif`
  is already an `aws-client` dependency used exactly this way by `config_client`.

## Already Landed Underneath This Change

Commit `f5ab665`, specified by the `harden-fetcher-seam` change, reworked the
same seam after this proposal was written. Three of its results move the
starting line:

- **The BFS is already concurrent.** Each level's child listings run
  `buffer_unordered(CONCURRENCY)` and the tag pass runs `buffered(CONCURRENCY)`,
  with `CONCURRENCY = 8` already a named constant in `org_client`. The *speed*
  this change promised is banked; what remains is the *shape* — eager production
  so the bar's length can lead its position. Concurrency alone does not fix the
  bar, because the walk still collects to a `Vec` before any reporting starts.
- **`Fetcher::fetch` is now dyn-compatible**, returning a boxed `FetchFuture`
  rather than `impl Future`, with `Send + Sync` on the trait. This change's
  Non-Goal of "no trait change" still holds — the streaming rewrite happens
  entirely inside the OU fetcher's `Box::pin(async move { … })` body, and
  `ctx.rows` / `ctx.progress` remain exactly what it needs. But the rewrite must
  be written against the boxed signature, and anything it holds across an await
  must be `Send`.
- **Failure handling is stricter.** Spool write and flush failures now propagate
  instead of being logged, and `Authoritative` landing is transactional. This
  strengthens rather than complicates the "a partial stream must not be mistaken
  for a complete one" guarantee: a stream that errors mid-fetch and a spool that
  fails mid-write now both land nothing.

One deliberate reversal: `harden-fetcher-seam` made the tag pass *ordered*
(`buffered`, zipped back positionally) specifically so tags could not be
misattributed across concurrent completions. Attaching tags inside the stream
retires that constraint at its root — an OU and its tags travel together and are
never re-paired by position — so unordered enrichment becomes safe again here.

Not in scope: extracting a reusable pipeline helper. The managed-policy fetcher
needs a database-consulting filter between discovery and enrichment that OUs do
not, so the abstraction is left to be extracted once a second fetcher has
exercised the shape.

## Capabilities

### New Capabilities
- `fetcher-progress`: what a fetcher's progress reporting must mean — that
  reported progress tracks the underlying work rather than post-hoc
  serialization, and how fetchers over APIs with no count endpoint report a total
  that grows as work is discovered.

### Modified Capabilities
<!-- None. The OU row contract is unchanged: same rows, same hierarchy, same
     attribution, same soft-fail behavior. `organizational-units` and
     `custom-fetchers` requirements are all still satisfied, and their existing
     scenarios serve as the regression suite for this refactor. -->

## Impact

- `aws-client/src/org_client.rs` — new client handle; BFS becomes an eager,
  concurrent, bar-incrementing stream with tags attached inline. The existing
  `fetch_organizational_units_inner` seam is replaced, so its mock-based tests
  are restructured onto the new surface.
- `acd-cli/src/builtin_fetchers/organizational_units.rs` — `fetch` consumes the
  stream inside its existing boxed future; `ou_to_json_line` and its tests are
  untouched.
- `acd-cli/src/build.rs` and `builtin_fetchers.rs` — unchanged. The OU fetcher is
  registered through `builtin_fetchers::all()` and the build no longer names it,
  so reshaping its internals touches nothing there.
- `aws-client` gains an `indicatif` dependency on the Organizations path
  (already a dependency of the crate).
- No database, schema, CLI, or configuration surface changes. No user-visible
  change beyond the progress bar telling the truth and the fetch being faster.
