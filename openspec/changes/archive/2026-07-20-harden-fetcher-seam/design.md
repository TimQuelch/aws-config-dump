## Context

The fetcher seam landed with a clear and correct central invariant: *a fetch that
breaks must land nothing, because under `Authoritative` semantics an empty or
short row set is a delete instruction.* `fetch_rows` returns `Ok(None)` on
fetcher error precisely so "the fetch broke" can never be read as "this type is
now empty", and the tests say so explicitly.

Review found the invariant had two holes, both downstream of the check that
enforces it:

1. `util::temp_file_writer` — the spool between a fetcher and the landing step —
   logged write and flush failures and returned `Ok(path)`. A truncated file is
   indistinguishable from a legitimately short one, so a dropped write became a
   delete. The same writer backs the Config fetch, which had the same exposure.
2. `db::land_fetched_rows` ran the `Authoritative` delete and the insert as two
   separate statements on a pooled connection with no transaction. A failure in
   between left the claimed types empty.

Separately, the seam's shape did not match its stated purpose. `Fetcher::fetch`
returned `-> impl Future`. Return-position `impl Trait` in a trait method is not
dyn-compatible, so `Vec<Box<dyn Fetcher>>` was impossible; `build.rs` named
`OrganizationalUnitsFetcher` in three places and had a single `Option<JoinHandle>`
for "the" fetcher. The seam was documented as a registry the managed-policy fetch
would join, but adding fetcher #2 required reworking it first.

Finally, `fetch_organizational_units` walked the tree one parent at a time and
then fetched tags one OU at a time — two serialized round trips per OU.

All of this is implemented in commit `f5ab665`; this change records the resulting
contract.

## Goals / Non-Goals

**Goals:**

- Close both silent-delete paths so the seam's stated invariant actually holds.
- Make the registry plural, so the next fetcher is an entry in a list rather than
  a copy of the dispatch logic.
- Remove the per-OU serialization from the Organizations walk.
- Specify all of the above, so none of it regresses silently.

**Non-Goals:**

- Progress reporting. The OU bar is still built after the fetch completes and
  still reads `0/N` then snaps; fixing that is `stream-ou-fetch-progress`.
- Config-driven (non-compiled-in) fetchers.
- Extracting a reusable discovery/enrichment pipeline helper. That waits for a
  second fetcher to exercise the shape, per the existing design note.
- Any change to row shape, schema, configuration, or CLI surface.

## Decisions

### Propagate spool write errors rather than logging them

The writer task returns `Err` on the first write or flush failure. `fetch_rows`
already `??`s the join handle, so the error surfaces there, and `build.rs`'s
existing `Err(error) => error!(...)` arm turns it into the same soft failure as a
fetcher error: logged, nothing landed, prior rows kept. The desired behavior fell
out of the existing structure; only the swallow had to go.

The task keeps draining the channel after a failure instead of returning early.
Returning early would drop the receiver, and a fetcher still sending would then
block on a full channel or get a send error at an arbitrary point — turning a
disk error into a confusing hang or a different error. Draining costs nothing on
the failure path and keeps the failure attributable.

*Alternative considered:* have `fetch_rows` treat a write failure as
`Ok(None)` rather than `Err`. Rejected — the outcome is identical (nothing
lands), but `Err` keeps the distinction between "the upstream API failed", which
is routine and expected, and "we could not write to local disk", which is not.

### One transaction for the `Authoritative` replace

The delete and insert move into a single `with_conn` closure using an explicit
transaction. `with_conn` hands out `&mut Connection` and the pooled connection is
held for the whole of `land_fetched_rows`, so the staging temp table stays
visible inside the transaction.

Only `Authoritative` needs this. `Merge` is a single `MERGE` statement and is
already atomic, and it deletes nothing, so a partial application is stale rather
than destructive.

*Alternative considered:* a single `DELETE ... ; INSERT ...` batch. Rejected —
DuckDB would still autocommit each statement separately, so it does not actually
close the window.

### Box the fetch future to make `Fetcher` dyn-compatible

`fetch` becomes `fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a>`
where `FetchFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>`.
Implementors wrap their body in `Box::pin(async move { ... })`. `Fetcher` also
gains `Send + Sync`, which the build's `task::spawn` needs and which was
previously satisfied only incidentally through monomorphization.

`fetch_rows` relaxes to `F: Fetcher + ?Sized` so it accepts `&dyn Fetcher`
unchanged; `dyn Fetcher` implements `Fetcher` automatically once the trait is
dyn-compatible.

One heap allocation per fetch, against a call that makes network requests — not
measurable.

*Alternatives considered:* `async_trait` (adds a dependency and boxes anyway);
keeping `-> impl Future` plus a separate erasure wrapper trait (two traits to
understand and implement, for the same result — worse on the simplicity axis
that motivated the review).

### `builtin_fetchers::all()` as the single registration point

Returns `Vec<Box<dyn Fetcher>>`, unconditionally — enabling and profile choice
are config concerns applied by the build, not registry concerns. `build.rs` gains
`spawn_fetchers`, which filters by `fetcher_enabled` and spawns one task each,
and `land_fetcher_output` takes the resulting `Vec` and lands each in turn,
logging and continuing past failures.

Landing stays sequential even though fetching is concurrent: landing is fast
relative to fetching, and serializing it avoids concurrent writers on one
`resources` table for no real gain.

Uniqueness of names and claimed types is enforced by tests over `all()` rather
than at runtime. The registry is a compile-time constant — a duplicate is a
programming error that should fail CI, not a condition to handle in a build.

### Bounded concurrency for the Organizations walk

Each BFS level's child listings run through `buffer_unordered(CONCURRENCY)`;
the tag pass runs through `buffered(CONCURRENCY)`. The walk stays level-by-level
because a level's parent ids are only known once the previous level returns.

Each tag result carries the OU id it belongs to and is matched back by that id,
not by position. The first implementation zipped results onto the OU vector
positionally and used ordered `buffered` to keep that safe — correct, but only
while the combinator preserves order, and silently wrong the moment it does not.

Writing the test for it showed the invariant is *unfalsifiable* here: a mocked
AWS call completes on its first poll, so `buffer_unordered` yields in input order
anyway and no mock-based test can distinguish the two. An invariant that cannot
fail in tests but can fail in production is the worst kind to depend on, so the
ordering requirement was removed rather than guarded — keying by id makes
attribution correct under any combinator, and the test becomes meaningful
because breaking the keying does fail it.

`CONCURRENCY = 8`, matching the value the `stream-ou-fetch-progress` design
already settled on for the same calls. Organizations throttles aggressively and
OU counts are modest, so this captures nearly all of the available speedup while
staying well clear of the rate limits.

*Alternative considered:* rewriting the walk as an eager stream now, per
`stream-ou-fetch-progress`. Rejected for this change — that reshapes the public
`org_client` surface and its tests to fix a *progress reporting* problem. The
concurrency fix is a contained change to the existing function that does not
block it.

## Risks / Trade-offs

- **Higher Organizations API request rate could hit throttling on very large
  organizations** → Bounded at 8 in flight. The SDK retries throttled requests by
  default, and errors propagate as a fetch failure, which soft-fails and keeps
  prior rows.
- **Write errors now fail a fetch that previously "succeeded"** → Intended: those
  successes were silently deleting rows. A build that starts failing here was
  already producing wrong data, and the failure is a soft one that keeps the last
  good rows.
- **Boxed futures make `Fetcher` marginally less ergonomic to implement** →
  One `Box::pin(async move { ... })` wrapper per implementor, the standard
  pre-`dyn async` idiom, in exchange for a registry that can hold more than one
  fetcher.
- **Overlap with `stream-ou-fetch-progress`** → That change's tasks 2.1 and 3.2
  now have their performance half already done, and its `org_client.rs:81-89`
  line references are stale. A note at the top of its `tasks.md` records this;
  whoever picks it up should re-read the current code first.
- **Concurrency-ordering bugs are invisible to mock-based tests** → Confirmed
  during implementation: mocked calls complete on first poll, so completion
  order always equals input order and no fixture can produce the interleaving
  that would expose an ordering bug. Addressed by removing the dependence on
  order (tags keyed by OU id) rather than by testing for it. The remaining
  fixtures do use distinct per-OU tags, so a keying regression fails loudly.
- **Whether calls are genuinely issued concurrently is likewise unobservable
  under mocks** → An in-flight counter would read 1 no matter the combinator,
  for the same reason. Left untested deliberately rather than asserted with a
  test that cannot fail; the concurrency is visible in the code and in wall-clock
  time against a real organization.

## Migration Plan

None. No schema, configuration, CLI, or row-shape surface changes, so existing
databases and config files are unaffected and no rebuild is required. Rollback is
a plain revert.

## Open Questions

- `check_claimed_types_free` runs only on the `FetchSource::Api` path, and runs
  *after* fetchers are already spawned. Harmless today — nothing lands when it
  fails — but weaker than its docstring implies. Worth tightening separately.
- An unknown fetcher name in configuration is silently ignored, so a typo in
  `[databases.x.fetchers.<name>]` reads as "all defaults" rather than an error.
  Out of scope here; a validation pass over the registry's names would fix it.
