## Why

A review of the fetcher seam and its first instance, the OU fetcher, found two
paths that could silently destroy stored rows, a fetch that cost two serialized
AWS round trips per OU, and a trait signature that made a second fetcher
impossible to add without reworking the seam. The fixes are implemented; this
change records the behavior they establish so it is specified rather than merely
observed, and so the next fetcher inherits it.

The two data-loss paths matter because they defeated an invariant the seam was
explicitly built around — that a broken fetch lands nothing. `Authoritative`
semantics turn "fewer rows than last time" into deletions, so any path that
converts a failure into a short-but-successful row set is a silent delete.

## What Changes

- A fetcher's row set becomes all-or-nothing. Failing to write a row to the
  spool file is now a fetch failure rather than a logged warning followed by a
  successful-looking short file, which under `Authoritative` semantics deleted
  every row the truncation dropped.
- Landing an `Authoritative` fetcher's rows becomes atomic. The delete of
  existing rows and the insert of new ones ran as separate statements, so a
  failure between them left the claimed types empty rather than stale — a worse
  outcome than not having run at all.
- Fetchers become independent of one another. One fetcher failing no longer
  prevents a later fetcher's rows from landing.
- The registry becomes genuinely plural. `Fetcher::fetch` returned
  `-> impl Future`, which is not dyn-compatible, so the "registry" could only
  ever hold one fetcher and the build named the OU fetcher directly in three
  places. Fetchers are now held as `Vec<Box<dyn Fetcher>>` behind a single
  registration point, and the build spawns and lands the whole set.
- Fetcher names and claimed resource types are required to be unique across the
  registry — a duplicate name would make one fetcher's configuration govern
  another, and a duplicate claim would make two `Authoritative` landings delete
  each other's rows.
- The OU walk issues its independent AWS calls concurrently. Each BFS level's
  child listings and the whole tag pass previously ran one call at a time,
  costing two serialized round trips per OU.

No breaking changes: no configuration, schema, CLI, or row-shape surface moves.

## Capabilities

### New Capabilities
<!-- None. Every behavior here tightens an existing capability rather than
     introducing a new one; the fetcher seam and the OU fetcher are both
     already specified. -->

### Modified Capabilities
- `custom-fetchers`: a fetcher's emitted rows are all-or-nothing and an
  incomplete row set is a failure; `Authoritative` landing is atomic; fetchers
  are independent so one failing does not stop others landing; the registry
  holds many fetchers, whose names and claimed types must be unique.
- `organizational-units`: the OU walk issues independent Organizations calls
  concurrently, bounded, rather than serially per OU.

## Impact

- `acd-cli/src/util.rs` — `temp_file_writer` propagates write and flush failures
  instead of logging them. Shared with the Config fetch path, which had the same
  silent-truncation exposure.
- `acd-cli/src/db.rs` — `land_fetched_rows` runs its `Authoritative` delete and
  insert in one transaction.
- `acd-cli/src/fetcher.rs` — `Fetcher` gains `Send + Sync` and `fetch` returns a
  boxed `FetchFuture`, making the trait dyn-compatible.
- `acd-cli/src/builtin_fetchers.rs` — gains `all()`, the single registration
  point.
- `acd-cli/src/build.rs` — `fetcher_resource_types` and a new `spawn_fetchers`
  operate over the registry; `land_fetcher_output` lands a set.
- `aws-client/src/org_client.rs` — bounded-concurrency BFS and tag pass.
- No new dependencies. `futures` is already used in `aws-client`.
- Interacts with the pending `stream-ou-fetch-progress` change, which reshapes
  the same OU walk into an eager stream; see design.md.
