> **Retroactive change.** Sections 1–5 were implemented in commit `f5ab665`
> ("Fix data-loss holes and serial fetch in fetchers") before this change was
> written, and are checked off. Section 6 lists test coverage the implementation
> did *not* add — each specified behavior there currently rests on reasoning
> rather than on a test. Section 7 is verification. Start at 6.1.

## 1. Spool write failures become fetch failures

- [x] 1.1 Make `util::temp_file_writer`'s task return `Err` on the first
  `write_all` or `flush` failure instead of logging and returning `Ok(path)`
- [x] 1.2 Keep draining the receiver after a failure, so a still-sending fetcher
  never blocks on a full channel or gets an unrelated send error
- [x] 1.3 Include the spool path in the error so a disk failure is attributable
- [x] 1.4 Drop the now-unused `tracing::error` import from `util.rs`
- [x] 1.5 Confirm the error reaches `build.rs`'s existing `Err(_) => error!(...)`
  arm via `fetch_rows`'s `file_handle.await??`, i.e. soft-fails and lands nothing

## 2. Atomic authoritative landing

- [x] 2.1 Move `land_fetched_rows`'s `Authoritative` delete and insert into a
  single `with_conn` closure wrapped in an explicit transaction
- [x] 2.2 Keep the row count returned by the delete for the existing debug log
- [x] 2.3 Leave the `Merge` path alone — a single `MERGE` is already atomic and
  deletes nothing

## 3. Dyn-compatible fetcher trait

- [x] 3.1 Add `pub type FetchFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>`
- [x] 3.2 Change `Fetcher::fetch` to `fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a>`
  and add `Send + Sync` supertraits
- [x] 3.3 Relax `fetch_rows` to `F: Fetcher + ?Sized` so it takes `&dyn Fetcher`
- [x] 3.4 Update the implementors — `OrganizationalUnitsFetcher`, and the
  `FakeFetcher` and `DbReadingFetcher` test doubles — to `Box::pin(async move { … })`

## 4. Plural registry

- [x] 4.1 Add `builtin_fetchers::all() -> Vec<Box<dyn Fetcher>>` as the single
  registration point, unconditional (config is the build's concern)
- [x] 4.2 Rewrite `fetcher_resource_types` to take the registry and collect the
  claims of every enabled fetcher
- [x] 4.3 Add `spawn_fetchers`, which returns early under a non-fetching
  `FetchSource`, filters by `fetcher_enabled`, and spawns one task per fetcher
- [x] 4.4 Change `land_fetcher_output` to take a `Vec<FetcherHandle>` and land
  each in turn, logging and continuing past a failure
- [x] 4.5 Remove the three direct references to `OrganizationalUnitsFetcher` from
  `build.rs`
- [x] 4.6 Add registry tests: names are unique; no two fetchers claim the same
  resource type
- [x] 4.7 Add a build test that a disabled fetcher is not spawned while its
  neighbours are
- [x] 4.8 Add a build test that a failing fetcher does not stop a later one landing
- [x] 4.9 Repoint the existing `no_fetch_build_never_runs_a_fetcher` test at the
  real `spawn_fetchers` gate rather than a hand-copy of it

## 5. Concurrent Organizations walk

- [x] 5.1 Add a bounded `CONCURRENCY` constant to `org_client` with a comment on
  why it is bounded
- [x] 5.2 Run each BFS level's child listings through `buffer_unordered(CONCURRENCY)`,
  keeping the level-by-level structure
- [x] 5.3 Run the tag pass concurrently, keying each result by the OU id it
  belongs to so attribution does not depend on completion order (revised from
  "ordered `buffered` + positional zip" during 6.4 — see design.md)
- [x] 5.4 Note the overlap and the stale line references at the top of
  `stream-ou-fetch-progress/tasks.md`

## 6. Outstanding test coverage

- [x] 6.1 Test that a spool write failure is reported rather than swallowed.
  Extracted `write_lines(rx, sink)` from `temp_file_writer` and drove it with a
  `FailingSink`, since a real disk error cannot be provoked in-process. Verified
  by restoring the swallow and watching the test fail
- [x] 6.2 Test that a spool *flush* failure is caught too. Covered by the same
  sink with `fail_flush`; a third test asserts the writer keeps draining after a
  failure so senders are not stranded. Also added an end-to-end test that
  `temp_file_writer` records every line
- [x] 6.3 Test that a failure partway through an `Authoritative` landing leaves
  the previously stored rows in place. Induced with a `resources` table whose
  column count does not match the staging table, so the `DELETE` succeeds and
  the `INSERT ... SELECT *` fails. Verified by removing the transaction and
  watching the row vanish
- [x] 6.4 Rework the OU tag fixtures to give each OU distinct tags and assert
  each OU receives its own. **This task changed the implementation**: the test
  initially passed under `buffer_unordered` too, proving it could not detect
  misattribution — mocked calls complete on first poll, so completion order
  always equals input order. Rather than keep an invariant no test could
  falsify, tags are now keyed by OU id (5.3). The test now fails if the keying
  is broken
- [x] 6.5 Test that a failing call during the concurrent walk surfaces as an
  error rather than a silently truncated tree, for both the listing and the tag
  pass
- [x] 6.6 Considered and deliberately **not** implemented. An in-flight counter
  would read 1 regardless of the combinator, for the reason found in 6.4:
  mocked calls never overlap. Any such test would pass whether or not the calls
  were concurrent, so it would assert nothing while implying coverage. Recorded
  in design.md under Risks

## 7. Verification

- [x] 7.1 `cargo nextest run` — all tests pass (195 at time of implementation)
- [x] 7.2 `cargo clippy --all-targets` clean under pedantic with no new `allow`s
- [x] 7.3 `cargo fmt --check` clean
- [x] 7.4 Re-run 7.1–7.3 after section 6 — 204 tests pass, clippy and fmt clean
- [x] 7.5 `nix flake check` — all checks passed
