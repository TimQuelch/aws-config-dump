> **Note:** commit `f5ab665` (specified by the `harden-fetcher-seam` change) reworked
> this seam after these tasks were written. Before starting, re-read the current code —
> all line references below are stale and have been removed. What changed:
>
> - `fetch_organizational_units_inner` is already concurrent: each BFS level's listings
>   run `buffer_unordered(CONCURRENCY)` and the tag pass runs `buffered(CONCURRENCY)`,
>   with `CONCURRENCY = 8` already a named constant. The *performance* half of tasks 2.1
>   and 3.2 is done — but against the collect-then-return shape, so the progress bar is
>   no better than before. What remains is the streaming half: eager production through
>   a channel so the bar's length grows during the walk.
> - `Fetcher::fetch` returns a boxed `FetchFuture<'a>` and the trait is `Send + Sync`.
>   Section 4 works inside the OU fetcher's existing `Box::pin(async move { … })` body;
>   the trait itself does not move. Everything held across an await must be `Send`.
> - The current tag pass is *ordered* on purpose (`buffered` + positional zip) so tags
>   cannot be misattributed. Task 3.2 removes that constraint rather than violating it —
>   see design.md, "Concurrency".
> - `builtin_fetchers::all()` is now the single registration point and `build.rs` no
>   longer names the OU fetcher, so no task here touches `build.rs`.

## 1. Client handle

- [x] 1.1 Add an `OrgClient` handle type in `aws-client/src/org_client.rs` wrapping
  `aws_sdk_organizations::Client`, with a constructor taking `Option<String>`
  profile and calling `sdk_config::load_config`
- [x] 1.2 Move `fetch_management_account_id` onto the handle, keeping the existing
  `_inner` mock seam working via a constructor that takes a client directly
- [x] 1.3 Port `fetch_management_account_id_returns_master_account` onto the handle
  and confirm `cargo nextest run -p aws-client` passes before any behavior changes

## 2. Eager, concurrent discovery

- [x] 2.1 Move the existing level-by-level BFS into a private producer, keeping
  its `buffer_unordered(CONCURRENCY)` listing as is — the concurrency is already
  correct; only its position in the pipeline changes
- [x] 2.2 Have the producer run as a spawned task pushing `OrganizationalUnit`
  values (tags empty) through a bounded `mpsc` channel, adding a named capacity
  constant of 256 alongside the existing `CONCURRENCY`
- [x] 2.3 Propagate producer errors to the consumer as `Err` items on the channel,
  and ensure a `JoinError` from the task surfaces as an error rather than a
  silent early end-of-stream

## 3. Progress reporting and tag enrichment

- [x] 3.1 Give the OU stream entry point a `ProgressBar` parameter, following
  `config_client`'s existing bar parameter; call `inc_length(1)` where the BFS
  enqueues each discovered OU, before it is sent
- [x] 3.2 Attach tags inside the stream: map each discovered OU to a future
  calling `list_tags_for_resource`, run `CONCURRENCY`-wide with
  `buffer_unordered`, and yield fully-tagged `OrganizationalUnit` values. This
  deletes the ordered `buffered` + positional-zip tag pass; unordered is safe
  here only because each OU carries its own tags — do not carry the zip forward
- [x] 3.3 Expose the composed result as `impl Stream<Item = anyhow::Result<OrganizationalUnit>>`
  and delete `fetch_organizational_units` / `fetch_organizational_units_inner`
- [x] 3.4 Confirm nothing else calls the removed functions (`fetch_org_accounts`
  is separate and stays as is)

## 4. Fetcher consumes the stream

- [x] 4.1 Rewrite the body of `OrganizationalUnitsFetcher::fetch`'s existing
  `Box::pin(async move { … })` to build the client once, create the bar via
  `util::progress_bar(msg, 0)` added to `ctx.progress`, and hand the bar to the
  stream. Keep the `fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a>`
  signature exactly as it is
- [x] 4.2 Consume the stream with `try_for_each`, mapping each OU through the
  untouched `ou_to_json_line` and sending to `ctx.rows`, calling `bar.inc(1)` per
  emitted row
- [x] 4.3 Remove the post-hoc `progress_bar("fetching organizational units", ous.len())`
  and the collect it forced
- [x] 4.4 Verify a stream error still propagates out of `fetch`, so `fetch_rows`
  returns `Ok(None)` and lands nothing
- [x] 4.5 Confirm the future still satisfies `FetchFuture`'s `Send` bound — the
  channel receiver and anything held across an await must be `Send`, which a
  non-`Send` stream adapter would break at compile time

## 5. Tests

- [x] 5.1 Port the existing mock scenarios onto the stream by collecting it to a
  `Vec`, preserving every assertion: deeply nested OUs, parent recording, nested
  pagination, tag attachment, and empty organization
- [x] 5.1a Give each OU *distinct* tags in the fixtures and assert each receives
  its own. The current fixtures use one uniform tag rule, so they pass even if
  concurrent enrichment misattributes every result — which makes them no
  coverage at all for task 3.2. Shared with `harden-fetcher-seam` task 6.4;
  whichever change lands first does it
- [x] 5.2 Add a test that discovery leads completion — with a `ProgressBar`, the
  total rises above the position during a fetch and they are equal at the end
- [x] 5.3 Add a test that a failing tag or list call surfaces as an `Err` item
  rather than truncating the stream silently
- [x] 5.4 Add a test that the emitted rows are unchanged from the pre-refactor
  shape for a given mocked organization

## 6. Verification

- [x] 6.1 `cargo nextest run` — all tests, including the untouched
  `ou_to_json_line` tests and the `custom-fetchers` soft-fail coverage
- [x] 6.2 `cargo clippy` clean under pedantic, without new `allow`s
- [ ] 6.3 Run `acd build` against a real organization and confirm the bar's total
  climbs while the position trails it, rather than sitting at `0/N` then snapping
- [x] 6.4 `nix flake check`

## Known gaps

- The `JoinError` arm of task 2.3 is implemented but not directly covered by a
  test. Inducing a producer panic needs a mocked call to panic, and
  `aws-smithy-mocks` poisons its own interceptor mutex when a rule panics, so the
  panic lands on the test thread instead of the spawned task. Covering it would
  mean restructuring the producer purely for testability; the error-item path
  (a failing call) *is* covered, and the join arm is a two-line match.
- Task 5.1a landed earlier, with `harden-fetcher-seam` (`cec6a4f`): the
  `distinct_tags_rules` fixtures and
  `fetch_organizational_units_gives_each_ou_its_own_tags` already existed and
  now exercise the streaming tag pass unchanged.
