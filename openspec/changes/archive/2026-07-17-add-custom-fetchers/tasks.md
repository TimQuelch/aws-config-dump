## 1. Fetcher trait and registry

- [x] 1.1 Add `acd-cli/src/fetcher.rs` with the `Fetcher` trait (`name`, `resource_types`, `semantics`, `fetch`), the `Semantics` enum (`Authoritative`, `Merge`), and `FetchContext` (`profile`, `db: ConnectionPool`, `rows: mpsc::Sender<String>`, `progress`)
- [x] 1.2 Add `async fn fetch_rows<F: Fetcher>(fetcher: &F, pool: &ConnectionPool, profile: Option<String>, progress: &MultiProgress) -> anyhow::Result<Option<FetchOutput>>` as the per-fetcher driver, paired with `db::land_fetched_rows` so fetching can run concurrently with Config while landing waits until the resources table is settled. Generic so tests can pass a fake, and a free function rather than a trait method so the `Err` → `Ok(None)` soft-fail invariant can't be overridden by an impl. Do **not** use a `LazyLock` registry (can't inject fakes) or `Vec<Box<dyn Fetcher>>` (`async fn` in trait isn't dyn-safe and `async-trait` isn't a dependency); see design.md
- [x] 1.3 Add a `FakeFetcher` test double (`#[cfg(test)]`) that emits canned JSON-line rows, can be configured to fail, and records whether it ran — the vehicle for every pipeline test in sections 3 and 4
- [x] 1.4 Add a helper exposing the union of claimed resource types across the builtin fetchers — needed by tasks 2.1/2.2. With one builtin this is a short list in `build.rs`, not machinery

## 2. Scope Config's control plane

- [x] 2.1 Scope the identifier-driven delete (`db.rs:253`) to exclude fetcher-claimed types, so non-Config rows survive incremental builds
- [x] 2.2 Scope `get_timestamp_cutoff` (`db.rs:135`) to Config-sourced types only, so fetcher timestamps can't drag Config's cutoff forward and cause under-fetching
- [x] 2.3 Test: seed `resources` with a row of a claimed type absent from `identifiers_temp`, run the merge, assert the row survives while an unclaimed Config row missing from identifiers is still deleted
- [x] 2.4 Test: seed a claimed-type row with a `configurationItemCaptureTime` newer than every Config row, assert `get_timestamp_cutoff` ignores it

## 3. Landing fetcher rows on the spine

- [x] 3.1 Add a `db.rs` path that COPYs a fetcher's JSON-lines temp file into `resources`, honouring its `Semantics` (`Authoritative` → delete claimed types then insert; `Merge` → insert-or-update only). Reuse `temp_file_writer` (`build.rs:233`) rather than duplicating it
- [x] 3.2 Give each fetcher its own temp file rather than fanning into Config's `resources_tx` (`build.rs:159`), so fetcher rows never enter the identifier-delete MERGE

Tests 3.3-3.6 use `FakeFetcher` (1.3) against `connect_to_db_in_memory` (`db.rs:97`) — no AWS, no network.

- [x] 3.3 Test `Authoritative`: rows present in a prior build but absent from the new fetch are removed
- [x] 3.4 Test `Merge`: rows absent from the new fetch are retained, emitted rows are updated
- [x] 3.5 Test that a fetcher failing *after* emitting zero rows does **not** wipe existing rows under `Authoritative` semantics (the soft-fail guarantee — the dangerous interaction between "errors are non-fatal" and "authoritative replace"). `FakeFetcher` fails on demand, so this is directly reachable offline
- [x] 3.6 Test that a fetcher succeeding with genuinely zero rows *does* clear its claimed types under `Authoritative` — the counterpart to 3.5, confirming "failed" and "found nothing" stay distinguishable

## 4. Wire fetchers into the build

- [x] 4.1 In `build_database` (`build.rs:31`), spawn registered fetchers concurrently with the Config fetch, following the existing `task::spawn` handle pattern (`build.rs:47`, `build.rs:58`)
- [x] 4.2 Ensure fetchers land before `build_derived_tables` (`build.rs:119`) so their types get derived tables and reach the `resourceTypes` view
- [x] 4.3 Handle `FetchSource::Snapshots`: `build_resources_table_from_snapshots` (`db.rs:305`) does `CREATE OR REPLACE TABLE resources`, so fetcher rows must land *after* it, not concurrently, or they'll be dropped
- [x] 4.4 Skip fetchers entirely under `FetchSource::Skip` so `--no-fetch` makes no network calls, honouring the flag's documented "Don't fetch data, only build the resource tables" (`cli.rs:57`)
- [x] 4.5 Log and skip fetcher errors without failing the build, matching `build.rs:67` and `db.rs:507`
- [x] 4.6 Add a progress bar per fetcher via the shared `MultiProgress`, consistent with existing phases
- [x] 4.7 Guard against latent type collisions: if a fetcher-claimed type appears in Config's `get_resource_counts` output (already fetched at `build.rs:133`), fail loudly rather than let `Authoritative` semantics delete Config's rows. Note this only covers the `Api` path — `Snapshots` never calls `get_resource_counts`
- [x] 4.8 Test with `FakeFetcher` that it does not run under `FetchSource::Skip` and does run under `Api` and `Snapshots` — the fake records whether it ran, so this needs no network (spec: "Fetchers do not run when fetching is disabled")
- [x] 4.9 Test that a `--no-fetch` build leaves previously fetched rows intact — seed rows, run with `Skip`, assert they survive. Guards the same trap as 3.5 by a different route: no fetcher ran, which must not read as "authoritative replace with nothing"
- [x] 4.10 Test the collision guard from 4.7 by feeding a synthetic type-count map containing a claimed type, asserting it fails loudly rather than deleting

## 5. OU fetcher

- [x] 5.1 In `aws-client/src/org_client.rs`, add the OU fetch split as `fetch_organizational_units(profile)` → `fetch_organizational_units_inner(client)`, mirroring `fetch_org_accounts` (`org_client.rs:14-21`), so the inner fn is mockable. It returns plain data; it does not know about JSON lines or the `resources` schema
- [x] 5.2 Implement the walk: `ListRoots`, then recursive `ListOrganizationalUnitsForParent`, paginated via `PaginationStreamExt` like `fetch_org_accounts` (`org_client.rs:24`)
- [x] 5.3 Fetch OU tags via `ListTagsForResource`, and the management account ID via `DescribeOrganization`
- [x] 5.4 Unit-test the recursion with `aws_smithy_mocks` (already a dev-dep of `aws-client`), following the `fetch_org_accounts` test style (`org_client.rs:52`): assert OUs nested several levels deep are all returned, not just the root's children
- [x] 5.5 Unit-test pagination **at a nested level**, not just at the root (multi-page `ListOrganizationalUnitsForParent`), mirroring `fetch_org_accounts_multi_page` (`org_client.rs:78`). Root-only pagination mocks would hide a walk that truncates nested pages — see the mock-drift risk in design.md
- [x] 5.6 Unit-test the empty case: an organization with roots but no OUs returns empty, not an error
- [x] 5.7 Add the OU `Fetcher` impl in `acd-cli/src/builtin_fetchers/`, claiming `AWS::Organizations::OrganizationalUnit` with `Authoritative` semantics. It maps the plain data from 5.1 to resources-schema JSON lines: parent ID into `configuration`, `awsRegion` = `'global'`, management account ID as `accountId`, NULL for the Config-only fields (`configurationItemStatus`, `configurationStateId`, `resourceCreationTime`)
- [x] 5.8 Unit-test the mapping in `acd-cli` with hand-built fake OU values — no AWS, no mocks, no new dev-dependency. Assert the emitted JSON lines COPY cleanly into the `resources` schema and land the fields from 5.7 where 5.7 says
- [x] 5.9 Add it to the builtin fetcher list in `build.rs`

## 6. Verification

No step here may reach AWS or assume credentials. The behavioural regressions
that earlier drafts checked by hand against a live org are covered by the offline
tests above — the delete trap by 2.3, the cutoff by 2.4, the soft-fail/authoritative
interaction by 3.5 and 4.9. Those are stronger than a manual check because they
run every build. What remains is downstream wiring and honest gap-tracking.

- [x] 6.1 Simulate a second incremental build offline: seed `resources` with fetcher rows and Config rows, run the Config merge with an `identifiers_temp` that omits the fetcher rows, assert they survive while an absent Config row is still deleted. This is the delete trap (`db.rs:253`) — the highest-value test in the change, and it needs no network
- [x] 6.2 Using the fetcher-landing path plus `build_derived_tables` against an in-memory database, assert a fetcher-claimed type gets its derived table (`organizations_organizationalunit`) and reaches the `resourceTypes` view — proving the "lands on the spine, everything downstream is free" claim with no fetcher-specific downstream code
- [x] 6.3 Assert OU rows carry `awsRegion = 'global'` and that the `regions` view (`db.rs:397`) gains no value that OU rows alone introduced (spec: "OUs introduce no new region")
- [x] 6.4 Write a recursive CTE over synthetic multi-level OU rows reconstructing the full path, validating the "flatten in acquisition, derive in SQL" decision against fake data
- [x] 6.5 Run `cargo nextest run` and `cargo clippy` (pedantic — fix, don't suppress). Confirm the whole suite passes with no network and no AWS credentials present — if any test needs either, it's a bug in the test
- [x] 6.6 Record what offline testing cannot establish, so first live use knows where to look: (a) that the real Organizations API paginates and shapes the OU tree as our mocks assume (mock drift — see design.md); (b) behaviour on real tree depth and OU counts; (c) that the credential/profile path works at all, since `sdk_config::load_config` is never exercised for real. None of these are blockers, but the first `acd build` against a real org is the actual test of them and should be treated as such

## 7. Per-database fetcher configuration

- [x] 7.1 Add `FetcherConfig` to `acd-cli/src/config.rs` (`enabled: Option<bool>`, `aws_profile: Option<String>`), and a `#[serde(default)] fetchers: HashMap<String, FetcherConfig>` field on `DbConfig` keyed by `Fetcher::name()`
- [x] 7.2 Carry `fetchers` through `DbConfig::resolve` / `ResolvedDbConfig` into `Config`, alongside the existing per-database `aws_profile` and `aggregator_name`
- [x] 7.3 Add `Config::fetcher_enabled(name) -> bool` (absent config → true) and `Config::fetcher_profile(name) -> Option<String>` (absent profile → the database's `aws_profile`), so an existing config file behaves exactly as before
- [x] 7.4 In `build.rs`, gate the fetcher spawn on `fetcher_enabled` as well as `FetchSource::fetches()`, and pass `fetcher_profile` instead of `config.aws_profile`
- [x] 7.5 Make `fetcher_resource_types` take the config and return no types for a disabled fetcher, so its rows fall back under Config's authority and get reaped. Keep `--no-fetch` unaffected — it must still claim, so test 4.9 keeps passing
- [x] 7.6 Test deserialization: a `[databases.x.fetchers.organizational_units]` table with `enabled`/`aws_profile` parses, and a config file with no `fetchers` key still parses (following the `custom_table_deserializes_correctly` style, `config.rs:454`)
- [x] 7.7 Test the accessors: unconfigured → enabled with the database's profile; configured profile wins; `enabled = false` reads as disabled
- [x] 7.8 Test that a disabled fetcher claims no resource types, and that an enabled one still does
- [x] 7.9 Test the reaping end-to-end offline: seed fetched rows, run `build_resources_table` with the claim dropped (as a disabled fetcher would), assert the rows are removed while Config's rows are untouched — this is `unscoped_delete_would_reap_fetched_rows` promoted from a control into real behaviour
- [x] 7.10 Test that `--no-fetch` still preserves rows for an *enabled* fetcher, guarding the transient/durable distinction from collapsing
- [x] 7.11 Update the config documentation in `README.md` if it enumerates config options
- [x] 7.12 Run `cargo nextest run` and `cargo clippy` (pedantic — fix, don't suppress) with no network and no AWS credentials
