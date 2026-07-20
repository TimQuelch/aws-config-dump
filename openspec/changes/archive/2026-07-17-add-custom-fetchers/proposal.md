## Why

`acd build` can only ingest what AWS Config records. Resources Config doesn't
support — Organizational Units being the motivating example — have no way into
the database, even though they're cheap to fetch from other AWS APIs and are
exactly the kind of thing users want to join against (`accounts` → OU → SCPs).

The pipeline already fetches non-Config data twice, but both cases are
hardcoded: `fetch_org_accounts` (`build.rs:47`) hits the Organizations API, and
`list_aws_managed_policies` (`build.rs:58`) hits IAM. Each is a bespoke
`task::spawn` block in `build_database` writing to its own ad-hoc side table.
There is no seam to add a third. This change generalizes the pattern that
already exists twice, before it exists three times.

Note that `custom_tables` (`db.rs:440`) is **not** this mechanism: it runs SQL
over tables that already exist. It is a *derivation* layer. This change adds the
missing *acquisition* layer beneath it.

## What Changes

- Add a `Fetcher` trait and a compiled-in registry, run as a new phase of
  `acd build` alongside the existing Config fetch.
- Fetchers emit rows shaped like the `resources` schema as JSON lines, landing
  on the `resources` spine rather than in side tables. Everything downstream —
  per-type derived tables, the `resourceTypes` view, `acd query`, completions —
  then works with no additional plumbing.
- Each fetcher declares the `resourceType`s it claims, and whether its rows are
  `Authoritative` (the complete set for those types) or `Merge` (insert/update
  only, never delete).
- **Scope Config's control plane to Config's own rows.** The identifier-driven
  delete (`db.rs:253`) and the incremental cutoff (`db.rs:135`) currently
  operate over the whole `resources` table, implicitly assuming Config is the
  sole authority on what exists and how fresh it is. Both are narrowed to
  exclude fetcher-claimed types. Without this, fetched rows are silently
  deleted by the next incremental build.
- Ship one builtin fetcher — Organizational Units — as the first
  implementation and the proof the seam works.
- Let each database configure its builtin fetchers in `config.toml`, keyed by
  fetcher name: turn a fetcher off, and give it its own AWS profile. The
  Organizations API has to be called against the management (or delegated
  admin) account, which is routinely not the profile the Config data is read
  with — so without this the OU fetcher is unusable on exactly the
  cross-account setups it's most useful for. Both settings sit per-database
  because that's where `aws_profile` already lives and because two databases
  can point at different organizations. Purely additive: absent config, a
  fetcher is enabled and uses the database's `aws_profile`, exactly as today.

Not in scope (deliberately — these are what the seam is *for*, not what proves
it works):

- The user-declared external-command fetcher (`[[fetchers]]` in `config.toml`).
  The trait is designed to accommodate it as one more impl. Note this is a
  different axis from the per-fetcher settings above: configuring a builtin
  fetcher ships here, *declaring* a new fetcher in config does not.
- Migrating `accounts` and `aws_managed_policy` onto the new model. Both have
  live consumers (`query.rs:145` LEFT JOINs `accounts`;
  `builtin_alterations.toml:41` declares `aws_managed_policy` as a dependency)
  and would need to survive as derivations over fetched data. See design.md for
  the phase-ordering landmine this will hit.

No breaking changes: no existing table, flag, or query behavior changes.

## Capabilities

### New Capabilities
- `custom-fetchers`: Defines how `acd build` acquires data from sources other
  than AWS Config — the fetcher contract, the resource types a fetcher claims,
  its delete/merge semantics, how its rows land on the `resources` spine, and
  how failures are handled.
- `organizational-units`: Defines the Organizational Unit data `acd` makes
  available — the resource type, row shape, and the hierarchy the tree is
  flattened into.

### Modified Capabilities
(none — neither the build pipeline nor the `resources` schema is currently
specified, and `query-output-format` is unaffected)

## Impact

- `aws-client/src/org_client.rs`: new OU fetch (`ListRoots` +
  recursive `ListOrganizationalUnitsForParent`, plus `ListTagsForResource`),
  testable with `aws_smithy_mocks` like the existing `fetch_org_accounts` tests.
- `acd-cli/src/`: new `fetcher.rs` (trait, registry, context) and
  `builtin_fetchers/` (the OU impl).
- `acd-cli/src/config.rs`: `DbConfig` gains a `fetchers` map keyed by fetcher
  name (`enabled`, `aws_profile`), resolved through `Config` alongside the
  existing per-database settings.
- `acd-cli/src/build.rs`: `build_database` spawns registered fetchers
  concurrently with the Config fetch, mirroring how the org-accounts and
  managed-policy handles already run in parallel.
- `acd-cli/src/db.rs`: scope the identifier delete (`db.rs:253`) and
  `get_timestamp_cutoff` (`db.rs:135`) to exclude fetcher-claimed types; new
  path to land fetcher JSON lines onto `resources`.
- New DuckDB state: the set of fetcher-claimed resource types must be known to
  the delete/cutoff queries.
- No new dependencies — `aws-sdk-organizations` is already in use by
  `org_client`.
