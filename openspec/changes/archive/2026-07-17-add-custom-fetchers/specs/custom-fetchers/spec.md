## ADDED Requirements

### Requirement: Fetcher registry
`acd build` SHALL run a registry of compiled-in fetchers that acquire resource
data from sources other than AWS Config. Each fetcher SHALL declare a unique
name, the set of `resourceType` values it claims, and its merge semantics
(`Authoritative` or `Merge`). Fetchers SHALL run on builds that fetch — whether
resources come from the Config API or from snapshots — and SHALL NOT run on
builds where fetching is disabled.

#### Scenario: Registered fetchers run during build
- **WHEN** a user runs `acd build`
- **THEN** every registered fetcher runs and its rows are present in the
  `resources` table on completion

#### Scenario: Fetchers run on snapshot builds
- **WHEN** a user runs `acd build --with-snapshots`
- **THEN** every registered fetcher runs, since a Config snapshot contains no
  fetcher-sourced resources either

#### Scenario: Fetchers do not run when fetching is disabled
- **WHEN** a user runs `acd build --no-fetch`
- **THEN** no fetcher runs and no fetcher network calls are made, consistent
  with the flag's meaning of building tables from already-stored data

#### Scenario: Disabled fetching leaves fetched rows intact
- **GIVEN** a previous build stored rows for a fetcher's claimed type
- **WHEN** a user runs `acd build --no-fetch`
- **THEN** those rows remain in `resources` and are not deleted, despite no
  fetcher having emitted them during this build

#### Scenario: Fetchers run concurrently with the Config fetch
- **WHEN** `acd build` runs with `FetchSource::Api`
- **THEN** registered fetchers are dispatched without waiting for the Config
  fetch to complete, consistent with the existing org-accounts and
  managed-policy fetches

#### Scenario: Fetcher rows are queryable as resources
- **GIVEN** a fetcher claiming resource type `T` has produced rows
- **WHEN** a user runs `acd query --type T`
- **THEN** the fetched rows are returned, and `T` appears in the `resourceTypes`
  view and in shell completions, without any fetcher-specific query code

#### Scenario: Fetcher rows get a derived table
- **GIVEN** a fetcher claiming resource type `AWS::Foo::Bar` has produced rows
- **WHEN** the build completes
- **THEN** a derived table named `foo_bar` exists, built by the same mechanism
  as Config-sourced derived tables

### Requirement: Builtin fetchers are configurable per database
Each database in the configuration file SHALL be able to configure its builtin
fetchers by fetcher name, controlling whether a fetcher runs and which AWS
profile it authenticates with. When a fetcher has no configuration, it SHALL run,
and it SHALL use the database's own `aws_profile` — so a configuration file that
says nothing about fetchers behaves as though this option did not exist.

#### Scenario: Fetcher configured with its own profile
- **GIVEN** a database configuring a fetcher with an `aws_profile` different from
  the database's own
- **WHEN** that fetcher runs
- **THEN** it authenticates with the profile it was configured with, not the
  database's

#### Scenario: Fetcher profile defaults to the database's
- **GIVEN** a database that configures a fetcher without naming a profile
- **WHEN** that fetcher runs
- **THEN** it authenticates with the database's `aws_profile`

#### Scenario: Unconfigured fetcher runs against the database profile
- **GIVEN** a configuration file that says nothing about fetchers
- **WHEN** a user runs `acd build`
- **THEN** every builtin fetcher runs, using the database's `aws_profile`

#### Scenario: Disabled fetcher does not run
- **GIVEN** a database that disables a fetcher
- **WHEN** a user runs `acd build`
- **THEN** that fetcher does not run and makes no network calls

#### Scenario: Fetchers are configured independently per database
- **GIVEN** two databases, one disabling a fetcher and one not
- **WHEN** a user builds the database that does not disable it
- **THEN** the fetcher runs, unaffected by the other database's configuration

### Requirement: A disabled fetcher releases its resource types
A fetcher that is disabled SHALL NOT claim its resource types, so rows it
previously stored fall back under AWS Config's authority and are removed. This
SHALL be distinct from a build that merely skips fetching, which retains both the
claim and the rows.

#### Scenario: Disabling a fetcher removes its stored rows
- **GIVEN** a database holding rows previously stored by a fetcher
- **WHEN** the fetcher is disabled and a subsequent `acd build` fetches from the
  Config API
- **THEN** those rows are removed, rather than remaining indefinitely with no
  fetcher able to refresh them

#### Scenario: Disabling does not disturb Config's own rows
- **GIVEN** a fetcher is disabled
- **WHEN** a subsequent `acd build` runs
- **THEN** resources recorded by AWS Config are unaffected

#### Scenario: Skipping the fetch is not the same as disabling
- **GIVEN** a database holding rows previously stored by an enabled fetcher
- **WHEN** a user runs `acd build --no-fetch`
- **THEN** those rows remain, because the fetcher is still configured to exist
  and has merely not run this time

### Requirement: Fetcher failures do not fail the build
A fetcher that returns an error SHALL be logged and skipped. The build SHALL
continue, and rows previously fetched for that fetcher's claimed types SHALL be
left intact rather than deleted.

#### Scenario: A fetcher errors
- **WHEN** a fetcher's `fetch` returns an error (e.g. an AWS API permission
  failure)
- **THEN** the error is logged, the build continues to completion, and the build
  exits successfully

#### Scenario: Prior rows survive a fetcher error
- **GIVEN** a previous build stored rows for an `Authoritative` fetcher's claimed
  type
- **WHEN** a subsequent build's fetcher errors before producing rows
- **THEN** the previously stored rows remain in `resources` and are not replaced
  by an empty set

### Requirement: Fetcher merge semantics
A fetcher declaring `Authoritative` semantics SHALL have its emitted rows treated
as the complete set for its claimed types: existing rows of those types are
removed and replaced. A fetcher declaring `Merge` semantics SHALL have its rows
inserted or updated only, and SHALL NOT cause deletion of existing rows of its
claimed types.

#### Scenario: Authoritative fetcher removes vanished rows
- **GIVEN** a previous build stored rows `A` and `B` for an `Authoritative`
  fetcher's claimed type
- **WHEN** a subsequent build's fetcher emits only row `A`
- **THEN** row `B` is no longer present in `resources`

#### Scenario: Merge fetcher retains rows it did not emit
- **GIVEN** a previous build stored rows `A` and `B` for a `Merge` fetcher's
  claimed type
- **WHEN** a subsequent build's fetcher emits only an updated row `A`
- **THEN** row `A` reflects the update and row `B` remains present

### Requirement: Fetchers may read the database while fetching
The context passed to a fetcher SHALL provide access to the database connection
pool, so a fetcher can determine what to fetch based on data already stored.

#### Scenario: Fetcher diffs against stored state
- **GIVEN** a fetcher whose upstream API exposes a cheap listing with version
  identifiers and an expensive per-item fetch
- **WHEN** the fetcher runs
- **THEN** it can query existing rows and fetch only the items whose version
  identifier differs from what is stored

### Requirement: Config's control plane is scoped to Config-sourced rows
The identifier-driven deletion and the incremental timestamp cutoff SHALL
operate only over resource types not claimed by a fetcher.

#### Scenario: Fetched rows survive an incremental rebuild
- **GIVEN** a fetcher has stored rows for its claimed type, which AWS Config's
  resource identifier listing does not include
- **WHEN** a subsequent incremental `acd build` runs
- **THEN** the fetched rows remain in `resources` and are not deleted

#### Scenario: Fetched rows do not advance the Config cutoff
- **GIVEN** fetched rows carry a `configurationItemCaptureTime` more recent than
  any Config-sourced row
- **WHEN** a subsequent incremental `acd build` computes its timestamp cutoff
- **THEN** the cutoff is derived only from Config-sourced rows, so Config
  fetching is unaffected by fetcher timestamps

#### Scenario: Config rows are still reaped normally
- **GIVEN** a Config-sourced resource no longer appears in Config's identifier
  listing
- **WHEN** an incremental `acd build` runs
- **THEN** that row is deleted, unchanged from current behavior
