## ADDED Requirements

### Requirement: A fetcher's emitted rows are all-or-nothing
The rows a fetcher emits SHALL reach the landing step complete or not at all. If
any part of collecting a fetcher's output fails — including failing to write an
emitted row to, or flush it from, the intermediate spool — the fetch SHALL be
treated as failed, exactly as though the fetcher itself had returned an error.

An incomplete row set SHALL NOT be landed. Under `Authoritative` semantics a
short row set is indistinguishable from a shrunken one, so landing a truncated
set deletes every row the truncation dropped.

#### Scenario: A row cannot be written to the spool
- **GIVEN** a fetcher that emits rows successfully
- **WHEN** writing one of those rows to the intermediate spool fails
- **THEN** the fetch is reported as failed, nothing is landed, and rows
  previously stored for the fetcher's claimed types remain

#### Scenario: Flushing the spool fails
- **GIVEN** a fetcher that has emitted all of its rows
- **WHEN** flushing the spool to disk fails
- **THEN** the fetch is reported as failed and nothing is landed, rather than
  the successfully buffered prefix being treated as the complete row set

#### Scenario: A complete empty row set is still valid
- **GIVEN** a fetcher that succeeds and emits no rows
- **WHEN** the spool is written and flushed without error
- **THEN** the empty row set is landed normally, since finding nothing is
  distinct from failing to record what was found

## MODIFIED Requirements

### Requirement: Fetcher registry
`acd build` SHALL run a registry of compiled-in fetchers that acquire resource
data from sources other than AWS Config. Each fetcher SHALL declare a unique
name, the set of `resourceType` values it claims, and its merge semantics
(`Authoritative` or `Merge`). Fetchers SHALL run on builds that fetch — whether
resources come from the Config API or from snapshots — and SHALL NOT run on
builds where fetching is disabled.

The registry SHALL hold an arbitrary number of fetchers, and adding one SHALL
require registering it in one place rather than duplicating the build's
dispatch and landing logic. No two registered fetchers SHALL share a name, and
no two SHALL claim the same `resourceType`.

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

#### Scenario: Multiple fetchers land together
- **GIVEN** two registered fetchers claiming different resource types
- **WHEN** a user runs `acd build`
- **THEN** both fetchers run and the rows of both are present in `resources`

#### Scenario: Fetcher names are unique
- **WHEN** the registry is constructed
- **THEN** no two fetchers share a name, since names key per-fetcher
  configuration and a duplicate would let one fetcher's settings govern another

#### Scenario: Claimed resource types are unique
- **WHEN** the registry is constructed
- **THEN** no two fetchers claim the same `resourceType`, since each
  `Authoritative` fetcher replaces the stored rows of its claimed types and a
  shared claim would make the second to land delete the first's rows

### Requirement: Fetcher failures do not fail the build
A fetcher that returns an error SHALL be logged and skipped. The build SHALL
continue, and rows previously fetched for that fetcher's claimed types SHALL be
left intact rather than deleted.

Fetchers SHALL be independent of one another. A fetcher that fails SHALL NOT
prevent any other fetcher's rows from being landed, regardless of the order in
which they run or land.

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

#### Scenario: One fetcher failing does not block another
- **GIVEN** two registered fetchers, the first of which fails
- **WHEN** a user runs `acd build`
- **THEN** the second fetcher's rows are landed and the build exits
  successfully, rather than the first failure short-circuiting the rest

### Requirement: Fetcher merge semantics
A fetcher declaring `Authoritative` semantics SHALL have its emitted rows treated
as the complete set for its claimed types: existing rows of those types are
removed and replaced. A fetcher declaring `Merge` semantics SHALL have its rows
inserted or updated only, and SHALL NOT cause deletion of existing rows of its
claimed types.

An `Authoritative` replacement SHALL be atomic: the removal of existing rows and
the insertion of the fetched rows SHALL either both take effect or neither do.
A partially applied replacement would leave the claimed types empty, which is a
worse outcome than the fetcher not having run.

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

#### Scenario: A failed replacement leaves the prior rows in place
- **GIVEN** a previous build stored rows for an `Authoritative` fetcher's claimed
  type
- **WHEN** landing this build's rows fails partway, after the existing rows would
  have been removed but before the new rows are inserted
- **THEN** the previously stored rows are still present, rather than the claimed
  types being left empty
