## Purpose

Defines the registry of compiled-in fetchers that acquire resource data from
sources other than AWS Config, how they are configured per database, and how
their rows coexist with Config-sourced rows in the `resources` table.
## Requirements
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

### Requirement: Fetchers emit typed rows
A fetcher SHALL emit its rows as a structured value describing one resource,
not as a serialised representation of a database row. The structured value
SHALL carry only what the fetcher knows: the resource's identity (`arn`,
account, region, resource type, resource id, and optional name), the time the
fetcher observed it, its tags, and its type-specific configuration.

The seam SHALL supply every remaining field of the stored row. A fetcher SHALL
NOT be able to set, omit, or misshape those fields, since they are properties
of the `resources` table rather than of any fetcher's source.

#### Scenario: A fetcher supplies only resource-specific fields
- **WHEN** a fetcher emits a resource it has fetched
- **THEN** it provides that resource's identity, observation time, tags and
  configuration, and provides nothing else

#### Scenario: Common fields are filled without the fetcher's involvement
- **GIVEN** a fetcher that emits a row
- **WHEN** that row is landed
- **THEN** the columns no fetcher populates are present and correctly shaped,
  without the fetcher having named them

#### Scenario: A new fetcher inherits the row shape
- **GIVEN** a second fetcher added to the registry
- **WHEN** it emits rows
- **THEN** those rows carry the same common-field shape as the first fetcher's,
  without that shape being restated in the new fetcher

### Requirement: The emitted row shape is an invariant of the seam
The representation a fetcher's rows are converted into SHALL satisfy what the
landing step requires, as a property of the conversion itself:

- `tags` SHALL be emitted as a sequence of key/value entries, since landing
  stages them as an entry array before converting them to a map. A fetcher with
  no tags SHALL yield an empty sequence, not a null. Tags SHALL be carried as
  key/value entries from the source API through to serialisation, rather than
  being converted into a keyed map and back, since no step between the two
  looks a tag up by key.
- `supplementaryConfiguration` SHALL be an empty object rather than null, since
  the derived-table build unnests that column and rejects an all-null one. A
  row with a null there lands on the spine but silently loses its derived
  table.
- Columns that exist only in AWS Config's item format SHALL be null rather than
  carry a sentinel, so nothing downstream reads a placeholder as a real value.
- Each row SHALL be emitted as a single self-delimiting record, terminated so
  that rows can be appended to a spool and read back one per line.

#### Scenario: Tags become key/value entries
- **GIVEN** a fetcher emitting a resource with tags
- **WHEN** its row is landed
- **THEN** the tags are present in the `tags` map column, having been staged as
  key/value entries

#### Scenario: An untagged resource yields empty tags
- **GIVEN** a fetcher emitting a resource with no tags
- **WHEN** its row is landed
- **THEN** its `tags` column is empty rather than null

#### Scenario: Fetched rows always get a derived table
- **GIVEN** a fetcher emitting rows for a claimed type
- **WHEN** the build completes
- **THEN** the derived table for that type is built, because the emitted rows
  carry an object in `supplementaryConfiguration` rather than a null

#### Scenario: Config-only columns are absent, not faked
- **WHEN** a fetched row is queried
- **THEN** the columns that only AWS Config can populate are null, and no
  placeholder value appears in them

### Requirement: Fetched rows record when they were observed
Each emitted row SHALL carry the time the fetcher observed the resource, stored
in the same column AWS Config uses for its capture time. The observation time
SHALL be supplied by the fetcher rather than read from the clock during
serialisation, so that a row's recorded time is the time of the fetch and is
determinable under test.

#### Scenario: Observation time is stored
- **GIVEN** a fetcher that observes a resource at a known time
- **WHEN** its row is landed
- **THEN** that time is stored as the row's capture time, not null

#### Scenario: Serialisation does not read the clock
- **GIVEN** a row carrying a fixed observation time
- **WHEN** it is serialised twice
- **THEN** both serialisations record that same time

