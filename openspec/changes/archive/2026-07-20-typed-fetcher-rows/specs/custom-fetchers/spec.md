## ADDED Requirements

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
