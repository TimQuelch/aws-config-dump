# fetcher-progress Specification

## Purpose

Defines what a fetcher's reported progress must mean: that it tracks the
underlying remote work rather than the serialization of results already in
hand, and how a fetcher over an API offering no cheap count reports a total
that grows as work is discovered instead of deferring all reporting until the
total is known.
## Requirements
### Requirement: Fetcher progress tracks the underlying work
A fetcher's reported progress SHALL advance as its underlying work completes, not
as its results are serialized. A fetcher SHALL NOT complete the whole of its
remote work before beginning to report progress.

#### Scenario: Progress advances during the fetch, not after it
- **GIVEN** a fetcher whose remote calls dominate its runtime
- **WHEN** the fetcher runs
- **THEN** its reported position advances while those calls are still
  outstanding, rather than remaining at zero until every call has returned and
  then jumping to completion

#### Scenario: Each completed unit of work is counted once
- **GIVEN** a fetcher that has discovered N items
- **WHEN** the fetch completes successfully
- **THEN** the reported position is N and equals the reported total

#### Scenario: Concurrent work is counted correctly
- **GIVEN** a fetcher enriching items concurrently
- **WHEN** several items complete out of the order they were discovered in
- **THEN** each is counted exactly once and the position reflects the number
  completed, independent of ordering

### Requirement: A total that is not known up front grows as work is discovered
A fetcher over an API that offers no cheap count of the work SHALL report a total
that grows as work is discovered, rather than deferring all reporting until the
total is known. Discovering a unit of work SHALL raise the reported total, and
completing it SHALL raise the reported position.

#### Scenario: Total grows during discovery
- **GIVEN** a fetcher that discovers work incrementally
- **WHEN** discovery finds further items partway through the fetch
- **THEN** the reported total rises to include them, without the fetch having
  been delayed until the full count was known

#### Scenario: Discovery is not gated by enrichment
- **GIVEN** a fetcher whose enrichment of discovered items is slower than its
  discovery of them
- **WHEN** the fetch runs
- **THEN** discovery proceeds ahead of enrichment, so the reported total leads
  the reported position rather than tracking it

#### Scenario: Totals settle at completion
- **GIVEN** a fetch that has discovered every item
- **WHEN** the last item completes
- **THEN** the reported total no longer changes and equals the reported position

### Requirement: Progress reporting does not alter fetch outcomes
Reporting progress SHALL NOT change what a fetcher emits or how its failures are
handled. A fetch that reports partial progress and then fails SHALL be treated as
a failure, not as a partial success.

#### Scenario: A fetch that fails partway lands nothing
- **GIVEN** a fetcher that has reported progress for some items
- **WHEN** a later remote call fails and the fetch returns an error
- **THEN** nothing is landed and previously stored rows for its claimed types
  survive, consistent with fetcher failure handling generally

#### Scenario: Emitted rows are unaffected by reporting
- **GIVEN** a fetcher reporting progress as described above
- **WHEN** the fetch completes
- **THEN** the rows it emits are identical to those it would emit without any
  progress reporting

