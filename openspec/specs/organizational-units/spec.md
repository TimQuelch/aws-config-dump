## Purpose

Defines how AWS Organizational Units are fetched into the database, how the
organization hierarchy is preserved on the stored rows, and how those rows are
attributed for account- and region-scoped querying.
## Requirements
### Requirement: Organizational Units are fetched into the database
Unless disabled for the database being built, `acd build` SHALL fetch every
Organizational Unit in the AWS Organization via a builtin fetcher, storing them
in `resources` under a dedicated resource type with `Authoritative` semantics.
The fetcher SHALL walk the OU tree from the organization roots, recursing through
all nesting levels. It SHALL authenticate with the profile configured for it,
which matters because the AWS Organizations API must be called against the
organization's management or delegated administrator account — not necessarily
the account the Config data is read from.

The walk SHALL issue its independent AWS Organizations calls concurrently, under
a bounded limit. Listing the children of the OUs at a given depth, and fetching
tags for discovered OUs, are each sets of independent calls; issuing them one at
a time costs a serialized round trip per OU and dominates the fetch on any
organization of realistic size. The concurrency limit SHALL be bounded so the
fetch stays clear of the Organizations API's request rate limits.

#### Scenario: Nested OUs are fetched
- **GIVEN** an Organization with OUs nested several levels below the root
- **WHEN** a user runs `acd build`
- **THEN** every OU at every level is present, not only the root's direct
  children

#### Scenario: OUs are queryable
- **WHEN** a user queries the OU resource type
- **THEN** each OU is returned with its ID as `resourceId`, its name as
  `resourceName`, and its ARN as `arn`

#### Scenario: OU tags are available
- **GIVEN** an OU with tags applied
- **WHEN** the OU is queried
- **THEN** its tags are present in the `tags` map column, consistent with how
  Config-sourced resource tags are stored

#### Scenario: Deleted OUs are removed
- **GIVEN** a previous build stored an OU that has since been deleted from the
  Organization
- **WHEN** a subsequent `acd build` runs
- **THEN** that OU is no longer present in `resources`

#### Scenario: Not an organization member
- **GIVEN** credentials for an account that is not part of an AWS Organization,
  or that lacks Organizations read permissions
- **WHEN** a user runs `acd build`
- **THEN** the failure is logged and the build completes successfully without OU
  rows

#### Scenario: OUs fetched with a management account profile
- **GIVEN** a database reading Config data with one profile and configuring the
  OU fetcher with a management account profile
- **WHEN** a user runs `acd build`
- **THEN** OUs are fetched using the management account profile, while Config
  resources continue to be read with the database's own profile

#### Scenario: OU fetching disabled
- **GIVEN** a database that disables the OU fetcher
- **WHEN** a user runs `acd build`
- **THEN** no Organizations API calls are made and the build completes without
  OU rows

#### Scenario: Sibling OUs are listed concurrently
- **GIVEN** an Organization with many OUs at the same depth
- **WHEN** the fetcher walks that depth
- **THEN** their child listings are issued concurrently up to the bound, rather
  than one parent at a time

#### Scenario: Tags are fetched concurrently
- **GIVEN** an Organization with many OUs
- **WHEN** the fetcher attaches tags
- **THEN** the per-OU tag calls are issued concurrently up to the bound

#### Scenario: Each OU receives its own tags
- **GIVEN** an Organization with more OUs than the concurrency bound, each
  carrying distinct tags
- **WHEN** the concurrent tag calls complete in an arbitrary order
- **THEN** every OU carries the tags fetched for that OU, matched by its
  identifier rather than by the order results arrived in

#### Scenario: A failed call during a concurrent walk fails the fetch
- **GIVEN** concurrent listing or tag calls in flight
- **WHEN** one of them fails
- **THEN** the fetch reports an error rather than returning a partial tree,
  so nothing is landed and previously stored OU rows remain

### Requirement: OU rows carry the organization hierarchy
Each stored OU SHALL record the identifier of its parent (an OU or a root) in its
`configuration`, so that the OU tree can be reconstructed by querying alone.

#### Scenario: Parent is recorded
- **WHEN** an OU nested under another OU is queried
- **THEN** its `configuration` identifies the parent OU

#### Scenario: Top-level parent is recorded
- **WHEN** an OU directly beneath an organization root is queried
- **THEN** its `configuration` identifies that root

#### Scenario: Full tree is reconstructible
- **GIVEN** OUs nested several levels deep
- **WHEN** a user runs a recursive query over the stored parent identifiers
- **THEN** the complete path from each OU up to its root can be derived without
  further AWS API calls

### Requirement: OU rows are attributed to the management account and the global region
Each stored OU SHALL carry the AWS Organization's management account ID as its
`accountId` and `global` as its `awsRegion`, so OU rows behave consistently with
existing account- and region-scoped filtering and joins, and with the
Config-sourced rows for other global resource types already in the table.

#### Scenario: Account join resolves
- **WHEN** an OU row is joined against the `accounts` table on `accountId`
- **THEN** it resolves to the management account rather than yielding no match

#### Scenario: Account filtering includes OUs
- **WHEN** a user filters a query by the management account ID
- **THEN** OU rows are included in the results

#### Scenario: Region filtering treats OUs as global
- **WHEN** a user filters a query by the global region
- **THEN** OU rows are included, alongside the Config-sourced global resources

#### Scenario: OUs introduce no new region
- **WHEN** a user inspects the `regions` view after a build
- **THEN** it contains no region value that OU rows alone introduced

