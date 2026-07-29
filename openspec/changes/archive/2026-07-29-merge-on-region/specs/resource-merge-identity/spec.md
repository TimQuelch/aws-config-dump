## ADDED Requirements

### Requirement: Region is part of a resource's merge identity

The build phase SHALL identify a stored resource for the purpose of matching it
across builds by the tuple `(accountId, awsRegion, resourceType, resourceId)`.
Two resources that agree on `accountId`, `resourceType`, and `resourceId` but
differ in `awsRegion` SHALL be treated as distinct resources.

This applies to every operation that matches an incoming row against the stored
`resources` table: the config-data merge, the fetcher landing merge, and the
identifier-driven delete.

#### Scenario: Same resource id in two regions does not collide

- **GIVEN** a build supplies two resources with the same `accountId`,
  `resourceType`, and `resourceId` but different `awsRegion`
- **WHEN** the build merges its config data into `resources`
- **THEN** both resources are stored as separate rows, and neither overwrites
  the other

#### Scenario: Re-merging updates only the matching region

- **GIVEN** `resources` holds two rows that share `accountId`, `resourceType`,
  and `resourceId` but differ in `awsRegion`
- **WHEN** a later build supplies an updated config for one of those regions
- **THEN** only the row for that region is updated, and the other region's row
  is left unchanged

### Requirement: Identifier-driven delete reaps per region

The identifier-driven delete SHALL remove a stored Config-sourced row when no
identifier in the current build's listing matches it on
`(accountId, awsRegion, resourceType, resourceId)`. A resource that no longer
exists in one region SHALL be reaped even when a resource with the same
`accountId`, `resourceType`, and `resourceId` still exists in another region.

To make this possible, every resource identifier the build produces SHALL carry
its `awsRegion`, whether the identifier comes from an aggregator query, a
non-aggregator advanced query, or a `ListDiscoveredResources` listing.

#### Scenario: Resource deleted in one region is reaped despite a same-id survivor

- **GIVEN** `resources` holds a row for `resourceId` in region A and a row for
  the same `resourceId` in region B
- **AND** the current build's identifier listing includes the region B resource
  but not the region A resource
- **WHEN** the identifier-driven delete runs
- **THEN** the region A row is deleted and the region B row remains

#### Scenario: Identifiers carry their region on every path

- **WHEN** the build produces resource identifiers, whether from an aggregator
  query, a non-aggregator advanced query, or a `ListDiscoveredResources` listing
- **THEN** each identifier includes the resource's `awsRegion`

### Requirement: A single database spans multiple regions only via an aggregator

A single database SHALL be considered to span multiple regions only when it is
built against a Config aggregator, whose identifier listing covers every region
it aggregates. A non-aggregator build SHALL be treated as covering a single
region, and successive non-aggregator builds into one database SHALL be assumed
to target that same region.

Because a non-aggregator build is single-region and an aggregator build's
listing is complete across its regions, the identifier-driven delete SHALL
reap over the whole table without scoping the delete to a per-build region set.

#### Scenario: Aggregator listing reaps across all its regions

- **GIVEN** a build against an aggregator whose listing covers regions A and B
- **WHEN** the identifier-driven delete runs
- **THEN** rows in either region that the listing does not report are reaped,
  and same-id rows that the listing still reports in their own region are kept

### Requirement: The merge identity is fully populated

Every row and every identifier that participates in a merge or delete SHALL
carry a non-null `awsRegion`. Config-sourced rows carry the region AWS Config
records, and rows for global services carry the literal `global` that AWS Config
already stores for them. A null `awsRegion` would make the identity match
undefined and reintroduce silent collisions.

#### Scenario: Global resources share a stable region value

- **GIVEN** a global-service resource, which AWS Config stores with `awsRegion`
  set to `global`
- **WHEN** it participates in the config-data merge or the identifier-driven
  delete
- **THEN** it matches on `awsRegion = global` rather than on a null region
