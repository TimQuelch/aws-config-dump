## Why

The build phase matches a resource against the stored `resources` table using
`(accountId, resourceType, resourceId)`. Some resource types reuse the same
`resourceId` in different regions, so two distinct resources collide on this
key. The config-data merge then overwrites one region's row with the other's,
and the identifier-driven delete cannot reap a resource that vanished in one
region while a same-id resource still exists in another. The stored `awsRegion`
column already distinguishes these rows; the merge identity simply ignores it.

## What Changes

- Add `awsRegion` to the identity used to match a resource across builds, so the
  match key becomes `(accountId, awsRegion, resourceType, resourceId)`.
- Widen the config-data merge and the fetcher landing merge to key on this
  identity, so same-id rows in different regions no longer overwrite each other.
- Widen the identifier-driven delete to key on this identity, so a resource that
  disappeared in one region is reaped even when a same-id resource survives in
  another. This requires the resource identifier listing to carry `awsRegion`,
  which is added to every path that produces identifiers.
- Establish the contract that a single database spans multiple regions only when
  built against an aggregator; non-aggregator builds are single-region. This
  keeps the whole-table delete correct without per-build region scoping.

## Capabilities

### New Capabilities
- `resource-merge-identity`: How the build phase identifies a resource for the
  purpose of merging config data, landing fetcher rows, and reaping deleted
  rows, including the role of region in that identity and the single-database
  multi-region contract.

### Modified Capabilities
<!-- The custom-fetchers spec describes fetcher merge semantics and the
     identifier-driven delete only in terms of abstract rows; it does not pin
     the identity columns, so its requirements remain true unchanged. No delta. -->

## Impact

- `acd-cli/src/db.rs`: the config-data merge, the identifier-driven delete, the
  `fetched_temp` merge in `land_fetched_rows`, and the `identifiers_temp` schema.
- `aws-client/src/config_client.rs`: the `SELECT` identifier query, both
  identifier serializers, and `AccountFetcher` capturing its region so
  `ListDiscoveredResources` identifiers can be stamped with it.
- No new dependencies. No user-facing CLI changes. Existing single-region
  databases are unaffected; the identity gains a column whose value is constant
  for them.
