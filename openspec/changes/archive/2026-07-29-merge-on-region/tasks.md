## 1. Widen the config and fetcher merges (tier 1: stop the overwrite)

- [x] 1.1 Add `awsRegion` to the `USING (...)` key of the config-data merge in
      `build_resources_table` (`acd-cli/src/db.rs`)
- [x] 1.2 Add `awsRegion` to the `USING (...)` key of the `fetched_temp` merge in
      `land_fetched_rows` (`acd-cli/src/db.rs`)
- [x] 1.3 Add a `db-client`/`acd-cli` test: two rows sharing `(accountId,
      resourceType, resourceId)` but differing in `awsRegion` both survive a
      config-data merge and update independently
      (`same_resource_id_in_two_regions_does_not_collide`)

## 2. Plumb region into the identifier listing (tier 2 prerequisite)

- [x] 2.1 Add an `awsRegion` column to `identifiers_temp` in
      `build_resources_table` (`acd-cli/src/db.rs`)
- [x] 2.2 Add `awsRegion` to the `SELECT` in `get_resource_identifiers_with_select`
      (`aws-client/src/config_client.rs`)
- [x] 2.3 Emit `awsRegion` from `source_region()` in
      `WrappedAggregateResourceIdentifier` and bump its serialized field count
      (`aws-client/src/config_client.rs`)
- [x] 2.4 Capture the configured region in `AccountFetcher` at construction (from
      the loaded `SdkConfig`) and stamp it onto `WrappedResourceIdentifier`,
      bumping its serialized field count (`aws-client/src/config_client.rs`)

## 3. Widen the identifier-driven delete (tier 2: fix the reap)

- [x] 3.1 Add `awsRegion` to the `USING (...)` key of the identifier-driven
      delete in `build_resources_table` (`acd-cli/src/db.rs`)
- [x] 3.2 Add a test: a row present in region A and a same-id row in region B,
      with an identifier listing that omits the region A resource, reaps only
      the region A row and keeps region B
      (`resource_deleted_in_one_region_is_reaped_despite_a_same_id_survivor`)

## 4. Guard the identity and verify global resources

- [x] 4.1 Add a test asserting every produced identifier carries a non-null
      `awsRegion` (covers the SELECT path; assert the serializers emit the field).
      Done via `account_serialize_identifier`, `aggregate_serialize_identifier`,
      and the two `*_get_resource_configs_and_identifiers_with_batch` tests, all
      of which now assert `awsRegion`.
- [ ] 4.2 Resolve the open question: inspect a real build's config item for an
      unselectable global resource type and confirm whether `awsRegion` reads
      `global` or the recording region; if `global`, stamp `global` for those
      types on the non-aggregator `ListDiscoveredResources` path.
      NOT done: requires a real AWS build (credentials + network), which the
      sandbox cannot run. The risk is documented in a comment on
      `AccountFetcher::region`; selectable global types are already safe because
      their identifiers come from the advanced-query path.
- [x] 4.3 Confirm global/fetcher rows (region `global`) still match on the config
      merge and are not reaped by the delete. Covered by the existing delete-trap
      tests, whose OU (`global`) and Config (`global`) rows round-trip correctly
      now that `identifier_json` emits `awsRegion = global`.

## 5. Verify

- [x] 5.1 `cargo nextest run` passes (210 tests, 0 failures)
- [x] 5.2 `cargo clippy --all-targets` is clean under the pedantic settings (no
      new lints, no suppressions)
- [x] 5.3 Run `openspec validate merge-on-region` and fix any reported issues
