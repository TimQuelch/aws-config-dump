## Context

The build phase lands three streams onto the `resources` spine, and all three
match rows by `(accountId, resourceType, resourceId)`:

1. Config-data merge (`build_resources_table`, `db.rs`): merges freshly fetched
   config items, updating a row when the incoming `configurationItemCaptureTime`
   is newer, else inserting.
2. Identifier-driven delete (`build_resources_table`): an anti-join against the
   identifier listing that deletes stored rows Config no longer reports
   (`WHEN NOT MATCHED BY SOURCE ... THEN DELETE`), excluding fetcher-claimed
   types.
3. Fetcher landing merge (`land_fetched_rows`, `db.rs`): merges a `Merge`-
   semantics fetcher's rows.

`resources` already stores `awsRegion` (`RESOURCES_COLUMNS`), and both staging
tables (`resources_temp`, `fetched_temp`) carry it. The identifier staging
table (`identifiers_temp`) does not: it is created with only `(accountId,
resourceType, resourceId)`, and the identifier serializers in
`aws-client/src/config_client.rs` emit exactly those three fields.

AWS Config reuses `resourceId` across regions for some resource types. Because
the merge identity omits `awsRegion`, two such resources collide: the config
merge overwrites one region's row with the other's, and the delete cannot reap
a resource that vanished in one region while a same-id resource survives in
another (the surviving identifier keeps the whole key "matched").

## Goals / Non-Goals

**Goals:**
- Make `awsRegion` part of the merge identity across all three operations.
- Give the identifier listing a region on every path that produces one, so the
  delete anti-join can key on region.
- Fix the cross-region delete staleness, not just the merge overwrite.
- Keep existing single-region databases behaving exactly as before.

**Non-Goals:**
- Supporting a multi-region database assembled from multiple single-region
  (non-aggregator) builds. Multi-region is an aggregator-only contract.
- Scoping the delete to a per-build set of observed regions. The contract above
  makes the whole-table delete correct without it.
- Any change to the resources table schema (it already has `awsRegion`) or to
  user-facing CLI surface.

## Decisions

### Decision: Match key becomes `(accountId, awsRegion, resourceType, resourceId)`

Add `awsRegion` to the `USING (...)` clause of all three merges. For the two
config/fetcher merges this is a one-token change, since their staging tables
already carry the column.

Alternative considered: a synthetic composite key column. Rejected: it adds a
derived column to maintain and hides which fields form the identity, for no
gain over listing the columns DuckDB already has.

### Decision: The delete stays a whole-table anti-join; region enters the key, not a scope

A non-aggregator build talks to exactly one region (the profile/env region), so
its identifier set covers that one region; an aggregator build's set covers all
its regions. In both cases every region present in `resources` is fully covered
by the listing, so `WHEN NOT MATCHED BY SOURCE THEN DELETE` over the whole table
is correct once region is in the key.

Alternative considered: restrict the delete to `(accountId, awsRegion)` pairs
the build actually observed, to support multi-region databases built by running
non-aggregator builds against several regions. Rejected for now: it adds an
observed-scope table and join for a workflow we are explicitly declaring
unsupported (multi-region == aggregator). Widening the key alone does not make
that workflow safe anyway, because a single-region build's listing still fails
to match the other regions' rows and would reap them.

### Decision: Plumb region into every identifier path

`identifiers_temp` gains an `awsRegion` column, and each identifier source
supplies it:

- Advanced-query identifiers (`get_resource_identifiers_with_select`): add
  `awsRegion` to the `SELECT`. Works for both the aggregator and non-aggregator
  clients, which return the recorded region per row.
- Aggregator `ListDiscoveredResources` path
  (`WrappedAggregateResourceIdentifier`): emit `source_region()`, already on the
  `AggregateResourceIdentifier`.
- Non-aggregator `ListDiscoveredResources` path (`WrappedResourceIdentifier`):
  the `ResourceIdentifier` has no region, but the call is single-region.
  `AccountFetcher` captures its region from the loaded `SdkConfig` at
  construction and stamps every identifier with it.

Alternative considered: derive the identifier's region from the config item
returned by `batch_get_resource_config` in the same batch flow. Rejected: the
identifier file is written from the listing stream before the batch fetch, and
the account's configured region is a simpler, equally correct source for a
single-region call.

### Decision: Treat `awsRegion` as always populated

Config supplies `awsRegion` for every config item, using the literal `global`
for global services; the OU fetcher and other global rows already use `global`.
The design relies on this: a null region column would make the join undefined
(`NULL != NULL`), silently reintroducing collisions and, for the delete,
reaping rows whose region failed to match. The stamping above ensures no
identifier path emits a null region.

## Risks / Trade-offs

- Global resources on the non-aggregator `ListDiscoveredResources` path get the
  account's configured region stamped onto their identifier, while their config
  item may carry `awsRegion = global`. A mismatch would make the delete reap a
  live global resource. -> Global services are the ones AWS Config records with
  `global`; verify against a real build which value the config item carries for
  the unselectable global types, and stamp `global` for them if needed. Note it
  in tasks as a validation step, not an assumption.
- An identifier path that forgets to emit region would produce null-region rows
  and silently break matching. -> Add a test asserting identifiers carry region,
  and keep the identity fully populated per the spec requirement.
- Widening the key changes merge/delete counts on the first build against an
  existing database if any prior collisions had collapsed rows. -> Expected and
  benign: previously-lost region rows reappear as inserts.

## Migration Plan

No schema migration: `resources` already has `awsRegion`. On the first build
after this change, an existing single-region database sees the region column
added to a key whose value is constant, so matching is unchanged. An existing
database that had silently collapsed cross-region rows will re-insert the
previously-lost rows on the next build. No rollback concern beyond reverting the
code; the stored data remains valid under the old key.

## Open Questions

- For unselectable global resource types on the non-aggregator path, does the
  config item's `awsRegion` read `global` or the recording region? This decides
  whether `AccountFetcher` should stamp `global` for those types rather than its
  configured region. Resolve by inspecting a real build's output.
