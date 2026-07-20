> **Partially implemented.** Commit `3b2e9ad` ("wip") started this refactor.
> The items it completed are checked off; the rest either finish what it began
> or correct where its emitted shape diverges from what the spine accepts. The
> tree does not currently compile -- section 2 is the minimum to get it back.

## 1. The `Row` type

- [x] 1.1 Add `Row` to `fetcher.rs` with identity fields, `tags`, and
  `configuration` as `serde_json::Value`
- [x] 1.2 Change `FetchContext.rows` to `mpsc::Sender<Row>`
- [x] 1.3 Add the `Row` -> spine JSON line conversion, filling `relationships`,
  `supplementaryConfiguration` and the Config-only columns
- [x] 1.4 Add `captured_at: DateTime<Utc>` to `Row` and emit it as
  `configurationItemCaptureTime`; the WIP emits null, which drops the only
  record of when a fetched row was observed
- [x] 1.5 Change `Row::tags` to `Vec<Tag>` and serialise it straight through as
  an array of `{"key": ..., "value": ...}` objects, not as a JSON object --
  `land_fetched_rows` stages `tags` as `STRUCT(key, value)[]` and converts with
  `map_from_entries`, so an object fails the `COPY`. See section 4: no map is
  built anywhere upstream, so there is nothing to reshape here
- [x] 1.6 Document on `Row` that the omitted fields are spine-wide constants,
  with the reason each is what it is (`supplementaryConfiguration` must be an
  object for the derived-table unnest; Config-only columns are null rather than
  sentinels)
- [x] 1.7 Drop the unused `std::fmt::Display` import the WIP added

## 2. Wiring the typed channel to the spool

- [x] 2.1 Add the forwarding task between the `Row` channel and
  `util::temp_file_writer`'s `String` channel
- [x] 2.2 Propagate the forwarding task's send failures instead of discarding
  them with `let _`; join its outcome into the fetch result so a dead spool
  writer fails the fetch rather than yielding a truncated file that looks
  successful
- [x] 2.3 Check the shutdown ordering still holds: dropping `FetchContext` must
  close the `Row` channel, which ends the forwarding task, which drops the
  `String` sender, which lets `file_handle` resolve

## 3. The OU fetcher

- [x] 3.1 Replace `ou_to_json_line` with `ou_to_row`
- [x] 3.2 Fix the signature mismatch: `ou_to_row` takes `OrganizationalUnit` by
  value while `fetch` passes a reference, so the tree does not compile. Take it
  by value and adjust the call, or take a reference and clone the strings
- [x] 3.3 Restore the `configuration` keys the WIP dropped -- `Id`, `Arn`,
  `Name` and `ParentId`, capitalised. `db.rs` reads the hierarchy through
  `configuration->>'ParentId'`
- [x] 3.4 Pass `captured_at` through to the `Row`
- [x] 3.5 Remove the now-unneeded `serde_json::json` usage for the common
  fields, keeping it only for `configuration`

## 4. Tags stop round-tripping through a map

- [x] 4.1 Add an owned `Tag { key: String, value: String }` to `aws-client`,
  deriving `Serialize`, `Clone`, `Debug`, `Eq` and `PartialEq`. Owned rather
  than `aws_sdk_organizations::types::Tag`, so `Row` does not name one
  service's SDK type
- [x] 4.2 Change `org_client::list_tags_for_resource` to return `Vec<Tag>`,
  collecting the pages' entries instead of folding them into a `HashMap`
- [x] 4.3 Change `OrganizationalUnit::tags` to `Vec<Tag>` and drop the
  now-unused `HashMap` import if nothing else in the module needs it
- [x] 4.4 Have `ou_to_row` pass the entries straight through
- [x] 4.5 Update the `org_client` tag tests, which look tags up by key
  (`fetch_organizational_units_attaches_tags`,
  `fetch_organizational_units_gives_each_ou_its_own_tags`, and the
  `HashMap::from` fixture). A small `find`-by-key helper keeps them readable
  without reintroducing a map in the production path
- [x] 4.6 Check no other caller of `OrganizationalUnit::tags` relies on map
  behaviour

## 5. Tests

- [x] 5.1 Update the five OU tests that still call `ou_to_json_line` -- they do
  not compile. Rewrite `maps_identity_fields_onto_resource_columns`,
  `attributes_to_management_account_and_global_region` and
  `records_parent_for_hierarchy_reconstruction` to assert on `Row` fields
- [x] 5.2 Move `config_only_fields_are_null_not_sentinels`,
  `supplementary_configuration_is_an_object_not_null` and
  `untagged_ou_emits_empty_tags_not_null` onto the conversion in `fetcher.rs`.
  They assert spine-wide invariants, which are no longer the OU fetcher's to
  get wrong
- [x] 5.3 Keep `capture_time_is_the_observation_time` as a conversion test,
  driven by a `Row` with a fixed `captured_at`
- [x] 5.4 Add a conversion test that `tags` serialise as key/value entries, and
  that an untagged resource yields an empty array rather than null or `{}`
- [x] 5.5 Add a conversion test that the emitted line ends in a newline and
  parses as a single JSON object
- [x] 5.6 Update `FakeFetcher` to hold `Vec<Row>`, or a small builder that makes
  one, and update the four `fetcher.rs` tests that construct it with raw JSON
  strings
- [x] 5.7 Update `DbReadingFetcher::fetch` to emit a `Row` rather than a
  formatted JSON string, keeping `resourceId` as what the test asserts on
- [x] 5.8 Add a test that the emitted line loads into the staging table used by
  `land_fetched_rows` -- the `tags` and `supplementaryConfiguration`
  constraints are DuckDB's, and no in-process assertion on the JSON alone
  catches a violation
- [x] 5.9 Confirm `db.rs`'s OU hierarchy test still passes unchanged; it is the
  contract for the `configuration` keys

## 6. Verification

- [x] 6.1 `cargo check --all-targets` clean
- [x] 6.2 `cargo clippy --all-targets` clean at pedantic
- [x] 6.3 `cargo nextest run` passes
- [x] 6.4 Diff a real `acd build`'s spooled OU rows against the pre-change
  output, or assert equality against a checked example, to confirm the emitted
  shape is unchanged
