## Why

A fetcher currently emits its rows as pre-serialised JSON lines: it builds the
whole `resources` object itself, including the twelve fields every fetcher must
fill the same way, and terminates the line with a newline. The OU fetcher is the
only fetcher today and it already carries about forty lines of that boilerplate,
most of it commented to explain constraints that are properties of the spine
rather than of OUs -- `supplementaryConfiguration` must be `{}` not null,
`tags` must be an array of key/value entries, the Config-only columns must be
null rather than sentinels.

Every one of those constraints is invisible at the point where the next fetcher
is written. Getting one wrong is not a compile error and usually not a landing
error either: a null `supplementaryConfiguration` lands fine and silently loses
the derived table, and a `tags` object instead of an entry array fails only
inside DuckDB, at `map_from_entries`, after the fetch has already succeeded.
The seam should make the common fields unwritable rather than merely documented.

## What Changes

- Fetchers emit a typed `Row` struct instead of a `String`. `FetchContext.rows`
  becomes `mpsc::Sender<Row>`, and a fetcher supplies only the fields it
  actually knows: identity (`arn`, `account_id`, `aws_region`, `resource_type`,
  `resource_id`, `resource_name`), `captured_at`, `tags`, and a free-form
  `configuration`.
- Tags stop being converted into a map and back. AWS Organizations returns
  key/value pairs, the spine stores key/value entries, and nothing in between
  looks a tag up by key, so `org_client::list_tags_for_resource` stops folding
  them into a `HashMap` and the entry list is carried through unchanged.
- Serialisation to the spine's JSON-line shape moves into one place. The
  spine-wide invariants -- entry-array `tags`, empty-object
  `supplementaryConfiguration`, empty `relationships`, null Config-only columns,
  trailing newline -- become properties of that one conversion, exercised by its
  own tests, rather than something each fetcher restates.
- The OU fetcher's row mapping shrinks to the fields specific to an OU. Its
  emitted row shape is unchanged: the same JSON reaches the spool, so nothing
  downstream of the fetch moves.

No breaking changes. No configuration, schema, CLI, or stored-row surface moves;
this is an internal refactor of how fetchers express a row.

## Capabilities

### New Capabilities
<!-- None. This restructures how an existing capability is expressed. -->

### Modified Capabilities
- `custom-fetchers`: a fetcher emits typed rows and the spine-wide fields are
  supplied by the seam, not by the fetcher. The row shape that reaches the
  database is specified as an invariant of the seam rather than of each
  fetcher.

## Impact

- `acd-cli/src/fetcher.rs` -- adds `Row` and its conversion to a spine JSON
  line; `FetchContext.rows` changes type; `fetch_rows` adapts the typed channel
  onto `util::temp_file_writer`'s `String` channel. Test doubles (`FakeFetcher`,
  `DbReadingFetcher`) move to emitting `Row`.
- `acd-cli/src/builtin_fetchers/organizational_units.rs` -- `ou_to_json_line`
  becomes `ou_to_row`; its tests move from asserting on a JSON line to asserting
  on struct fields, with the shared-shape assertions relocating to `fetcher.rs`.
- `aws-client/src/org_client.rs` -- `list_tags_for_resource` returns the
  key/value entries the API gave it instead of folding them into a `HashMap`;
  `OrganizationalUnit::tags` changes type to match. Its tag tests move from
  map lookups to searching the entry list.
- `aws-client/src/lib.rs` -- a small owned `Tag { key, value }` type, so `Row`
  can carry tags without naming any one service's SDK type.
- `acd-cli/src/util.rs` -- unchanged. The spool stays a `String` sink, so its
  all-or-nothing failure behaviour is untouched.
- `acd-cli/src/build.rs` -- its `fake_row` test helper built the same JSON by
  hand and becomes a `Row`, which is the boilerplate reduction showing up in
  the tests as well.
- `acd-cli/src/db.rs` -- `land_fetched_rows` is unchanged, and that is the
  point: it and the OU hierarchy tests are the contract the conversion must
  keep. Gains one test that lands a row built by the conversion itself, since
  the `tags` and `supplementaryConfiguration` constraints belong to DuckDB and
  no in-process assertion on the JSON can catch a violation.
