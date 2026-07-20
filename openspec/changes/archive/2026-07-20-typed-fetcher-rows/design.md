# Design

## Context

`FetchContext.rows` is an `mpsc::Sender<String>` carrying newline-terminated
JSON objects shaped like the `resources` table. A fetcher builds that object
itself. Of the fifteen fields in the object, three are fetcher-specific
(`resourceName`, `tags`, `configuration`), three more are identity the fetcher
knows (`arn`, `resourceId`, `resourceType`, plus `accountId` and `awsRegion`),
and the rest are the same for every fetcher that will ever exist.

The WIP commit `3b2e9ad` starts the refactor: it introduces `Row`, a
`From<Row> for String`, and an adapter task in `fetch_rows`. It does not
compile, and the shape it emits differs from what the spine accepts in three
ways. This design settles what `Row` should carry and pins the constraints the
conversion must respect.

## Goals / Non-Goals

Goals:
- A fetcher cannot get the spine-wide fields wrong, because it cannot write
  them.
- The JSON reaching the spool is byte-equivalent in shape to what the OU
  fetcher emits today.
- The seam's failure behaviour is unchanged: a failed write is still a failed
  fetch.

Non-goals:
- Changing the `resources` schema, the landing logic, or any stored row.
- Modelling `relationships` or `supplementaryConfiguration` as typed fields. No
  fetcher populates them, and a constant is a smaller thing to be wrong about
  than an unused type.

## Decisions

### `Row` carries `captured_at`, not a clock read

`configurationItemCaptureTime` is the one non-identity common field a fetcher
must supply, because only the fetcher knows when it observed the resource. The
WIP emits `null` for it, which is a regression on two counts: the OU fetcher's
existing `capture_time_is_the_observation_time` test asserts a real timestamp,
and a null column removes the only signal of when a fetched row was seen.

It stays a parameter rather than a `Utc::now()` inside the conversion, for the
reason the existing code comments give: it keeps the mapping deterministic
under test. The OU fetcher already reads the clock once per fetch and passes
the same value to every row, which is also better than a per-row read -- one
build's rows share one capture time.

### `tags` are key/value entries the whole way through

`db.rs` builds its staging table with
`tags STRUCT(key VARCHAR, value VARCHAR)[]` and then converts with
`map_from_entries`. Serialising a `HashMap` directly, as the WIP does, produces
a JSON object, and the `COPY` into the staging table fails.

The obvious fix is to reshape a map into entries inside the conversion, but
that would leave a round trip that exists for no reason. AWS Organizations
returns tags as a list of key/value pairs; `org_client::list_tags_for_resource`
folds that list into a `HashMap`, and the conversion would immediately unfold it
back into a list. Nothing between the two ever does a lookup by key -- the map
is built, moved through `OrganizationalUnit` and `Row`, and taken apart again.

So the entry list is the representation throughout. `list_tags_for_resource`
returns what the API gave it, `OrganizationalUnit::tags` and `Row::tags` carry
that, and serialisation is a straight write with no reshaping. This also matches
the rest of the codebase: `config_client` treats `tags` as an array end to end,
to the point of coercing a non-array value back to `[]` before it reaches the
spine. The `HashMap` in `org_client` was the outlier.

A small owned `Tag { key, value }` type carries them, rather than the SDK's
`aws_sdk_organizations::types::Tag`. `Row` is a fetcher-facing type and must not
name any one service's SDK, since the next fetcher will come from a different
one. It lives in `aws-client` beside the client that produces it and derives
`Serialize` so the conversion can write it directly.

Losing the map costs key lookup, which only the tests used, and gains
deterministic ordering: the emitted rows now preserve the order AWS returned,
where a `HashMap` gave an arbitrary one per run.

This is exactly the class of mistake the change exists to prevent, which makes
it worth a test of its own on the conversion rather than only on the OU
fetcher.

### `configuration` stays free-form JSON, and the OU fetcher keeps its keys

`configuration` is per-resource-type by definition, so it stays a
`serde_json::Value`. The WIP narrows the OU's configuration to a single
`parentId` key. That breaks two things: `db.rs` queries the hierarchy through
`configuration->>'ParentId'` (capitalised), and dropping `Id`, `Arn` and `Name`
removes the mirror of the identity fields that Config-sourced rows all carry.
The OU fetcher keeps emitting `Id`, `Arn`, `Name` and `ParentId`.

If the identity mirror is worth having for every fetcher, that is a separate
change to the conversion, not something to lose by accident here.

### The typed channel adapts onto the existing spool

`util::temp_file_writer` keeps its `String` channel; `fetch_rows` puts a small
forwarding task between the fetcher and the spool. The alternative -- making
`temp_file_writer` generic over a serialisable item -- would pull `Row` into
the Config fetch path, which shares the spool and has no use for it.

The forwarding task must propagate send failures rather than discard them. The
WIP writes `let _ = rows.send(...)`, which drops the error: if the spool's
writer has given up, the fetcher keeps emitting into a channel nobody reads and
the fetch reports success with a truncated file. That is the precise data-loss
path `A fetcher's emitted rows are all-or-nothing` was written to close, so the
adapter must not reopen it. The forwarding task's outcome is joined into the
fetch result.

## Risks / Trade-offs

- A `Row` field that no fetcher sets is dead weight visible to every fetcher.
  Kept to the fields the OU fetcher and the anticipated managed-policy fetch
  both need.
- The conversion becomes a single point of failure for every fetcher's row
  shape. That is the intent, and it is why the shape assertions move onto it as
  direct tests rather than being reached only through a fetcher.

## Migration

None. No stored data or configuration is affected; a database built before this
change is indistinguishable from one built after.
