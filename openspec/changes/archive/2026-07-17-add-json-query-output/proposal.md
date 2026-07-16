## Why

`acd query` only ever emits tab-separated output, because it shells out to the
`duckdb` CLI with `.mode tabs` (`acd-cli/src/query.rs:170-185`) specifically to
avoid implementing formatting itself. TSV is awkward to consume from scripts
that want structured data (e.g. piping into `jq`, feeding another program, or
preserving nested/array fields like `configuration` cleanly) — those callers
currently have to re-parse TSV by hand. DuckDB's CLI already has built-in
`.mode json`, `.mode csv`, and `.mode jsonlines` modes, so this capability can
be added with effectively no new formatting code. CSV output serves
spreadsheet/CSV-consuming tooling, and newline-delimited JSON ("NDJSON")
serves callers that want to stream/process one JSON object per line without
buffering the whole result set as a single array.

## What Changes

- Add a `--format`/`-o` flag to `acd query` accepting `tsv` (default,
  preserves current behavior), `json`, `csv`, or `ndjson`.
- Each non-default format maps to the corresponding DuckDB CLI `.mode`:
  `json` → `.mode json` (single JSON array of row objects), `csv` →
  `.mode csv`, `ndjson` → `.mode jsonlines` (one JSON object per line).
- No changes to query building, filtering, or the REPL/build commands.

## Capabilities

### New Capabilities
- `query-output-format`: Defines the supported output formats for `acd
  query` results (currently TSV, JSON, CSV, and NDJSON) and how the format
  is selected.

### Modified Capabilities
(none — output formatting is not yet specified elsewhere)

## Impact

- `acd-cli/src/cli.rs`: new `--format`/`-o` arg on `QueryArgs`, likely a
  `clap::ValueEnum`.
- `acd-cli/src/query.rs`: `query()` picks the `.mode` string passed to the
  `duckdb` CLI subprocess based on the selected format.
- `acd-cli/src/main.rs`: thread the new field through to `query::QueryParams`.
- Shell completions regenerate automatically via `clap_complete` for the new
  flag; no manual completion script changes expected.
- No new dependencies — the DuckDB CLI binary already supports `.mode json`,
  `.mode csv`, and `.mode jsonlines` (exposed to users as `ndjson`).
