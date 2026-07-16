## Context

`acd query` (`acd-cli/src/query.rs:170-185`) builds a SQL string and hands it
to the external `duckdb` CLI binary as a subprocess:

```rust
Command::new("duckdb")
    .args(["-readonly", "-safe", "-cmd", ".mode tabs"])
    .arg(&config.db_path)
    .arg(&final_query)
    .spawn()?
    .wait()?;
```

stdout/stderr are inherited (not captured), so output streams directly to
the terminal or whatever the caller redirects it to. The doc comment on
`query()` states this subprocess approach exists specifically "so we don't
need to implement TSV formatting" in Rust. The DuckDB CLI supports several
`.mode` values, including `json`, `csv`, and `jsonlines`, so the same
delegation pattern extends naturally to all three.

There is no existing `clap::ValueEnum` anywhere in the codebase; the closest
precedent for a mode-like choice is `FetchSource` (`acd-cli/src/build.rs:25-29`),
a plain enum built from separate boolean flags rather than a single
multi-valued flag. This change introduces the first `ValueEnum` flag.

## Goals / Non-Goals

**Goals:**
- Let users get `acd query` results as JSON, CSV, or NDJSON instead of TSV,
  selected with a single flag.
- Keep `tsv` as the default so existing scripts/pipelines are unaffected.
- Reuse the DuckDB CLI's own formatting (no hand-rolled serialization).

**Non-Goals:**
- Changing `acd repl`'s output mode — the REPL stays interactive/tabular via
  the DuckDB CLI's own defaults.
- Pretty-printing/formatting flags (indentation, color, etc.) for JSON
  output, or custom dialect options (delimiter, quote character) for CSV
  output — only DuckDB's default `.mode` behavior for each format is
  exposed.
- Capturing/post-processing subprocess stdout in Rust — output still streams
  straight from the `duckdb` CLI subprocess.

## Decisions

**Flag shape: `clap::ValueEnum` with `-o`/`--format <tsv|json|csv|ndjson>`, default `tsv`.**
A single enum flag (vs. boolean flags per format) keeps the CLI surface
small and makes it straightforward to add further formats later without a
proliferation of mutually-exclusive boolean flags. Short flag `-o` (for
"output format") was chosen over `-j`/`-c` since it doesn't presuppose any
one format is privileged over the others. `-o` is not used elsewhere in
`QueryArgs` or the global args.

**Format-to-`.mode` mapping: `tsv`→`tabs`, `json`→`json`, `csv`→`csv`, `ndjson`→`jsonlines`.**
Three of the four map straightforwardly by name. `ndjson` is the exception:
DuckDB's CLI calls this mode `jsonlines`, but `ndjson` (Newline-Delimited
JSON) is the more widely recognized name for this format outside DuckDB
specifically (e.g. ndjson.org, tooling like `jq`/`fx`), so the CLI-facing
flag value is `ndjson` while the internal `.mode` string passed to the
subprocess stays `jsonlines`. The enum variant is named `Ndjson` (not
`NdJson`) so clap's default kebab-case rendering produces the CLI value
`ndjson` directly without needing an explicit `#[value(name = ...)]`
override. `json` keeps DuckDB's own naming (single JSON array of row
objects) since it matches what most users mean by "give me JSON" and what
tools like `jq` expect by default; `ndjson` is offered as the explicit
streaming-friendly alternative rather than replacing it.

**Mapping happens in `query.rs`, not `cli.rs`.**
`QueryArgs` gains a `format: OutputFormat` field (clap-derived); `query.rs`'s
`query()` function maps `OutputFormat` to the corresponding `.mode` string
(`"tabs"` / `"json"` / `"csv"` / `"jsonlines"`) when building the `-cmd`
argument, keeping DuckDB-CLI knowledge out of the arg-parsing module,
consistent with how `build.rs`'s `FetchSource` is interpreted in
`build.rs`/`main.rs` rather than `cli.rs`.

## Risks / Trade-offs

- **[Risk]** DuckDB's JSON/CSV value formatting (timestamps, decimals,
  NULLs, nested `configuration` JSON columns, CSV quoting/escaping of
  commas and newlines within field values) may differ from how the same
  values render in TSV mode, surprising users who diff output across
  formats. → **Mitigation**: this is inherent to delegating formatting to
  DuckDB; no Rust-side normalization is in scope. Document the flag as
  using DuckDB's own `.mode` behavior for each format so behavior is
  traceable to DuckDB's docs.
- **[Risk]** `.mode json` buffers/renders as a single JSON array, which may
  have different memory/streaming characteristics than the row-by-row
  `tabs`/`csv`/`jsonlines` modes for very large result sets. →
  **Mitigation**: default stays `tsv`; JSON is opt-in for callers who need
  a single structured document and accept the trade-off. Callers who want
  a streaming-friendly structured format can use `ndjson` instead.

## Migration Plan

None required — purely additive, opt-in flag with a backward-compatible
default (`tsv`). No data or schema changes.

## Open Questions

None outstanding; flag shape and the format-to-mode mapping (including the
`ndjson`/`jsonlines` naming split) are decided above.
