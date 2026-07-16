## 1. CLI flag

- [x] 1.1 Add `OutputFormat` enum (`Tsv`, `Json`) deriving `clap::ValueEnum`, `Clone`, `Copy` in `acd-cli/src/cli.rs`, with `Tsv` as `#[default]`
- [x] 1.2 Add `format: OutputFormat` field to `QueryArgs` with `#[arg(short = 'o', long, default_value_t = OutputFormat::Tsv)]`

## 2. Query execution

- [x] 2.1 Add `format: OutputFormat` to `query::QueryParams`
- [x] 2.2 In `query.rs`, map `OutputFormat` to the DuckDB `.mode` string (`"tabs"` for `Tsv`, `"json"` for `Json`) and use it in the `-cmd` argument passed to the `duckdb` subprocess (replacing the hardcoded `.mode tabs`)

## 3. Wiring

- [x] 3.1 Thread `format` from `cli::QueryArgs` through `main.rs`'s `Command::Query` match arm into `query::QueryParams`

## 4. Verification

- [x] 4.1 Add/extend a test (or manual `cargo run -- query --format json` check) confirming JSON output is valid, parseable JSON matching the queried columns
- [x] 4.2 Confirm default (`acd query` with no `--format`) still produces identical TSV output to before the change
- [x] 4.3 Run `cargo nextest run`, `cargo clippy`, and check `acd query --help` / shell completions reflect the new flag correctly

## 5. Extend output formats (CSV, NDJSON)

- [x] 5.1 Add `Csv` and `Ndjson` variants to `OutputFormat` in `acd-cli/src/cli.rs` (`clap::ValueEnum`) — name the variant `Ndjson` (not `NdJson`) so clap's default kebab-case rendering produces the CLI value `ndjson` directly; extend the `Display` impl accordingly
- [x] 5.2 In `query.rs`, extend the `OutputFormat` → DuckDB `.mode` mapping: `Csv` → `"csv"`, `Ndjson` → `"jsonlines"`
- [x] 5.3 Add/extend a test (or manual `cargo run -- query --format csv` / `--format ndjson` check) confirming CSV output is well-formed (header row + comma-separated rows) and NDJSON output is one valid JSON object per line
- [x] 5.4 Confirm `tsv`/`json` behavior is unchanged after the enum extension
- [x] 5.5 Run `cargo nextest run`, `cargo clippy`, and check `acd query --help` / shell completions list `csv` and `ndjson` correctly
