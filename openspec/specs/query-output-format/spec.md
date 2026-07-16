## Purpose

Defines the supported output formats for `acd query` results and how the
format is selected.

## Requirements

### Requirement: Query output format selection
The `acd query` command SHALL accept a `--format`/`-o` option with allowed
values `tsv`, `json`, `csv`, and `ndjson`. When omitted, the command SHALL
default to `tsv`, preserving prior behavior.

#### Scenario: Default format is TSV
- **WHEN** a user runs `acd query` without a `--format`/`-o` flag
- **THEN** results are printed tab-separated, identical to current behavior

#### Scenario: Explicit TSV format
- **WHEN** a user runs `acd query --format tsv`
- **THEN** results are printed tab-separated

#### Scenario: JSON format
- **WHEN** a user runs `acd query --format json` (or `-o json`)
- **THEN** results are printed as a single JSON array of row objects, using
  DuckDB's own `.mode json` output, with one object per result row and
  object keys matching the selected/queried column names

#### Scenario: CSV format
- **WHEN** a user runs `acd query --format csv` (or `-o csv`)
- **THEN** results are printed as comma-separated values, using DuckDB's own
  `.mode csv` output, with a header row of the selected/queried column names
  followed by one row per result

#### Scenario: NDJSON format
- **WHEN** a user runs `acd query --format ndjson` (or `-o ndjson`)
- **THEN** results are printed as newline-delimited JSON (one JSON object
  per result row, with no enclosing array), using DuckDB's own
  `.mode jsonlines` output, with object keys matching the selected/queried
  column names

#### Scenario: Invalid format value
- **WHEN** a user runs `acd query --format xml` (or any value other than
  `tsv`/`json`/`csv`/`ndjson`)
- **THEN** the command SHALL fail with a CLI argument error before executing
  any query, consistent with how clap rejects other invalid enum values
