# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# aws-config-dump

CLI tool that fetches AWS Config data and stores it in a local DuckDB database.

## Build & Run
- `cargo build` / `cargo run` / `cargo check`
- `nix build` for a fully reproducible build. Don't use this for iterative checks though as it takes a while
- `nix flake check` runs all checks (clippy, tests, fmt, audit, deny). Expensive, prefer cargo directly

## Development
- Pre-commit hooks auto-run: rustfmt, clippy --fix, nixfmt, taplo
- Commit messages follow the 50/72 rule: subject ≤50 chars, body lines wrapped at 72

## Code Style
- Clippy pedantic warnings enabled — fix, don't suppress
- rustfmt for formatting; taplo for TOML formatting
- Nix files formatted with nixfmt

## Testing
- `cargo nextest run` for tests
- `cargo nextest run <test-name>` to run a single test
- `cargo deny check` for license/dependency policy
- `cargo audit` for security advisories

## Architecture

The tool has three main commands: **build** (fetch AWS Config data into DuckDB), **snapshot** (download Config snapshots from S3), and **query** (run SQL against the local database).

### Module Overview

- **`cli.rs`** — `clap`-based CLI definitions for all subcommands and their options
- **`config.rs`** — Singleton global config (database path via XDG dirs) using `OnceLock`
- **`build.rs`** — Core database-building logic: fetches resources, merges into DuckDB, prunes deleted resources, and creates derived per-resource-type tables
- **`config_fetch_client.rs`** — AWS API abstraction layer (see below)
- **`snapshot.rs`** — Downloads Config snapshots from S3, traversing `AWSLogs/[account]/Config/[year]/[month]/[day]/ConfigSnapshot/` structure
- **`query.rs`** — Executes DuckDB queries via subprocess, supports field selection and account/type filtering
- **`completion.rs`** — Dynamic shell completion powered by live database queries (resource types, accounts, field names)
- **`util.rs`** — `resource_table_name()`: converts `AWS::EC2::Instance` → `ec2_instance`

### AWS API Abstraction (`config_fetch_client.rs`)

The fetch client uses a trait-based design to support both single-account and cross-account aggregator modes without duplication:

- **`ConfigFetchClient` trait** — High-level interface used by `build.rs`
- **`DispatchingClient`** — Routes to either `AccountFetcher` or `AggregateFetcher` based on CLI args; uses a `dispatch!` macro to reduce boilerplate
- **`ConfigFetcher` trait** — Generic over the two fetch strategies (Select API vs Batch API)
- Concurrent fetches are limited to 8 via a `Semaphore`; results stream to temp files via `mpsc` channels

### Database Schema

The main persistent table is `resources` with columns: `arn`, `accountId`, `awsRegion`, `resourceType`, `resourceId`, `resourceName`, `availabilityZone`, `resourceCreationTime`, `configurationItemCaptureTime`, `configurationItemStatus`, `configurationStateId`, `tags`, `relationships`, `configuration` (JSON), `supplementaryConfiguration` (JSON).

Derived views (`resourceTypes`, `accounts`, `regions`) and per-resource-type tables (e.g., `ec2_instance`) that unnest the `configuration` JSON are created automatically during build.

### Build Flow

1. Fetch resource counts to enumerate all types (Select API for supported types, Batch API for the rest)
2. Stream results to temp JSON files concurrently
3. Load JSON into a staging table, MERGE into `resources` (insert/update)
4. MERGE-delete resources absent from the latest fetch
5. Create derived tables per resource type
