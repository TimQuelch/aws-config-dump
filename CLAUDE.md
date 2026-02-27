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
- Prefer async over blocking where practical. Specifically, try to never use `std::{fs,io}` and prefer to use `tokio::{fs,io}`

## Testing
- `cargo nextest run` for tests
- `cargo nextest run <test-name>` to run a single test
- `cargo deny check` for license/dependency policy
- `cargo audit` for security advisories

## Architecture

The tool has three main commands: **build** (fetch AWS Config data into DuckDB), **snapshot** (download Config snapshots from S3), and **query** (run SQL against the local database).

