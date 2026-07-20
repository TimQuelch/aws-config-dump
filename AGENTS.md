# AGENTS.md

This file provides guidance to coding agents working with code in this repository. `CLAUDE.md` is a symlink to this file, so Claude Code picks it up too.

# acd

CLI tool that fetches AWS Config data and stores it in a local DuckDB database.

## Build & Run
- `cargo build` / `cargo run` / `cargo check`
- `nix build` for a fully reproducible build. Don't use this for iterative checks though as it takes a while
- `nix flake check` runs all checks (clippy, tests, fmt, audit, deny). Expensive, prefer cargo directly

## Development
- Pre-commit hooks auto-run: rustfmt, clippy --fix, nixfmt, taplo
- Commit messages follow the 50/72 rule: subject 50 chars or fewer, body lines wrapped at 72
- Changes are planned with openspec in `openspec/`: propose -> apply -> archive

## Code Style
- Clippy pedantic warnings enabled. Fix, don't suppress
- rustfmt for formatting; taplo for TOML formatting
- Nix files formatted with nixfmt
- Every source file starts with SPDX headers. Copy them from a neighbouring file, nothing lints this
- Prefer async over blocking where practical. Specifically, try to never use `std::{fs,io}` and prefer to use `tokio::{fs,io}`

## Testing
- `cargo nextest run` for tests
- `cargo nextest run <test-name>` to run a single test
- `cargo deny check` for license/dependency policy
- `cargo audit` for security advisories

Unit tests should be assumed to run in a sandbox and independently of other tests.
- No network calls
- No assumptions on layout of file system
- No assumptions of available credentials (either via environment variables, files, or configuration)
- No dependencies on other unit tests, or ordering of tests

## Architecture

Cargo workspace with three crates: `acd-cli` (the binary), `aws-client` (AWS SDK wrappers for Config, IAM, Organizations, and S3 snapshots), and `db-client` (DuckDB access).

The tool has three commands: `build` (fetch AWS Config data into DuckDB), `query` (run SQL against the local database), and `repl` (interactive DuckDB shell). Config snapshots are a build option (`build --with-snapshots`), not a separate command.

DuckDB is blocking, so `db-client` drives connections from a dedicated thread behind a bb8 pool. This is the reason for the async preference above.

## Writing style

Follow the following general style guidelines when writing prose in comments, documentation, or specs
- Avoid non-standard, non-ascii, or hard to type characters such as em dashes and arrows. Prefer ascii alternatives (e.g. --, ->).
- Use simple, precise, concise language
- Don't overuse text formatting such as bold or underline.
