## Why

The config file is the only part of the documentation with no live source to
defer to. Every other high-churn section of the README can point at
`acd <command> --help`, but a config file has no equivalent, so the README
carries a hand-written TOML example that nothing verifies.

That example has already gone wrong once. It used a `description` field on
`schema_alterations`, which the deserializer rejects, so anyone who copied it
got a config that failed to load. The sibling change `docs-durable-readme`
fixes the text, but leaves the guarantee resting on whoever remembers to update
the README when a config struct changes.

This change replaces that discipline with machinery: an example config that a
test loads, so a struct change that invalidates the example fails CI.

## What Changes

- Add `example-config.toml` at the repository root covering the documented
  config surface: `default_database`, `databases`, `query_extra_columns`,
  `schema_alterations`, `global_schema_alterations`, `custom_tables`, and
  `builtin_alterations`
- Add a test that loads the file through `ConfigFile` and asserts it
  deserializes, so a renamed or newly required field fails the test suite
- Warn on unknown config fields instead of ignoring them, using
  `serde_ignored` at the single deserialization site. An unknown field is
  usually a typo or a renamed field, and today it is silently discarded
- Keep a short config section in the README covering database selection and
  defaulting, the aggregator and non-aggregator cases, and how to disable
  built-in alterations, with the example file linked for everything else
- Add a comment above the config type definitions pointing editors at the
  example and the README, so the coupling is visible at the place it is
  broken

## Capabilities

### New Capabilities

- `example-config`: an example configuration that is verified by a test and
  serves as the reference for the config file format
- `config-validation`: how the config loader reports fields it does not
  recognise

### Modified Capabilities

- `project-documentation`: the requirement that the README config example be
  loadable becomes a stronger requirement that the example be mechanically
  verified, and the README delegates to the example file. Depends on
  `docs-durable-readme` landing first, since it introduces that spec

## Impact

- New `example-config.toml` at the repository root and a new test in `acd-cli`
- `acd-cli/src/config.rs`: one deserialization site changes to report ignored
  fields. New `serde_ignored` dependency, which must clear `cargo deny` and
  `cargo audit`
- `README.md` config section shortened to the common cases and pointed at the
  example
- Behaviour change: a config with an unrecognised field now prints a warning
  where it previously printed nothing. The config still loads, and no existing
  config stops working
- The example is documentation and is not read at runtime. It reaches the test
  through `include_str!`, not the filesystem
- Sequencing: land `docs-durable-readme` first. This change supersedes the
  hand-written example that change leaves in place
