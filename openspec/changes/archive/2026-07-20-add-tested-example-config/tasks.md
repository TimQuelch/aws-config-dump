## 1. Write the example

- [x] 1.1 Write `example-config.toml` at the repository root, covering
      `default_database`, `databases`, `query_extra_columns`,
      `schema_alterations`, `global_schema_alterations`, `custom_tables`, and
      `builtin_alterations`
- [x] 1.2 Order it from common to advanced, with database selection first
- [x] 1.3 Show both an aggregator and a non-aggregator database entry, since
      that is the distinction most likely to confuse a new user
- [x] 1.4 Add the SPDX header, matching the convention used by other files

## 2. Warn on unknown fields

- [x] 2.1 Add the `serde_ignored` dependency to `acd-cli`
- [x] 2.2 Route `ConfigFile::load` through `serde_ignored` so ignored fields
      invoke a callback, and emit a `warn!` naming the full path of each
- [x] 2.3 Add a test that a config with an unknown field still loads
- [x] 2.4 Add a test that a config missing a required field still fails, so
      the change does not soften genuine errors
- [x] 2.5 Run `cargo deny check` and `cargo audit` for the new dependency
- [x] 2.6 Add a comment above the config type definitions in
      `acd-cli/src/config.rs` reminding editors that a change to the config
      interface or behaviour needs `example-config.toml` and the README config
      section updated to match

## 3. Test the example

- [x] 3.1 Add a test that loads `example-config.toml` with `include_str!` and
      deserializes it into `ConfigFile`, following the pattern in
      `acd-cli/src/builtin_alterations.rs`
- [x] 3.2 Assert on a few parsed values so an emptied example does not pass
- [x] 3.3 Confirm the test makes no filesystem, network, credential, or
      ordering assumptions
- [x] 3.4 Verify the test fails when a required field is removed from the
      example, then restore it

## 4. Point the README at it

- [x] 4.1 Shorten the README config section to database selection and
      defaulting, the aggregator and non-aggregator cases, and disabling
      built-in alterations
- [x] 4.2 Link `example-config.toml` as the reference for everything else
- [x] 4.3 Confirm no config field name appears in the README that is not also
      in the tested example

## 5. Verify

- [x] 5.1 `cargo nextest run` passes
- [x] 5.2 `cargo clippy --all-targets -- --deny warnings` passes
- [x] 5.3 Re-read the README and example against
      `specs/example-config/spec.md` and the delta in
      `specs/project-documentation/spec.md`
