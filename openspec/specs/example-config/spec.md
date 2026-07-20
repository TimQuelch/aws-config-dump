## Purpose

Defines the example configuration file that documents the full config format,
the test that keeps it honest, and how the README and the config types point at
it. The example is the authoritative reference, so the README can stay short.

## Requirements

### Requirement: Repository ships an example configuration
The repository SHALL contain an example configuration file that demonstrates
the supported config file format.

#### Scenario: Located at the repository root
- **WHEN** a user looks for the example
- **THEN** it is at `example-config.toml` in the repository root, where it is
  visible without navigating into the crate layout

#### Scenario: Covers the documented surface
- **WHEN** the example is written
- **THEN** it includes `default_database`, at least two entries under
  `databases`, `query_extra_columns`, `schema_alterations`,
  `global_schema_alterations`, `custom_tables`, and `builtin_alterations`

#### Scenario: Ordered from common to advanced
- **WHEN** a reader opens the example
- **THEN** database selection appears before schema alterations and custom
  tables, so the common case is readable without understanding the rest

### Requirement: The example is verified by a test
The test suite SHALL deserialize the example configuration into the config
types, so that a change to those types which invalidates the example fails the
suite.

#### Scenario: Example deserializes
- **WHEN** `cargo nextest run` executes
- **THEN** a test parses the example into `ConfigFile` and the parse succeeds

#### Scenario: A required field is added to a config struct
- **WHEN** a new required field is added to `SchemaAlteration`, `CustomTable`,
  or any other config struct, and the example is not updated
- **THEN** the test fails

#### Scenario: Example is meaningfully populated
- **WHEN** the test runs
- **THEN** it asserts on selected values from the parsed result, so an example
  emptied of content does not pass merely by being valid TOML

#### Scenario: Test is sandbox safe
- **WHEN** the test runs
- **THEN** it reads the example through `include_str!` at compile time, making
  no assumption about filesystem layout, network access, or credentials, and
  no assumption about test ordering

### Requirement: Config types point editors at the documentation
The config type definitions in `acd-cli/src/config.rs` SHALL carry a comment
reminding an editor that changing the config interface or behaviour requires
updating the example configuration and the README config section.

#### Scenario: Editing a config struct
- **WHEN** a developer opens `acd-cli/src/config.rs` to add or change a field
- **THEN** a comment above the config type definitions tells them that
  `example-config.toml` and the README config section need to match

#### Scenario: Covers what the test cannot
- **WHEN** the reminder is written
- **THEN** it names the README config section explicitly, since the example
  file is already protected by a test but the README excerpt is not

### Requirement: README delegates the full config format to the example
The README SHALL keep a short config section covering the common cases, and
SHALL delegate the full format to the example configuration file rather than
duplicating it inline.

#### Scenario: Common cases stay in the README
- **WHEN** the README config section is written
- **THEN** it covers selecting a database and how defaulting works, the
  difference between an aggregator and a non-aggregator setup, and how to
  disable built-in alterations

#### Scenario: Reader wants the full format
- **WHEN** a reader needs config options beyond those common cases
- **THEN** the README directs them to `example-config.toml`

#### Scenario: A config struct changes
- **WHEN** a config struct gains, loses, or renames a field outside the common
  cases the README covers
- **THEN** the README needs no edit, because the authoritative example is
  covered by a test

#### Scenario: README stays consistent with the tested example
- **WHEN** the README names a config field
- **THEN** that field also appears in the example configuration, so the two do
  not disagree
