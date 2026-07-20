## Purpose

Defines what the README and AGENTS.md must contain so that both stay correct as
the CLI changes. The README delegates the CLI reference to `--help` and keeps
only the behaviour and rationale that help text cannot express. AGENTS.md
records the real command surface and the conventions no check enforces.

## Requirements

### Requirement: README delegates CLI reference to the live source
The README SHALL NOT contain exhaustive lists of CLI flags. Where a flag list
would otherwise appear, the README SHALL show a small number of illustrative
examples and direct the reader to `acd <command> --help`.

#### Scenario: A new flag is added to an existing command
- **WHEN** a flag is added to `acd build` or `acd query`
- **THEN** the README requires no edit, because it never claimed to list every
  flag and `--help` reflects the new flag automatically

#### Scenario: Reader wants the full set of options
- **WHEN** a reader needs options the examples do not cover
- **THEN** the README points them to `acd <command> --help`

#### Scenario: Command aliases
- **WHEN** the README shows example invocations
- **THEN** it notes the short aliases `acd b`, `acd q`, and `acd r` once,
  rather than repeating them per command

### Requirement: README documents behaviour that --help cannot express
The README SHALL retain prose explaining behaviour and rationale that is not
derivable from flag help text, such as why builds are incremental and when a
cross-account aggregator applies.

#### Scenario: Incremental build behaviour
- **WHEN** a reader wants to know why a second `acd build` is faster
- **THEN** the README explains that only resources updated since the last build
  are refetched, and that some resource types are unavailable to the select
  query and so are refetched every build

#### Scenario: Purpose of the tool
- **WHEN** a reader lands on the repository
- **THEN** the intro explains why the tool exists, independently of any flag

### Requirement: README config example is loadable
The config example in the README SHALL use only fields accepted by the config
deserializer, and SHALL include every field that the deserializer requires. The
README SHALL NOT carry the full config format inline. It SHALL delegate to the
example configuration file, which is verified by a test, while keeping a short
excerpt covering database selection and defaulting, the aggregator and
non-aggregator cases, and disabling built-in alterations, so a reader can see
the shape without leaving the page.

#### Scenario: Copying the example
- **WHEN** a user copies the README config excerpt or the example
  configuration file into their config file
- **THEN** `acd` loads it without a deserialization error

#### Scenario: Alteration entries are named
- **WHEN** the example declares a `[[schema_alterations]]` or
  `[[global_schema_alterations]]` entry
- **THEN** the entry uses the required `name` field, not `description`, since
  `description` exists only on `custom_tables`

#### Scenario: Full format lives in the tested example
- **WHEN** a reader needs config options beyond database selection
- **THEN** the README points them at the example configuration file rather than
  restating the format inline

#### Scenario: Config struct changes
- **WHEN** a config struct gains, loses, or renames a field
- **THEN** the failure surfaces as a failing test rather than as documentation
  that silently disagrees with the code

### Requirement: AGENTS.md describes the real command surface
AGENTS.md SHALL describe the commands the CLI actually exposes and SHALL NOT
describe commands that do not exist.

#### Scenario: Command list
- **WHEN** AGENTS.md lists the commands
- **THEN** it lists `build`, `query`, and `repl`

#### Scenario: Snapshots
- **WHEN** AGENTS.md mentions Config snapshots
- **THEN** it describes them as the `build --with-snapshots` option rather than
  a standalone command

### Requirement: AGENTS.md records conventions that nothing enforces
AGENTS.md SHALL state repository conventions that are not caught by any check,
so that they survive without relying on reviewer memory.

#### Scenario: SPDX headers
- **WHEN** a new source file is created
- **THEN** AGENTS.md has already stated that every source file starts with SPDX
  headers and that no lint enforces this

#### Scenario: Spec-driven workflow
- **WHEN** a change is planned through openspec
- **THEN** AGENTS.md describes the propose -> apply -> archive flow and where
  the artifacts live

#### Scenario: Non-obvious architecture
- **WHEN** code touching the database is written
- **THEN** AGENTS.md has already explained that DuckDB is blocking and that
  `db-client` drives connections from a dedicated thread behind a bb8 pool,
  which is the reason for the async preference stated elsewhere in the file
