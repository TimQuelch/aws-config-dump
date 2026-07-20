## MODIFIED Requirements

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
