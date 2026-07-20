## Purpose

Defines how the config loader responds to input it cannot fully interpret, so
that mistakes in a user's config file surface instead of being silently
discarded.

## Requirements

### Requirement: Unknown config fields are reported, not silently ignored
The config loader SHALL emit a warning naming each field in the user's config
file that it does not recognise. It SHALL NOT treat an unknown field as an
error, and the config SHALL continue to load.

#### Scenario: Unknown field present
- **WHEN** a config file contains a field the deserializer does not recognise
- **THEN** a warning is emitted that names the field by its full path, for
  example `schema_alterations.0.description`, and loading continues

#### Scenario: Warning is visible by default
- **WHEN** the unknown field warning is emitted and no `-v` flag was given
- **THEN** the warning still appears, because default verbosity is already
  `WARN`

#### Scenario: Unknown fields do not break existing configs
- **WHEN** a config file that loads successfully today contains an
  unrecognised field
- **THEN** it continues to load, with the addition of a warning

#### Scenario: Missing required fields remain errors
- **WHEN** a config file omits a field the deserializer requires, such as
  `name` on a `[[schema_alterations]]` entry
- **THEN** loading fails with an error, unchanged from current behaviour,
  because the config genuinely cannot be interpreted
