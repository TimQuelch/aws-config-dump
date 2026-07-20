## Why

The README and CLAUDE.md have drifted from the code, and in two places they are
now wrong rather than merely incomplete. The README config example fails to
load because it uses a `description` field that `SchemaAlteration` does not
have, and CLAUDE.md describes a `snapshot` command that does not exist.

The drift is not slow decay. The README documents 6 of 13 query flags, and the
7 missing ones all arrive from the last two feature commits. Hand-maintained
copies of `--help` go stale the moment a flag is added, so the fix is
structural: document the parts that have no other source, and delegate the rest
to the live source.

## What Changes

README:

- Replace the exhaustive build, query, and global flag lists with a few
  illustrative examples plus a pointer to `acd <command> --help`
- Keep the prose that explains behaviour (incremental builds, aggregators),
  since that is what `--help` cannot say
- Fix the config example so it loads: `description` becomes `name` on both
  `schema_alterations` and `global_schema_alterations`
- Keep the config example fairly simple rather than exhaustive. A tested
  example config file is deferred to a follow-up change
- Leave the intro paragraph and installation sections untouched

CLAUDE.md:

- Correct the architecture section. The commands are `build`, `query`, and
  `repl`. Snapshots are a `build --with-snapshots` option, not a command
- Add one line on the crate layout and one on why `db-client` wraps DuckDB
  (DuckDB is blocking, so connections are driven from a dedicated thread behind
  a bb8 pool). This is the reason for the existing async preference
- Add one line noting SPDX headers are expected on every source file and that
  nothing lints them
- Add one line describing the openspec propose -> apply -> archive flow

## Capabilities

### New Capabilities

- `project-documentation`: how the README and CLAUDE.md are structured so they
  resist going out of date, including which content delegates to a live source
  and which is documented in full

### Modified Capabilities

None. No runtime behaviour changes.

## Impact

- `README.md` and `CLAUDE.md` only. No code, dependencies, or CLI behaviour
  changes, so no test or check impact
- The config section remains hand-written and can still rot. The follow-up
  change `add-tested-example-config` closes that gap by moving the example into
  a file that a test loads
