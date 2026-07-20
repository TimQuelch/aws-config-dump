## Context

`docs-durable-readme` establishes that documentation should defer to a live
source wherever one exists. It found exactly one high-churn section with no
live source available: the config file example.

The config surface is not small. `ConfigFile` in `acd-cli/src/config.rs` has
seven top-level fields, three of which are arrays of nested structs with their
own required and optional fields. Several of those fields are undocumented
today. The failure is not hypothetical: the README example omitted a required
`name` and would not load.

There is an existing pattern in the repository for exactly this shape of
problem. `acd-cli/src/builtin_alterations.rs` embeds
`builtin_alterations.toml` with `include_str!`, parses it into the same config
types, and has a test asserting the parse succeeds and expected names are
present. This change applies that pattern to a user-facing example.

## Goals / Non-Goals

Goals:

- An example config that cannot silently disagree with the config structs
- Coverage of the full documented config surface, including the parts the
  README never mentioned
- A README config section that delegates rather than duplicates

Non-Goals:

- Runtime use of the example. It is documentation that happens to be tested
- Validating the SQL inside the example against a live database. The test
  asserts the config parses, not that its alterations are correct
- Changing the config format itself

## Decisions

### Embed with `include_str!`, do not read from disk at test time

CLAUDE.md requires that unit tests make no assumptions about filesystem layout.
Reading the example with `tokio::fs` at test time would break that rule and
would couple the test to where the binary runs from.

`include_str!` resolves at compile time relative to the source file, so the
test has no runtime filesystem dependency, stays sandbox-safe, and matches how
`builtin_alterations.rs` already loads its TOML. This also means the example
file must live somewhere reachable from an `include_str!` path in `acd-cli`.

Alternative considered: a test that walks up to the repository root to find the
file. Rejected because it assumes filesystem layout, which the testing rules
forbid.

### Parse into `ConfigFile`, not just generic TOML

Asserting that the file is valid TOML would not have caught the original bug,
since `description` was syntactically fine and the failure was a missing
required field. The test must deserialize into `ConfigFile` so that required
fields, renamed fields, and type changes all surface.

Note that `ConfigFile` does not set `deny_unknown_fields`, so a stale field
name in the example would still pass silently. Adding `deny_unknown_fields` is
worth considering as part of this change, since it is what turns a typo in a
user's real config into an error rather than a silent no-op. Recorded as an
open question because it is a behaviour change for existing users.

### Example lives at the repository root

The example config sits at the repository root as `example-config.toml`. Root
is where someone evaluating the tool will look, and `include_str!` reaches it
from `acd-cli/src/config.rs` with a relative path, so the discoverability and
test-reachability goals do not conflict after all.

### Warn on unknown fields rather than rejecting them

An unknown field in a user's config is almost always a typo or a field that
has been renamed, and today it is silently ignored. Making it a hard error via
`deny_unknown_fields` would break configs that load fine now, which is a harsh
response to what is usually a stale line in a personal config file.

`serde_ignored` wraps a serde deserializer and invokes a callback for each
ignored field, reporting the full path to it. There is exactly one
deserialization site, `ConfigFile::load` in `acd-cli/src/config.rs`, so this is
a small change at a single place plus one dependency.

The payoff is a better message than either alternative. The original README bug
would report `schema_alterations.0.description` by name, rather than a generic
parse failure or nothing at all. Default verbosity is already `WARN`, so the
warning appears without `-v`.

Note this covers only half of the original bug. The missing required `name` is
already a hard error, and should stay one, since the config genuinely cannot
load. The stray `description` is the half that becomes a warning.

Alternative considered: `deny_unknown_fields`. Rejected because it turns
working configs into failures for no safety gain, given that an ignored field
never changes behaviour. Also considered a `#[serde(flatten)]` catch-all map,
which needs no dependency but has to be added to every config struct and
interacts badly with other serde attributes.

### A comment at the config types, for the gap the test cannot cover

This change is mostly about replacing discipline with machinery, so adding a
comment that asks people to remember something is worth justifying rather than
assuming.

The test covers the example config completely: change a struct in a way the
example does not satisfy and the suite fails. It does not cover the README
excerpt, which restates a handful of fields in prose. Nothing mechanical can
reasonably check that prose, so a comment at the point of change is the best
available option for that specific gap.

The comment goes above the config type definitions in `acd-cli/src/config.rs`,
which is where someone adding or renaming a field is already looking. It should
name both `example-config.toml` and the README config section, even though only
the second is unprotected, because an editor should not have to remember which
of the two is covered.

Alternative considered: no comment, relying on the test plus review. Rejected
because the failure mode is silent for the README, which is exactly the failure
this whole change exists to remove.

### Assert on content, not only on parsing

Following the `builtin_alterations` test, the test should assert a few expected
values survive the round trip, so that the example remains meaningfully
populated rather than degenerating to an empty file that still parses.

## Risks / Trade-offs

- The example grows to cover the whole surface and becomes less approachable
  than a short snippet -> mitigated by ordering it from common to advanced, and
  by the open question below on keeping a short README excerpt
- A tested example proves the config loads, not that it is good advice ->
  accepted, since the failure being fixed is exactly the loading case
- Readers must open a second file to see the full format -> mitigated by
  keeping a short README excerpt covering the cases most people need
- The README excerpt is not covered by the example config test, so it can
  still drift -> accepted, because it is deliberately small and covers the
  fields least likely to change. Task 4.2 requires every field named in the
  README to also appear in the tested example
- `serde_ignored` is a new dependency -> small and widely used, but it needs
  to clear `cargo deny` and `cargo audit` like any other
- The reminder comment can itself go stale, for instance if the example or the
  README section is renamed or moved -> accepted, since a slightly stale
  pointer still sends the reader to roughly the right place, which is more
  than the current silence does

## Open Questions

None. The three questions on file location, the README excerpt, and unknown
field handling are resolved in Decisions above.
