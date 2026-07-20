## Context

Two documentation files have drifted from the code. The README documents 6 of
13 query flags and 4 of 5 build flags, and its config example fails to load.
CLAUDE.md describes a `snapshot` command that has never existed.

The missing query flags (`--format`, `--sort`, `--reverse`, `--id`, `--name`,
`--exclude-fields`, `--all-fields`) all arrived in the last two feature
commits. That is the useful signal: the README did not decay slowly, it fell
out of date the moment a feature shipped. Any fix that restores the lists
without changing their structure will fail the same way on the next commit.

The two files have different readers and different economics. The README is
read once by a human evaluating or configuring the tool. CLAUDE.md is injected
into the agent's context every session, so every line costs tokens and carries
its own staleness risk.

## Goals / Non-Goals

Goals:

- Make README sections structurally resistant to going out of date
- Correct the two statements that are actively wrong
- Keep CLAUDE.md short, and only state things an agent could not cheaply infer

Non-Goals:

- Comprehensive CLI reference in the README. That is what `--help` is for
- A verified config example. Deferred to `add-tested-example-config`
- Any change to code, CLI behaviour, or checks
- Restructuring the intro or installation sections, which do not drift

## Decisions

### Delegate to a live source wherever one exists

A section is durable when it defers to something that cannot disagree with the
code. The README already does this once, for shell completions, by linking to
the `clap_complete` docs instead of restating them.

Auditing each section for an available live source:

| Section        | Churn | Live source        |
| -------------- | ----- | ------------------ |
| intro / why    | none  | n/a, concepts      |
| installation   | low   | n/a                |
| completions    | low   | upstream docs      |
| build flags    | high  | `acd build --help` |
| query flags    | high  | `acd query --help` |
| global flags   | low   | `acd --help`       |
| repl           | none  | n/a                |
| config TOML    | high  | none               |

Every high-churn section has a live source except the config file. Flag lists
therefore collapse to examples plus a `--help` pointer.

Alternative considered: complete the flag lists. Rejected because it grows the
README by roughly 25 lines and re-arms the same trap. The reader gains a
reference they can already get from `--help` and shell completion.

### Examples over lists, and why examples are safer

Examples are not immune to drift, but they fail on a rarer event. Adding a flag
leaves existing examples valid; only renaming or removing a flag breaks them.
Adding is what actually happens in this repository, four times across the last
two commits.

### Keep behavioural prose

Flag help describes what an option does, not why the tool behaves as it does.
Incremental build semantics and the role of aggregators stay in the README
because `--help` cannot carry them, and because concepts churn far more slowly
than flag names.

### Config example stays hand-written, for now

The config file has no `--help` equivalent, so delegation is unavailable.
Options considered:

1. Keep prose and accept rot. Status quo, already demonstrated to fail
2. Point at the `ConfigFile` struct in `acd-cli/src/config.rs`. Durable but
   unfriendly to someone who just wants to write a config file
3. Ship an `example-config.toml` that a test loads, and link it from the README

Option 3 is the only one that moves the guarantee from discipline to
machinery, but it adds a file and a test, which is beyond a docs edit. Decision
is to fix and simplify the example now under option 1, and schedule option 3 as
`add-tested-example-config`.

### CLAUDE.md: correctness first, brevity second

The phantom `snapshot` command is the highest-cost error in either file,
because an agent acts on it. Beyond correctness, only content that cannot be
cheaply inferred earns a line. The crate map gets one line, since three crates
named `acd-cli`, `aws-client`, and `db-client` largely describe themselves. The
blocking-DuckDB rationale gets a line because it is not inferable from a
directory listing and it constrains how database code is written.

## Risks / Trade-offs

- Readers lose an at-a-glance capability list on the GitHub landing page ->
  mitigated by choosing examples that exercise the interesting flags, so
  capability is still visible
- Examples can still drift if a flag is renamed -> accepted, since renames are
  rare here and a rename already breaks users' scripts, making it visible
- The config example remains unverified -> tracked and closed by
  `add-tested-example-config`
- The openspec line in CLAUDE.md describes the flow without setting a threshold
  for when to use it -> intentional, since the workflow is invoked explicitly
