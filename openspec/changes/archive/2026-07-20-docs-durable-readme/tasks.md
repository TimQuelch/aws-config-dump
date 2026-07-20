## 1. Fix what is wrong

- [x] 1.1 In README.md, change `description` to `name` on the
      `[[schema_alterations]]` and `[[global_schema_alterations]]` entries so
      the example loads
- [x] 1.2 In CLAUDE.md, replace the architecture line that names a `snapshot`
      command with the real commands: `build`, `query`, `repl`, and describe
      snapshots as the `build --with-snapshots` option

## 2. Make the README resistant to drift

- [x] 2.1 Replace the build flag list with two or three example invocations
      that exercise the interesting options, keeping the existing prose about
      incremental builds
- [x] 2.2 Replace the query flag list with examples covering `--format`,
      `--sort`, and an id or name filter, alongside the existing examples
- [x] 2.3 Replace the global flag list with a single pointer to
      `acd <command> --help`, and note the `b` / `q` / `r` aliases once
- [x] 2.4 Trim the config example to the common cases: database selection and
      how defaulting works, an aggregator entry alongside a non-aggregator
      one, and how to disable built-in alterations. Keep `custom_tables`
      represented, since it is documented nowhere else
- [x] 2.5 Confirm the intro and installation sections are unchanged

## 3. Tighten CLAUDE.md

- [x] 3.1 Add one line naming the three workspace crates and their roles
- [x] 3.2 Add one line explaining that DuckDB is blocking, so `db-client`
      drives connections from a dedicated thread behind a bb8 pool, and link
      that to the existing async preference
- [x] 3.3 Add one line under Code Style noting SPDX headers on every source
      file and that nothing lints them
- [x] 3.4 Add one line under Development describing the openspec
      propose -> apply -> archive flow, without a usage threshold
- [x] 3.5 Fix the typos in the testing section ("envionrment varialables")
- [x] 3.6 Replace the em dashes on the commit-message and clippy lines with
      ascii, so the file follows its own writing-style rule

## 4. Verify

- [x] 4.1 Parse the README config example as TOML and load it through
      `ConfigFile` to confirm it deserializes
- [x] 4.2 Check every flag and command named in the README against
      `acd <command> --help`
- [x] 4.3 Re-read both files against the spec requirements in
      `specs/project-documentation/spec.md`
