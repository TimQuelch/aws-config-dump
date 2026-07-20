<!--
SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>

SPDX-License-Identifier: GPL-3.0-only
-->

# acd

Fetches AWS Config resource data into a local [DuckDB](https://duckdb.org/) database for offline querying. AWS API results are inconsistent between services. AWS Config provides some consistent structure between different resources. By dumping everything into a local database first, you get fast, uniform SQL queries across all resource types without repeated API calls. Works with both single-account Config setups and multi-account setups with aggregators.

## Installation

### Nix

Run directly without installing:

```sh
nix run github:TimQuelch/acd -- build
```

Or add to your NixOS/home-manager config:

```nix
inputs.acd.url = "github:TimQuelch/acd";
# then reference inputs.acd.packages.${system}.default
```

### Cargo

```sh
cargo install --git https://github.com/TimQuelch/acd
```

Install shell completions as described here https://docs.rs/clap_complete/latest/clap_complete/env/index.html

## Usage

### Build the database

Fetch all resources from AWS Config into a local DuckDB file:

```sh
acd build
```

Builds are incremental by default. Only resources which have been updated since the last build will be retrieved with the select query. Some resource types are not available with the select query, and instead must be retrieved every build.

```sh
acd build --aggregator-name my-aggregator  # read cross-account data from a Config aggregator
acd build --with-snapshots                 # source from S3 Config snapshots instead of the API
acd build --rebuild                        # delete and recreate the database from scratch
```

Account names can be pulled from AWS Organizations with `--fetch-org-accounts`.

### Query

```sh
acd query --resource-type AWS::EC2::Instance
acd query --resource-type AWS::EC2::Instance --fields resourceId resourceName vpcId
acd query --resource-type AWS::EC2::Instance --where state=running
acd query --resource-type AWS::EC2::Instance --name web- --sort resourceName
acd query --resource-type AWS::EC2::Instance --format json
acd query --resource-type AWS::EC2::Instance --query "SELECT resourceId, tags FROM input WHERE accountId = '123456789012'"
```

Results are tab-separated by default. `--format` also accepts `csv`, `json`, and `ndjson`. `--id` and `--name` match resource IDs and names by regex, so they find things without needing an exact value. Within `--query`, `input` is the table of resources being queried.

### REPL

Open an interactive DuckDB shell against the local database:

```sh
acd repl
```

### Getting help

`acd --help` lists the commands and the global options. `acd <command> --help` lists everything a command accepts, and shell completion covers the same ground interactively. Commands have short aliases: `acd b`, `acd q`, `acd r`.

## Configuration

The config file is read from `$XDG_CONFIG_HOME/acd/config.toml` (typically `~/.config/acd/config.toml`). Pass `--config` to read a different file.

Databases are named, and each one is a separate DuckDB file. `acd` uses `default_database` unless you pass `--db <name>`, so a single config can hold several accounts or environments side by side.

```toml
default_database = "prod"

# A multi-account setup, reading from a Config aggregator
[databases.prod]
aggregator_name = "my-aggregator"
aws_profile = "prod"

# A single-account setup, with no aggregator
[databases.dev]
aws_profile = "dev"
# path defaults to the XDG data dir; override if needed:
# path = "/path/to/dev.duckdb"

# acd ships with built-in schema alterations that tidy up common resource
# types. Turn them off individually, or all at once with disable_all = true
[builtin_alterations]
disabled = ["name_from_tags"]
```

Alongside AWS Config data, acd runs builtin fetchers for things Config does not cover, such as organizational units from AWS Organizations. Fetchers are configured per database under `[databases.<name>.fetchers.<fetcher>]`, and run by default. Set `enabled = false` to turn one off, or `aws_profile` to authenticate it against a different account. That last one matters for organizational units, because the Organizations API has to be called against the management or delegated admin account, which is often not where the Config data is read from.

acd can also run your own SQL once the database is built: alterations against a single table, alterations applied to every table in turn, extra tables derived from the resource tables, and extra columns surfaced by `acd query`.

There are a number of builtin schema alterations that run by default to make some resource types a bit more useful. These can be viewed in [`acd-cli/src/builtin_alterations.toml`](acd-cli/src/builtin_alterations.toml). This format is the same as the user provided config file, so these can be used as an example to build your own.

See [`example-config.toml`](example-config.toml) for a worked example of every option. That file is loaded by a test, so it always matches the format the current version accepts.
