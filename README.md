<!--
SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>

SPDX-License-Identifier: GPL-3.0-only
-->

# aws-config-dump

Fetches AWS Config resource data into a local [DuckDB](https://duckdb.org/) database for offline querying. AWS API results are inconsistent between services. AWS Config provides some consistent structure between different resources. By dumping everything into a local database first, you get fast, uniform SQL queries across all resource types without repeated API calls. Works with both single-account Config setups and multi-account setups with aggregators.

## Installation

### Nix

Run directly without installing:

```sh
nix run github:TimQuelch/aws-config-dump -- build
```

Or add to your NixOS/home-manager config:

```nix
inputs.aws-config-dump.url = "github:TimQuelch/aws-config-dump";
# then reference inputs.aws-config-dump.packages.${system}.default
```

### Cargo

```sh
cargo install --git https://github.com/TimQuelch/aws-config-dump
```

Install shell completions as described here https://docs.rs/clap_complete/latest/clap_complete/env/index.html

## Usage

### Build the database

Fetch all resources from AWS Config into a local DuckDB file:

```sh
aws-config-dump build
```

Builds are incremental by default. Only resources which have been updated since the last build will be retrieved with the select query. Some resource types are not available with the select query, and instead must be retrieved every build.

Key flags:

- `--aggregator-name`: use a cross-account Config aggregator
- `--with-snapshots`: use Config snapshots from S3 instead of the API
- `--fetch-org-accounts`: pull account names from AWS Organizations
- `--rebuild`: delete and recreate the database from scratch

### Query

```sh
aws-config-dump query --resource-type AWS::EC2::Instance
aws-config-dump query --resource-type AWS::EC2::Instance --fields resourceId resourceName vpcId
aws-config-dump query --resource-type AWS::EC2::Instance --where state=running
aws-config-dump query --resource-type AWS::EC2::Instance --query "SELECT resourceId, tags FROM input WHERE accountId = '123456789012'"
```

Key flags:

- `--resource-type` / `-r`: filter by resource type
- `--accounts` / `-a`: filter by account ID(s)
- `--fields` / `-f`: select specific fields
- `--where` / `-w`: filter with `key=value` clauses
- `--where-raw` / `-W`: arbitrary SQL WHERE expressions
- `--query` / `-q`: full SQL query (`input` is the resource table)

### REPL

Open an interactive DuckDB shell against the local database:

```sh
aws-config-dump repl
```

### Global flags

- `--db`: select a named database from the config file
- `--config`: path to config file (overrides XDG default)
- `-v` / `-vv` / `-vvv`: increase log verbosity

## Configuration

The config file is read from `$XDG_CONFIG_HOME/aws-config-dump/config.toml` (typically `~/.config/aws-config-dump/config.toml`).

```toml
default_database = "prod"

[databases.prod]
aggregator_name = "my-aggregator" # use a multi account aggregator
aws_profile = "prod"

[databases.dev]
aws_profile = "dev"
# path defaults to XDG data dir; override if needed:
# path = "/path/to/dev.duckdb"

# Extra columns surfaced by the query command for specific resource types
[query_extra_columns]
ec2_vpc = ["cidrBlock"]
ec2_subnet = ["vpcId", "cidrBlock", "availableIpAddressCount"]

# Per-resource-type SQL alterations run after the database is built
[[schema_alterations]]
description = "Extract AWS::EC2::Instance state struct"
dependencies = ["ec2_instance"]
sql = '''
ALTER TABLE ec2_instance ALTER COLUMN state TYPE VARCHAR USING struct_extract(state, 'name');
'''

# SQL alterations applied conditionally to every resource table
[[global_schema_alterations]]
description = "Add cost code column to all tables"
condition = 'SELECT count(*) > 0 FROM "{table}" WHERE tags['CostCode'] IS NOT NULL'
sql = '''
ALTER TABLE "{table}" ADD COLUMN costCode VARCHAR;
UPDATE "{table}" SET costCode = tags['CostCode'];
'''
```
