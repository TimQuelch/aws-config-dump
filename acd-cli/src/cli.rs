// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use clap::{ArgAction, Args, Parser, Subcommand};
use clap_complete::{ArgValueCandidates, ArgValueCompleter};

use crate::completion;

#[derive(Parser)]
#[command(version, about, long_about = None, bin_name = "acd")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    /// Path to config file (overrides XDG config dir)
    #[arg(short, long, global = true)]
    pub config: Option<std::path::PathBuf>,

    /// DB to use
    #[arg(short, long, global = true)]
    pub db: Option<String>,

    /// Increase log verbosity (-v=info, -vv=debug, -vvv=trace)
    #[arg(short, long, action = ArgAction::Count, global = true)]
    pub verbose: u8,
}

#[allow(
    clippy::large_enum_variant,
    reason = "only constructed once per invocation. Query is largest type but also the most common"
)]
#[derive(Subcommand)]
pub enum Command {
    /// Build the offline database from AWS Config
    #[command(visible_alias = "b")]
    Build(BuildArgs),
    /// Open an interactive `DuckDB` REPL against the local database
    #[command(visible_alias = "r")]
    Repl,
    /// Query the offline database
    #[command(visible_alias = "q")]
    Query(QueryArgs),
}

#[allow(clippy::struct_excessive_bools, reason = "valid for CLI args")]
#[derive(Args)]
pub struct BuildArgs {
    /// Use cross-account aggregated data
    #[arg(short, long)]
    pub aggregator_name: Option<String>,

    /// Use snapshots
    #[arg(short = 's', long)]
    pub with_snapshots: bool,

    /// Don't fetch data, only build the resource tables
    #[arg(short, long)]
    pub no_fetch: bool,

    /// Force re-fetching all resources
    #[arg(short, long)]
    pub rebuild: bool,

    /// Fetch account names from the AWS Organizations API
    #[arg(long)]
    pub fetch_org_accounts: bool,
}

#[derive(Args)]
pub struct QueryArgs {
    /// Filter on resource type
    #[arg(short, long, add = ArgValueCandidates::new(completion::resource_type_candidates))]
    pub resource_type: Option<String>,
    /// Filter on account
    #[arg(short, long, num_args(1..), add = ArgValueCandidates::new(completion::account_candidates))]
    pub accounts: Option<Vec<String>>,
    /// Select fields
    #[arg(short, long, num_args(1..), add = ArgValueCandidates::new(completion::field_candidates))]
    pub fields: Option<Vec<String>>,
    /// Exclude fields from the query
    #[arg(short = 'x', long, num_args(1..), add = ArgValueCandidates::new(completion::field_candidates))]
    pub exclude_fields: Option<Vec<String>>,
    /// Include all fields
    #[arg(short = 'F', long)]
    pub all_fields: bool,
    /// Where clause in the form `key=value`
    #[arg(short, long, num_args(1..), value_parser = parse_where_clause, add = ArgValueCompleter::new(completion::where_clause_completer))]
    pub r#where: Option<Vec<(String, String)>>,
    /// Where clause in arbitrary format
    #[arg(short = 'W', long, num_args(1..))]
    pub where_raw: Option<Vec<String>>,
    /// Filter resourceId by regex (partial match; repeatable, OR-combined)
    #[arg(short = 'i', long, num_args(1..), add = ArgValueCompleter::new(completion::id_candidates))]
    pub id: Option<Vec<String>>,
    /// Filter resourceName by regex (partial match; repeatable, OR-combined)
    #[arg(short = 'n', long, num_args(1..), add = ArgValueCompleter::new(completion::name_candidates))]
    pub name: Option<Vec<String>>,
    /// Sort output by fields
    #[arg(short = 's', long, num_args(1..), add = ArgValueCandidates::new(completion::field_candidates))]
    pub sort: Option<Vec<String>>,
    /// Reverse sort order (descending)
    #[arg(long)]
    pub reverse: bool,
    /// Query
    #[arg(short, long, default_value = "SELECT * FROM input")]
    pub query: String,
}

fn parse_where_clause(arg: &str) -> Result<(String, String), String> {
    let Some((k, v)) = arg.split_once('=') else {
        return Err(format!("where clause '{arg}' does not contain '='"));
    };

    if k.is_empty() || v.is_empty() {
        return Err(format!("where clause '{arg}' not in the form 'key=value'"));
    }

    Ok((k.to_string(), v.to_string()))
}
