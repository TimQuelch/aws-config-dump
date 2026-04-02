// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use clap::{ArgAction, Parser, Subcommand};
use clap_complete::{ArgValueCandidates, ArgValueCompleter};

use crate::completion;

#[derive(Parser)]
#[command(version, about, long_about = None)]
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

#[derive(Subcommand)]
pub enum Command {
    /// Build the offline database from AWS Config
    Build {
        /// Use cross-account aggregated data
        #[arg(short, long)]
        aggregator_name: Option<String>,

        /// Use snapshots
        #[arg(short = 's', long)]
        with_snapshots: bool,

        /// Don't fetch data, only build the resource tables
        #[arg(short, long)]
        no_fetch: bool,

        /// Force re-fetching all resources
        #[arg(short, long)]
        rebuild: bool,

        /// Fetch account names from the AWS Organizations API
        #[arg(long)]
        fetch_org_accounts: bool,
    },
    /// Open an interactive `DuckDB` REPL against the local database
    Repl,
    /// Query the offline database
    Query {
        /// Filter on resource type
        #[arg(short, long, add = ArgValueCandidates::new(completion::resource_type_candidates))]
        resource_type: Option<String>,
        /// Filter on account
        #[arg(short, long, num_args(1..), add = ArgValueCandidates::new(completion::account_candidates))]
        accounts: Option<Vec<String>>,
        /// Select fields
        #[arg(short, long, num_args(1..), add = ArgValueCandidates::new(completion::field_candidates))]
        fields: Option<Vec<String>>,
        /// Include all fields
        #[arg(short = 'F', long)]
        all_fields: bool,
        /// Where clause in the form `key=value`
        #[arg(short, long, num_args(1..), value_parser = parse_where_clause, add = ArgValueCompleter::new(completion::where_clause_completer))]
        r#where: Option<Vec<(String, String)>>,
        /// Where clause in arbitrary format
        #[arg(short = 'W', long, num_args(1..))]
        where_raw: Option<Vec<String>>,
        /// Query
        #[arg(short, long, default_value = "SELECT * FROM input")]
        query: String,
    },
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
