// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use clap::{Parser, Subcommand};
use clap_complete::ArgValueCandidates;

use crate::completion;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,

    #[arg(short, long, global = true, default_value = "db")]
    pub db_name: String,
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
        #[arg(short, long, add = ArgValueCandidates::new(completion::ResourceTypeCandidates::new()))]
        resource_type: Option<String>,
        /// Filter on account
        #[arg(short, long, num_args(1..), add = ArgValueCandidates::new(completion::AccountCandidates::new()))]
        accounts: Option<Vec<String>>,
        /// Select fields
        #[arg(short, long, num_args(1..), add = ArgValueCandidates::new(completion::FieldCandidates::new()))]
        fields: Option<Vec<String>>,
        /// all fields
        #[arg(short = 'F', long)]
        all_fields: bool,
        /// Query
        #[arg(short, long, default_value = "SELECT * FROM input")]
        query: String,
    },
}
