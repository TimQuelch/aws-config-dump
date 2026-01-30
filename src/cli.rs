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
    },
    /// Query the offline database
    Query {
        /// Filter on resource type
        #[arg(short, long, add = ArgValueCandidates::new(completion::ResourceTypeCandidates::new()))]
        resource_type: Option<String>,
        /// Filter on account
        #[arg(short, long, add = ArgValueCandidates::new(completion::AccountCandidates::new()))]
        account: Option<String>,
        /// Query
        #[arg(default_value = "SELECT * FROM input")]
        query: String,
    },
}
