use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{ArgValueCandidates, CompleteEnv};

mod build;
mod completion;
mod config;
mod query;
mod util;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,

    #[arg(short, long, global = true, default_value = "db")]
    db_name: String,
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

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    CompleteEnv::with_factory(Cli::command).complete();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    config::Config::init(cli.db_name);

    match cli.command {
        Command::Build { aggregator_name } => build::build_database(aggregator_name).await,
        Command::Query {
            resource_type,
            account,
            query,
        } => query::query(resource_type.as_deref(), account.as_deref(), &query),
    }
}
