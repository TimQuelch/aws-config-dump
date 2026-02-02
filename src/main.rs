use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;

use cli::{Cli, Command};

mod build;
mod cli;
mod completion;
mod config;
mod query;
mod util;

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
        Command::Build {
            aggregator_name,
            no_fetch,
        } => build::build_database(aggregator_name, !no_fetch).await,
        Command::Query {
            resource_type,
            account,
            fields,
            query,
        } => query::query(resource_type.as_deref(), account.as_deref(), fields, &query),
    }
}
