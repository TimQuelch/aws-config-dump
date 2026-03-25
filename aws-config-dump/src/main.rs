// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use clap::{CommandFactory, Parser};
use clap_complete::CompleteEnv;

use cli::{Cli, Command};
use config::{Config, ConfigFile};

mod build;
mod builtin_alterations;
mod cli;
mod completion;
mod config;
mod db;
mod query;
mod schema_alterations;
mod util;

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    CompleteEnv::with_factory(Cli::command).complete();

    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    let config = Config::load(
        ConfigFile::load(cli.config.as_deref()).await?,
        cli.db.as_deref(),
    )?;

    match cli.command {
        Command::Build {
            aggregator_name,
            no_fetch,
            rebuild,
            with_snapshots,
            fetch_org_accounts,
        } => {
            let fetch_source = if no_fetch {
                build::FetchSource::Skip
            } else if with_snapshots {
                build::FetchSource::Snapshots
            } else {
                build::FetchSource::Api
            };
            let effective_aggregator = aggregator_name.or_else(|| config.aggregator_name.clone());
            build::build_database(
                &config,
                effective_aggregator,
                fetch_source,
                rebuild,
                fetch_org_accounts,
            )
            .await
        }
        Command::Repl => query::repl(&config),
        Command::Query {
            resource_type,
            accounts,
            fields,
            all_fields,
            r#where,
            where_raw,
            query,
        } => query::query(
            &config,
            resource_type.as_deref(),
            accounts.as_deref(),
            fields,
            all_fields,
            r#where,
            where_raw,
            &query,
        ),
    }
}
