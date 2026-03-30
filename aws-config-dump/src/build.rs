// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::collections::{HashMap, HashSet};

use aws_client::{
    config_client::{ConfigFetchClient, DispatchingClient},
    org_client, snapshot,
};
use chrono::{DateTime, Utc};
use indicatif::MultiProgress;
use tempfile::TempPath;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::{self, JoinHandle},
};
use tracing::info;

use crate::db;
use crate::schema_alterations;
use crate::{config::Config, util};

pub enum FetchSource {
    Api,
    Snapshots,
    Skip,
}

pub async fn build_database(
    config: &Config,
    aggregator: Option<String>,
    fetch_source: FetchSource,
    should_rebuild: bool,
    fetch_org_accounts: bool,
) -> anyhow::Result<()> {
    if should_rebuild {
        db::delete_db(&config.db_path).await?;
    }

    let progress = MultiProgress::new();

    let db_pool = db::connect_to_db(&config.db_path).await?;

    match fetch_source {
        FetchSource::Snapshots => {
            let mut dir = snapshot::get_snapshots().await?;
            dir.disable_cleanup(true);
            db::build_resources_table_from_snapshots(&db_pool, &dir).await?;
        }
        FetchSource::Api => {
            let cutoff = if should_rebuild {
                None
            } else {
                db::get_timestamp_cutoff(&db_pool).await?
            };

            if let Some(cutoff_timestamp) = cutoff {
                info!(%cutoff_timestamp, "fetching updated resources");
            } else {
                info!("fetching all resources");
            }

            let (resources_path, identifiers_path) =
                fetch_resources(aggregator, cutoff, progress.clone()).await?;
            db::build_resources_table(&db_pool, &resources_path, &identifiers_path).await?;
        }
        FetchSource::Skip => {}
    }

    let org_accounts = if fetch_org_accounts {
        Some(org_client::fetch_org_accounts().await?)
    } else {
        None
    };

    db::build_derived_tables(&db_pool, org_accounts, progress.clone()).await?;
    schema_alterations::apply_schema_alterations(config, &db_pool, progress).await?;
    Ok(())
}

async fn fetch_resources(
    aggregator: Option<String>,
    cutoff: Option<DateTime<Utc>>,
    progress: MultiProgress,
) -> anyhow::Result<(TempPath, TempPath)> {
    let config_client = DispatchingClient::new(aggregator.clone()).await?;

    let type_counts = config_client.get_resource_counts().await;
    let all_types: HashSet<_> = type_counts.keys().cloned().collect();

    let selectable_type_counts = config_client.get_resource_counts_with_select().await;
    let selectable_types: HashSet<_> = selectable_type_counts.keys().cloned().collect();
    let unselectable_types: HashSet<_> = all_types.difference(&selectable_types).cloned().collect();

    let modified_counts = if let Some(cutoff) = cutoff {
        config_client
            .get_resource_counts_modified_since_cutoff(cutoff)
            .await
    } else {
        HashMap::new()
    };

    info!(?modified_counts, "modified counts");

    let (resources_tx, resources_handle) = temp_file_writer();
    let (identifiers_tx, identifiers_handle) = temp_file_writer();

    info!("fetching modified resource configs for selectable types");
    {
        let c = config_client.clone();
        let resources_tx = resources_tx.clone();
        let bar = progress.add(util::progress_bar(
            "fetching modified resources",
            if cutoff.is_some() {
                selectable_type_counts.values().sum::<i64>()
            } else {
                type_counts.values().sum()
            }
            .try_into()
            .unwrap(),
        ));
        task::spawn(async move {
            c.get_resource_configs_with_select(resources_tx, cutoff, bar)
                .await;
        });
    }

    info!("fetching all resource identifiers for selectable types");
    {
        let c = config_client.clone();
        let identifiers_tx = identifiers_tx.clone();
        let bar = progress.add(util::progress_bar(
            "fetching resource identifers",
            type_counts.values().sum::<i64>().try_into().unwrap(),
        ));
        task::spawn(async move {
            c.get_resource_identifiers_with_select(identifiers_tx.clone(), bar)
                .await;
        });
    }

    info!("fetching all resource configs and identifiers for non-selectable types");

    {
        let c = config_client.clone();
        let bar = progress.add(util::progress_bar(
            "fetching remaining resources",
            (type_counts.values().sum::<i64>() - selectable_type_counts.values().sum::<i64>())
                .try_into()
                .unwrap(),
        ));
        task::spawn(async move {
            c.get_resource_configs_and_identifiers_with_batch(
                resources_tx,
                identifiers_tx,
                unselectable_types.iter().cloned(),
                cutoff,
                bar,
            )
            .await;
        });
    }

    let resources_path = resources_handle.await?;
    let identifiers_path = identifiers_handle.await?;

    Ok((resources_path, identifiers_path))
}

fn temp_file_writer() -> (mpsc::Sender<String>, JoinHandle<TempPath>) {
    let (tx, mut rx) = mpsc::channel::<String>(1024);

    let file_handle = task::spawn(async move {
        let (file, path) = task::spawn_blocking(|| tempfile::NamedTempFile::with_suffix(".json"))
            .await
            .unwrap()
            .unwrap()
            .into_parts();

        let mut writer =
            tokio::io::BufWriter::with_capacity(128 * 1024, tokio::fs::File::from_std(file));

        while let Some(x) = rx.recv().await {
            writer.write_all(x.as_bytes()).await.unwrap();
        }

        // Flush both the buffered writer and the underlying file
        writer.flush().await.unwrap();
        writer.into_inner().flush().await.unwrap();

        path
    });

    (tx, file_handle)
}
