// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::HashSet, time::Duration};

use anyhow::anyhow;
use aws_config::retry::RetryConfig;
use chrono::{DateTime, Utc};
use tempfile::TempPath;
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::{self, JoinHandle},
};
use tracing::info;

use crate::config_fetch_client::{ConfigFetchClient, DispatchingClient};
use crate::db;
use crate::snapshot;

pub async fn build_database(
    aggregator: Option<String>,
    should_fetch: bool,
    should_rebuild: bool,
    with_snapshots: bool,
) -> anyhow::Result<()> {
    if should_rebuild {
        db::delete_db().await?;
    }

    let db_conn = db::connect_to_db()?;

    if should_fetch {
        if with_snapshots {
            let mut dir = snapshot::get_snapshots().await?;
            dir.disable_cleanup(true);
            db::build_resources_table_from_snapshots(&db_conn, &dir)?;
        } else {
            let cutoff = if should_rebuild {
                info!("fetching all resources");
                None
            } else {
                let cutoff = db::get_timestamp_cutoff(&db_conn)?;
                info!(%cutoff, "fetching updated resources");
                Some(cutoff)
            };

            let (resources_path, identifiers_path) = fetch_resources(aggregator, cutoff).await?;
            db::build_resources_table(&db_conn, &resources_path, &identifiers_path)?;
        }
    }

    db::build_derived_tables(&db_conn)?;
    Ok(())
}

async fn fetch_resources(
    aggregator: Option<String>,
    cutoff: Option<DateTime<Utc>>,
) -> anyhow::Result<(TempPath, TempPath)> {
    let config = aws_config::from_env()
        .retry_config(
            RetryConfig::standard()
                .with_initial_backoff(Duration::from_millis(50))
                .with_max_backoff(Duration::from_secs(60))
                .with_max_attempts(100),
        )
        .load()
        .await;
    let client = aws_sdk_config::Client::new(&config);

    let account_id = aws_sdk_sts::Client::new(&config)
        .get_caller_identity()
        .send()
        .await?
        .account
        .ok_or(anyhow!("could not get caller identity"))?;

    let config_client = DispatchingClient::new(&client, aggregator.clone(), account_id);

    let type_counts = config_client.get_resource_counts().await;
    let all_types: HashSet<_> = type_counts.keys().cloned().collect();

    let selectable_type_counts = config_client.get_resource_counts_with_select().await;
    let selectable_types: HashSet<_> = selectable_type_counts.keys().cloned().collect();
    let unselectable_types: HashSet<_> = all_types.difference(&selectable_types).cloned().collect();

    let (resources_tx, resources_handle) = temp_file_writer();
    config_client
        .get_resource_configs_with_select(resources_tx.clone(), cutoff)
        .await;

    info!("fetching resources which are not available with select API");

    config_client
        .get_resource_configs_with_batch(resources_tx, unselectable_types.into_iter(), cutoff)
        .await;

    info!("fetching all resource identifiers to determine which have been deleted");

    let (identifiers_file_tx, identifiers_handle) = temp_file_writer();

    config_client
        .get_resource_identifiers_with_batch(identifiers_file_tx, all_types.into_iter())
        .await;

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
