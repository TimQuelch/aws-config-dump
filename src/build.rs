use std::{collections::HashSet, time::Duration};

use aws_config::retry::RetryConfig;
use duckdb::params;
use tempfile::{NamedTempFile, TempPath};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::{self, JoinHandle},
};
use tracing::{error, info};

use crate::config::Config;
use crate::config_fetch_client::{ConfigFetchClient, DispatchingClient};
use crate::util;

pub async fn build_database(aggregator: Option<String>, should_fetch: bool) -> anyhow::Result<()> {
    let json_path = if should_fetch {
        Some(fetch_resources(aggregator).await.await.unwrap())
    } else {
        None
    };

    task::spawn_blocking(move || build_duckdb_db(json_path))
        .await
        .unwrap()
        .unwrap();

    Ok(())
}

async fn fetch_resources(aggregator: Option<String>) -> JoinHandle<TempPath> {
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
    let config_client = DispatchingClient::new(&client, aggregator.clone());

    let type_counts = config_client.get_resource_counts().await;
    let all_types: HashSet<_> = type_counts.keys().cloned().collect();

    let (file_tx, file_handle) = temp_file_writer();
    let seen_set = config_client
        .get_resource_configs_with_select(file_tx.clone())
        .await;

    info!("fetching resources which are not available with select API");

    config_client
        .get_resource_configs_with_batch(file_tx, all_types.difference(&seen_set).cloned())
        .await;

    file_handle
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

fn build_duckdb_db(json_path: Option<TempPath>) -> anyhow::Result<()> {
    let db_conn = duckdb::Connection::open(Config::get().db_path())
        .inspect_err(|e| error!(error = %e, "failed open duckdb database"))
        .unwrap();

    // helps with read_json on very large tables
    db_conn.execute_batch("SET preserve_insertion_order = false")?;

    if let Some(json_path) = json_path {
        db_conn.execute("
        CREATE OR REPLACE TABLE resources (
            arn VARCHAR,
            accountId VARCHAR,
            awsRegion VARCHAR,
            resourceType VARCHAR,
            resourceId VARCHAR,
            resourceName VARCHAR,
            availabilityZone VARCHAR,
            resourceCreationTime TIMESTAMPTZ,
            configurationItemCaptureTime TIMESTAMPTZ,
            configurationItemStatus VARCHAR,
            configurationStateId VARCHAR,
            tags STRUCT(key VARCHAR, value VARCHAR)[],
            relationships STRUCT(relationshipName VARCHAR, resourceType VARCHAR, resourceId VARCHAR, resourceName VARCHAR)[],
            configuration JSON,
            supplementaryConfiguration JSON,
        );",
        [],
    )?;

        db_conn.execute(
            "COPY resources FROM ? (AUTO_DETECT true);",
            [json_path.to_str().unwrap()],
        )?;

        db_conn.execute("ALTER TABLE resources ALTER tags SET DATA TYPE MAP(VARCHAR, VARCHAR) USING map_from_entries(tags);",
            [],)?;
    }

    db_conn.execute_batch(concat!(
        "CREATE OR REPLACE VIEW resourceTypes AS SELECT DISTINCT resourceType FROM resources;",
        "CREATE OR REPLACE VIEW accounts AS SELECT DISTINCT accountId FROM resources;",
        "CREATE OR REPLACE VIEW regions AS SELECT DISTINCT awsRegion FROM resources;",
    ))?;

    db_conn
        .prepare_cached("SELECT DISTINCT resourceType FROM resources;")?
        .query_map([], |row| row.get("resourceType"))?
        .filter_map(|result| {
            result
                .inspect_err(|e| error!(err = %e, "failed to query resource type"))
                .ok()
        })
        .for_each(|resource_type: String| {
            let table_name = util::resource_table_name(&resource_type);

            let file = NamedTempFile::with_suffix(".json")
                .unwrap()
                .into_temp_path();
            let filename = file.to_string_lossy();

            match db_conn.execute(
                format!(
                    "COPY (
                        SELECT
                            accountId,
                            resourceType,
                            resourceId,
                            configuration,
                            supplementaryConfiguration
                        FROM resources
                        WHERE resourceType = ?
                    ) TO '{filename}';"
                )
                .as_str(),
                params![&resource_type],
            ) {
                Ok(_) => info!(resource_type, "created temporary resource json file"),
                Err(err) => {
                    error!(resource_type, %err, "failed to create temporary resource json file");
                }
            }

            match db_conn.execute(
                format!(
                    "CREATE OR REPLACE TABLE {table_name} AS
                        SELECT
                            r.* EXCLUDE(configuration, supplementaryConfiguration),
                            unnest(j.configuration),
                            j.supplementaryConfiguration
                        FROM
                            read_json(?) j
                            JOIN resources r
                                ON j.accountId = r.accountId
                                AND j.resourceType = r.resourceType
                                AND j.resourceId = r.resourceId;"
                )
                .as_str(),
                params![filename],
            ) {
                Ok(_) => info!(resource_type, "created resource table"),
                Err(err) => error!(resource_type, %err, "failed to create resource table"),
            }
        });

    Ok(())
}
