use std::{
    collections::{HashMap, HashSet},
    time::Duration,
};

use aws_config::retry::RetryConfig;
use chrono::{DateTime, Days, Utc};
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

pub async fn build_database(
    aggregator: Option<String>,
    should_fetch: bool,
    should_rebuild: bool,
) -> anyhow::Result<()> {
    if should_rebuild {
        delete_db().await?;
    }

    let db_conn = connect_to_db()?;

    if should_fetch {
        let cutoff = if should_rebuild {
            info!("fetching all resources");
            None
        } else {
            let cutoff = get_timestamp_cutoff(&db_conn)?;
            info!(%cutoff, "fetching updated resources");
            Some(cutoff)
        };

        let json_path = fetch_resources(aggregator, cutoff).await?;
        build_resources_table(&db_conn, &json_path)?;
    }

    build_derived_tables(&db_conn)?;
    Ok(())
}

async fn fetch_resources(
    aggregator: Option<String>,
    cutoff: Option<DateTime<Utc>>,
) -> anyhow::Result<TempPath> {
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

    let selectable_type_counts = config_client.get_resource_counts_with_select().await;
    let selectable_types: HashSet<_> = selectable_type_counts.keys().cloned().collect();
    let unselectable_types: HashSet<_> = all_types.difference(&selectable_types).cloned().collect();

    let (file_tx, file_handle) = temp_file_writer();
    config_client
        .get_resource_configs_with_select(file_tx.clone(), cutoff)
        .await;

    info!("fetching resources which are not available with select API");

    config_client
        .get_resource_configs_with_batch(file_tx, unselectable_types.into_iter(), cutoff)
        .await;

    file_handle.await.map_err(std::convert::Into::into)
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

async fn delete_db() -> anyhow::Result<()> {
    let path = Config::get().db_path();
    if path.exists() {
        info!(db = %path.to_string_lossy(), "deleting existing database");
        tokio::fs::remove_file(path).await?;
    }
    Ok(())
}

fn connect_to_db() -> anyhow::Result<duckdb::Connection> {
    let db_conn = duckdb::Connection::open(Config::get().db_path())
        .inspect_err(|e| error!(error = %e, "failed open duckdb database"))?;

    // helps with read_json on very large tables
    db_conn.execute_batch("SET preserve_insertion_order = false")?;

    Ok(db_conn)
}

fn get_timestamp_cutoff(db_conn: &duckdb::Connection) -> anyhow::Result<DateTime<Utc>> {
    let max_time_in_db: DateTime<Utc> = db_conn
        .prepare_cached("SELECT max(configurationItemCaptureTime) from resources;")?
        .query_one([], |x| x.get(0))?;

    Ok(max_time_in_db - Days::new(1))
}

fn build_resources_table(db_conn: &duckdb::Connection, json_path: &TempPath) -> anyhow::Result<()> {
    db_conn.execute("
        CREATE OR REPLACE TEMPORARY TABLE resources_temp (
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
        "COPY resources_temp FROM ? (AUTO_DETECT true);",
        [json_path.to_str().unwrap()],
    )?;

    let count: i64 =
        db_conn.query_row("SELECT count(*) FROM resources_temp;", [], |row| row.get(0))?;

    info!(count, "loaded retrieved resources into database");

    db_conn.execute_batch("
        ALTER TABLE resources_temp ALTER tags SET DATA TYPE MAP(VARCHAR, VARCHAR) USING map_from_entries(tags);
        CREATE TABLE IF NOT EXISTS resources (
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
            tags MAP(VARCHAR, VARCHAR),
            relationships STRUCT(relationshipName VARCHAR, resourceType VARCHAR, resourceId VARCHAR, resourceName VARCHAR)[],
            configuration JSON,
            supplementaryConfiguration JSON,
        );
    ")?;

    let mut merge_counter = HashMap::<String, i64>::new();
    let merge_query = "
        MERGE INTO resources old
            USING resources_temp new
            USING (accountId, resourceType, resourceId)
            WHEN MATCHED AND new.configurationItemCaptureTime > old.configurationItemCaptureTime THEN UPDATE
            WHEN NOT MATCHED THEN INSERT
            RETURNING merge_action;
    ";
    db_conn
        .prepare_cached(merge_query)?
        .query_map([], |row| row.get(0))?
        .map(|x| x.unwrap())
        .for_each(|action: String| *merge_counter.entry(action).or_insert(0) += 1);

    info!(
        num_new = merge_counter.get("INSERT").unwrap_or(&0),
        "inserted new resources"
    );
    info!(
        num_updated = merge_counter.get("UPDATE").unwrap_or(&0),
        "updated existing resources"
    );
    Ok(())
}

fn build_derived_tables(db_conn: &duckdb::Connection) -> anyhow::Result<()> {
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
                    "
                    COPY (
                        SELECT
                            accountId,
                            resourceType,
                            resourceId,
                            configuration,
                            supplementaryConfiguration
                        FROM resources
                        WHERE resourceType = ?
                    ) TO '{filename}';
                "
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
                    "
                    CREATE OR REPLACE TABLE {table_name} AS
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
