use std::{pin::Pin, time::Duration};

use aws_config::retry::RetryConfig;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use duckdb::params;
use futures::{Stream, future, stream::StreamExt};
use tempfile::{NamedTempFile, TempPath};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc,
    task::{self, JoinHandle},
};
use tracing::{error, info, warn};

use crate::config::Config;
use crate::util;

const QUERY: &str = concat!(
    "SELECT ",
    "arn,",
    "accountId,",
    "awsRegion,",
    "resourceType,",
    "resourceId,",
    "resourceName,",
    "availabilityZone,",
    "resourceCreationTime,",
    "configurationItemCaptureTime,",
    "configurationItemStatus,",
    "configurationStateId,",
    "tags,",
    "relationships,",
    "configuration,",
    "supplementaryConfiguration;",
);

fn select_resource_config_stream(
    client: &aws_sdk_config::Client,
) -> Pin<Box<dyn Stream<Item = String>>> {
    Box::pin(
        client
            .select_resource_config()
            .expression(QUERY)
            .into_paginator()
            .items()
            .send()
            .into_stream_03x()
            .filter_map(|response| {
                future::ready(
                    response
                        .inspect_err(|err| error!(err = %err, "API error"))
                        .ok(),
                )
            }),
    )
}

fn select_aggregate_resource_config_stream(
    client: &aws_sdk_config::Client,
    aggregator: String,
) -> Pin<Box<dyn Stream<Item = String>>> {
    Box::pin(
        client
            .select_aggregate_resource_config()
            .configuration_aggregator_name(aggregator)
            .expression(QUERY)
            .into_paginator()
            .items()
            .send()
            .into_stream_03x()
            .filter_map(|response| {
                future::ready(
                    response
                        .inspect_err(|err| error!(err = %err, "API error"))
                        .ok(),
                )
            }),
    )
}

pub async fn build_database(aggregator: Option<String>) -> anyhow::Result<()> {
    let config = aws_config::from_env()
        .retry_config(
            RetryConfig::standard()
                .with_initial_backoff(Duration::from_millis(100))
                .with_max_attempts(100),
        )
        .load()
        .await;
    let client = aws_sdk_config::Client::new(&config);

    let (file_tx, file_handle) = temp_file_writer();

    aggregator
        .map_or_else(
            || select_resource_config_stream(&client),
            |aggregator| select_aggregate_resource_config_stream(&client, aggregator),
        )
        .for_each_concurrent(None, async |resource| {
            let inner_tx = file_tx.clone();
            task::spawn_blocking(move || process_resource_config(&inner_tx, resource))
                .await
                .unwrap();
        })
        .await;

    drop(file_tx);
    let json_path = file_handle.await.unwrap();
    task::spawn_blocking(move || build_duckdb_db(&json_path))
        .await
        .unwrap()
        .unwrap();

    Ok(())
}

fn temp_file_writer() -> (mpsc::Sender<String>, JoinHandle<TempPath>) {
    let (tx, mut rx) = mpsc::channel::<String>(1024);

    let file_handle = task::spawn(async move {
        let (file, path) = task::spawn_blocking(|| tempfile::NamedTempFile::with_suffix(".json"))
            .await
            .unwrap()
            .unwrap()
            .into_parts();

        let mut writer = tokio::io::BufWriter::new(tokio::fs::File::from_std(file));

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

fn process_resource_config(sender: &mpsc::Sender<String>, mut resource: String) {
    // AWS is frustratingly inconsistent. I've found that there are some resources which
    // will return an empty object as the tag field instead of an empty array. So far I've
    // noticed AWS::Config::ConformancePackCompliance does this. Here we work around this
    // by parsing the JSON and setting tags field to an empty array if it is not already
    // an array.
    //
    // If this were not the case then we could omit parsing json here and simply write
    // directly to file.
    let mut value: serde_json::Value = serde_json::from_str(&resource).unwrap();
    if let Some(tags) = value.get("tags")
        && !tags.is_array()
    {
        warn!(%tags, resource, "tags field is not an array, setting to empty array");
        value
            .as_object_mut()
            .unwrap()
            .insert("tags".into(), serde_json::Value::Array(vec![]));
        resource = value.to_string();
    }
    sender.blocking_send(resource).unwrap();
}

fn build_duckdb_db(json_path: &TempPath) -> anyhow::Result<()> {
    let db_conn = duckdb::Connection::open(Config::get().db_path())
        .inspect_err(|e| error!(error = %e, "failed open duckdb database"))
        .unwrap();

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

    db_conn.execute_batch(concat!(
        "ALTER TABLE resources ALTER tags SET DATA TYPE MAP(VARCHAR, VARCHAR) USING map_from_entries(tags);",
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
