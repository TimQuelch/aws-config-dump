use std::{
    collections::{HashMap, HashSet},
    pin::Pin,
    sync::{Arc, RwLock},
    time::Duration,
};

use aws_config::retry::RetryConfig;
use aws_sdk_config::types::{
    AggregateResourceIdentifier, BaseConfigurationItem, ResourceCountGroupKey, ResourceKey,
    ResourceType,
};
use aws_smithy_types_convert::{date_time::DateTimeExt, stream::PaginationStreamExt};
use duckdb::params;
use futures::{
    Stream,
    stream::{self, StreamExt},
};
use serde_json::json;
use tempfile::{NamedTempFile, TempPath};
use tokio::{
    io::AsyncWriteExt,
    sync::{Semaphore, mpsc},
    task::{self, JoinHandle},
};
use tokio_stream::wrappers::ReceiverStream;
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
            .filter_map(async |response| {
                response
                    .inspect_err(|err| error!(err = %err, "API error"))
                    .ok()
            }),
    )
}

fn select_aggregate_resource_config_stream(
    client: &aws_sdk_config::Client,
    aggregator: &str,
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
            .filter_map(async |response| {
                response
                    .inspect_err(|err| error!(err = %err, "API error"))
                    .ok()
            }),
    )
}

fn get_discovered_resource_counts_stream(
    client: &aws_sdk_config::Client,
) -> Pin<Box<dyn Stream<Item = (ResourceType, i64)>>> {
    Box::pin(
        client
            .get_discovered_resource_counts()
            .into_paginator()
            .send()
            .into_stream_03x()
            .filter_map(async |response| {
                response
                    .inspect_err(|err| error!(err = %err, "API error"))
                    .ok()
            })
            .flat_map(|page| {
                stream::iter(
                    page.resource_counts()
                        .iter()
                        .map(|resource_count| {
                            (
                                resource_count.resource_type().unwrap().clone(),
                                resource_count.count(),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            }),
    )
}

fn get_aggregate_discovered_resource_counts_stream(
    client: &aws_sdk_config::Client,
    aggregator: &str,
) -> Pin<Box<dyn Stream<Item = (ResourceType, i64)>>> {
    Box::pin(
        client
            .get_aggregate_discovered_resource_counts()
            .configuration_aggregator_name(aggregator)
            .group_by_key(ResourceCountGroupKey::ResourceType)
            .into_paginator()
            .send()
            .into_stream_03x()
            .filter_map(async |response| {
                response
                    .inspect_err(|err| error!(err = %err, "API error"))
                    .ok()
            })
            .flat_map(|page| {
                stream::iter(
                    page.grouped_resource_counts()
                        .iter()
                        .map(|resource_count| {
                            (
                                ResourceType::from(resource_count.group_name()),
                                resource_count.resource_count(),
                            )
                        })
                        .collect::<Vec<_>>(),
                )
            }),
    )
}

fn item_to_json(item: &BaseConfigurationItem) -> serde_json::Value {
    let configuration = item.configuration().map(|x| {
        serde_json::from_str::<serde_json::Value>(x).unwrap_or(serde_json::Value::String(x.into()))
    });
    let supplementary_configuration =
        item.supplementary_configuration()
            .map_or_else(serde_json::Map::new, |supp_conf_map| {
                supp_conf_map
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            serde_json::from_str::<serde_json::Value>(v)
                                .unwrap_or(serde_json::Value::String(v.into())),
                        )
                    })
                    .collect()
            });

    // These aren't returned with batch-get so we use default empty values
    let tags = serde_json::Value::Array(vec![]);
    let relationships = serde_json::Value::Array(vec![]);

    json!({
        "arn": item.arn(),
        "accountId": item.account_id(),
        "awsRegion": item.aws_region(),
        "resourceType": item.resource_type().map(aws_sdk_config::types::ResourceType::as_str),
        "resourceId": item.resource_id(),
        "resourceName": item.resource_name(),
        "availabilityZone": item.availability_zone(),
        "resourceCreationTime": item.resource_creation_time().map(|x| x.to_chrono_utc().unwrap()),
        "configurationItemCaptureTime": item.configuration_item_capture_time().map(|x| x.to_chrono_utc().unwrap()),
        "configurationItemStatus": item.configuration_item_status().map(aws_sdk_config::types::ConfigurationItemStatus::as_str),
        "configurationStateId": item.configuration_state_id(),
        "tags": tags,
        "relationships": relationships,
        "configuration": configuration,
        "supplementaryConfiguration":supplementary_configuration,
    })
}

fn get_non_selectable_resources(
    client: &aws_sdk_config::Client,
    file_tx: &mpsc::Sender<String>,
    resource_types: impl IntoIterator<Item = ResourceType>,
) {
    let (batch_tx, batch_rx) = mpsc::channel::<ResourceKey>(256); // single batch is 100

    {
        let client = client.clone();
        let file_tx = file_tx.clone();
        tokio::spawn(async move {
            ReceiverStream::new(batch_rx)
                .chunks(100)
                .for_each(async |batch| {
                    let client = client.clone();
                    let file_tx = file_tx.clone();
                    tokio::spawn(async move {
                        info!(batch_length = batch.len(), "getting batch of resources");

                        let response = client
                            .batch_get_resource_config()
                            .set_resource_keys(Some(batch))
                            .send()
                            .await
                            .unwrap();

                        if !response.unprocessed_resource_keys().is_empty() {
                            warn!(
                                num_to_reprocess = response.unprocessed_resource_keys().len(),
                                "reprocessing failed batch items"
                            );
                            // for ri in response.unprocessed_resource_keys() {
                            //     batch_tx.send(ri.clone()).await.unwrap();
                            // }
                        }

                        for item in response.base_configuration_items() {
                            let json = item_to_json(item);
                            file_tx.send(json.to_string()).await.unwrap();
                        }
                    });
                })
                .await;
        });
    }

    let list_limiter = Arc::new(Semaphore::new(4));

    resource_types.into_iter().for_each(move |resource_type| {
        let client = client.clone();
        let batch_tx = batch_tx.clone();
        let list_limiter = list_limiter.clone();
        tokio::spawn(async move {
            let _permit = list_limiter.acquire().await.unwrap();
            info!(%resource_type, "listing resource type");
            client
                .list_discovered_resources()
                .resource_type(resource_type)
                .into_paginator()
                .items()
                .send()
                .into_stream_03x()
                .filter_map(async |response| {
                    response
                        .inspect_err(|err| error!(err = %err, "API error"))
                        .ok()
                })
                .for_each(async |resource_identifier| {
                    batch_tx
                        .send(
                            ResourceKey::builder()
                                .resource_id(resource_identifier.resource_id().unwrap())
                                .resource_type(resource_identifier.resource_type().unwrap().clone())
                                .build()
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                })
                .await;
        });
    });
}

fn get_aggregate_non_selectable_resources(
    client: &aws_sdk_config::Client,
    file_tx: &mpsc::Sender<String>,
    resource_types: impl IntoIterator<Item = ResourceType>,
    aggregator: &str,
) {
    let (batch_tx, batch_rx) = mpsc::channel::<AggregateResourceIdentifier>(256); // single batch is 100

    {
        let client = client.clone();
        let aggregator = aggregator.to_owned();
        let file_tx = file_tx.clone();
        tokio::spawn(async move {
            ReceiverStream::new(batch_rx)
                .chunks(100)
                .for_each(async |batch| {
                    let aggregator = aggregator.clone();
                    let client = client.clone();
                    let file_tx = file_tx.clone();
                    tokio::spawn(async move {
                        info!(batch_length = batch.len(), "getting batch of resources");

                        let response = client
                            .batch_get_aggregate_resource_config()
                            .configuration_aggregator_name(&aggregator)
                            .set_resource_identifiers(Some(batch))
                            .send()
                            .await
                            .unwrap();

                        if !response.unprocessed_resource_identifiers().is_empty() {
                            warn!(
                                num_to_reprocess =
                                    response.unprocessed_resource_identifiers().len(),
                                "failed batch items"
                            );
                            // for ri in response.unprocessed_resource_identifiers() {
                            //     batch_tx.send(ri.clone()).await.unwrap();
                            // }
                        }

                        for item in response.base_configuration_items() {
                            let json = item_to_json(item);
                            file_tx.send(json.to_string()).await.unwrap();
                        }
                    });
                })
                .await;
        });
    }

    let list_limiter = Arc::new(Semaphore::new(4));

    resource_types.into_iter().for_each(move |resource_type| {
        let client = client.clone();
        let batch_tx = batch_tx.clone();
        let aggregator = aggregator.to_owned();
        let list_limiter = list_limiter.clone();
        tokio::spawn(async move {
            let _permit = list_limiter.acquire().await.unwrap();
            info!(%resource_type, "listing resource type");
            client
                .list_aggregate_discovered_resources()
                .configuration_aggregator_name(&aggregator)
                .resource_type(resource_type)
                .into_paginator()
                .items()
                .send()
                .into_stream_03x()
                .filter_map(async |response| {
                    response
                        .inspect_err(|err| error!(err = %err, "API error"))
                        .ok()
                })
                .for_each(async |resource_identifier| {
                    batch_tx.send(resource_identifier).await.unwrap();
                })
                .await;
        });
    });
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

    let (file_tx, file_handle) = temp_file_writer();

    let type_counts: HashMap<_, _> = aggregator
        .as_ref()
        .map_or_else(
            || get_discovered_resource_counts_stream(&client),
            |aggregator| get_aggregate_discovered_resource_counts_stream(&client, aggregator),
        )
        .collect()
        .await;

    let all_types: HashSet<_> = type_counts.keys().cloned().collect();

    let seen_set = Arc::new(RwLock::new(HashSet::<ResourceType>::new()));

    aggregator
        .as_ref()
        .map_or_else(
            || select_resource_config_stream(&client),
            |aggregator| select_aggregate_resource_config_stream(&client, aggregator),
        )
        .for_each(async |resource| {
            let inner_file_tx = file_tx.clone();
            let seen_set = seen_set.clone();
            task::spawn_blocking(move || {
                process_resource_config(&inner_file_tx, &seen_set, resource);
            });
        })
        .await;

    let guard = seen_set.read().unwrap();
    let discovered_but_not_seen: HashSet<_> = all_types.difference(&guard).cloned().collect();

    info!(
        ?discovered_but_not_seen,
        "fetching resources which are not available with select API"
    );

    if let Some(aggregator) = aggregator {
        get_aggregate_non_selectable_resources(
            &client,
            &file_tx,
            discovered_but_not_seen,
            &aggregator,
        );
    } else {
        get_non_selectable_resources(&client, &file_tx, discovered_but_not_seen);
    }

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

fn process_resource_config(
    file_sender: &mpsc::Sender<String>,
    seen_map: &Arc<RwLock<HashSet<ResourceType>>>,
    mut resource: String,
) {
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
        warn!(
            %tags,
            resourceId = %value.get("resourceId").unwrap_or(&serde_json::Value::Null),
            arn = %value.get("arn").unwrap_or(&serde_json::Value::Null),
            "tags field is not an array, setting to empty array"
        );
        value
            .as_object_mut()
            .unwrap()
            .insert("tags".into(), serde_json::Value::Array(vec![]));
        resource = value.to_string();
    }

    let resource_type = ResourceType::from(value.get("resourceType").unwrap().as_str().unwrap());
    if !seen_map.read().unwrap().contains(&resource_type)
        && seen_map.write().unwrap().insert(resource_type.clone())
    {
        info!(%resource_type, "seen new resource type");
    }

    file_sender.blocking_send(resource).unwrap();
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
