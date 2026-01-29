use std::{io::Write, pin::Pin, time::Duration};

use aws_config::retry::RetryConfig;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use duckdb::params;
use futures::{Stream, future, stream::StreamExt};
use tracing::{error, info, warn};

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

    let mut json_file = tempfile::NamedTempFile::with_suffix(".json").unwrap();

    aggregator
        .map_or_else(
            || select_resource_config_stream(&client),
            |aggregator| select_aggregate_resource_config_stream(&client, aggregator),
        )
        .for_each(|mut resource| {
            // AWS is frustratingly inconsistent. I've found that there are some resources which will
            // return an empty object as the tag field instead of an empty array. So far I've
            // noticed AWS::Config::ConformancePackCompliance does this. Here we work around this by
            // parsing the JSON and setting tags field to an empty array if it is not already an
            // array.
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

            json_file.write_all(resource.as_bytes()).unwrap();
            future::ready(())
        })
        .await;

    let json_path = json_file.into_temp_path();

    let db_conn = duckdb::Connection::open("./db.duckdb")
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
        "CREATE OR REPLACE VIEW resourceTypes AS SELECT DISTINCT resourceType FROM resources;",
        "CREATE OR REPLACE VIEW accounts AS SELECT DISTINCT accountId FROM resources;",
        "CREATE OR REPLACE VIEW regions AS SELECT DISTINCT awsRegion FROM resources;",
    ))?;

    db_conn
        .prepare_cached(
            "SELECT
                resourceType,
                json_group_structure(configuration) AS conf_schema,
                json_group_structure(supplementaryConfiguration) AS supp_schema
                FROM resources GROUP BY resourceType;",
        )?
        .query_map([], |row| {
            Ok((
                row.get("resourceType")?,
                row.get("conf_schema")?,
                row.get("supp_schema")?,
            ))
        })?
        .filter_map(|result| {
            result
                .inspect_err(|e| error!(err = %e, "failed to query schema"))
                .ok()
        })
        .for_each(
            |(resource_type, conf_schema, supp_schema): (String, String, String)| {
                let table_name = util::resource_table_name(&resource_type);

                let result = db_conn.execute(
                    format!(
                        "CREATE OR REPLACE TABLE {table_name} AS SELECT
                        * EXCLUDE (configuration, supplementaryConfiguration),
                        unnest(json_transform(configuration, ?)),
                        json_transform(supplementaryConfiguration, ?) as extra,
                        FROM resources WHERE resourceType == ?;"
                    )
                    .as_str(),
                    params![conf_schema, supp_schema, &resource_type],
                );
                match result {
                    Ok(_) => info!(resource_type, "created resource table"),
                    Err(err) => error!(resource_type, %err, "failed to create resource table"),
                }
            },
        );

    Ok(())
}
