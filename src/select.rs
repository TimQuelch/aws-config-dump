use std::{collections::HashMap, time::Duration};

use aws_config::retry::RetryConfig;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures::{
    future,
    stream::{self, StreamExt},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::error;

use crate::output::Output;

const QUERY: &str = "SELECT accountId, awsRegion, resourceType, resourceId, resourceName, arn, tags, configuration, supplementaryConfiguration, relationships;";

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Tag {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Relationship {
    #[expect(clippy::struct_field_names)]
    relationship_name: String,
    resource_type: String,
    resource_id: Option<String>,
    resource_name: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct QueryResponse {
    account_id: String,
    aws_region: String,
    resource_type: String,
    resource_id: String,
    resource_name: Option<String>,
    arn: Option<String>,
    configuration: Value,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    supplementary_configuration: HashMap<String, Value>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<Tag>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    relationships: Vec<Relationship>,
}

pub async fn select_resources(mut output: impl Output) -> anyhow::Result<()> {
    let config = aws_config::from_env()
        .retry_config(
            RetryConfig::standard()
                .with_initial_backoff(Duration::from_millis(100))
                .with_max_attempts(100),
        )
        .load()
        .await;
    let client = aws_sdk_config::Client::new(&config);

    client
        .select_resource_config()
        .expression(QUERY)
        .into_paginator()
        .send()
        .into_stream_03x()
        .filter_map(|r| future::ready(r.inspect_err(|err| error!(err = %err, "API error")).ok()))
        .flat_map(|page| stream::iter(page.results().to_owned().into_iter()))
        .for_each_concurrent(None, async |r| {
            let resource = serde_json::from_str::<QueryResponse>(&r).unwrap();
            output
                .send(
                    resource.resource_type.clone(),
                    serde_json::to_string(&resource).unwrap(),
                )
                .await
                .unwrap();
        })
        .await;

    output.close().await?;

    Ok(())
}
