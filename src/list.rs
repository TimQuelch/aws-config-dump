use std::sync::LazyLock;

use aws_sdk_config::types::{ResourceIdentifier, ResourceKey, ResourceType};
use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures::{
    future,
    stream::{self, Stream, StreamExt},
};
use serde_json::{Value, json};

static ALL_TYPES: LazyLock<Vec<ResourceType>> = LazyLock::new(|| {
    ResourceType::values()
        .iter()
        .copied()
        .map(ResourceType::from)
        .collect()
});

pub async fn resource_id_stream(
    resource_types: &[ResourceType],
) -> impl Stream<Item = ResourceIdentifier> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_config::Client::new(&config);

    let type_iter = if resource_types.is_empty() {
        ALL_TYPES.iter()
    } else {
        resource_types.iter()
    };

    stream::iter(type_iter)
        .flat_map_unordered(None, move |resource_type| {
            client
                .list_discovered_resources()
                .resource_type(resource_type.clone())
                .into_paginator()
                .send()
                .into_stream_03x()
        })
        .filter_map(|r| {
            future::ready(
                r.inspect_err(|e| eprintln!("{}", e.as_service_error().unwrap()))
                    .ok(),
            )
        })
        .flat_map(|page| stream::iter(page.resource_identifiers().to_owned().into_iter()))
}

pub async fn resource_configs(resource_types: &[ResourceType]) -> anyhow::Result<()> {
    let config = aws_config::load_from_env().await;
    let client = aws_sdk_config::Client::new(&config);

    resource_id_stream(resource_types)
        .await
        .chunks(100)
        .map(|batch| {
            client
                .batch_get_resource_config()
                .set_resource_keys(Some(
                    batch
                        .into_iter()
                        .map(|r| {
                            ResourceKey::builder()
                                .resource_id(r.resource_id().unwrap())
                                .resource_type(r.resource_type().unwrap().clone())
                                .build()
                                .unwrap()
                        })
                        .collect(),
                ))
                .send()
        })
        .buffer_unordered(10)
        .filter_map(|r| future::ready(r.inspect_err(|e| eprintln!("{}", e.as_service_error().unwrap())).ok()))
        .flat_map(|page| stream::iter(page.base_configuration_items().to_owned().into_iter()))
        .for_each(|r| {
            serde_json::to_writer(
                &std::io::stdout(),
                &json!({
                    "resource_type": r.resource_type().map(aws_sdk_config::types::ResourceType::as_str),
                    "resource_id": r.resource_id(),
                    "resource_name": r.resource_name(),
                    "arn": r.arn(),
                    "configuration": r.configuration().and_then(|x| serde_json::from_str::<Value>(x).ok()),
                    "supplementary_configuration": r.supplementary_configuration(),
                }),
            )
            .unwrap();
            future::ready(
                (),
            )
        })
        .await;

    Ok(())
}

pub async fn list(resource_types: &[ResourceType]) -> anyhow::Result<()> {
    resource_id_stream(resource_types)
        .await
        .for_each(|r| {
            serde_json::to_writer(
                &std::io::stdout(),
                &json!({
                    "resource_type": r.resource_type().map(aws_sdk_config::types::ResourceType::as_str),
                    "resource_id": r.resource_id(),
                    "resource_name": r.resource_name(),
                }),
            )
            .unwrap();
            future::ready(
                (),
            )
        })
        .await;

    Ok(())
}
