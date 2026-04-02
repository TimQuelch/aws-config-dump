// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::HashMap, convert::Into, error::Error, sync::Arc};

use anyhow::anyhow;
use aws_sdk_config::{
    operation::{
        batch_get_aggregate_resource_config::BatchGetAggregateResourceConfigOutput,
        batch_get_resource_config::BatchGetResourceConfigOutput,
        get_aggregate_discovered_resource_counts::GetAggregateDiscoveredResourceCountsOutput,
        get_discovered_resource_counts::GetDiscoveredResourceCountsOutput,
    },
    types::{
        AggregateResourceIdentifier, BaseConfigurationItem, ResourceCountGroupKey,
        ResourceIdentifier, ResourceKey, ResourceType,
    },
};
use aws_smithy_async::future::pagination_stream::PaginationStream;
use aws_smithy_types_convert::{date_time::DateTimeExt, stream::PaginationStreamExt};
use chrono::{DateTime, Utc};
use futures::stream::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use serde::{self, Deserialize, Serialize, ser::SerializeStruct};
use tokio::{
    sync::{Semaphore, mpsc},
    task,
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace, warn};

use crate::sdk_config;

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
    "supplementaryConfiguration",
);

pub trait ConfigFetchClient {
    fn get_resource_counts(
        &self,
    ) -> impl Future<Output = anyhow::Result<HashMap<ResourceType, i64>>>;
    fn get_resource_counts_with_select(
        &self,
    ) -> impl Future<Output = anyhow::Result<HashMap<ResourceType, i64>>>;
    fn get_resource_counts_modified_since_cutoff(
        &self,
        cutoff: DateTime<Utc>,
    ) -> impl Future<Output = anyhow::Result<HashMap<ResourceType, i64>>>;
    fn get_resource_configs_with_select(
        &self,
        file_tx: mpsc::Sender<String>,
        cutoff: Option<DateTime<Utc>>,
        progress: ProgressBar,
    ) -> impl Future<Output = ()>;
    fn get_resource_configs_and_identifiers_with_batch(
        &self,
        configs_file_tx: mpsc::Sender<String>,
        identifiers_file_tx: mpsc::Sender<String>,
        resource_types: impl Iterator<Item = ResourceType>,
        cutoff: Option<DateTime<Utc>>,
        progress: ProgressBar,
    ) -> impl Future<Output = ()>;
    fn get_resource_identifiers_with_select(
        &self,
        file_tx: mpsc::Sender<String>,
        progress: ProgressBar,
    ) -> impl Future<Output = ()>;
}

trait ConfigFetcher {
    type Identifier: Send + Clone;

    fn get_discovered_resource_counts_call(
        &self,
    ) -> PaginationStream<Result<impl ResourceCountPage, impl Error + Send + Sync + 'static>>;
    fn select_resource_config_call(
        &self,
        query: String,
    ) -> PaginationStream<Result<String, impl Error + Send + Sync + 'static>>;
    fn list_discovered_resources_call(
        &self,
        resource_type: ResourceType,
    ) -> PaginationStream<Result<Self::Identifier, impl Error + Send + Sync + 'static>>;
    fn batch_get_resources_call(
        &self,
        identifiers: Vec<Self::Identifier>,
    ) -> impl Future<Output = Result<impl BatchResponse + Send, impl Error + Send + Sync + 'static>> + Send;
    fn serialize_identifier(&self, identifier: Self::Identifier) -> String;
}

trait ResourceCountPage {
    fn count_iter(self) -> impl Iterator<Item = (ResourceType, i64)>;
}

trait BatchResponse {
    fn num_unprocessed(&self) -> usize;
    fn into_items(self) -> Vec<BaseConfigurationItem>;
}

#[derive(Clone)]
pub struct DispatchingClient {
    fetcher: DispatchTarget,
}

#[derive(Clone)]
enum DispatchTarget {
    Account(AccountFetcher),
    Aggregate(AggregateFetcher),
}

#[derive(Clone)]
struct AccountFetcher {
    client: aws_sdk_config::Client,
    account_id: String,
}

#[derive(Clone)]
struct AggregateFetcher {
    client: aws_sdk_config::Client,
    aggregator: String,
}

#[derive(Deserialize)]
struct ResourceCountWithSelectResultRow<'a> {
    #[serde(rename = "resourceType")]
    resource_type: &'a str,
    #[serde(rename = "COUNT(*)")]
    count: i64,
}

struct WrappedAggregateResourceIdentifier(AggregateResourceIdentifier);

impl Serialize for WrappedAggregateResourceIdentifier {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Identifier", 3)?;
        state.serialize_field("resourceType", self.0.resource_type.as_str())?;
        state.serialize_field("accountId", self.0.source_account_id())?;
        state.serialize_field("resourceId", self.0.resource_id())?;
        state.end()
    }
}

struct WrappedResourceIdentifier<'a> {
    inner: ResourceIdentifier,
    account_id: &'a str,
}

impl Serialize for WrappedResourceIdentifier<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("Identifier", 3)?;
        state.serialize_field("resourceType", self.inner.resource_type().unwrap().as_str())?;
        state.serialize_field("accountId", self.account_id)?;
        state.serialize_field("resourceId", self.inner.resource_id().unwrap())?;
        state.end()
    }
}

impl DispatchingClient {
    /// Returns a dispatching client that uses the named aggregator if provided
    ///
    /// # Errors
    /// If no aggregator is provided, and the API call to retrieve the account id fails
    pub async fn new(aggregator: Option<String>, profile: Option<String>) -> anyhow::Result<Self> {
        let config = &sdk_config::load_config(profile).await;
        let client = aws_sdk_config::Client::new(config);
        Ok(Self {
            fetcher: if let Some(aggregator) = aggregator {
                DispatchTarget::Aggregate(AggregateFetcher {
                    client: client.clone(),
                    aggregator,
                })
            } else {
                DispatchTarget::Account(AccountFetcher {
                    client: client.clone(),
                    account_id: aws_sdk_sts::Client::new(config)
                        .get_caller_identity()
                        .send()
                        .await?
                        .account
                        .ok_or(anyhow!("failed to get account id from credentials"))?,
                })
            },
        })
    }
}

macro_rules! dispatch {
    ($method:ident($($arg:ident: $arg_ty:ty),*) $(-> $ret:ty)?) => {
        async fn $method(&self $(, $arg: $arg_ty)*) $(-> $ret)? {
            match &self.fetcher {
                DispatchTarget::Aggregate(f) => f.$method($($arg),*).await,
                DispatchTarget::Account(f) => f.$method($($arg),*).await,
            }
        }
    };
}

impl ConfigFetchClient for DispatchingClient {
    dispatch!(get_resource_counts() -> anyhow::Result<HashMap<ResourceType, i64>>);

    dispatch!(get_resource_counts_with_select() -> anyhow::Result<HashMap<ResourceType, i64>>);

    dispatch!(
        get_resource_counts_modified_since_cutoff(
            cutoff: DateTime<Utc>
        ) -> anyhow::Result<HashMap<ResourceType, i64>>
    );

    dispatch!(
        get_resource_configs_with_select(
            file_tx: mpsc::Sender<String>,
            cutoff: Option<DateTime<Utc>>,
            progress: ProgressBar
        )
    );

    dispatch!(
        get_resource_configs_and_identifiers_with_batch(
            configs_file_tx: mpsc::Sender<String>,
            identifiers_file_tx: mpsc::Sender<String>,
            resource_types: impl Iterator<Item = ResourceType>,
            cutoff: Option<DateTime<Utc>>,
            progress: ProgressBar
        )
    );

    dispatch!(
        get_resource_identifiers_with_select(
            file_tx: mpsc::Sender<String>,
            progress: ProgressBar
        )
    );
}

impl<C: ConfigFetcher + Clone + Send + Sync + 'static> ConfigFetchClient for C {
    async fn get_resource_counts(&self) -> anyhow::Result<HashMap<ResourceType, i64>> {
        self.get_discovered_resource_counts_call()
            .into_stream_03x()
            .err_into()
            .try_fold(HashMap::new(), async |mut acc, page| {
                acc.extend(page.count_iter());
                Ok(acc)
            })
            .await
    }

    async fn get_resource_counts_with_select(&self) -> anyhow::Result<HashMap<ResourceType, i64>> {
        let query = "SELECT resourceType, COUNT(*) GROUP BY resourceType;".to_string();

        self.select_resource_config_call(query)
            .into_stream_03x()
            .err_into()
            .map_ok(|row| {
                let parsed: ResourceCountWithSelectResultRow = serde_json::from_str(&row).unwrap();
                (parsed.resource_type.into(), parsed.count)
            })
            .try_collect()
            .await
    }

    async fn get_resource_counts_modified_since_cutoff(
        &self,
        cutoff: DateTime<Utc>,
    ) -> anyhow::Result<HashMap<ResourceType, i64>> {
        let query = format!(
            "SELECT resourceType, COUNT(*) WHERE configurationItemCaptureTime > '{}' GROUP BY resourceType;",
            cutoff.to_rfc3339()
        );

        self.select_resource_config_call(query)
            .into_stream_03x()
            .err_into()
            .map_ok(|row| {
                let parsed: ResourceCountWithSelectResultRow = serde_json::from_str(&row).unwrap();
                (parsed.resource_type.into(), parsed.count)
            })
            .try_collect()
            .await
    }

    async fn get_resource_configs_with_select(
        &self,
        file_tx: mpsc::Sender<String>,
        cutoff: Option<DateTime<Utc>>,
        progress: ProgressBar,
    ) {
        let query = if let Some(dt) = cutoff {
            format!(
                "{QUERY} WHERE configurationItemCaptureTime > '{}';",
                dt.to_rfc3339()
            )
        } else {
            format!("{QUERY};")
        };
        debug!(
            has_cutoff = cutoff.is_some(),
            "fetching resource configs with select"
        );

        self.select_resource_config_call(query)
            .into_stream_03x()
            .try_for_each(async |resource| {
                let resource = task::spawn_blocking(move || sanitize_resource_config(resource))
                    .await
                    .unwrap();
                file_tx.send(resource).await.unwrap();
                progress.inc(1);
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn get_resource_configs_and_identifiers_with_batch(
        &self,
        configs_file_tx: mpsc::Sender<String>,
        identifiers_file_tx: mpsc::Sender<String>,
        resource_types: impl Iterator<Item = ResourceType>,
        cutoff: Option<DateTime<Utc>>,
        progress: ProgressBar,
    ) {
        // single batch is 100
        let (batch_tx, batch_rx) = mpsc::channel::<C::Identifier>(256);

        {
            let new_self = self.clone();
            tokio::spawn(async move {
                ReceiverStream::new(batch_rx)
                    .chunks(100)
                    .for_each(async |batch| {
                        let file_tx = configs_file_tx.clone();
                        let new_self = new_self.clone();
                        let progress = progress.clone();
                        tokio::spawn(async move {
                            let batch_length = batch.len();
                            debug!(batch_length, "getting batch of resources");

                            let response = new_self.batch_get_resources_call(batch).await.unwrap();

                            let num = response.num_unprocessed();
                            if num > 0 {
                                warn!(num, "unprocessed items in batch response");
                            }

                            for item in response.into_items() {
                                if cutoff.is_none_or(|cutoff| {
                                    item.configuration_item_capture_time.is_some_and(|item_dt| {
                                        item_dt
                                            .to_chrono_utc()
                                            .map_or(true, |item_dt| item_dt > cutoff)
                                    })
                                }) {
                                    trace!(
                                        resource_type = item
                                            .resource_type
                                            .as_ref()
                                            .map(aws_sdk_config::types::ResourceType::as_str),
                                        resource_id = item.resource_id.as_deref(),
                                        "sending resource config"
                                    );
                                    let json = item_to_json(item);
                                    file_tx.send(json.to_string()).await.unwrap();
                                }
                            }
                            progress.inc(batch_length.try_into().unwrap());
                        });
                    })
                    .await;
            });
        }

        let list_limiter = Arc::new(Semaphore::new(8));

        resource_types.for_each(|resource_type| {
            let batch_tx = batch_tx.clone();
            let identifiers_file_tx = identifiers_file_tx.clone();
            let list_limiter = list_limiter.clone();
            let new_self = self.clone();
            tokio::spawn(async move {
                let _permit = list_limiter.acquire().await.unwrap();
                debug!(%resource_type, "listing resource type");
                new_self
                    .list_discovered_resources_call(resource_type)
                    .into_stream_03x()
                    .try_for_each(async |resource_identifier| {
                        batch_tx.send(resource_identifier.clone()).await.unwrap();
                        identifiers_file_tx
                            .send(new_self.serialize_identifier(resource_identifier))
                            .await
                            .unwrap();
                        Ok(())
                    })
                    .await
                    .unwrap();
            });
        });
    }

    async fn get_resource_identifiers_with_select(
        &self,
        file_tx: mpsc::Sender<String>,
        progress: ProgressBar,
    ) {
        debug!("fetching resource identifiers with select");
        self.select_resource_config_call("SELECT resourceType, accountId, resourceId;".to_string())
            .into_stream_03x()
            .try_for_each(async |resource| {
                file_tx.send(resource).await.unwrap();
                progress.inc(1);
                Ok(())
            })
            .await
            .unwrap(); // TODO remove
    }
}

impl ConfigFetcher for AccountFetcher {
    type Identifier = ResourceIdentifier;

    fn get_discovered_resource_counts_call(
        &self,
    ) -> PaginationStream<Result<impl ResourceCountPage, impl Error + Send + Sync + 'static>> {
        self.client
            .get_discovered_resource_counts()
            .into_paginator()
            .send()
    }

    fn select_resource_config_call(
        &self,
        query: String,
    ) -> PaginationStream<Result<String, impl Error + Send + Sync + 'static>> {
        self.client
            .select_resource_config()
            .expression(query)
            .limit(100)
            .into_paginator()
            .items()
            .send()
    }

    fn list_discovered_resources_call(
        &self,
        resource_type: ResourceType,
    ) -> PaginationStream<Result<ResourceIdentifier, impl Error + Send + Sync + 'static>> {
        self.client
            .list_discovered_resources()
            .resource_type(resource_type)
            .limit(100)
            .into_paginator()
            .items()
            .send()
    }

    fn batch_get_resources_call(
        &self,
        identifiers: Vec<Self::Identifier>,
    ) -> impl Future<Output = Result<impl BatchResponse + Send, impl Error + Send + Sync + 'static>> + Send
    {
        self.client
            .batch_get_resource_config()
            .set_resource_keys(Some(
                identifiers
                    .into_iter()
                    .map(|x| {
                        ResourceKey::builder()
                            .resource_id(x.resource_id.unwrap())
                            .resource_type(x.resource_type.unwrap())
                            .build()
                            .unwrap()
                    })
                    .collect(),
            ))
            .send()
    }

    fn serialize_identifier(&self, identifier: Self::Identifier) -> String {
        serde_json::to_string(&WrappedResourceIdentifier {
            inner: identifier,
            account_id: &self.account_id,
        })
        .unwrap()
    }
}

impl ConfigFetcher for AggregateFetcher {
    type Identifier = AggregateResourceIdentifier;

    fn get_discovered_resource_counts_call(
        &self,
    ) -> PaginationStream<Result<impl ResourceCountPage, impl Error + Send + Sync + 'static>> {
        self.client
            .get_aggregate_discovered_resource_counts()
            .configuration_aggregator_name(&self.aggregator)
            .group_by_key(ResourceCountGroupKey::ResourceType)
            .into_paginator()
            .send()
    }

    fn select_resource_config_call(
        &self,
        query: String,
    ) -> PaginationStream<Result<String, impl Error + Send + Sync + 'static>> {
        self.client
            .select_aggregate_resource_config()
            .configuration_aggregator_name(&self.aggregator)
            .expression(query)
            .limit(100)
            .into_paginator()
            .items()
            .send()
    }

    fn list_discovered_resources_call(
        &self,
        resource_type: ResourceType,
    ) -> PaginationStream<Result<Self::Identifier, impl Error + Send + Sync + 'static>> {
        self.client
            .list_aggregate_discovered_resources()
            .configuration_aggregator_name(&self.aggregator)
            .resource_type(resource_type)
            .limit(100)
            .into_paginator()
            .items()
            .send()
    }

    fn batch_get_resources_call(
        &self,
        identifiers: Vec<Self::Identifier>,
    ) -> impl Future<Output = Result<impl BatchResponse + Send, impl Error + Send + Sync + 'static>> + Send
    {
        self.client
            .batch_get_aggregate_resource_config()
            .configuration_aggregator_name(&self.aggregator)
            .set_resource_identifiers(Some(identifiers))
            .send()
    }

    fn serialize_identifier(&self, identifier: Self::Identifier) -> String {
        serde_json::to_string(&WrappedAggregateResourceIdentifier(identifier)).unwrap()
    }
}

impl ResourceCountPage for GetDiscoveredResourceCountsOutput {
    fn count_iter(self) -> impl Iterator<Item = (ResourceType, i64)> {
        self.resource_counts
            .unwrap_or_default()
            .into_iter()
            .map(|rc| (rc.resource_type.unwrap(), rc.count))
    }
}

impl ResourceCountPage for GetAggregateDiscoveredResourceCountsOutput {
    fn count_iter(self) -> impl Iterator<Item = (ResourceType, i64)> {
        self.grouped_resource_counts
            .unwrap_or_default()
            .into_iter()
            .map(|rc| {
                (
                    ResourceType::from(rc.group_name.as_str()),
                    rc.resource_count,
                )
            })
    }
}

impl BatchResponse for BatchGetResourceConfigOutput {
    fn num_unprocessed(&self) -> usize {
        self.unprocessed_resource_keys().len()
    }

    fn into_items(self) -> Vec<BaseConfigurationItem> {
        self.base_configuration_items.unwrap_or_default()
    }
}

impl BatchResponse for BatchGetAggregateResourceConfigOutput {
    fn num_unprocessed(&self) -> usize {
        self.unprocessed_resource_identifiers().len()
    }

    fn into_items(self) -> Vec<BaseConfigurationItem> {
        self.base_configuration_items.unwrap_or_default()
    }
}

fn sanitize_resource_config(mut resource: String) -> String {
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
    resource
}

fn item_to_json(item: BaseConfigurationItem) -> serde_json::Value {
    let configuration = item.configuration.map(|x| {
        serde_json::from_str::<serde_json::Value>(&x).unwrap_or(serde_json::Value::String(x))
    });
    let supplementary_configuration =
        item.supplementary_configuration
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

    serde_json::json!({
        "arn": item.arn,
        "accountId": item.account_id,
        "awsRegion": item.aws_region,
        "resourceType": item.resource_type.map(|x| x.as_str().to_owned()),
        "resourceId": item.resource_id,
        "resourceName": item.resource_name,
        "availabilityZone": item.availability_zone,
        "resourceCreationTime": item.resource_creation_time.map(|x| x.to_chrono_utc().unwrap()),
        "configurationItemCaptureTime": item.configuration_item_capture_time.map(|x| x.to_chrono_utc().unwrap()),
        "configurationItemStatus": item.configuration_item_status.map(|x| x.as_str().to_owned()),
        "configurationStateId": item.configuration_state_id,
        "tags": tags,
        "relationships": relationships,
        "configuration": configuration,
        "supplementaryConfiguration":supplementary_configuration,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use aws_sdk_config::{
        operation::{
            batch_get_aggregate_resource_config::BatchGetAggregateResourceConfigOutput,
            batch_get_resource_config::BatchGetResourceConfigOutput,
            get_aggregate_discovered_resource_counts::GetAggregateDiscoveredResourceCountsOutput,
            get_discovered_resource_counts::GetDiscoveredResourceCountsOutput,
            list_aggregate_discovered_resources::ListAggregateDiscoveredResourcesOutput,
            list_discovered_resources::ListDiscoveredResourcesOutput,
            select_resource_config::SelectResourceConfigOutput,
        },
        types::{
            AggregateResourceIdentifier, GroupedResourceCount, ResourceCount, ResourceIdentifier,
        },
    };
    use aws_smithy_mocks::{RuleMode, mock, mock_client};
    use indicatif::ProgressBar;

    use super::*;

    #[test]
    fn sanitize_tags_already_array() {
        let input = r#"{"resourceId":"i-123","tags":[{"key":"env","value":"prod"}]}"#.to_string();
        let output = sanitize_resource_config(input);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert!(parsed["tags"].is_array());
        assert_eq!(parsed["tags"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn sanitize_tags_object_replaced_with_empty_array() {
        let input = r#"{"resourceId":"i-123","tags":{}}"#.to_string();
        let output = sanitize_resource_config(input);
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(parsed["tags"], serde_json::json!([]));
    }

    #[test]
    fn item_to_json_basic_fields() {
        let item = BaseConfigurationItem::builder()
            .arn("arn:aws:ec2:us-east-1:123456789012:instance/i-123")
            .account_id("123456789012")
            .aws_region("us-east-1")
            .resource_id("i-123")
            .build();
        let json = item_to_json(item);
        assert_eq!(
            json["arn"],
            "arn:aws:ec2:us-east-1:123456789012:instance/i-123"
        );
        assert_eq!(json["accountId"], "123456789012");
        assert_eq!(json["awsRegion"], "us-east-1");
        assert_eq!(json["resourceId"], "i-123");
        assert_eq!(json["tags"], serde_json::json!([]));
        assert_eq!(json["relationships"], serde_json::json!([]));
        assert!(json["configuration"].is_null());
    }

    #[test]
    fn item_to_json_configuration_valid_json() {
        let item = BaseConfigurationItem::builder()
            .configuration(r#"{"instanceType":"t3.micro","state":"running"}"#)
            .build();
        let json = item_to_json(item);
        assert_eq!(json["configuration"]["instanceType"], "t3.micro");
        assert_eq!(json["configuration"]["state"], "running");
    }

    #[test]
    fn item_to_json_configuration_invalid_json() {
        let item = BaseConfigurationItem::builder()
            .configuration("not-valid-json")
            .build();
        let json = item_to_json(item);
        assert_eq!(json["configuration"], "not-valid-json");
    }

    #[test]
    fn item_to_json_supplementary_configuration() {
        let item = BaseConfigurationItem::builder()
            .set_supplementary_configuration(Some(HashMap::from([
                (
                    "PublicAccessBlockConfiguration".to_string(),
                    r#"{"blockPublicAcls":true}"#.to_string(),
                ),
                ("BucketVersioning".to_string(), "not-json".to_string()),
            ])))
            .build();
        let json = item_to_json(item);
        assert_eq!(
            json["supplementaryConfiguration"]["PublicAccessBlockConfiguration"]["blockPublicAcls"],
            true
        );
        assert_eq!(
            json["supplementaryConfiguration"]["BucketVersioning"],
            "not-json"
        );
    }

    #[test]
    fn item_to_json_no_supplementary_configuration() {
        let item = BaseConfigurationItem::builder().build();
        let json = item_to_json(item);
        assert_eq!(json["supplementaryConfiguration"], serde_json::json!({}));
    }

    fn account_fetcher(client: aws_sdk_config::Client) -> AccountFetcher {
        AccountFetcher {
            client,
            account_id: "123456789012".to_string(),
        }
    }

    #[tokio::test]
    async fn account_get_resource_counts_single_page() {
        let rule =
            mock!(aws_sdk_config::Client::get_discovered_resource_counts).then_output(|| {
                GetDiscoveredResourceCountsOutput::builder()
                    .resource_counts(
                        ResourceCount::builder()
                            .resource_type(ResourceType::Instance)
                            .count(5)
                            .build(),
                    )
                    .resource_counts(
                        ResourceCount::builder()
                            .resource_type(ResourceType::Bucket)
                            .count(3)
                            .build(),
                    )
                    .build()
            });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let counts = fetcher.get_resource_counts().await.unwrap();
        assert_eq!(counts[&ResourceType::Instance], 5);
        assert_eq!(counts[&ResourceType::Bucket], 3);
    }

    #[tokio::test]
    async fn account_get_resource_counts_multi_page() {
        let rule1 =
            mock!(aws_sdk_config::Client::get_discovered_resource_counts).then_output(|| {
                GetDiscoveredResourceCountsOutput::builder()
                    .resource_counts(
                        ResourceCount::builder()
                            .resource_type(ResourceType::Instance)
                            .count(5)
                            .build(),
                    )
                    .next_token("page2-token")
                    .build()
            });
        let rule2 =
            mock!(aws_sdk_config::Client::get_discovered_resource_counts).then_output(|| {
                GetDiscoveredResourceCountsOutput::builder()
                    .resource_counts(
                        ResourceCount::builder()
                            .resource_type(ResourceType::Bucket)
                            .count(3)
                            .build(),
                    )
                    .build()
            });
        let fetcher = account_fetcher(mock_client!(
            aws_sdk_config,
            RuleMode::Sequential,
            [&rule1, &rule2]
        ));
        let counts = fetcher.get_resource_counts().await.unwrap();
        assert_eq!(counts[&ResourceType::Instance], 5);
        assert_eq!(counts[&ResourceType::Bucket], 3);
    }

    #[tokio::test]
    async fn account_get_resource_counts_with_select() {
        let rule = mock!(aws_sdk_config::Client::select_resource_config).then_output(|| {
            SelectResourceConfigOutput::builder()
                .results(r#"{"resourceType":"AWS::EC2::Instance","COUNT(*)":7}"#)
                .results(r#"{"resourceType":"AWS::S3::Bucket","COUNT(*)":2}"#)
                .build()
        });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let counts = fetcher.get_resource_counts_with_select().await.unwrap();
        assert_eq!(counts[&ResourceType::Instance], 7);
        assert_eq!(counts[&ResourceType::Bucket], 2);
    }

    #[tokio::test]
    async fn account_get_resource_counts_modified_since_cutoff() {
        let rule = mock!(aws_sdk_config::Client::select_resource_config).then_output(|| {
            SelectResourceConfigOutput::builder()
                .results(r#"{"resourceType":"AWS::EC2::Instance","COUNT(*)":4}"#)
                .build()
        });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let cutoff = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .unwrap()
            .to_utc();
        let counts = fetcher
            .get_resource_counts_modified_since_cutoff(cutoff)
            .await
            .unwrap();
        assert_eq!(counts[&ResourceType::Instance], 4);
    }

    #[tokio::test]
    async fn account_get_resource_configs_with_select_sends_items() {
        let rule = mock!(aws_sdk_config::Client::select_resource_config).then_output(|| {
            SelectResourceConfigOutput::builder()
                .results(r#"{"resourceId":"i-1","tags":[]}"#)
                .results(r#"{"resourceId":"i-2","tags":[]}"#)
                .build()
        });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let (tx, mut rx) = mpsc::channel(8);
        fetcher
            .get_resource_configs_with_select(tx, None, ProgressBar::hidden())
            .await;
        let mut items = vec![];
        while let Ok(s) = rx.try_recv() {
            items.push(s);
        }
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn account_get_resource_configs_with_select_sanitizes_tags() {
        let rule = mock!(aws_sdk_config::Client::select_resource_config).then_output(|| {
            SelectResourceConfigOutput::builder()
                .results(r#"{"resourceId":"i-1","tags":{}}"#)
                .build()
        });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let (tx, mut rx) = mpsc::channel(8);
        fetcher
            .get_resource_configs_with_select(tx, None, ProgressBar::hidden())
            .await;
        let item: serde_json::Value = serde_json::from_str(&rx.try_recv().unwrap()).unwrap();
        assert_eq!(item["tags"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn account_get_resource_identifiers_with_select() {
        let rule = mock!(aws_sdk_config::Client::select_resource_config).then_output(|| {
            SelectResourceConfigOutput::builder()
                .results(
                    r#"{"resourceType":"AWS::EC2::Instance","accountId":"123","resourceId":"i-1"}"#,
                )
                .results(
                    r#"{"resourceType":"AWS::EC2::Instance","accountId":"123","resourceId":"i-2"}"#,
                )
                .build()
        });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let (tx, mut rx) = mpsc::channel(8);
        fetcher
            .get_resource_identifiers_with_select(tx, ProgressBar::hidden())
            .await;
        let mut items = vec![];
        while let Ok(s) = rx.try_recv() {
            items.push(s);
        }
        assert_eq!(items.len(), 2);
    }

    #[tokio::test]
    async fn account_get_resource_configs_and_identifiers_with_batch() {
        let list_rule =
            mock!(aws_sdk_config::Client::list_discovered_resources).then_output(|| {
                ListDiscoveredResourcesOutput::builder()
                    .resource_identifiers(
                        ResourceIdentifier::builder()
                            .resource_type(ResourceType::Instance)
                            .resource_id("i-1")
                            .build(),
                    )
                    .resource_identifiers(
                        ResourceIdentifier::builder()
                            .resource_type(ResourceType::Instance)
                            .resource_id("i-2")
                            .build(),
                    )
                    .build()
            });
        let batch_rule =
            mock!(aws_sdk_config::Client::batch_get_resource_config).then_output(|| {
                BatchGetResourceConfigOutput::builder()
                    .base_configuration_items(
                        BaseConfigurationItem::builder()
                            .resource_id("i-1")
                            .resource_type(ResourceType::Instance)
                            .build(),
                    )
                    .base_configuration_items(
                        BaseConfigurationItem::builder()
                            .resource_id("i-2")
                            .resource_type(ResourceType::Instance)
                            .build(),
                    )
                    .build()
            });
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, [&list_rule, &batch_rule]));

        let (configs_tx, mut configs_rx) = mpsc::channel(16);
        let (ids_tx, mut ids_rx) = mpsc::channel(16);
        fetcher
            .get_resource_configs_and_identifiers_with_batch(
                configs_tx,
                ids_tx,
                vec![ResourceType::Instance].into_iter(),
                None,
                ProgressBar::hidden(),
            )
            .await;

        let mut identifiers = vec![];
        tokio::time::timeout(Duration::from_secs(5), async {
            while identifiers.len() < 2 {
                identifiers.push(ids_rx.recv().await.unwrap());
            }
        })
        .await
        .unwrap();

        let mut configs = vec![];
        tokio::time::timeout(Duration::from_secs(5), async {
            while configs.len() < 2 {
                configs.push(configs_rx.recv().await.unwrap());
            }
        })
        .await
        .unwrap();

        let id_json: serde_json::Value = serde_json::from_str(&identifiers[0]).unwrap();
        assert_eq!(id_json["accountId"], "123456789012");
    }

    #[test]
    fn account_serialize_identifier() {
        let fetcher = account_fetcher(mock_client!(aws_sdk_config, []));
        let identifier = ResourceIdentifier::builder()
            .resource_type(ResourceType::Instance)
            .resource_id("i-123")
            .build();
        let json: serde_json::Value =
            serde_json::from_str(&fetcher.serialize_identifier(identifier)).unwrap();
        assert_eq!(json["resourceType"], "AWS::EC2::Instance");
        assert_eq!(json["accountId"], "123456789012");
        assert_eq!(json["resourceId"], "i-123");
    }

    fn aggregate_fetcher(client: aws_sdk_config::Client) -> AggregateFetcher {
        AggregateFetcher {
            client,
            aggregator: "my-aggregator".to_string(),
        }
    }

    #[tokio::test]
    async fn aggregate_get_resource_counts() {
        let rule = mock!(aws_sdk_config::Client::get_aggregate_discovered_resource_counts)
            .then_output(|| {
                GetAggregateDiscoveredResourceCountsOutput::builder()
                    .grouped_resource_counts(
                        GroupedResourceCount::builder()
                            .group_name("AWS::EC2::Instance")
                            .resource_count(10)
                            .build()
                            .unwrap(),
                    )
                    .grouped_resource_counts(
                        GroupedResourceCount::builder()
                            .group_name("AWS::S3::Bucket")
                            .resource_count(6)
                            .build()
                            .unwrap(),
                    )
                    .build()
            });
        let fetcher = aggregate_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let counts = fetcher.get_resource_counts().await.unwrap();
        assert_eq!(counts[&ResourceType::Instance], 10);
        assert_eq!(counts[&ResourceType::Bucket], 6);
    }

    #[tokio::test]
    async fn aggregate_get_resource_configs_and_identifiers_with_batch() {
        let list_rule = mock!(aws_sdk_config::Client::list_aggregate_discovered_resources)
            .then_output(|| {
                ListAggregateDiscoveredResourcesOutput::builder()
                    .resource_identifiers(
                        AggregateResourceIdentifier::builder()
                            .source_account_id("111111111111")
                            .source_region("us-east-1")
                            .resource_id("bucket-1")
                            .resource_type(ResourceType::Bucket)
                            .build()
                            .unwrap(),
                    )
                    .build()
            });
        let batch_rule = mock!(aws_sdk_config::Client::batch_get_aggregate_resource_config)
            .then_output(|| {
                BatchGetAggregateResourceConfigOutput::builder()
                    .base_configuration_items(
                        BaseConfigurationItem::builder()
                            .account_id("111111111111")
                            .resource_id("bucket-1")
                            .resource_type(ResourceType::Bucket)
                            .build(),
                    )
                    .build()
            });
        let fetcher = aggregate_fetcher(mock_client!(aws_sdk_config, [&list_rule, &batch_rule]));

        let (configs_tx, mut configs_rx) = mpsc::channel(16);
        let (ids_tx, mut ids_rx) = mpsc::channel(16);
        fetcher
            .get_resource_configs_and_identifiers_with_batch(
                configs_tx,
                ids_tx,
                vec![ResourceType::Bucket].into_iter(),
                None,
                ProgressBar::hidden(),
            )
            .await;

        let id = tokio::time::timeout(Duration::from_secs(5), ids_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let id_json: serde_json::Value = serde_json::from_str(&id).unwrap();
        assert_eq!(id_json["accountId"], "111111111111");
        assert_eq!(id_json["resourceId"], "bucket-1");

        let _config = tokio::time::timeout(Duration::from_secs(5), configs_rx.recv())
            .await
            .unwrap()
            .unwrap();
    }

    #[test]
    fn aggregate_serialize_identifier() {
        let rule = mock!(aws_sdk_config::Client::get_discovered_resource_counts)
            .then_output(|| GetDiscoveredResourceCountsOutput::builder().build());
        let fetcher = aggregate_fetcher(mock_client!(aws_sdk_config, [&rule]));
        let identifier = AggregateResourceIdentifier::builder()
            .source_account_id("222222222222")
            .source_region("eu-west-1")
            .resource_id("sg-abc")
            .resource_type(ResourceType::SecurityGroup)
            .build()
            .unwrap();
        let json: serde_json::Value =
            serde_json::from_str(&fetcher.serialize_identifier(identifier)).unwrap();
        assert_eq!(json["accountId"], "222222222222");
        assert_eq!(json["resourceType"], "AWS::EC2::SecurityGroup");
        assert_eq!(json["resourceId"], "sg-abc");
    }
}
