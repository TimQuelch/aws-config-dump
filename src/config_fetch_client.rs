use std::{
    collections::{HashMap, HashSet},
    error::Error,
    sync::{Arc, RwLock},
};

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
use futures::stream::{self, StreamExt};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::TaskTracker;
use tracing::{error, info, warn};

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

pub trait ConfigFetchClient {
    async fn get_resource_counts(&self) -> HashMap<ResourceType, i64>;
    async fn get_resource_configs_with_select(
        &self,
        file_tx: mpsc::Sender<String>,
    ) -> HashSet<ResourceType>;
    async fn get_resource_configs_with_batch(
        &self,
        file_tx: mpsc::Sender<String>,
        resource_types: impl Iterator<Item = ResourceType>,
    );
}

trait ConfigFetcher {
    type Identifier: Send;

    fn get_discovered_resource_counts_call(
        &self,
    ) -> PaginationStream<Result<impl ResourceCountPage, impl Error>>;
    fn select_resource_config_call(&self) -> PaginationStream<Result<String, impl Error>>;
    fn list_discovered_resources_call(
        &self,
        resource_type: ResourceType,
    ) -> PaginationStream<Result<Self::Identifier, impl Error + Send>>;
    fn batch_get_resources_call(
        &self,
        identifiers: Vec<Self::Identifier>,
    ) -> impl Future<Output = Result<impl BatchResponse + Send, impl Error>> + Send;
}

trait ResourceCountPage {
    fn count_iter(&self) -> impl Iterator<Item = (ResourceType, i64)>;
}

trait BatchResponse {
    fn num_unprocessed(&self) -> usize;
    fn into_items(self) -> Vec<BaseConfigurationItem>;
}

pub struct DispatchingClient {
    fetcher: DispatchTarget,
}

enum DispatchTarget {
    Account(AccountFetcher),
    Aggregate(AggregateFetcher),
}

#[derive(Clone)]
struct AccountFetcher {
    client: aws_sdk_config::Client,
}

#[derive(Clone)]
struct AggregateFetcher {
    client: aws_sdk_config::Client,
    aggregator: String,
}

impl DispatchingClient {
    pub fn new(client: &aws_sdk_config::Client, aggregator: Option<String>) -> Self {
        Self {
            fetcher: aggregator.map_or_else(
                || {
                    DispatchTarget::Account(AccountFetcher {
                        client: client.clone(),
                    })
                },
                |aggregator| {
                    DispatchTarget::Aggregate(AggregateFetcher {
                        client: client.clone(),
                        aggregator,
                    })
                },
            ),
        }
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
    dispatch!(get_resource_counts() -> HashMap<ResourceType, i64>);

    dispatch!(
        get_resource_configs_with_select(
            file_tx: mpsc::Sender<String>
        ) -> HashSet<ResourceType>
    );

    dispatch!(
        get_resource_configs_with_batch(
            file_tx: mpsc::Sender<String>,
            resource_types: impl Iterator<Item = ResourceType>
        )
    );
}

impl<C: ConfigFetcher + Clone + Send + Sync + 'static> ConfigFetchClient for C {
    async fn get_resource_counts(&self) -> HashMap<ResourceType, i64> {
        self.get_discovered_resource_counts_call()
            .into_stream_03x()
            .filter_map(async |response| {
                response
                    .inspect_err(|err| error!(err = %err, "API error"))
                    .ok()
            })
            .flat_map(|page| stream::iter(page.count_iter().collect::<Vec<_>>()))
            .collect()
            .await
    }

    async fn get_resource_configs_with_select(
        &self,
        file_tx: mpsc::Sender<String>,
    ) -> HashSet<ResourceType> {
        let seen_set = Arc::new(RwLock::new(HashSet::<ResourceType>::new()));

        let tt = TaskTracker::new();

        self.select_resource_config_call()
            .into_stream_03x()
            .filter_map(async |response| {
                response
                    .inspect_err(|err| error!(err = %err, "API error"))
                    .ok()
            })
            .for_each(async |resource| {
                let inner_file_tx = file_tx.clone();
                let seen_set = seen_set.clone();
                tt.spawn_blocking(move || {
                    process_resource_config(&inner_file_tx, &seen_set, resource);
                });
            })
            .await;

        // must wait until all the resources are processed to ensure that the seen_set is complete
        tt.close();
        tt.wait().await;

        Arc::try_unwrap(seen_set).unwrap().into_inner().unwrap()
    }

    async fn get_resource_configs_with_batch(
        &self,
        file_tx: mpsc::Sender<String>,
        resource_types: impl Iterator<Item = ResourceType>,
    ) {
        // single batch is 100
        let (batch_tx, batch_rx) = mpsc::channel::<C::Identifier>(256);

        {
            let new_self = self.clone();
            tokio::spawn(async move {
                ReceiverStream::new(batch_rx)
                    .chunks(100)
                    .for_each(async |batch| {
                        let file_tx = file_tx.clone();
                        let new_self = new_self.clone();
                        tokio::spawn(async move {
                            info!(batch_length = batch.len(), "getting batch of resources");

                            let response = new_self.batch_get_resources_call(batch).await.unwrap();

                            let num = response.num_unprocessed();
                            if num > 0 {
                                warn!(num, "unprocessed items in batch response");
                            }

                            for item in response.into_items() {
                                let json = item_to_json(item);
                                file_tx.send(json.to_string()).await.unwrap();
                            }
                        });
                    })
                    .await;
            });
        }

        let list_limiter = Arc::new(Semaphore::new(8));

        resource_types.for_each(|resource_type| {
            let batch_tx = batch_tx.clone();
            let list_limiter = list_limiter.clone();
            let new_self = self.clone();
            tokio::spawn(async move {
                let _permit = list_limiter.acquire().await.unwrap();
                info!(%resource_type, "listing resource type");
                new_self
                    .list_discovered_resources_call(resource_type)
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
}

impl ConfigFetcher for AccountFetcher {
    type Identifier = ResourceIdentifier;

    fn get_discovered_resource_counts_call(
        &self,
    ) -> PaginationStream<Result<impl ResourceCountPage, impl Error>> {
        self.client
            .get_discovered_resource_counts()
            .into_paginator()
            .send()
    }

    fn select_resource_config_call(&self) -> PaginationStream<Result<String, impl Error>> {
        self.client
            .select_resource_config()
            .expression(QUERY)
            .into_paginator()
            .items()
            .send()
    }

    fn list_discovered_resources_call(
        &self,
        resource_type: ResourceType,
    ) -> PaginationStream<Result<ResourceIdentifier, impl Error>> {
        self.client
            .list_discovered_resources()
            .resource_type(resource_type)
            .into_paginator()
            .items()
            .send()
    }

    fn batch_get_resources_call(
        &self,
        identifiers: Vec<Self::Identifier>,
    ) -> impl Future<Output = Result<impl BatchResponse + Send, impl Error>> + Send {
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
}

impl ConfigFetcher for AggregateFetcher {
    type Identifier = AggregateResourceIdentifier;

    fn get_discovered_resource_counts_call(
        &self,
    ) -> PaginationStream<Result<impl ResourceCountPage, impl Error>> {
        self.client
            .get_aggregate_discovered_resource_counts()
            .configuration_aggregator_name(&self.aggregator)
            .group_by_key(ResourceCountGroupKey::ResourceType)
            .into_paginator()
            .send()
    }

    fn select_resource_config_call(&self) -> PaginationStream<Result<String, impl Error>> {
        self.client
            .select_aggregate_resource_config()
            .configuration_aggregator_name(&self.aggregator)
            .expression(QUERY)
            .into_paginator()
            .items()
            .send()
    }

    fn list_discovered_resources_call(
        &self,
        resource_type: ResourceType,
    ) -> PaginationStream<Result<Self::Identifier, impl Error>> {
        self.client
            .list_aggregate_discovered_resources()
            .configuration_aggregator_name(&self.aggregator)
            .resource_type(resource_type)
            .into_paginator()
            .items()
            .send()
    }

    fn batch_get_resources_call(
        &self,
        identifiers: Vec<Self::Identifier>,
    ) -> impl Future<Output = Result<impl BatchResponse + Send, impl Error>> + Send {
        self.client
            .batch_get_aggregate_resource_config()
            .configuration_aggregator_name(&self.aggregator)
            .set_resource_identifiers(Some(identifiers))
            .send()
    }
}

impl ResourceCountPage for GetDiscoveredResourceCountsOutput {
    fn count_iter(&self) -> impl Iterator<Item = (ResourceType, i64)> {
        self.resource_counts().iter().map(|resource_count| {
            (
                resource_count.resource_type().unwrap().clone(),
                resource_count.count(),
            )
        })
    }
}

impl ResourceCountPage for GetAggregateDiscoveredResourceCountsOutput {
    fn count_iter(&self) -> impl Iterator<Item = (ResourceType, i64)> {
        self.grouped_resource_counts().iter().map(|resource_count| {
            (
                ResourceType::from(resource_count.group_name()),
                resource_count.resource_count(),
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
