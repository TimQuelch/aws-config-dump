// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures::{StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use tracing::error;

use crate::sdk_config;

/// List all AWS-managed IAM policies, returning their ARN and default version ID
///
/// # Errors
/// If any API query fails
pub async fn list_aws_managed_policies(
    profile: Option<String>,
) -> anyhow::Result<Vec<(String, String)>> {
    let config = sdk_config::load_config(profile).await;
    list_aws_managed_policies_inner(aws_sdk_iam::Client::new(&config)).await
}

async fn list_aws_managed_policies_inner(
    client: aws_sdk_iam::Client,
) -> anyhow::Result<Vec<(String, String)>> {
    client
        .list_policies()
        .max_items(400) // default 100, there are currently 1520. This'll be 4 requests for a while
        .scope(aws_sdk_iam::types::PolicyScopeType::Aws)
        .into_paginator()
        .items()
        .send()
        .into_stream_03x()
        .map_err(anyhow::Error::from)
        .try_filter_map(|p| async move { Ok(p.arn.zip(p.default_version_id)) })
        .try_collect()
        .await
}

pub async fn fetch_policy_documents(
    profile: Option<String>,
    policies: Vec<(String, String)>,
    bar: ProgressBar,
) -> Vec<(String, String, String)> {
    let config = sdk_config::load_config(profile).await;
    fetch_policy_documents_inner(aws_sdk_iam::Client::new(&config), policies, bar).await
}

async fn fetch_policy_documents_inner(
    client: aws_sdk_iam::Client,
    policies: Vec<(String, String)>,
    bar: ProgressBar,
) -> Vec<(String, String, String)> {
    futures::stream::iter(policies)
        .map(|(arn, version_id)| {
            let client = client.clone();
            let bar = bar.clone();
            async move {
                let result = get_policy_version_document(&client, &arn, &version_id).await;
                bar.inc(1);
                match result {
                    Ok(doc) => Some((arn, version_id, doc)),
                    Err(e) => {
                        error!(arn, %e, "failed to fetch managed policy document");
                        None
                    }
                }
            }
        })
        .buffer_unordered(32)
        .filter_map(|x| async move { x })
        .collect()
        .await
}

async fn get_policy_version_document(
    client: &aws_sdk_iam::Client,
    policy_arn: &str,
    version_id: &str,
) -> anyhow::Result<String> {
    client
        .get_policy_version()
        .policy_arn(policy_arn)
        .version_id(version_id)
        .send()
        .await?
        .policy_version
        .and_then(|v| v.document)
        .ok_or_else(|| anyhow::anyhow!("missing document in GetPolicyVersion response"))
}

#[cfg(test)]
mod tests {
    use aws_sdk_iam::{
        Client,
        operation::{
            get_policy_version::GetPolicyVersionOutput, list_policies::ListPoliciesOutput,
        },
        types::{Policy, PolicyVersion},
    };
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    use super::*;

    #[tokio::test]
    async fn list_returns_arn_and_version_id() {
        let rule = mock!(Client::list_policies).then_output(|| {
            ListPoliciesOutput::builder()
                .policies(
                    Policy::builder()
                        .arn("arn:aws:iam::aws:policy/ReadOnlyAccess")
                        .default_version_id("v5")
                        .build(),
                )
                .policies(
                    Policy::builder()
                        .arn("arn:aws:iam::aws:policy/AdministratorAccess")
                        .default_version_id("v1")
                        .build(),
                )
                .is_truncated(false)
                .build()
        });
        let client = mock_client!(aws_sdk_iam, [&rule]);
        let result = list_aws_managed_policies_inner(client).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&(
            "arn:aws:iam::aws:policy/ReadOnlyAccess".to_string(),
            "v5".to_string()
        )));
        assert!(result.contains(&(
            "arn:aws:iam::aws:policy/AdministratorAccess".to_string(),
            "v1".to_string()
        )));
    }

    #[tokio::test]
    async fn list_paginates() {
        let rule1 = mock!(Client::list_policies).then_output(|| {
            ListPoliciesOutput::builder()
                .policies(
                    Policy::builder()
                        .arn("arn:aws:iam::aws:policy/PolicyA")
                        .default_version_id("v1")
                        .build(),
                )
                .is_truncated(true)
                .marker("page2")
                .build()
        });
        let rule2 = mock!(Client::list_policies).then_output(|| {
            ListPoliciesOutput::builder()
                .policies(
                    Policy::builder()
                        .arn("arn:aws:iam::aws:policy/PolicyB")
                        .default_version_id("v2")
                        .build(),
                )
                .is_truncated(false)
                .build()
        });
        let client = mock_client!(aws_sdk_iam, RuleMode::Sequential, [&rule1, &rule2]);
        let result = list_aws_managed_policies_inner(client).await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn fetch_documents_returns_raw_document() {
        let encoded = "%7B%22Version%22%3A%222012-10-17%22%7D";
        let rule = mock!(Client::get_policy_version).then_output(move || {
            GetPolicyVersionOutput::builder()
                .policy_version(
                    PolicyVersion::builder()
                        .document(encoded)
                        .is_default_version(true)
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_iam, [&rule]);
        let result =
            get_policy_version_document(&client, "arn:aws:iam::aws:policy/ReadOnlyAccess", "v5")
                .await
                .unwrap();
        assert_eq!(result, encoded);
    }

    #[tokio::test]
    async fn fetch_documents_returns_all_results() {
        let encoded = "%7B%7D"; // {}
        let rule = mock!(Client::get_policy_version).then_output(move || {
            GetPolicyVersionOutput::builder()
                .policy_version(
                    PolicyVersion::builder()
                        .document(encoded)
                        .is_default_version(true)
                        .build(),
                )
                .build()
        });
        let rule2 = mock!(Client::get_policy_version).then_output(move || {
            GetPolicyVersionOutput::builder()
                .policy_version(
                    PolicyVersion::builder()
                        .document(encoded)
                        .is_default_version(true)
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_iam, RuleMode::MatchAny, [&rule, &rule2]);
        let policies = vec![
            ("arn:aws:iam::aws:policy/A".to_string(), "v1".to_string()),
            ("arn:aws:iam::aws:policy/B".to_string(), "v1".to_string()),
        ];
        let results = fetch_policy_documents_inner(client, policies, ProgressBar::hidden()).await;
        assert_eq!(results.len(), 2);
    }
}
