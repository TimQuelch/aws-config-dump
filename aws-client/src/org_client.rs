// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures::TryStreamExt;

use crate::sdk_config;

/// Get AWS Organizations account information
///
/// # Errors
/// If any API query fails
pub async fn fetch_org_accounts() -> anyhow::Result<Vec<(String, Option<String>)>> {
    let config = sdk_config::load_config().await;
    fetch_org_accounts_inner(aws_sdk_organizations::Client::new(&config)).await
}

async fn fetch_org_accounts_inner(
    client: aws_sdk_organizations::Client,
) -> anyhow::Result<Vec<(String, Option<String>)>> {
    client
        .list_accounts()
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_err(anyhow::Error::from)
        .try_fold(Vec::new(), |mut acc, page| async move {
            if let Some(accounts) = page.accounts {
                acc.extend(
                    accounts
                        .into_iter()
                        .filter_map(|a| a.id.map(|id| (id, a.name))),
                );
            }
            Ok(acc)
        })
        .await
}

#[cfg(test)]
mod tests {
    use aws_sdk_organizations::{
        Client, operation::list_accounts::ListAccountsOutput, types::Account,
    };
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    use super::*;

    #[tokio::test]
    async fn fetch_org_accounts_returns_accounts() {
        let rule = mock!(Client::list_accounts).then_output(|| {
            ListAccountsOutput::builder()
                .accounts(
                    Account::builder()
                        .id("111111111111")
                        .name("account-one")
                        .build(),
                )
                .accounts(
                    Account::builder()
                        .id("222222222222")
                        .name("account-two")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_organizations, [&rule]);
        let result = fetch_org_accounts_inner(client).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("111111111111".to_string(), Some("account-one".to_string()))));
        assert!(result.contains(&("222222222222".to_string(), Some("account-two".to_string()))));
    }

    #[tokio::test]
    async fn fetch_org_accounts_multi_page() {
        let rule1 = mock!(Client::list_accounts).then_output(|| {
            ListAccountsOutput::builder()
                .accounts(
                    Account::builder()
                        .id("111111111111")
                        .name("account-one")
                        .build(),
                )
                .next_token("page2-token")
                .build()
        });
        let rule2 = mock!(Client::list_accounts).then_output(|| {
            ListAccountsOutput::builder()
                .accounts(
                    Account::builder()
                        .id("222222222222")
                        .name("account-two")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::Sequential,
            [&rule1, &rule2]
        );
        let result = fetch_org_accounts_inner(client).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("111111111111".to_string(), Some("account-one".to_string()))));
        assert!(result.contains(&("222222222222".to_string(), Some("account-two".to_string()))));
    }
}
