// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures::TryStreamExt;

pub async fn fetch_org_accounts() -> anyhow::Result<Vec<(String, Option<String>)>> {
    let config = aws_config::from_env().load().await;
    let client = aws_sdk_organizations::Client::new(&config);

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
