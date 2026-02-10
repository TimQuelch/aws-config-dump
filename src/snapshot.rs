use std::path::PathBuf;

use anyhow::anyhow;
use aws_smithy_types_convert::stream::PaginationStreamExt;
use flate2::read::GzDecoder;
use futures::TryStreamExt;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt, BufWriter},
    task::{self, JoinSet},
};
use tracing::{info, warn};

pub async fn get_snapshots() -> anyhow::Result<()> {
    let config = aws_config::from_env().load().await;
    let config_client = aws_sdk_config::Client::new(&config);

    let (bucket, prefix) = get_snapshot_bucket(config_client).await?;

    let s3 = aws_sdk_s3::Client::new(&config);

    let mut snapshot_keys = get_latest_snapshot_keys(s3.clone(), bucket.clone(), prefix).await?;

    let mut downloads = JoinSet::new();

    while let Some(result) = snapshot_keys.join_next().await {
        if let Some(key) = result?? {
            info!(key, "got snapshot key");
            downloads.spawn(download_snapshot_object(s3.clone(), bucket.clone(), key));
        }
    }

    while let Some(result) = downloads.join_next().await {
        result??;
    }

    Ok(())
}

async fn get_latest_snapshot_keys(
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
) -> anyhow::Result<JoinSet<anyhow::Result<Option<String>>>> {
    let mut js = JoinSet::new();

    let account_prefixes = next_prefixes(&client, &bucket, format!("{prefix}/AWSLogs/")).await?;

    info!(?account_prefixes, "found account prefixes");

    for mut prefix in account_prefixes {
        prefix.push_str("Config/");
        let region_prefixes = next_prefixes(&client, &bucket, prefix).await?;
        for prefix in region_prefixes {
            js.spawn(get_latest_snapshot_for_account_region_prefix(
                client.clone(),
                bucket.clone(),
                prefix,
            ));
        }
    }

    Ok(js)
}

async fn get_latest_snapshot_for_account_region_prefix(
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
) -> anyhow::Result<Option<String>> {
    let years = next_prefixes(&client, &bucket, prefix).await?;

    for year_prefix in years.into_iter().rev() {
        let months = next_prefixes(&client, &bucket, year_prefix).await?;

        for month_prefix in months.into_iter().rev() {
            let days = next_prefixes(&client, &bucket, month_prefix).await?;

            for mut day_prefix in days.into_iter().rev() {
                day_prefix.push_str("ConfigSnapshot/");
                let mut day_snapshot_objects = client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .prefix(&day_prefix)
                    .send()
                    .await?
                    .contents
                    .unwrap_or_default();

                if day_snapshot_objects.len() > 1 {
                    warn!(day_prefix, "multiple objects in snapshot prefix");
                }

                if let Some(obj) = day_snapshot_objects.pop() {
                    return Ok(obj.key);
                }
            }
        }
    }

    Ok(None)
}

async fn next_prefixes(
    client: &aws_sdk_s3::Client,
    bucket: impl Into<String>,
    prefix: impl Into<String>,
) -> anyhow::Result<Vec<String>> {
    let mut prefixes = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter("/")
        .into_paginator()
        .send()
        .into_stream_03x()
        .try_fold(Vec::new(), async |mut acc, x| {
            let prefix_iter = x
                .common_prefixes
                .unwrap_or_default()
                .into_iter()
                .map(|x| x.prefix.expect("missing common prefix"));
            acc.extend(prefix_iter);
            Ok(acc)
        })
        .await?;
    prefixes.sort_unstable();
    Ok(prefixes)
}

async fn get_snapshot_bucket(
    config_client: aws_sdk_config::Client,
) -> anyhow::Result<(String, String)> {
    let mut channels = config_client
        .describe_delivery_channels()
        .send()
        .await?
        .delivery_channels
        .map(|mut v| {
            v.retain(|v| v.config_snapshot_delivery_properties.is_some());
            v
        })
        .and_then(|v| (!v.is_empty()).then_some(v))
        .ok_or(anyhow!("no delivery channels with snapshots configured"))?;

    if channels.len() > 1 {
        return Err(anyhow!(
            "multiple delivery channels with snapshots configured"
        ));
    }

    let channel = channels.pop().unwrap();

    let bucket = channel
        .s3_bucket_name
        .ok_or(anyhow!("s3 bucket not configured"))?;
    let prefix = channel
        .s3_key_prefix
        .ok_or(anyhow!("s3 prefix not configured"))?;

    info!(
        name = channel.name.unwrap_or_default(),
        bucket, prefix, "found delivery channel"
    );

    Ok((bucket, prefix))
}

pub async fn download_snapshot_object(
    client: aws_sdk_s3::Client,
    bucket: String,
    key: String,
) -> anyhow::Result<()> {
    let mut reader = client
        .get_object()
        .bucket(bucket)
        .key(&key)
        .send()
        .await?
        .body
        .into_async_read();

    let mut v = Vec::new();
    reader.read_to_end(&mut v).await?;

    let items = task::spawn_blocking(move || -> anyhow::Result<_> {
        let mut parsed: serde_json::Value = serde_json::from_reader(GzDecoder::new(v.as_slice()))?;

        let items = std::mem::take(
            parsed
                .get_mut("configurationItems")
                .and_then(|items| items.as_array_mut())
                .ok_or(anyhow!("malformed json"))?,
        );

        Ok(items)
    })
    .await??;

    let filename = PathBuf::from(&key)
        .with_extension("")
        .file_name()
        .ok_or(anyhow!("failed to get filename"))?
        .to_owned();

    let mut file_writer =
        BufWriter::with_capacity(128 * 1024, fs::File::create_new(&filename).await?);

    for item in items {
        let s = task::spawn_blocking(move || {
            let mut s = item.to_string();
            s.push('\n');
            s
        })
        .await?;

        file_writer.write_all(s.as_bytes()).await?;
    }

    // Flush both the buffered writer and the underlying file
    file_writer.flush().await?;
    file_writer.into_inner().flush().await?;

    info!(file = %filename.to_string_lossy(), "downloaded");

    anyhow::Result::<_>::Ok(())
}
