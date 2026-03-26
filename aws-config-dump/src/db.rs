// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Days, Utc};
use db_client::{ConnectionManager, ConnectionPool};
use duckdb::params;
use tempfile::{NamedTempFile, TempDir, TempPath};
use tokio::task::{self, JoinSet};
use tracing::{debug, error, info};

use crate::util;

pub async fn delete_db(path: &Path) -> anyhow::Result<()> {
    if tokio::fs::try_exists(path).await? {
        info!(db = %path.to_string_lossy(), "deleting existing database");
        tokio::fs::remove_file(path).await?;
    }
    Ok(())
}

#[cfg(test)]
pub async fn connect_to_db_in_memory() -> anyhow::Result<ConnectionPool> {
    // setting preserve_insertion_order = false helps with read_json on very large tables
    let config = duckdb::Config::default().with("preserve_insertion_order", "false")?;
    let manager = ConnectionManager::open(None::<&Path>, Some(config))?;
    let pool = bb8::Pool::builder().max_size(10).build(manager).await?;
    Ok(pool)
}

pub async fn connect_to_db(path: &Path) -> anyhow::Result<ConnectionPool> {
    // setting preserve_insertion_order = false helps with read_json on very large tables
    let config = duckdb::Config::default().with("preserve_insertion_order", "false")?;
    let manager = ConnectionManager::open(Some(path), Some(config))?;
    let pool = bb8::Pool::builder().max_size(10).build(manager).await?;
    Ok(pool)
}

pub async fn get_timestamp_cutoff(pool: &ConnectionPool) -> anyhow::Result<Option<DateTime<Utc>>> {
    let db = pool.get().await?;
    let table_exists: bool = db.with_conn(|c| {
        c.query_row(
            "SELECT count(*) > 0 FROM information_schema.tables WHERE table_name = 'resources' AND table_type = 'BASE TABLE';",
            [],
            |row| row.get(0),
        )
    }).await?;

    if !table_exists {
        return Ok(None);
    }

    let max_time_in_db: DateTime<Utc> = db
        .with_conn(|c| {
            c.prepare_cached("SELECT max(configurationItemCaptureTime) FROM resources;")?
                .query_one([], |x| x.get(0))
        })
        .await?;

    Ok(Some(max_time_in_db - Days::new(1)))
}

#[allow(clippy::too_many_lines)]
pub async fn build_resources_table(
    pool: &ConnectionPool,
    resources_json_path: &TempPath,
    identifiers_json_path: &TempPath,
) -> anyhow::Result<()> {
    let db = pool.get().await?;
    db.with_conn(|c| {
        c.execute_batch("
            CREATE OR REPLACE TEMPORARY TABLE resources_temp (
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
            );
            CREATE OR REPLACE TEMPORARY TABLE identifiers_temp (
                accountId VARCHAR,
                resourceType VARCHAR,
                resourceId VARCHAR,
            );")
    })
    .await?;

    let resources_json_path = resources_json_path.to_string_lossy().to_string();
    db.with_conn(move |c| {
        c.execute(
            "COPY resources_temp FROM ? (AUTO_DETECT true);",
            [resources_json_path],
        )?;
        c.query_row("SELECT count(*) FROM resources_temp;", [], |row| row.get(0))
    })
    .await
    .map(|new_configs_count: i64| {
        info!(
            new_configs_count,
            "loaded retrieved resources into database"
        );
    })?;

    let identifiers_json_path = identifiers_json_path.to_string_lossy().to_string();
    db.with_conn(move |c| {
        c.execute(
            "COPY identifiers_temp FROM ? (AUTO_DETECT true);",
            [identifiers_json_path],
        )?;
        c.query_row("SELECT count(*) FROM identifiers_temp;", [], |row| {
            row.get(0)
        })
    })
    .await
    .map(|all_identifiers_count: i64| {
        info!(
            all_identifiers_count,
            "loaded retrieved identifiers into database"
        );
    })?;

    db.with_conn(|c| {
        c.execute_batch("
            ALTER TABLE resources_temp ALTER tags SET DATA TYPE MAP(VARCHAR, VARCHAR) USING map_from_entries(tags);
            CREATE TABLE IF NOT EXISTS resources (
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
                tags MAP(VARCHAR, VARCHAR),
                relationships STRUCT(relationshipName VARCHAR, resourceType VARCHAR, resourceId VARCHAR, resourceName VARCHAR)[],
                configuration JSON,
                supplementaryConfiguration JSON,
            );
        ")
    })
    .await?;

    let mut merge_counter: HashMap<String, i64> = db
        .with_conn(|c| {
            let merge_query = "
                MERGE INTO resources old
                    USING resources_temp new
                    USING (accountId, resourceType, resourceId)
                    WHEN MATCHED AND new.configurationItemCaptureTime > old.configurationItemCaptureTime THEN UPDATE
                    WHEN NOT MATCHED THEN INSERT
                    RETURNING merge_action;
            ";
            let mut counter = HashMap::<String, i64>::new();
            c.prepare_cached(merge_query)?
                .query_map([], |row| row.get(0))?
                .map(|x| x.unwrap())
                .for_each(|action: String| *counter.entry(action).or_insert(0) += 1);
            Ok(counter)
        })
        .await?;

    db.with_conn(|c| {
        let delete_query = "
            MERGE INTO resources
                USING identifiers_temp
                USING (accountId, resourceType, resourceId)
                WHEN NOT MATCHED BY SOURCE THEN DELETE
                RETURNING merge_action;
        ";
        let mut counter = HashMap::<String, i64>::new();
        c.prepare_cached(delete_query)?
            .query_map([], |row| row.get(0))?
            .map(|x| x.unwrap())
            .for_each(|action: String| *counter.entry(action).or_insert(0) += 1);
        Ok(counter)
    })
    .await
    .map(|delete_counter| merge_counter.extend(delete_counter))?;

    let get_count = |k| merge_counter.get(k).unwrap_or(&0);
    info!(new = get_count("INSERT"), "inserted new resources");
    info!(updated = get_count("UPDATE"), "updated existing resources");
    info!(removed = get_count("DELETE"), "removed deleted resources");

    info!("Fixing 'AWS::Organizations::Policy' values");
    db.with_conn(|c| {
        c.execute_batch(r"
            UPDATE resources
                SET configuration = json_merge_patch(configuration, json_object('Content', configuration->>'Content'))
                WHERE resourceType = 'AWS::Organizations::Policy'
                AND json_type(configuration->'Content') = 'OBJECT';
        ")
    })
    .await?;

    Ok(())
}

pub async fn build_resources_table_from_snapshots(
    pool: &ConnectionPool,
    snapshot_dir: &TempDir,
) -> anyhow::Result<()> {
    let conn = pool
        .get()
        .await
        .map_err(|e| anyhow::anyhow!("failed to get db connection: {e:?}"))?;
    conn.with_conn(|c| {
        c.execute_batch("
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
                tags MAP(VARCHAR, VARCHAR),
                relationships STRUCT(relationshipName VARCHAR, resourceType VARCHAR, resourceId VARCHAR, resourceName VARCHAR)[],
                configuration JSON,
                supplementaryConfiguration JSON,
            );")
    })
    .await?;

    let glob_path = snapshot_dir
        .path()
        .join("*.json")
        .to_str()
        .unwrap()
        .to_owned();
    conn.with_conn(move |c| c.execute("COPY resources FROM ? (AUTO_DETECT true);", [glob_path]))
        .await?;

    Ok(())
}

#[allow(clippy::too_many_lines)]
pub async fn build_derived_tables(
    pool: &ConnectionPool,
    org_accounts: Option<Vec<(String, Option<String>)>>,
) -> anyhow::Result<()> {
    let db = pool.get().await?;
    db.with_conn(|c| {
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS accounts (
                accountId VARCHAR PRIMARY KEY,
                accountName VARCHAR,
            );
            CREATE OR REPLACE TEMPORARY TABLE org_accounts_temp (
                accountId VARCHAR,
                accountName VARCHAR,
            );",
        )
    })
    .await?;

    if let Some(accounts) = org_accounts {
        db.with_conn(move |c| {
            let mut stmt = c.prepare_cached("INSERT INTO org_accounts_temp VALUES (?, ?);")?;
            for (account_id, account_name) in &accounts {
                stmt.execute(params![account_id, account_name])?;
            }
            Ok(())
        })
        .await?;
    }

    db.with_conn(|c| {
        c.execute_batch(
            "MERGE INTO accounts
                USING (
                    SELECT accountId, accountName
                    FROM (SELECT DISTINCT accountId FROM resources)
                    LEFT JOIN org_accounts_temp USING (accountId)
                ) new
                USING (accountId)
                WHEN MATCHED AND new.accountName IS NOT NULL THEN UPDATE
                WHEN NOT MATCHED THEN INSERT
                WHEN NOT MATCHED BY SOURCE THEN DELETE;",
        )
    })
    .await?;

    db.with_conn(|c| {
        c.execute_batch(concat!(
            "CREATE OR REPLACE VIEW resourceTypes AS SELECT DISTINCT resourceType FROM resources;",
            "CREATE OR REPLACE VIEW regions AS SELECT DISTINCT awsRegion FROM resources;",
        ))
    })
    .await?;

    let resource_types: Vec<String> = db
        .with_conn(|c| {
            let types = c
                .prepare_cached("SELECT DISTINCT resourceType FROM resources;")?
                .query_map([], |row| row.get("resourceType"))?
                .filter_map(|result| {
                    result
                        .inspect_err(|e| error!(err = %e, "failed to query resource type"))
                        .ok()
                })
                .collect::<Vec<String>>();
            Ok(types)
        })
        .await?;

    let mut tasks: JoinSet<_> = resource_types
        .into_iter()
        .map(|rt| build_resource_derived_table(pool.clone(), rt))
        .collect();

    while let Some(result) = tasks.join_next().await {
        result??;
    }

    Ok(())
}

async fn build_resource_derived_table(
    pool: ConnectionPool,
    resource_type: String,
) -> anyhow::Result<()> {
    let db = pool.get().await?;
    let table_name = util::resource_table_name(&resource_type);

    let file = task::spawn_blocking(|| {
        NamedTempFile::with_suffix(".json")
            .unwrap()
            .into_temp_path()
    })
    .await?;
    let filename = file.to_string_lossy().into_owned();

    if let Err(err) = db
        .with_conn({
            let resource_type = resource_type.clone();
            let filename = filename.clone();
            move |c| {
                c.execute(
                    format!(
                        "
                        COPY (
                            SELECT
                                accountId,
                                resourceType,
                                resourceId,
                                CASE WHEN configuration = json('{{}}')
                                    THEN {{configuration: NULL}}::JSON
                                    ELSE configuration
                                END AS configuration,
                                CASE WHEN supplementaryConfiguration = json('{{}}')
                                    THEN {{supplementaryConfiguration: NULL}}::JSON
                                    ELSE supplementaryConfiguration
                                END AS supplementaryConfiguration
                            FROM resources
                            WHERE resourceType = ?
                        ) TO '{filename}';"
                    )
                    .as_str(),
                    params![&resource_type],
                )
            }
        })
        .await
    {
        error!(resource_type, %err, "failed to create temporary resource json file");
    } else {
        debug!(resource_type, "created temporary resource json file");
    }

    if let Err(err) = db
        .with_conn({
            let filename = filename.clone();
            move |c| {
                c.execute(
                    format!(
                        "
                        CREATE OR REPLACE TABLE {table_name} AS
                        SELECT
                            r.* EXCLUDE(configuration, supplementaryConfiguration),
                            unnest(j.configuration),
                            unnest(j.supplementaryConfiguration)
                        FROM
                            read_json(?) j
                            JOIN resources r USING (accountId, resourceType, resourceId);"
                    )
                    .as_str(),
                    params![filename],
                )
            }
        })
        .await
    {
        error!(resource_type, %err, "failed to create resource table");
    } else {
        info!(resource_type, "created resource table");
    }
    Ok(())
}
