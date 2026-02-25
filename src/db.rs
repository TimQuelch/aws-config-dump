// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::collections::HashMap;

use chrono::{DateTime, Days, Utc};
use duckdb::params;
use tempfile::{NamedTempFile, TempDir, TempPath};
use tracing::{debug, error, info};

use crate::config::Config;
use crate::util;

pub async fn delete_db() -> anyhow::Result<()> {
    let path = Config::get().db_path();
    if path.exists() {
        info!(db = %path.to_string_lossy(), "deleting existing database");
        tokio::fs::remove_file(path).await?;
    }
    Ok(())
}

pub fn connect_to_db() -> anyhow::Result<duckdb::Connection> {
    let db_conn = duckdb::Connection::open(Config::get().db_path())
        .inspect_err(|e| error!(error = %e, "failed open duckdb database"))?;

    // helps with read_json on very large tables
    db_conn.execute_batch("SET preserve_insertion_order = false")?;

    Ok(db_conn)
}

pub fn get_timestamp_cutoff(db_conn: &duckdb::Connection) -> anyhow::Result<Option<DateTime<Utc>>> {
    let table_exists: i64 = db_conn.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_name = 'resources' AND table_type = 'BASE TABLE';",
        [],
        |row| row.get(0),
    )?;

    if table_exists == 0 {
        return Ok(None);
    }

    let max_time_in_db: DateTime<Utc> = db_conn
        .prepare_cached("SELECT max(configurationItemCaptureTime) FROM resources;")?
        .query_one([], |x| x.get(0))?;

    Ok(Some(max_time_in_db - Days::new(1)))
}

pub fn build_resources_table(
    db_conn: &duckdb::Connection,
    resources_json_path: &TempPath,
    identifiers_json_path: &TempPath,
) -> anyhow::Result<()> {
    db_conn.execute_batch("
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
        );",
    )?;

    db_conn.execute(
        "COPY resources_temp FROM ? (AUTO_DETECT true);",
        [resources_json_path.to_str().unwrap()],
    )?;

    let new_configs_count: i64 =
        db_conn.query_row("SELECT count(*) FROM resources_temp;", [], |row| row.get(0))?;

    info!(
        new_configs_count,
        "loaded retrieved resources into database"
    );

    db_conn.execute(
        "COPY identifiers_temp FROM ? (AUTO_DETECT true);",
        [identifiers_json_path.to_str().unwrap()],
    )?;

    let all_identifiers_count: i64 =
        db_conn.query_row("SELECT count(*) FROM identifiers_temp;", [], |row| {
            row.get(0)
        })?;

    info!(
        all_identifiers_count,
        "loaded retrieved identifiers into database"
    );

    db_conn.execute_batch("
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
    ")?;

    let mut merge_counter = HashMap::<String, i64>::new();
    let merge_query = "
        MERGE INTO resources old
            USING resources_temp new
            USING (accountId, resourceType, resourceId)
            WHEN MATCHED AND new.configurationItemCaptureTime > old.configurationItemCaptureTime THEN UPDATE
            WHEN NOT MATCHED THEN INSERT
            RETURNING merge_action;
    ";
    db_conn
        .prepare_cached(merge_query)?
        .query_map([], |row| row.get(0))?
        .map(|x| x.unwrap())
        .for_each(|action: String| *merge_counter.entry(action).or_insert(0) += 1);

    let delete_query = "
        MERGE INTO resources
            USING identifiers_temp
            USING (accountId, resourceType, resourceId)
            WHEN NOT MATCHED BY SOURCE THEN DELETE
            RETURNING merge_action;
    ";
    db_conn
        .prepare_cached(delete_query)?
        .query_map([], |row| row.get(0))?
        .map(|x| x.unwrap())
        .for_each(|action: String| *merge_counter.entry(action).or_insert(0) += 1);

    let get_count = |k| merge_counter.get(k).unwrap_or(&0);
    info!(new = get_count("INSERT"), "inserted new resources");
    info!(updated = get_count("UPDATE"), "updated existing resources");
    info!(removed = get_count("DELETE"), "removed deleted resources");
    Ok(())
}

pub fn build_resources_table_from_snapshots(
    db_conn: &duckdb::Connection,
    snapshot_dir: &TempDir,
) -> anyhow::Result<()> {
    db_conn.execute_batch("
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
        );",
    )?;

    db_conn.execute(
        "COPY resources FROM ? (AUTO_DETECT true);",
        [snapshot_dir.path().join("*.json").to_str().unwrap()],
    )?;

    Ok(())
}

pub fn build_derived_tables(
    db_conn: &duckdb::Connection,
    org_accounts: Option<Vec<(String, Option<String>)>>,
) -> anyhow::Result<()> {
    db_conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS accounts (
            accountId VARCHAR PRIMARY KEY,
            accountName VARCHAR,
        );
        CREATE OR REPLACE TEMPORARY TABLE org_accounts_temp (
            accountId VARCHAR,
            accountName VARCHAR,
        );",
    )?;

    if let Some(accounts) = org_accounts {
        let mut stmt = db_conn.prepare_cached("INSERT INTO org_accounts_temp VALUES (?, ?);")?;
        for (account_id, account_name) in accounts {
            stmt.execute(params![account_id, account_name])?;
        }
    }

    db_conn.execute_batch(
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
    )?;

    db_conn.execute_batch(concat!(
        "CREATE OR REPLACE VIEW resourceTypes AS SELECT DISTINCT resourceType FROM resources;",
        "CREATE OR REPLACE VIEW regions AS SELECT DISTINCT awsRegion FROM resources;",
    ))?;

    db_conn
        .prepare_cached("SELECT DISTINCT resourceType FROM resources;")?
        .query_map([], |row| row.get("resourceType"))?
        .filter_map(|result| {
            result
                .inspect_err(|e| error!(err = %e, "failed to query resource type"))
                .ok()
        })
        .for_each(|resource_type: String| {
            let table_name = util::resource_table_name(&resource_type);

            let file = NamedTempFile::with_suffix(".json")
                .unwrap()
                .into_temp_path();
            let filename = file.to_string_lossy();

            match db_conn.execute(
                format!(
                    "
                    COPY (
                        SELECT
                            accountId,
                            resourceType,
                            resourceId,
                            nullif(configuration, json('{{}}')) as configuration,
                            supplementaryConfiguration
                        FROM resources
                        WHERE resourceType = ?
                    ) TO '{filename}';
                "
                )
                .as_str(),
                params![&resource_type],
            ) {
                Ok(_) => debug!(resource_type, "created temporary resource json file"),
                Err(err) => {
                    error!(resource_type, %err, "failed to create temporary resource json file");
                }
            }

            match db_conn.execute(
                format!(
                    "
                    CREATE OR REPLACE TABLE {table_name} AS
                    SELECT
                        r.* EXCLUDE(configuration, supplementaryConfiguration),
                        unnest(j.configuration),
                        j.supplementaryConfiguration
                    FROM
                        read_json(?) j
                        JOIN resources r USING (accountId, resourceType, resourceId);"
                )
                .as_str(),
                params![filename],
            ) {
                Ok(_) => info!(resource_type, "created resource table"),
                Err(err) => error!(resource_type, %err, "failed to create resource table"),
            }
        });

    Ok(())
}
