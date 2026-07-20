// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, Days, Utc};
use db_client::{ConnectionManager, ConnectionPool};
use duckdb::{params, params_from_iter};
use indicatif::MultiProgress;
use tempfile::{NamedTempFile, TempDir, TempPath};
use tokio::task::{self, JoinSet};
use tracing::{debug, error, info, trace};

use aws_client::iam_client;

use crate::util;

pub async fn build_aws_managed_policies_table(
    pool: &ConnectionPool,
    all_policies: Vec<(String, String)>,
    profile: Option<String>,
    progress: MultiProgress,
) -> anyhow::Result<()> {
    let db = pool.get().await?;

    db.with_conn(|c| {
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS aws_managed_policy (
                arn VARCHAR PRIMARY KEY,
                defaultVersionId VARCHAR,
                policyDocument JSON,
                policyStatements JSON[] AS (
                    CASE json_type(json_extract(policyDocument, '$.Statement'))
                        WHEN 'ARRAY'  THEN json_extract(policyDocument, '$.Statement')
                        WHEN 'OBJECT' THEN json_array(json_extract(policyDocument, '$.Statement'))
                        ELSE []
                    END
                ),
            );",
        )
    })
    .await?;

    let existing: HashMap<String, String> = db
        .with_conn(|c| {
            Ok(
                c.prepare_cached("SELECT arn, defaultVersionId FROM aws_managed_policy")?
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .filter_map(Result::ok)
                    .collect(),
            )
        })
        .await?;

    let to_fetch: Vec<(String, String)> = all_policies
        .into_iter()
        .filter(|(arn, version_id)| existing.get(arn) != Some(version_id))
        .collect();

    if to_fetch.is_empty() {
        info!("all AWS managed policies up to date");
        return Ok(());
    }

    let bar = progress.add(util::progress_bar(
        "fetching managed policies",
        to_fetch.len().try_into().unwrap(),
    ));

    let fetched = iam_client::fetch_policy_documents(profile, to_fetch, bar).await;
    info!(
        count = fetched.len(),
        "fetched AWS managed policy documents"
    );

    db.with_conn(move |c| {
        let mut stmt = c.prepare_cached(
            "INSERT OR REPLACE INTO aws_managed_policy VALUES (?, ?, json(url_decode(?)))",
        )?;
        for (arn, version_id, doc) in &fetched {
            stmt.execute(duckdb::params![arn, version_id, doc])?;
        }
        Ok(())
    })
    .await?;

    Ok(())
}

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
    let pool = bb8::Pool::builder().max_size(4).build(manager).await?;
    Ok(pool)
}

pub async fn connect_to_db(path: &Path) -> anyhow::Result<ConnectionPool> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    // setting preserve_insertion_order = false helps with read_json on very large tables
    let config = duckdb::Config::default().with("preserve_insertion_order", "false")?;
    let manager = ConnectionManager::open(Some(path), Some(config))?;
    let pool = bb8::Pool::builder().max_size(4).build(manager).await?;
    debug!(db = %path.display(), "opened database connection pool");
    Ok(pool)
}

/// SQL placeholder list (`?, ?, ?`) for `n` bound values.
fn placeholders(n: usize) -> String {
    vec!["?"; n].join(", ")
}

/// Columns of the `resources` table, with `tags` as a queryable map.
const RESOURCES_COLUMNS: &str = "
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
";

/// Columns as JSON delivers them, before `tags` is converted to a map.
const RESOURCES_STAGING_COLUMNS: &str = "
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
";

/// Land a fetcher's rows on the `resources` spine.
///
/// Runs after the Config data is in place: the snapshot path replaces the whole
/// table (`build_resources_table_from_snapshots`), so landing concurrently with
/// it would silently drop these rows.
///
/// Honours the fetcher's [`Semantics`]. Under [`Semantics::Authoritative`] an
/// empty row set is meaningful — it means the claimed types genuinely have no
/// resources, and stored rows are cleared. A *failed* fetch never reaches here;
/// [`crate::fetcher::fetch_rows`] returns `None` for that case precisely so the
/// two can't be confused.
///
/// # Errors
/// If any database query fails
pub async fn land_fetched_rows(
    pool: &ConnectionPool,
    output: &crate::fetcher::FetchOutput,
) -> anyhow::Result<()> {
    use crate::fetcher::Semantics;

    let db = pool.get().await?;

    // The table may not exist yet: --no-fetch on a fresh database never calls
    // build_resources_table.
    db.with_conn(|c| {
        c.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS resources ({RESOURCES_COLUMNS});"
        ))
    })
    .await?;

    // An empty file means the fetcher emitted no rows. DuckDB can't infer a
    // schema from zero bytes, so skip the COPY rather than let it error.
    let has_rows = tokio::fs::metadata(&output.path).await?.len() > 0;

    if has_rows {
        db.with_conn(|c| {
            c.execute_batch(&format!(
                "CREATE OR REPLACE TEMPORARY TABLE fetched_temp ({RESOURCES_STAGING_COLUMNS});"
            ))
        })
        .await?;

        let path = output.path.to_string_lossy().to_string();
        let name = output.name.clone();
        db.with_conn(move |c| {
            c.execute("COPY fetched_temp FROM ? (AUTO_DETECT true);", [path])?;
            c.query_row("SELECT count(*) FROM fetched_temp;", [], |row| row.get(0))
        })
        .await
        .map(|count: i64| info!(fetcher = name, count, "loaded fetched rows"))?;

        db.with_conn(|c| {
            c.execute_batch(
                "ALTER TABLE fetched_temp ALTER tags SET DATA TYPE MAP(VARCHAR, VARCHAR) USING map_from_entries(tags);",
            )
        })
        .await?;
    }

    match output.semantics {
        Semantics::Authoritative => {
            let delete_query = format!(
                "DELETE FROM resources WHERE resourceType IN ({});",
                placeholders(output.resource_types.len())
            );
            let types = output.resource_types.clone();

            // One transaction: the delete clears every stored row for the
            // claimed types, so a failure between it and the insert would leave
            // the types empty rather than merely stale.
            let removed = db
                .with_conn(move |c| {
                    let tx = c.transaction()?;
                    let removed = tx.execute(&delete_query, params_from_iter(types.iter()))?;
                    if has_rows {
                        tx.execute_batch("INSERT INTO resources SELECT * FROM fetched_temp;")?;
                    }
                    tx.commit()?;
                    Ok(removed)
                })
                .await?;
            debug!(
                fetcher = output.name,
                removed, "cleared existing rows for claimed types"
            );
        }
        Semantics::Merge => {
            if has_rows {
                db.with_conn(|c| {
                    c.execute_batch(
                        "MERGE INTO resources
                            USING fetched_temp
                            USING (accountId, resourceType, resourceId)
                            WHEN MATCHED THEN UPDATE
                            WHEN NOT MATCHED THEN INSERT;",
                    )
                })
                .await?;
            }
        }
    }

    info!(fetcher = output.name, "landed fetched rows");
    Ok(())
}

/// Get the timestamp to fetch modified Config resources from.
///
/// `fetcher_types` are excluded from the calculation. Fetched rows carry the
/// time they were fetched, not the time AWS recorded them, so letting them into
/// this `max()` would drag the cutoff forward and make Config skip resources it
/// should have refetched.
///
/// # Errors
/// If any database query fails
pub async fn get_timestamp_cutoff(
    pool: &ConnectionPool,
    fetcher_types: &[String],
) -> anyhow::Result<Option<DateTime<Utc>>> {
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

    let filter = if fetcher_types.is_empty() {
        String::new()
    } else {
        format!(
            " WHERE resourceType NOT IN ({})",
            placeholders(fetcher_types.len())
        )
    };
    let query = format!("SELECT max(configurationItemCaptureTime) FROM resources{filter};");
    let fetcher_types = fetcher_types.to_vec();

    // NULL when no Config-sourced rows exist yet — an empty table, or one
    // holding only fetched rows.
    let max_time_in_db: Option<DateTime<Utc>> = db
        .with_conn(move |c| {
            c.prepare_cached(&query)?
                .query_one(params_from_iter(fetcher_types.iter()), |x| x.get(0))
        })
        .await?;

    let Some(max_time_in_db) = max_time_in_db else {
        debug!("no config-sourced resources in database, fetching all");
        return Ok(None);
    };

    let cutoff = max_time_in_db - Days::new(1);
    debug!(%cutoff, "using timestamp cutoff");
    Ok(Some(cutoff))
}

/// Build the `resources` table from fetched Config data.
///
/// `fetcher_types` are resource types owned by a [`crate::fetcher::Fetcher`]
/// rather than by Config. They're excluded from the identifier-driven delete,
/// since Config's listing has no knowledge of them and would otherwise reap
/// them.
///
/// # Errors
/// If any database query fails
#[allow(clippy::too_many_lines)]
pub async fn build_resources_table(
    pool: &ConnectionPool,
    resources_json_path: &TempPath,
    identifiers_json_path: &TempPath,
    fetcher_types: &[String],
) -> anyhow::Result<()> {
    let db = pool.get().await?;
    db.with_conn(|c| {
        c.execute_batch(&format!(
            "CREATE OR REPLACE TEMPORARY TABLE resources_temp ({RESOURCES_STAGING_COLUMNS});
            CREATE OR REPLACE TEMPORARY TABLE identifiers_temp (
                accountId VARCHAR,
                resourceType VARCHAR,
                resourceId VARCHAR,
            );"
        ))
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
        c.execute_batch(&format!("
            ALTER TABLE resources_temp ALTER tags SET DATA TYPE MAP(VARCHAR, VARCHAR) USING map_from_entries(tags);
            CREATE TABLE IF NOT EXISTS resources ({RESOURCES_COLUMNS});
        "))
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

    // Config's identifier listing is authoritative only over Config's own rows.
    // Rows claimed by a fetcher are never in `identifiers_temp`, so without this
    // exclusion every fetched row would be deleted here on the next build.
    let delete_filter = if fetcher_types.is_empty() {
        String::new()
    } else {
        format!(
            " AND resourceType NOT IN ({})",
            placeholders(fetcher_types.len())
        )
    };
    let delete_query = format!(
        "MERGE INTO resources
            USING identifiers_temp
            USING (accountId, resourceType, resourceId)
            WHEN NOT MATCHED BY SOURCE{delete_filter} THEN DELETE
            RETURNING merge_action;"
    );
    let owned_fetcher_types = fetcher_types.to_vec();

    db.with_conn(move |c| {
        let mut counter = HashMap::<String, i64>::new();
        c.prepare_cached(&delete_query)?
            .query_map(params_from_iter(owned_fetcher_types.iter()), |row| {
                row.get(0)
            })?
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
    info!("Fixing 'AWS::SSM::PatchCompliance' values");
    db.with_conn(|c| {
        c.execute_batch(r"
            UPDATE resources
                SET configuration = json_merge_patch(configuration, json_object('AWS:ComplianceItem', configuration->>'AWS:ComplianceItem'))
                WHERE resourceType = 'AWS::SSM::PatchCompliance'
                AND json_type(configuration->'AWS:ComplianceItem') = 'OBJECT';
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
        c.execute_batch(&format!(
            "CREATE OR REPLACE TABLE resources ({RESOURCES_COLUMNS});"
        ))
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
    progress: MultiProgress,
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
        c.execute_batch(
            "CREATE TABLE IF NOT EXISTS custom_resource_types (resource_type VARCHAR PRIMARY KEY);
            CREATE OR REPLACE VIEW resourceTypes AS
                SELECT DISTINCT resourceType FROM resources
                UNION
                SELECT resource_type FROM custom_resource_types;
            CREATE OR REPLACE VIEW regions AS SELECT DISTINCT awsRegion FROM resources;",
        )
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

    let bar = progress.add(util::progress_bar(
        "building derived tables",
        resource_types.len().try_into().unwrap(),
    ));

    let mut tasks: JoinSet<_> = resource_types
        .into_iter()
        .map(|rt| build_resource_derived_table(pool.clone(), rt))
        .collect();

    while let Some(result) = tasks.join_next().await {
        result??;
        bar.inc(1);
    }

    Ok(())
}

pub async fn build_custom_tables(
    pool: &ConnectionPool,
    custom_tables: &[crate::config::CustomTable],
    progress: MultiProgress,
) -> anyhow::Result<()> {
    if custom_tables.is_empty() {
        return Ok(());
    }

    let bar = progress.add(util::progress_bar(
        "building custom tables",
        custom_tables.len().try_into().unwrap(),
    ));

    let db = pool.get().await?;
    db.with_conn(|c| c.execute_batch("DELETE FROM custom_resource_types;"))
        .await?;

    let existing_tables: Vec<String> = db
        .with_conn(|c| {
            Ok(
                c.prepare_cached("SELECT table_name FROM information_schema.tables;")?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .filter_map(std::result::Result::ok)
                    .collect(),
            )
        })
        .await?;

    let mut tasks = JoinSet::new();

    for ct in custom_tables.iter().cloned() {
        let deps_ok = ct.dependencies.iter().all(|d| existing_tables.contains(d));
        if !deps_ok {
            info!(name = %ct.name, "skipping custom table: missing dependencies");
            bar.dec_length(1);
            continue;
        }

        let bar = bar.clone();
        let db = pool.get_owned().await?;

        tasks.spawn(async move {
            let table_name = util::resource_table_name(&ct.name);
            let name = ct.name.clone();
            let sql = ct.sql.clone();
            let desc = ct.description.as_deref().unwrap_or(&name).to_string();

            let create_sql = format!(
                "CREATE OR REPLACE TABLE \"{table_name}\" AS SELECT *, ? AS resourceType FROM ({sql});"
            );

            match db
                .with_conn(move |c| {
                    c.execute(&create_sql, [&name])?;
                    c.execute(
                        "INSERT OR REPLACE INTO custom_resource_types VALUES (?);",
                        [&name],
                    )
                })
                .await
            {
                Ok(_) => info!(
                    description = desc,
                    table = table_name,
                    "created custom table"
                ),
                Err(err) => {
                    error!(
                        description = desc,
                        table = table_name,
                        %err,
                        "failed to create custom table"
                    );
                }
            }
            bar.inc(1);
        });
    }

    while let Some(result) = tasks.join_next().await {
        result?;
    }

    Ok(())
}

async fn build_resource_derived_table(
    pool: ConnectionPool,
    resource_type: String,
) -> anyhow::Result<()> {
    let db = pool.get().await?;
    let table_name = util::resource_table_name(&resource_type);
    trace!(resource_type, table_name, "building derived table");

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CustomTable;
    use crate::fetcher::{FetchOutput, Semantics};
    use indicatif::MultiProgress;

    const OU_TYPE: &str = "AWS::Organizations::OrganizationalUnit";
    const CONFIG_TYPE: &str = "AWS::EC2::Instance";

    fn ou_types() -> Vec<String> {
        vec![OU_TYPE.to_owned()]
    }

    /// A `resources`-shaped JSON line.
    ///
    /// `configuration` is a JSON *object*, matching what both real emitters
    /// produce: `item_to_json` parses Config's configuration string into a value
    /// (`config_client.rs:647`), and the OU fetcher builds an object directly.
    /// Stringifying it here would make these tests pass against data no
    /// production path can emit.
    fn row_json(resource_type: &str, resource_id: &str, capture_time: &str) -> String {
        resources_row(
            resource_type,
            resource_id,
            resource_id,
            capture_time,
            &serde_json::json!({}),
        )
    }

    fn resources_row(
        resource_type: &str,
        resource_id: &str,
        resource_name: &str,
        capture_time: &str,
        configuration: &serde_json::Value,
    ) -> String {
        serde_json::json!({
            "arn": format!("arn:test:{resource_id}"),
            "accountId": "111111111111",
            "awsRegion": "global",
            "resourceType": resource_type,
            "resourceId": resource_id,
            "resourceName": resource_name,
            "availabilityZone": null,
            "resourceCreationTime": null,
            "configurationItemCaptureTime": capture_time,
            "configurationItemStatus": null,
            "configurationStateId": null,
            "tags": [],
            "relationships": [],
            "configuration": configuration,
            // An object rather than null, matching both real emitters — a null
            // here would break the derived-table unnest.
            "supplementaryConfiguration": serde_json::json!({}),
        })
        .to_string()
            + "\n"
    }

    fn identifier_json(resource_type: &str, resource_id: &str) -> String {
        serde_json::json!({
            "accountId": "111111111111",
            "resourceType": resource_type,
            "resourceId": resource_id,
        })
        .to_string()
            + "\n"
    }

    async fn json_file(lines: &[String]) -> TempPath {
        let path = NamedTempFile::with_suffix(".json")
            .unwrap()
            .into_temp_path();
        tokio::fs::write(&path, lines.concat()).await.unwrap();
        path
    }

    async fn fetch_output(semantics: Semantics, lines: &[String]) -> FetchOutput {
        FetchOutput {
            name: "test-fetcher".to_owned(),
            resource_types: ou_types(),
            semantics,
            path: json_file(lines).await,
        }
    }

    async fn resource_ids(pool: &ConnectionPool) -> Vec<String> {
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                Ok(
                    c.prepare("SELECT resourceId FROM resources ORDER BY resourceId")?
                        .query_map([], |row| row.get::<_, String>(0))?
                        .filter_map(Result::ok)
                        .collect(),
                )
            })
            .await
            .unwrap()
    }

    async fn setup_pool() -> ConnectionPool {
        let pool = connect_to_db_in_memory().await.unwrap();
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.execute_batch(
                    "CREATE TABLE IF NOT EXISTS custom_resource_types (resource_type VARCHAR PRIMARY KEY);",
                )
            })
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    async fn custom_table_created_from_sql() {
        let pool = setup_pool().await;
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.execute_batch(
                    "CREATE TABLE source (accountId VARCHAR, val INTEGER);
                     INSERT INTO source VALUES ('123', 42);",
                )
            })
            .await
            .unwrap();

        let ct = CustomTable {
            name: "Custom::Test::Thing".to_owned(),
            description: None,
            dependencies: vec![],
            sql: "SELECT accountId, val FROM source".to_owned(),
        };

        build_custom_tables(&pool, &[ct], MultiProgress::new())
            .await
            .unwrap();

        let count: i64 = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'custom_test_thing'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn custom_table_has_injected_resource_type() {
        let pool = setup_pool().await;
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.execute_batch(
                    "CREATE TABLE source2 (accountId VARCHAR);
                     INSERT INTO source2 VALUES ('456');",
                )
            })
            .await
            .unwrap();

        let ct = CustomTable {
            name: "Custom::Test::Widget".to_owned(),
            description: None,
            dependencies: vec![],
            sql: "SELECT accountId FROM source2".to_owned(),
        };

        build_custom_tables(&pool, &[ct], MultiProgress::new())
            .await
            .unwrap();

        let rt: String = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row("SELECT resourceType FROM custom_test_widget", [], |row| {
                    row.get(0)
                })
            })
            .await
            .unwrap();
        assert_eq!(rt, "Custom::Test::Widget");
    }

    #[tokio::test]
    async fn custom_table_skipped_when_dependency_missing() {
        let pool = setup_pool().await;

        let ct = CustomTable {
            name: "Custom::Test::Skipped".to_owned(),
            description: None,
            dependencies: vec!["nonexistent_table".to_owned()],
            sql: "SELECT 1 AS accountId".to_owned(),
        };

        build_custom_tables(&pool, &[ct], MultiProgress::new())
            .await
            .unwrap();

        let count: i64 = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.tables WHERE table_name = 'custom_test_skipped'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn custom_resource_types_populated() {
        let pool = setup_pool().await;
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| c.execute_batch("CREATE TABLE src (accountId VARCHAR);"))
            .await
            .unwrap();

        let ct = CustomTable {
            name: "Custom::Test::Registered".to_owned(),
            description: None,
            dependencies: vec![],
            sql: "SELECT accountId FROM src".to_owned(),
        };

        build_custom_tables(&pool, &[ct], MultiProgress::new())
            .await
            .unwrap();

        let rt: String = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT resource_type FROM custom_resource_types",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(rt, "Custom::Test::Registered");
    }

    #[tokio::test]
    async fn custom_resource_types_cleared_on_rebuild() {
        let pool = setup_pool().await;
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.execute_batch(
                    "CREATE TABLE src_a (accountId VARCHAR);
                     CREATE TABLE src_b (accountId VARCHAR);",
                )
            })
            .await
            .unwrap();

        let ct_a = CustomTable {
            name: "Custom::Test::Alpha".to_owned(),
            description: None,
            dependencies: vec![],
            sql: "SELECT accountId FROM src_a".to_owned(),
        };
        build_custom_tables(&pool, &[ct_a], MultiProgress::new())
            .await
            .unwrap();

        let ct_b = CustomTable {
            name: "Custom::Test::Beta".to_owned(),
            description: None,
            dependencies: vec![],
            sql: "SELECT accountId FROM src_b".to_owned(),
        };
        build_custom_tables(&pool, &[ct_b], MultiProgress::new())
            .await
            .unwrap();

        let types: Vec<String> = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                Ok(c.prepare(
                    "SELECT resource_type FROM custom_resource_types ORDER BY resource_type",
                )?
                .query_map([], |row| row.get(0))?
                .filter_map(std::result::Result::ok)
                .collect())
            })
            .await
            .unwrap();
        assert_eq!(types, vec!["Custom::Test::Beta"]);
    }

    mod managed_policies {
        use super::*;

        async fn setup() -> ConnectionPool {
            let pool = connect_to_db_in_memory().await.unwrap();
            build_aws_managed_policies_table(&pool, vec![], None, MultiProgress::new())
                .await
                .unwrap();
            pool
        }

        #[tokio::test]
        async fn table_created_on_first_call() {
            let pool = setup().await;
            let count: i64 = pool
                .get()
                .await
                .unwrap()
                .with_conn(|c| {
                    c.query_row(
                        "SELECT count(*) FROM information_schema.tables \
                         WHERE table_name = 'aws_managed_policy'",
                        [],
                        |row| row.get(0),
                    )
                })
                .await
                .unwrap();
            assert_eq!(count, 1);
        }

        #[tokio::test]
        async fn table_is_idempotent_when_called_twice() {
            let pool = setup().await;
            build_aws_managed_policies_table(&pool, vec![], None, MultiProgress::new())
                .await
                .unwrap();
            let count: i64 = pool
                .get()
                .await
                .unwrap()
                .with_conn(|c| {
                    c.query_row("SELECT count(*) FROM aws_managed_policy", [], |row| {
                        row.get(0)
                    })
                })
                .await
                .unwrap();
            assert_eq!(count, 0);
        }

        #[tokio::test]
        async fn existing_policy_at_same_version_not_replaced() {
            let pool = setup().await;
            pool.get()
                .await
                .unwrap()
                .with_conn(|c| {
                    c.execute(
                        "INSERT INTO aws_managed_policy VALUES (?, ?, json(?))",
                        duckdb::params![
                            "arn:aws:iam::aws:policy/ReadOnlyAccess",
                            "v5",
                            r#"{"original":true}"#
                        ],
                    )
                })
                .await
                .unwrap();

            // Same arn+version in all_policies → diff is empty → no fetch call
            build_aws_managed_policies_table(
                &pool,
                vec![(
                    "arn:aws:iam::aws:policy/ReadOnlyAccess".to_string(),
                    "v5".to_string(),
                )],
                None,
                MultiProgress::new(),
            )
            .await
            .unwrap();

            let doc: String = pool
                .get()
                .await
                .unwrap()
                .with_conn(|c| {
                    c.query_row(
                        "SELECT policyDocument::VARCHAR FROM aws_managed_policy \
                         WHERE arn = 'arn:aws:iam::aws:policy/ReadOnlyAccess'",
                        [],
                        |row| row.get(0),
                    )
                })
                .await
                .unwrap();
            assert!(doc.contains("original"));
        }
    }

    // --- Config's control plane is scoped to Config's own rows ---------------

    /// The delete trap. Config's identifier listing knows nothing about fetched
    /// rows, so an unscoped `NOT MATCHED BY SOURCE ... DELETE` reaps them on the
    /// next incremental build — the fetched rows would survive the build that
    /// created them and vanish on the following one.
    #[tokio::test]
    async fn fetched_rows_survive_the_config_identifier_delete() {
        let pool = setup_pool().await;

        // A prior build left an OU row and two Config rows.
        let seed = [
            row_json(OU_TYPE, "ou-1", "2026-01-01T00:00:00Z"),
            row_json(CONFIG_TYPE, "i-live", "2026-01-01T00:00:00Z"),
            row_json(CONFIG_TYPE, "i-stale", "2026-01-01T00:00:00Z"),
        ];
        let identifiers = [identifier_json(CONFIG_TYPE, "i-live")];
        build_resources_table(
            &pool,
            &json_file(&seed).await,
            &json_file(&identifiers).await,
            &ou_types(),
        )
        .await
        .unwrap();

        let ids = resource_ids(&pool).await;
        assert!(
            ids.contains(&"ou-1".to_owned()),
            "fetched row was deleted by Config's identifier listing, which never knew about it"
        );
        assert!(ids.contains(&"i-live".to_owned()));
        assert!(
            !ids.contains(&"i-stale".to_owned()),
            "a Config row absent from the identifier listing should still be reaped"
        );
    }

    /// Serves double duty. As a control for the test above, it shows the
    /// exclusion is what saves the OU row rather than some other accident of the
    /// merge. And it is the specified behaviour of *disabling* a fetcher:
    /// `fetcher_resource_types` returns nothing for a disabled fetcher, which is
    /// precisely this call, so its rows are handed back to Config's delete and
    /// cleaned up rather than left stale with nothing able to refresh them.
    #[tokio::test]
    async fn releasing_the_claim_reaps_fetched_rows_but_spares_config_rows() {
        let pool = setup_pool().await;

        let seed = [
            row_json(OU_TYPE, "ou-1", "2026-01-01T00:00:00Z"),
            row_json(CONFIG_TYPE, "i-live", "2026-01-01T00:00:00Z"),
        ];
        let identifiers = [identifier_json(CONFIG_TYPE, "i-live")];

        // No claimed types: what a build sees when the fetcher is disabled.
        build_resources_table(
            &pool,
            &json_file(&seed).await,
            &json_file(&identifiers).await,
            &[],
        )
        .await
        .unwrap();

        let ids = resource_ids(&pool).await;
        assert!(
            !ids.contains(&"ou-1".to_owned()),
            "with the claim released the OU row should be reaped — if it survives, \
             disabling a fetcher would leave its rows stale forever, and the \
             delete-trap test above no longer proves the exclusion does anything"
        );
        assert!(
            ids.contains(&"i-live".to_owned()),
            "disabling a fetcher must not disturb Config's own rows"
        );
    }

    #[tokio::test]
    async fn timestamp_cutoff_ignores_fetched_rows() {
        let pool = setup_pool().await;

        // The OU row is much fresher than any Config row.
        let seed = [
            row_json(CONFIG_TYPE, "i-1", "2026-01-10T00:00:00Z"),
            row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z"),
        ];
        let identifiers = [identifier_json(CONFIG_TYPE, "i-1")];
        build_resources_table(
            &pool,
            &json_file(&seed).await,
            &json_file(&identifiers).await,
            &ou_types(),
        )
        .await
        .unwrap();

        let cutoff = get_timestamp_cutoff(&pool, &ou_types())
            .await
            .unwrap()
            .expect("config rows exist, so there should be a cutoff");

        // Cutoff is the newest Config row less one day. If the OU row leaked in,
        // it would be six months later and Config would skip real changes.
        let expected = DateTime::parse_from_rfc3339("2026-01-09T00:00:00Z").unwrap();
        assert_eq!(
            cutoff, expected,
            "fetched rows dragged the Config cutoff forward"
        );
    }

    /// Control: unscoped, the fetched row does set the cutoff.
    #[tokio::test]
    async fn unscoped_cutoff_would_follow_fetched_rows() {
        let pool = setup_pool().await;

        let seed = [
            row_json(CONFIG_TYPE, "i-1", "2026-01-10T00:00:00Z"),
            row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z"),
        ];
        let identifiers = [
            identifier_json(CONFIG_TYPE, "i-1"),
            identifier_json(OU_TYPE, "ou-1"),
        ];
        build_resources_table(
            &pool,
            &json_file(&seed).await,
            &json_file(&identifiers).await,
            &[],
        )
        .await
        .unwrap();

        let cutoff = get_timestamp_cutoff(&pool, &[]).await.unwrap().unwrap();
        let expected = DateTime::parse_from_rfc3339("2026-07-16T00:00:00Z").unwrap();
        assert_eq!(cutoff, expected);
    }

    /// A database holding only fetched rows has no Config history to resume
    /// from. The scoped `max()` is NULL there, which must read as "fetch
    /// everything" rather than blowing up.
    #[tokio::test]
    async fn timestamp_cutoff_is_none_when_only_fetched_rows_exist() {
        let pool = setup_pool().await;

        let output = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &output).await.unwrap();

        let cutoff = get_timestamp_cutoff(&pool, &ou_types()).await.unwrap();
        assert!(cutoff.is_none());
    }

    // --- Landing fetched rows ------------------------------------------------

    /// The atomicity guarantee. An `Authoritative` landing clears every stored
    /// row for its claimed types before inserting the new ones, so if the insert
    /// fails and the delete has already committed, the types are left *empty* —
    /// strictly worse than the fetcher not having run.
    ///
    /// The failure is induced with a `resources` table whose column count does
    /// not match the staging table, so the `INSERT ... SELECT *` errors while
    /// the preceding `DELETE` succeeds.
    #[tokio::test]
    async fn a_failed_authoritative_landing_rolls_back_its_delete() {
        let pool = setup_pool().await;

        // Narrower than RESOURCES_COLUMNS, so the insert cannot succeed. The
        // delete can, since it only needs resourceType.
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.execute_batch(
                    "CREATE TABLE resources (resourceType VARCHAR, resourceId VARCHAR);
                     INSERT INTO resources VALUES
                        ('AWS::Organizations::OrganizationalUnit', 'ou-previous');",
                )
            })
            .await
            .unwrap();

        let output = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-new", "2026-07-18T00:00:00Z")],
        )
        .await;

        let result = land_fetched_rows(&pool, &output).await;
        assert!(
            result.is_err(),
            "the landing should have failed on the mismatched insert"
        );

        assert_eq!(
            resource_ids(&pool).await,
            vec!["ou-previous".to_owned()],
            "a failed landing must roll back its delete, not leave the claimed types empty"
        );
    }

    /// The row shape a fetcher's rows are serialised into has constraints that
    /// are `DuckDB`'s, not JSON's -- `tags` must stage as `STRUCT(key, value)[]`
    /// before `map_from_entries` will take it. No in-process assertion on the
    /// emitted object catches a violation, so this lands a row built by the
    /// conversion itself rather than by a hand-written fixture.
    #[tokio::test]
    async fn a_row_serialised_by_the_fetcher_seam_lands_with_its_tags() {
        let pool = setup_pool().await;

        let row = crate::fetcher::Row {
            arn: "arn:test:ou-tagged".to_owned(),
            account_id: "111111111111".to_owned(),
            aws_region: "global".to_owned(),
            resource_type: OU_TYPE.to_owned(),
            resource_id: "ou-tagged".to_owned(),
            resource_name: Some("ou-tagged".to_owned()),
            captured_at: "2026-07-17T00:00:00Z".parse().expect("valid timestamp"),
            tags: vec![aws_client::Tag {
                key: "Environment".to_owned(),
                value: "prod".to_owned(),
            }],
            configuration: serde_json::json!({}),
        };

        let output = fetch_output(Semantics::Authoritative, &[String::from(row)]).await;
        land_fetched_rows(&pool, &output).await.unwrap();

        let tag: String = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT map_extract(tags, 'Environment')[1] FROM resources
                     WHERE resourceId = 'ou-tagged';",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();

        assert_eq!(tag, "prod");
    }

    #[tokio::test]
    async fn authoritative_landing_removes_rows_that_vanished() {
        let pool = setup_pool().await;

        let first = fetch_output(
            Semantics::Authoritative,
            &[
                row_json(OU_TYPE, "ou-kept", "2026-07-17T00:00:00Z"),
                row_json(OU_TYPE, "ou-deleted", "2026-07-17T00:00:00Z"),
            ],
        )
        .await;
        land_fetched_rows(&pool, &first).await.unwrap();

        // Second build: ou-deleted is gone from the organization.
        let second = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-kept", "2026-07-18T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &second).await.unwrap();

        assert_eq!(resource_ids(&pool).await, vec!["ou-kept".to_owned()]);
    }

    #[tokio::test]
    async fn merge_landing_retains_rows_it_did_not_emit() {
        let pool = setup_pool().await;

        let first = fetch_output(
            Semantics::Merge,
            &[
                row_json(OU_TYPE, "ou-a", "2026-07-17T00:00:00Z"),
                row_json(OU_TYPE, "ou-b", "2026-07-17T00:00:00Z"),
            ],
        )
        .await;
        land_fetched_rows(&pool, &first).await.unwrap();

        // Second build only re-emits ou-a; ou-b must not be treated as deleted.
        let second = fetch_output(
            Semantics::Merge,
            &[row_json(OU_TYPE, "ou-a", "2026-07-18T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &second).await.unwrap();

        assert_eq!(
            resource_ids(&pool).await,
            vec!["ou-a".to_owned(), "ou-b".to_owned()]
        );
    }

    #[tokio::test]
    async fn merge_landing_updates_rows_it_re_emits() {
        let pool = setup_pool().await;

        let first = fetch_output(
            Semantics::Merge,
            &[row_json(OU_TYPE, "ou-a", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &first).await.unwrap();

        let second = fetch_output(
            Semantics::Merge,
            &[row_json(OU_TYPE, "ou-a", "2026-07-18T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &second).await.unwrap();

        let capture: DateTime<Utc> = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT configurationItemCaptureTime FROM resources WHERE resourceId = 'ou-a'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(
            capture,
            DateTime::parse_from_rfc3339("2026-07-18T00:00:00Z").unwrap()
        );
        assert_eq!(
            resource_ids(&pool).await.len(),
            1,
            "merge should not duplicate"
        );
    }

    /// The counterpart to the soft-fail guarantee: an *empty* landing is
    /// meaningful. A fetcher that succeeded and found nothing means the type is
    /// genuinely empty, and its stale rows should go. (A fetcher that *failed*
    /// never produces a [`FetchOutput`] at all — see `fetcher::tests`.)
    #[tokio::test]
    async fn authoritative_landing_with_no_rows_clears_claimed_types() {
        let pool = setup_pool().await;

        let first = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &first).await.unwrap();
        assert_eq!(resource_ids(&pool).await.len(), 1);

        let empty = fetch_output(Semantics::Authoritative, &[]).await;
        land_fetched_rows(&pool, &empty).await.unwrap();

        assert!(resource_ids(&pool).await.is_empty());
    }

    /// Landing must not touch rows outside the fetcher's claimed types.
    #[tokio::test]
    async fn authoritative_landing_leaves_other_types_alone() {
        let pool = setup_pool().await;

        let config_rows = [row_json(CONFIG_TYPE, "i-1", "2026-01-01T00:00:00Z")];
        build_resources_table(
            &pool,
            &json_file(&config_rows).await,
            &json_file(&[identifier_json(CONFIG_TYPE, "i-1")]).await,
            &ou_types(),
        )
        .await
        .unwrap();

        let output = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &output).await.unwrap();

        let ids = resource_ids(&pool).await;
        assert!(
            ids.contains(&"i-1".to_owned()),
            "Config's rows were collateral"
        );
        assert!(ids.contains(&"ou-1".to_owned()));
    }

    /// `--no-fetch` on a fresh database never calls `build_resources_table`, so
    /// landing has to cope with the table not existing yet.
    #[tokio::test]
    async fn landing_creates_the_resources_table_if_absent() {
        let pool = connect_to_db_in_memory().await.unwrap();

        let output = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &output).await.unwrap();

        assert_eq!(resource_ids(&pool).await, vec!["ou-1".to_owned()]);
    }

    // --- Downstream wiring ---------------------------------------------------

    /// The payoff for landing on the spine: a fetched type gets a derived table
    /// and reaches the resourceTypes view with no fetcher-specific code.
    #[tokio::test]
    async fn fetched_types_get_derived_tables_and_reach_resource_types() {
        let pool = setup_pool().await;

        let output = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &output).await.unwrap();
        build_derived_tables(&pool, None, MultiProgress::new())
            .await
            .unwrap();

        let derived_count: i64 = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM organizations_organizationalunit",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(derived_count, 1, "fetched type should get a derived table");

        let in_view: bool = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) > 0 FROM resourceTypes WHERE resourceType = ?",
                    [OU_TYPE],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert!(in_view, "fetched type should appear in resourceTypes");
    }

    /// OUs are global, and `global` is a value Config already uses — so they
    /// must not introduce a new region of their own.
    #[tokio::test]
    async fn fetched_global_rows_introduce_no_new_region() {
        let pool = setup_pool().await;

        let config_rows = [row_json(CONFIG_TYPE, "i-1", "2026-01-01T00:00:00Z")];
        build_resources_table(
            &pool,
            &json_file(&config_rows).await,
            &json_file(&[identifier_json(CONFIG_TYPE, "i-1")]).await,
            &ou_types(),
        )
        .await
        .unwrap();
        build_derived_tables(&pool, None, MultiProgress::new())
            .await
            .unwrap();

        let regions_before = region_list(&pool).await;

        let output = fetch_output(
            Semantics::Authoritative,
            &[row_json(OU_TYPE, "ou-1", "2026-07-17T00:00:00Z")],
        )
        .await;
        land_fetched_rows(&pool, &output).await.unwrap();
        build_derived_tables(&pool, None, MultiProgress::new())
            .await
            .unwrap();

        assert_eq!(
            region_list(&pool).await,
            regions_before,
            "OU rows introduced a region of their own"
        );
    }

    async fn region_list(pool: &ConnectionPool) -> Vec<String> {
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                Ok(
                    c.prepare("SELECT awsRegion FROM regions ORDER BY awsRegion")?
                        .query_map([], |row| row.get::<_, String>(0))?
                        .filter_map(Result::ok)
                        .collect(),
                )
            })
            .await
            .unwrap()
    }

    /// Acquisition flattens the OU tree and leaves the hierarchy to SQL. This
    /// checks that decision holds up: the full path is reconstructible from the
    /// stored parent ids alone.
    #[tokio::test]
    async fn ou_hierarchy_is_reconstructible_with_a_recursive_query() {
        let pool = setup_pool().await;

        let ou_row = |id: &str, name: &str, parent: &str| {
            resources_row(
                OU_TYPE,
                id,
                name,
                "2026-07-17T00:00:00Z",
                &serde_json::json!({ "ParentId": parent }),
            )
        };

        // r-root > Prod > Workloads
        let output = fetch_output(
            Semantics::Authoritative,
            &[
                ou_row("ou-prod", "Prod", "r-root"),
                ou_row("ou-workloads", "Workloads", "ou-prod"),
            ],
        )
        .await;
        land_fetched_rows(&pool, &output).await.unwrap();

        let path: String = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                // The `->>` extractions are parenthesised deliberately: DuckDB
                // binds `->>` looser than `LIKE`, so
                // `configuration->>'ParentId' LIKE 'r-%'` chained after another
                // AND-ed comparison parses as an index lookup by a boolean and
                // fails with a cast error.
                c.query_row(
                    "WITH RECURSIVE ou(id, name, parent, path) AS (
                        SELECT resourceId, resourceName, (configuration->>'ParentId'),
                               '/' || resourceName
                        FROM resources
                        WHERE resourceType = 'AWS::Organizations::OrganizationalUnit'
                          AND (configuration->>'ParentId') LIKE 'r-%'
                        UNION ALL
                        SELECT r.resourceId, r.resourceName, (r.configuration->>'ParentId'),
                               ou.path || '/' || r.resourceName
                        FROM resources r
                        JOIN ou ON (r.configuration->>'ParentId') = ou.id
                        WHERE r.resourceType = 'AWS::Organizations::OrganizationalUnit'
                    )
                    SELECT path FROM ou WHERE id = 'ou-workloads'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();

        assert_eq!(path, "/Prod/Workloads");
    }
}
