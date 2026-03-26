// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use futures::{StreamExt, stream};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tracing::{debug, error, info};

use crate::config::{Config, GlobalSchemaAlteration, SchemaAlteration};
use crate::util;
use db_client::ConnectionPool;

pub async fn apply_schema_alterations(
    config: &Config,
    pool: &ConnectionPool,
) -> anyhow::Result<()> {
    apply_alterations(pool, config.alterations()).await?;

    let table_names: Vec<String> = match pool
        .get()
        .await?
        .with_conn(|c| {
            c.prepare_cached(r"SELECT resourceType FROM resourceTypes;")?
                .query_map([], |row| row.get::<_, String>(0))
                .map(|rows| {
                    rows.filter_map(std::result::Result::ok)
                        .map(|rt| util::resource_table_name(&rt))
                        .collect::<Vec<String>>()
                })
        })
        .await
    {
        Err(err) => {
            error!(%err, "failed to query resourceTypes for global schema alterations");
            return Err(anyhow!("TODO"));
        }
        Ok(names) => names,
    };

    apply_global_alterations(pool, config.global_alterations(), &table_names).await?;

    Ok(())
}

async fn apply_alterations<'a>(
    pool: &ConnectionPool,
    alterations: impl IntoIterator<Item = &'a SchemaAlteration>,
) -> anyhow::Result<()> {
    let names: Vec<String> = pool
        .get()
        .await?
        .with_conn(|c| {
            Ok(
                c.prepare("SELECT table_name FROM information_schema.tables;")?
                    .query_map([], |row| row.get::<_, String>(0))?
                    .map(|name| name.unwrap())
                    .collect(),
            )
        })
        .await?;

    let locks: HashMap<_, _> = names
        .into_iter()
        .map(|name| (name, Arc::new(Mutex::new(()))))
        .collect();

    let mut tasks = JoinSet::new();

    for alteration in alterations {
        let alteration = alteration.clone();

        let dep_locks: Vec<_> = alteration
            .dependencies
            .iter()
            .filter_map(|n| locks.get(n).cloned())
            .collect();

        if dep_locks.len() == alteration.dependencies.len() {
            let db = pool.get_owned().await?;
            tasks.spawn(async move {
                let _guards: Vec<_> = stream::iter(dep_locks)
                    .then(async |m| m.lock_owned().await)
                    .collect()
                    .await;

                let description = alteration
                    .description
                    .as_deref()
                    .unwrap_or("<unnamed>")
                    .to_string();

                if let Some(condition) = alteration.condition {
                    match db
                        .with_conn(move |c| {
                            c.query_row(&condition, [], |row| row.get::<_, bool>(0))
                        })
                        .await
                    {
                        Err(err) => {
                            error!(
                                description,
                                %err,
                                "failed to evaluate condition for schema alteration"
                            );
                            return;
                        }
                        Ok(false) => {
                            debug!(description, "skipping schema alteration: condition not met");
                            return;
                        }
                        Ok(true) => {}
                    }
                }

                match db
                    .with_conn(move |c| c.execute_batch(&alteration.sql))
                    .await
                {
                    Ok(()) => info!(description, "applied schema alteration"),
                    Err(err) => error!(
                        description,
                        %err,
                        "failed to apply schema alteration"
                    ),
                }
            });
        }
    }

    while let Some(result) = tasks.join_next().await {
        result?;
    }

    Ok(())
}

// TODO parallelisee this similarly to the normal schema alterations
async fn apply_global_alterations<'a>(
    pool: &ConnectionPool,
    alterations: impl IntoIterator<Item = &'a GlobalSchemaAlteration>,
    table_names: &[String],
) -> anyhow::Result<()> {
    let conn = pool.get().await?;
    for alteration in alterations {
        let description = alteration.description.as_deref().unwrap_or("<unnamed>");
        for table in table_names {
            if let Some(condition_template) = &alteration.condition {
                let condition_sql = condition_template.replace("{table}", table);
                let table_owned = table.clone();
                let desc_owned = description.to_string();
                match conn
                    .with_conn(move |c| {
                        c.query_row(&condition_sql, [], |row| row.get::<_, bool>(0))
                    })
                    .await
                {
                    Err(err) => {
                        error!(
                            table = table_owned.as_str(),
                            description = desc_owned.as_str(),
                            %err,
                            "failed to evaluate condition for global schema alteration"
                        );
                        continue;
                    }
                    Ok(false) => {
                        debug!(
                            table = table_owned.as_str(),
                            description = desc_owned.as_str(),
                            "skipping global schema alteration: condition not met"
                        );
                        continue;
                    }
                    Ok(true) => {}
                }
            }

            let sql = alteration.sql.replace("{table}", table);
            let table_owned = table.clone();
            let desc_owned = description.to_string();
            match conn.with_conn(move |c| c.execute_batch(&sql)).await {
                Ok(()) => info!(
                    table = table_owned.as_str(),
                    description = desc_owned.as_str(),
                    "applied global schema alteration"
                ),
                Err(err) => error!(
                    table = table_owned.as_str(),
                    description = desc_owned.as_str(),
                    %err,
                    "failed to apply global schema alteration"
                ),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use db_client::ConnectionPool;

    use super::*;
    use crate::{
        config::{GlobalSchemaAlteration, SchemaAlteration},
        db,
    };

    async fn make_pool() -> ConnectionPool {
        db::connect_to_db_in_memory().await.unwrap()
    }

    async fn make_table(pool: &ConnectionPool, name: impl ToString) {
        let name = name.to_string();
        pool.get()
            .await
            .unwrap()
            .with_conn(move |c| {
                c.execute_batch(&format!("CREATE TABLE {name} (id INTEGER, val VARCHAR)"))
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn config_alteration_skipped_when_dependency_missing() {
        let pool = make_pool().await;
        let alteration = SchemaAlteration {
            description: None,
            dependencies: vec!["nonexistent_table".to_string()],
            condition: None,
            sql: "ALTER TABLE nonexistent_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&pool, &[alteration]).await.unwrap();
    }

    #[tokio::test]
    async fn config_alteration_runs_when_dependencies_met() {
        let pool = make_pool().await;
        make_table(&pool, "my_table").await;
        let alteration = SchemaAlteration {
            description: None,
            dependencies: vec!["my_table".to_string()],
            condition: None,
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&pool, &[alteration]).await.unwrap();
        let count: i64 = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'my_table' AND column_name = 'foo'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn config_alteration_skipped_when_condition_false() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "my_table").await;
        let alteration = SchemaAlteration {
            description: None,
            dependencies: vec!["my_table".to_string()],
            condition: Some("SELECT false".to_string()),
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&pool, &[alteration]).await.unwrap();
        let count: i64 = conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'my_table' AND column_name = 'foo'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn config_alteration_runs_when_condition_true() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "my_table").await;
        let alteration = SchemaAlteration {
            description: None,
            dependencies: vec!["my_table".to_string()],
            condition: Some("SELECT true".to_string()),
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&pool, &[alteration]).await.unwrap();
        let count: i64 = conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'my_table' AND column_name = 'foo'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn config_global_alteration_applies_table_placeholder() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "tbl_a").await;
        make_table(&pool, "tbl_b").await;
        let alteration = GlobalSchemaAlteration {
            description: None,
            condition: None,
            sql: "ALTER TABLE {table} ADD COLUMN extra VARCHAR".to_string(),
        };
        apply_global_alterations(
            &pool,
            &[alteration],
            &["tbl_a".to_string(), "tbl_b".to_string()],
        )
        .await
        .unwrap();
        for tbl in &["tbl_a", "tbl_b"] {
            let tbl_owned = (*tbl).to_string();
            let count: i64 = conn
                .with_conn(move |c| {
                    c.query_row(
                        &format!(
                            "SELECT count(*) FROM information_schema.columns \
                             WHERE table_name = '{tbl_owned}' AND column_name = 'extra'"
                        ),
                        [],
                        |row| row.get(0),
                    )
                })
                .await
                .unwrap();
            assert_eq!(count, 1, "column not added to {tbl}");
        }
    }

    #[tokio::test]
    async fn alteration_with_no_dependencies_always_runs() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "my_table").await;
        let alteration = SchemaAlteration {
            description: None,
            dependencies: vec![],
            condition: None,
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&pool, &[alteration]).await.unwrap();
        let count: i64 = conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'my_table' AND column_name = 'foo'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn alteration_skipped_when_one_of_multiple_dependencies_missing() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "tbl_present").await;
        let alteration = SchemaAlteration {
            description: None,
            dependencies: vec!["tbl_present".to_string(), "tbl_absent".to_string()],
            condition: None,
            sql: "ALTER TABLE tbl_present ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&pool, &[alteration]).await.unwrap();
        let count: i64 = conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'tbl_present' AND column_name = 'foo'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn global_alteration_runs_when_condition_true() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "tbl_a").await;
        let alteration = GlobalSchemaAlteration {
            description: None,
            condition: Some("SELECT true".to_string()),
            sql: "ALTER TABLE {table} ADD COLUMN extra VARCHAR".to_string(),
        };
        apply_global_alterations(&pool, &[alteration], &["tbl_a".to_string()])
            .await
            .unwrap();
        let count: i64 = conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'tbl_a' AND column_name = 'extra'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn config_global_alteration_skipped_when_condition_false() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        make_table(&pool, "tbl_a").await;
        let alteration = GlobalSchemaAlteration {
            description: None,
            condition: Some("SELECT false".to_string()),
            sql: "ALTER TABLE {table} ADD COLUMN extra VARCHAR".to_string(),
        };
        apply_global_alterations(&pool, &[alteration], &["tbl_a".to_string()])
            .await
            .unwrap();
        let count: i64 = conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) FROM information_schema.columns \
                     WHERE table_name = 'tbl_a' AND column_name = 'extra'",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
