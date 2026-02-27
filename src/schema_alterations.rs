// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use tracing::{debug, error, info};

use crate::config::{Config, ConfigGlobalSchemaAlteration, ConfigSchemaAlteration};
use crate::util;

pub fn apply_schema_alterations(config: &Config, db_conn: &duckdb::Connection) {
    apply_alterations(db_conn, config.alterations_iter());

    let table_names: Vec<String> =
        match db_conn.prepare_cached(r"SELECT resourceType FROM resourceTypes;") {
            Err(err) => {
                error!(%err, "failed to resourceTypes for global schema alterations");
                return;
            }
            Ok(mut stmt) => stmt
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .filter_map(std::result::Result::ok)
                .map(|rt| util::resource_table_name(&rt))
                .collect(),
        };

    apply_global_alterations(db_conn, config.global_alterations_iter(), &table_names);
}

fn apply_alterations<'a>(
    db_conn: &duckdb::Connection,
    alterations: impl Iterator<Item = &'a ConfigSchemaAlteration>,
) {
    for alteration in alterations {
        let description = alteration.description.as_deref().unwrap_or("<unnamed>");
        let all_present = alteration.dependencies.iter().all(|table| {
            match db_conn.query_row(
                "SELECT count(*) > 0 FROM information_schema.tables
                 WHERE table_name = ? AND table_type = 'BASE TABLE';",
                [table.as_str()],
                |row| row.get::<_, bool>(0),
            ) {
                Err(err) => {
                    error!(
                        table,
                        description, %err,
                        "failed to check table existence for schema alteration"
                    );
                    false
                }
                Ok(false) => {
                    debug!(
                        table,
                        description, "skipping schema alteration: required table does not exist"
                    );
                    false
                }
                Ok(_) => true,
            }
        });

        if !all_present {
            continue;
        }

        if let Some(condition) = &alteration.condition {
            match db_conn.query_row(condition, [], |row| row.get::<_, bool>(0)) {
                Ok(false) => {
                    debug!(description, "skipping schema alteration: condition not met");
                    continue;
                }
                Err(err) => {
                    error!(
                        description, %err,
                        "failed to evaluate condition for schema alteration"
                    );
                    continue;
                }
                Ok(_) => {}
            }
        }

        match db_conn.execute_batch(&alteration.sql) {
            Ok(()) => info!(description, "applied schema alteration"),
            Err(err) => error!(
                description, %err,
                "failed to apply schema alteration"
            ),
        }
    }
}

fn apply_global_alterations<'a>(
    db_conn: &duckdb::Connection,
    alterations: impl Iterator<Item = &'a ConfigGlobalSchemaAlteration>,
    table_names: &[String],
) {
    for alteration in alterations {
        let description = alteration.description.as_deref().unwrap_or("<unnamed>");
        for table in table_names {
            if let Some(condition_template) = &alteration.condition {
                let condition_sql = condition_template.replace("{table}", table);
                match db_conn.query_row(&condition_sql, [], |row| row.get::<_, bool>(0)) {
                    Ok(false) => {
                        debug!(
                            table,
                            description, "skipping global schema alteration: condition not met"
                        );
                        continue;
                    }
                    Err(err) => {
                        error!(
                            table,
                            description, %err,
                            "failed to evaluate condition for global schema alteration"
                        );
                        continue;
                    }
                    Ok(_) => {}
                }
            }

            let sql = alteration.sql.replace("{table}", table);
            match db_conn.execute_batch(&sql) {
                Ok(()) => info!(table, description, "applied global schema alteration"),
                Err(err) => error!(
                    table,
                    description, %err,
                    "failed to apply global schema alteration"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConfigGlobalSchemaAlteration, ConfigSchemaAlteration};

    fn make_conn() -> duckdb::Connection {
        duckdb::Connection::open_in_memory().unwrap()
    }

    fn make_table(conn: &duckdb::Connection, name: &str) {
        conn.execute_batch(&format!("CREATE TABLE {name} (id INTEGER, val VARCHAR)"))
            .unwrap();
    }

    #[test]
    fn config_alteration_skipped_when_dependency_missing() {
        let conn = make_conn();
        let alteration = ConfigSchemaAlteration {
            description: None,
            dependencies: vec!["nonexistent_table".to_string()],
            condition: None,
            sql: "ALTER TABLE nonexistent_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&conn, [alteration].iter());
    }

    #[test]
    fn config_alteration_runs_when_dependencies_met() {
        let conn = make_conn();
        make_table(&conn, "my_table");
        let alteration = ConfigSchemaAlteration {
            description: None,
            dependencies: vec!["my_table".to_string()],
            condition: None,
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&conn, [alteration].iter());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'my_table' AND column_name = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn config_alteration_skipped_when_condition_false() {
        let conn = make_conn();
        make_table(&conn, "my_table");
        let alteration = ConfigSchemaAlteration {
            description: None,
            dependencies: vec!["my_table".to_string()],
            condition: Some("SELECT false".to_string()),
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&conn, [alteration].iter());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'my_table' AND column_name = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn config_alteration_runs_when_condition_true() {
        let conn = make_conn();
        make_table(&conn, "my_table");
        let alteration = ConfigSchemaAlteration {
            description: None,
            dependencies: vec!["my_table".to_string()],
            condition: Some("SELECT true".to_string()),
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&conn, [alteration].iter());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'my_table' AND column_name = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn config_global_alteration_applies_table_placeholder() {
        let conn = make_conn();
        make_table(&conn, "tbl_a");
        make_table(&conn, "tbl_b");
        let alteration = ConfigGlobalSchemaAlteration {
            description: None,
            condition: None,
            sql: "ALTER TABLE {table} ADD COLUMN extra VARCHAR".to_string(),
        };
        apply_global_alterations(
            &conn,
            [alteration].iter(),
            &["tbl_a".to_string(), "tbl_b".to_string()],
        );
        for tbl in &["tbl_a", "tbl_b"] {
            let count: i64 = conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM information_schema.columns
                         WHERE table_name = '{tbl}' AND column_name = 'extra'"
                    ),
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            assert_eq!(count, 1, "column not added to {tbl}");
        }
    }

    #[test]
    fn alteration_with_no_dependencies_always_runs() {
        let conn = make_conn();
        make_table(&conn, "my_table");
        let alteration = ConfigSchemaAlteration {
            description: None,
            dependencies: vec![],
            condition: None,
            sql: "ALTER TABLE my_table ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&conn, [alteration].iter());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'my_table' AND column_name = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn alteration_skipped_when_one_of_multiple_dependencies_missing() {
        let conn = make_conn();
        make_table(&conn, "tbl_present");
        let alteration = ConfigSchemaAlteration {
            description: None,
            dependencies: vec!["tbl_present".to_string(), "tbl_absent".to_string()],
            condition: None,
            sql: "ALTER TABLE tbl_present ADD COLUMN foo VARCHAR".to_string(),
        };
        apply_alterations(&conn, [alteration].iter());
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'tbl_present' AND column_name = 'foo'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn global_alteration_runs_when_condition_true() {
        let conn = make_conn();
        make_table(&conn, "tbl_a");
        let alteration = ConfigGlobalSchemaAlteration {
            description: None,
            condition: Some("SELECT true".to_string()),
            sql: "ALTER TABLE {table} ADD COLUMN extra VARCHAR".to_string(),
        };
        apply_global_alterations(&conn, [alteration].iter(), &["tbl_a".to_string()]);
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'tbl_a' AND column_name = 'extra'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn config_global_alteration_skipped_when_condition_false() {
        let conn = make_conn();
        make_table(&conn, "tbl_a");
        let alteration = ConfigGlobalSchemaAlteration {
            description: None,
            condition: Some("SELECT false".to_string()),
            sql: "ALTER TABLE {table} ADD COLUMN extra VARCHAR".to_string(),
        };
        apply_global_alterations(&conn, [alteration].iter(), &["tbl_a".to_string()]);
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.columns
                 WHERE table_name = 'tbl_a' AND column_name = 'extra'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}
