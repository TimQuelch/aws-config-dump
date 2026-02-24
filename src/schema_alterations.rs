// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use tracing::{debug, error, info};

use crate::util;

struct Alteration {
    required_tables: &'static [&'static str],
    description: &'static str,
    sql: &'static str,
}

static ALTERATIONS: &[Alteration] = &[Alteration {
    required_tables: &["ec2_instance"],
    description: "extract ec2_instance state name and reason",
    sql:
        "ALTER TABLE ec2_instance ALTER COLUMN state TYPE varchar USING state.name;
          ALTER TABLE ec2_instance ALTER COLUMN stateReason TYPE varchar USING stateReason.message;",
}];

struct GlobalAlteration {
    description: &'static str,
    condition: fn(&str) -> String,
    sql: fn(&str) -> String,
}

static GLOBAL_ALTERATIONS: &[GlobalAlteration] = &[GlobalAlteration {
    description: "add tagName column from tags['Name']",
    condition: |table| {
        format!("SELECT count(*) > 0 FROM \"{table}\" WHERE tags['Name'] IS NOT NULL")
    },
    sql: |table| {
        format!(
            "ALTER TABLE \"{table}\" ADD COLUMN IF NOT EXISTS tagName VARCHAR;
             UPDATE \"{table}\" SET tagName = tags['Name'];"
        )
    },
}];

pub fn apply_schema_alterations(db_conn: &duckdb::Connection) {
    for alteration in ALTERATIONS {
        let all_present = alteration.required_tables.iter().all(|table| {
            match db_conn.query_row(
                "SELECT count(*) > 0 FROM information_schema.tables
                 WHERE table_name = ? AND table_type = 'BASE TABLE';",
                [table],
                |row| row.get::<_, bool>(0),
            ) {
                Err(err) => {
                    error!(
                        table,
                        description = alteration.description,
                        %err,
                        "failed to check table existence for schema alteration"
                    );
                    false
                }
                Ok(false) => {
                    debug!(
                        table,
                        description = alteration.description,
                        "skipping schema alteration: required table does not exist"
                    );
                    false
                }
                Ok(_) => true,
            }
        });

        if !all_present {
            continue;
        }

        match db_conn.execute_batch(alteration.sql) {
            Ok(()) => info!(
                description = alteration.description,
                "applied schema alteration"
            ),
            Err(err) => error!(
                description = alteration.description,
                %err,
                "failed to apply schema alteration"
            ),
        }
    }

    let table_names: Vec<String> =
        match db_conn.prepare_cached("SELECT resourceType FROM resourceTypes;") {
            Err(err) => {
                error!(%err, "failed to query resourceTypes for global schema alterations");
                return;
            }
            Ok(mut stmt) => stmt
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .filter_map(std::result::Result::ok)
                .map(|rt| util::resource_table_name(&rt))
                .collect(),
        };

    for alteration in GLOBAL_ALTERATIONS {
        for table in &table_names {
            let condition_sql = (alteration.condition)(table);
            let condition_result: bool =
                match db_conn.query_row(&condition_sql, [], |row| row.get(0)) {
                    Ok(n) => n,
                    Err(err) => {
                        error!(
                            table,
                            description = alteration.description,
                            %err,
                            "failed to evaluate condition for global schema alteration"
                        );
                        continue;
                    }
                };

            if !condition_result {
                debug!(
                    table,
                    description = alteration.description,
                    "skipping global schema alteration: condition not met"
                );
                continue;
            }

            let sql = (alteration.sql)(table);
            match db_conn.execute_batch(&sql) {
                Ok(()) => info!(
                    table,
                    description = alteration.description,
                    "applied global schema alteration"
                ),
                Err(err) => error!(
                    table,
                    description = alteration.description,
                    %err,
                    "failed to apply global schema alteration"
                ),
            }
        }
    }
}
