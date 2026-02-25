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

static ALTERATIONS: &[Alteration] = &[
    Alteration {
        required_tables: &["ec2_instance"],
        description: "extract ec2_instance state name and reason",
        sql:
            "ALTER TABLE ec2_instance ALTER COLUMN state TYPE varchar USING struct_extract(state, 'name');
             ALTER TABLE ec2_instance ALTER COLUMN stateReason TYPE varchar USING struct_extract(stateReason, 'message');",
    },
    Alteration {
        required_tables: &["ssm_managedinstanceinventory"],
        description: "transform ssm_managedinstanceinventory",
        sql: r#"
            ALTER TABLE ssm_managedinstanceinventory ADD COLUMN IF NOT EXISTS applications STRUCT(
                ApplicationType VARCHAR,
                InstalledTime TIMESTAMP,
                Architecture VARCHAR,
                "Version" VARCHAR,
                Summary VARCHAR,
                PackageId VARCHAR,
                Publisher VARCHAR,
                "Release" VARCHAR,
                URL VARCHAR,
                "Name" VARCHAR,
                Epoch VARCHAR
            )[];
            UPDATE ssm_managedinstanceinventory SET applications = list_transform(
                list_concat(
                    list_filter(map_values("AWS:Application".Content), lambda x: json_type(x) == 'OBJECT'),
                    flatten(
                        list_transform(
                            list_filter(map_values("AWS:Application".Content), lambda x: json_type(x) == 'ARRAY'),
                            lambda x: x->'$[*]'
                        )
                    )
                ),
                lambda x: json_transform(x, '
                    {
                        "ApplicationType": "VARCHAR",
                        "InstalledTime": "TIMESTAMP",
                        "Architecture": "VARCHAR",
                        "Version": "VARCHAR",
                        "Summary": "VARCHAR",
                        "PackageId": "VARCHAR",
                        "Publisher": "VARCHAR",
                        "Release": "VARCHAR",
                        "URL": "VARCHAR",
                        "Name": "VARCHAR",
                        "Epoch": "VARCHAR"
                    }'
                )
            );

            ALTER TABLE ssm_managedinstanceinventory ADD COLUMN IF NOT EXISTS windowsUpdates MAP(
                VARCHAR, STRUCT(installedtime TIMESTAMP, description VARCHAR, hotfixid VARCHAR, installedby VARCHAR)
            );
            UPDATE ssm_managedinstanceinventory SET windowsUpdates = map_from_entries(list_filter(
                map_entries(CAST(
                    "AWS:WindowsUpdate".Content AS
                    MAP(VARCHAR, struct(installedtime TIMESTAMP, description VARCHAR, hotfixid VARCHAR, installedby VARCHAR))
                )),
                lambda x: x.value IS NOT NULL
            ));

            CREATE OR REPLACE TABLE ssm_managedinstanceinventory AS
                SELECT
                    * EXCLUDE("AWS:Application", "AWS:InstanceInformation", "AWS:WindowsUpdate"),
                    unnest("AWS:InstanceInformation".Content[resourceId])
                FROM ssm_managedinstanceinventory;"#
    },
    Alteration {
        required_tables: &["ssm_patchcompliance"],
        description: "transform ssm_patchcompliance",
        sql: r#"
            ALTER TABLE ssm_patchcompliance ADD COLUMN IF NOT EXISTS complianceSummary STRUCT(
                PatchBaselineId VARCHAR,
                PatchGroup VARCHAR,
                Status VARCHAR,
                OverallSeverity VARCHAR,
                NonCompliantCriticalCount INT,
                NonCompliantHighCount INT,
                NonCompliantMediumCount INT,
                NonCompliantLowCount INT,
                NonCompliantInformationalCount INT,
                NonCompliantUnspecifiedCount INT,
                CompliantCriticalCount INT,
                CompliantHighCount INT,
                CompliantMediumCount INT,
                CompliantLowCount INT,
                CompliantInformationalCount INT,
                CompliantUnspecifiedCount INT
            );
            UPDATE ssm_patchcompliance SET complianceSummary = "AWS:ComplianceItem".Content.Patch['ComplianceSummary'];

            ALTER TABLE ssm_patchcompliance ADD COLUMN IF NOT EXISTS patches MAP(
                VARCHAR,
                STRUCT(
                    Id VARCHAR,
                    Title VARCHAR,
                    Status VARCHAR,
                    InstalledTime TIMESTAMPTZ,
                    Severity VARCHAR,
                    PatchSeverity VARCHAR,
                    Classification VARCHAR,
                    PatchState VARCHAR,
                    PatchBaselineId VARCHAR,
                    PatchGroup VARCHAR,
                    CVEIds VARCHAR[]
                )
            );
            UPDATE ssm_patchcompliance SET patches = map_from_entries(
                list_transform(
                    list_filter(
                        map_entries("AWS:ComplianceItem".Content.Patch),
                        lambda kv: kv.key != 'ComplianceSummary'
                    ),
                    lambda kv: struct_update(kv, value := struct_update(kv.value,
                        Title := nullif(kv.value.Title, ''),
                        Status := nullif(kv.value.Status, ''),
                        InstalledTime := nullif(kv.value.InstalledTime, ''),
                        PatchSeverity := nullif(kv.value.PatchSeverity, ''),
                        Classification := nullif(kv.value.Classification, ''),
                        PatchGroup := nullif(kv.value.PatchGroup, ''),
                        CVEIds := split(kv.value.CVEIds, ',')
                    ))
                )
            );
            ALTER TABLE ssm_patchcompliance DROP COLUMN "AWS:ComplianceItem";"#
    },
];

struct GlobalAlteration {
    description: &'static str,
    condition: fn(&str) -> String,
    sql: fn(&str) -> String,
}

static GLOBAL_ALTERATIONS: &[GlobalAlteration] = &[GlobalAlteration {
    description: "add tagName column from tags['Name']",
    condition: |table| {
        format!(r#"SELECT count(*) > 0 FROM "{table}" WHERE tags['Name'] IS NOT NULL"#)
    },
    sql: |table| {
        format!(
            r#"ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS tagName VARCHAR;
               UPDATE "{table}" SET tagName = tags['Name'];"#
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
