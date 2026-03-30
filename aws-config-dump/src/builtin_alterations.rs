// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::sync::LazyLock;

use crate::config::{GlobalSchemaAlteration, SchemaAlteration};

pub static ALTERATIONS: LazyLock<Vec<SchemaAlteration>> = LazyLock::new(|| {
    vec![
        SchemaAlteration {
            description: Some("extract ec2_instance state name and reason".to_string()),
            dependencies: vec!["ec2_instance".to_string()],
            condition: None,
            sql: "
                ALTER TABLE ec2_instance ALTER COLUMN state TYPE varchar USING struct_extract(state, 'name');
                ALTER TABLE ec2_instance ALTER COLUMN stateReason TYPE varchar USING struct_extract(stateReason, 'message');"
                .to_string(),
        },
        SchemaAlteration {
            description: Some("transform ssm_managedinstanceinventory".to_string()),
            dependencies: vec!["ssm_managedinstanceinventory".to_string()],
            condition: None,
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
                .to_string(),
        },
        SchemaAlteration {
            description: Some("transform ssm_patchcompliance".to_string()),
            dependencies: vec!["ssm_patchcompliance".to_string()],
            condition: None,
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
                    CompliantUnspecifiedCount INT,
                    ExecutionType VARCHAR,
                    ComplianceType VARCHAR
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
                        list_transform(
                            list_filter(
                                map_entries("AWS:ComplianceItem".Content.Patch),
                                lambda kv: kv.key != 'ComplianceSummary'
                            ),
                            lambda kv: struct_update(
                                kv,
                                value := json_transform(
                                    kv.value::JSON,
                                    struct_pack(
                                        Id := 'VARCHAR',
                                        Title := 'VARCHAR',
                                        Status := 'VARCHAR',
                                        InstalledTime := 'TIMESTAMPTZ',
                                        Severity := 'VARCHAR',
                                        PatchSeverity := 'VARCHAR',
                                        Classification := 'VARCHAR',
                                        PatchState := 'VARCHAR',
                                        PatchBaselineId := 'VARCHAR',
                                        PatchGroup := 'VARCHAR',
                                        CVEIds := 'VARCHAR'
                                    )::JSON
                                )
                            )
                        ),
                        lambda kv: struct_update(kv, value := struct_update(
                            kv.value,
                            Title := nullif(kv.value.Title, ''),
                            Status := nullif(kv.value.Status, ''),
                            PatchSeverity := nullif(kv.value.PatchSeverity, ''),
                            Classification := nullif(kv.value.Classification, ''),
                            PatchGroup := nullif(kv.value.PatchGroup, ''),
                            CVEIds := split(kv.value.CVEIds, ',')
                        ))
                    )
                );
                ALTER TABLE ssm_patchcompliance DROP COLUMN "AWS:ComplianceItem";"#
                .to_string(),
        },
        SchemaAlteration {
            description: Some("transform sns_topic".to_string()),
            dependencies: vec!["sns_topic".to_string()],
            condition: None,
            sql: r"
                ALTER TABLE sns_topic ALTER COLUMN Policy TYPE JSON;
                ALTER TABLE sns_topic DROP COLUMN TopicArn;
                ALTER TABLE sns_topic DROP COLUMN Tags_1;
                ALTER TABLE sns_topic ALTER COLUMN EffectiveDeliveryPolicy TYPE STRUCT(
                    http STRUCT(
                        defaultHealthyRetryPolicy STRUCT(
                            minDelayTarget INT,
                            maxDelayTarget INT,
                            numRetries INT,
                            numMaxDelayRetries INT,
                            numNoDelayRetries INT,
                            numMinDelayRetries INT,
                            backoffFunction VARCHAR
                        ),
                        disableSubscriptionOverrides BOOLEAN,
                        defaultRequestPolicy STRUCT(
                            headerContentType VARCHAR
                        )
                    )
                );
            ".to_string(),
        },
        SchemaAlteration {
            description: Some("parse organizations_policy json content".to_string()),
            dependencies: vec!["organizations_policy".to_string()],
            condition: None,
            sql: r"
                ALTER TABLE organizations_policy ALTER COLUMN Content TYPE JSON;
                ALTER TABLE organizations_policy DROP COLUMN Arn_1;
            ".to_string(),
        },
    ]
});

pub static GLOBAL_ALTERATIONS: LazyLock<Vec<GlobalSchemaAlteration>> = LazyLock::new(|| {
    vec![
        GlobalSchemaAlteration {
            description: Some("add tagName column from tags['Name']".to_string()),
            condition: Some(
                r#"SELECT count(*) > 0 FROM "{table}" WHERE tags['Name'] IS NOT NULL"#.to_string(),
            ),
            sql: r#"
                ALTER TABLE "{table}" ADD COLUMN IF NOT EXISTS tagName VARCHAR;
                UPDATE "{table}" SET tagName = tags['Name'];
                UPDATE "{table}" SET resourceName = coalesce(resourceName, tagName);
            "#
            .to_string(),
        },
        // GlobalSchemaAlteration {
        //     description: Some("remove all null columns".to_string()),
        //     condition: None,
        //     sql: r#"
        //     CREATE TEMPORARY MACRO IF NOT EXISTS notnullcols(tablename) AS (
        //         FROM (
        //             UNPIVOT (FROM query_table(tablename) SELECT MAX(COLUMNS(*)))
        //             ON COLUMNS(*)::VARCHAR
        //         )
        //         SELECT list(name)
        //     );
        //     SET VARIABLE notnullcols = notnullcols('{table}');
        //     CREATE OR REPLACE TABLE "{table}" AS
        //         FROM "{table}" SELECT columns(
        //             lambda c: list_contains(getvariable('notnullcols'), c)
        //         );"#
        //     .to_string(),
        // },
    ]
});
