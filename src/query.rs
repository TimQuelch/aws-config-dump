// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::HashMap, os::unix::process::CommandExt, process::Command};

use aws_sdk_config::types::ResourceType;
use duckdb::Connection;

use crate::config::{self, Config};
use crate::util;

enum FieldSelection {
    All,
    Default(Option<ResourceType>),
    Custom(Vec<String>),
}

#[derive(Clone, Copy, Default)]
struct ColumnOptions {
    has_account_name: bool,
    has_tagname: bool,
}

fn build_query(
    resource_type: Option<&str>,
    accounts: Option<&[String]>,
    fields: Option<Vec<String>>,
    all_fields: bool,
    user_query: &str,
    column_options: ColumnOptions,
    extra_columns: &HashMap<String, Vec<String>>,
) -> String {
    let table = resource_type.map_or_else(|| "resources".to_string(), util::resource_table_name);
    let field_selection = if all_fields {
        FieldSelection::All
    } else {
        fields.map_or_else(
            || FieldSelection::Default(resource_type.map(Into::into)),
            FieldSelection::Custom,
        )
    };
    let columns = column_selection(field_selection, column_options, extra_columns);
    let account_clause = accounts
        .filter(|a| !a.is_empty())
        .map_or_else(String::new, |accounts| {
            let values = accounts
                .iter()
                .map(|a| format!("'{a}'"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("WHERE accountId IN ({values}) OR accountName IN ({values})")
        });
    format!(
        "CREATE OR REPLACE TEMPORARY VIEW input AS
            SELECT {columns} FROM query_table('{table}')
            LEFT JOIN accounts USING (accountId)
            {account_clause};
        {user_query};",
    )
}

fn has_account_names(db_conn: &Connection) -> bool {
    db_conn
        .query_row(
            "SELECT count(*) > 0 FROM accounts WHERE accountName IS NOT NULL;",
            [],
            |row| row.get(0),
        )
        .unwrap_or(false)
}

fn table_has_tagname(db_conn: &Connection, table: &str) -> bool {
    db_conn
        .query_row(
            "SELECT count(*) > 0 FROM information_schema.columns
             WHERE table_name = ? AND column_name = 'tagName';",
            [table],
            |row| row.get::<_, bool>(0),
        )
        .unwrap_or(false)
}

/// Query the database
///
/// Calls the duckdb CLI instead of using the SDK so we don't need to implement TSV formatting here
pub async fn query(
    config: &Config,
    resource_type: Option<&str>,
    accounts: Option<&[String]>,
    fields: Option<Vec<String>>,
    all_fields: bool,
    query: &str,
) -> anyhow::Result<()> {
    let table = resource_type.map_or_else(|| "resources".to_string(), util::resource_table_name);
    let (has_account_name, has_tagname) =
        if let Ok(db_conn) = Connection::open(config::db_path().await) {
            (
                has_account_names(&db_conn),
                table_has_tagname(&db_conn, &table),
            )
        } else {
            (false, false)
        };
    let final_query = build_query(
        resource_type,
        accounts,
        fields,
        all_fields,
        query,
        ColumnOptions {
            has_account_name,
            has_tagname,
        },
        &config.query_extra_columns,
    );

    Command::new("duckdb")
        .args(["-readonly", "-safe", "-cmd", ".mode tabs"])
        .arg(config::db_path().await)
        .arg(&final_query)
        .spawn()?
        .wait()?;

    Ok(())
}

/// Open an interactive `DuckDB` REPL against the local database
pub async fn repl() -> anyhow::Result<()> {
    let err = Command::new("duckdb").arg(config::db_path().await).exec();
    Err(anyhow::Error::from(err))
}

const DEFAULT_COLUMNS: &str = "resourceType, accountId, awsRegion, resourceId, resourceName";
const BASE_RESOURCE_DEFAULT_COLUMNS: &str = "accountId, awsRegion, resourceId";

fn column_selection(
    fields: FieldSelection,
    column_options: ColumnOptions,
    extra_columns: &HashMap<String, Vec<String>>,
) -> String {
    match fields {
        FieldSelection::All => "*".to_string(),
        FieldSelection::Custom(fields) => fields.join(","),
        FieldSelection::Default(None) => DEFAULT_COLUMNS.replacen(
            "accountId",
            "accountId, accountName",
            usize::from(column_options.has_account_name),
        ),
        FieldSelection::Default(Some(resource_type)) => {
            resource_default_columns(&resource_type, column_options.has_tagname, extra_columns)
                .replacen(
                    "accountId",
                    "accountId, accountName",
                    usize::from(column_options.has_account_name),
                )
        }
    }
}

fn resource_default_columns(
    resource_type: &ResourceType,
    has_tagname: bool,
    extra_columns: &HashMap<String, Vec<String>>,
) -> String {
    let table_name = util::resource_table_name(resource_type.as_str());
    let tagname = if has_tagname { ",tagName" } else { "" };
    if let Some(cols) = extra_columns.get(&table_name) {
        return format!(
            "{BASE_RESOURCE_DEFAULT_COLUMNS}{tagname},{}",
            cols.join(",")
        );
    }
    let built_in = match resource_type {
        ResourceType::Role => ",arn",
        ResourceType::Vpc => ",cidrBlock",
        ResourceType::Subnet => ",vpcId,cidrBlock,availabilityZone,availableIpAddressCount",
        _ => "",
    };
    format!("{BASE_RESOURCE_DEFAULT_COLUMNS}{tagname}{built_in}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn no_resource_type_uses_resources_table() {
        let q = build_query(
            None,
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("query_table('resources')"));
    }

    #[test]
    fn resource_type_maps_to_table_name() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("query_table('ec2_instance')"));
    }

    #[test]
    fn account_join_always_present() {
        let q = build_query(
            None,
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("LEFT JOIN accounts USING (accountId)"));
    }

    #[test]
    fn account_filter_generates_where_clause() {
        let q = build_query(
            None,
            Some(&["123456789012".to_string()]),
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(
            q.contains("WHERE accountId IN ('123456789012') OR accountName IN ('123456789012')")
        );
    }

    #[test]
    fn multiple_account_filters_generate_where_clause() {
        let q = build_query(
            None,
            Some(&["123456789012".to_string(), "prod".to_string()]),
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains(
            "WHERE accountId IN ('123456789012', 'prod') OR accountName IN ('123456789012', 'prod')"
        ));
    }

    #[test]
    fn no_account_filter_no_where_clause() {
        let q = build_query(
            None,
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn empty_account_filter_no_where_clause() {
        let q = build_query(
            None,
            Some(&[]),
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn user_query_included() {
        let q = build_query(
            None,
            None,
            None,
            false,
            "SELECT count(*) FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("SELECT count(*) FROM input;"));
    }

    #[test]
    fn all_fields_selects_star() {
        let q = build_query(
            None,
            None,
            None,
            true,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("SELECT * FROM query_table("));
    }

    #[test]
    fn all_fields_include_account_name_has_no_effect() {
        let a = build_query(
            None,
            None,
            None,
            true,
            "SELECT * FROM input",
            ColumnOptions {
                has_account_name: true,
                ..Default::default()
            },
            &HashMap::new(),
        );
        let b = build_query(
            None,
            None,
            None,
            true,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn custom_fields_as_select_list() {
        let fields = vec!["resourceId".to_string(), "awsRegion".to_string()];
        let q = build_query(
            None,
            None,
            Some(fields),
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("SELECT resourceId,awsRegion FROM"));
    }

    #[test]
    fn custom_fields_can_include_account_name() {
        let fields = vec!["resourceId".to_string(), "accountName".to_string()];
        let q = build_query(
            None,
            None,
            Some(fields),
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("SELECT resourceId,accountName FROM"));
    }

    #[test]
    fn custom_fields_include_account_name_no_effect() {
        let fields = vec!["resourceId".to_string()];
        let a = build_query(
            None,
            None,
            Some(fields.clone()),
            false,
            "SELECT * FROM input",
            ColumnOptions {
                has_account_name: true,
                ..Default::default()
            },
            &HashMap::new(),
        );
        let b = build_query(
            None,
            None,
            Some(fields),
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn default_columns_no_resource_type() {
        let q = build_query(
            None,
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains(&format!("SELECT {DEFAULT_COLUMNS} FROM")));
    }

    #[test]
    fn default_columns_no_resource_type_with_account_name() {
        let q = build_query(
            None,
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions {
                has_account_name: true,
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(q.contains(
            "SELECT resourceType, accountId, accountName, awsRegion, resourceId, resourceName FROM",
        ));
    }

    #[test]
    fn default_columns_unknown_resource_type() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS} FROM query_table"
        )));
    }

    #[test]
    fn default_columns_unknown_resource_type_with_account_name() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions {
                has_account_name: true,
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, accountName, awsRegion, resourceId FROM"));
    }

    #[test]
    fn default_columns_role() {
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains(&format!("SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},arn FROM")));
    }

    #[test]
    fn default_columns_role_with_account_name() {
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions {
                has_account_name: true,
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, accountName, awsRegion, resourceId,arn FROM"));
    }

    #[test]
    fn default_columns_vpc() {
        let q = build_query(
            Some("AWS::EC2::VPC"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},cidrBlock FROM"
        )));
    }

    #[test]
    fn default_columns_subnet() {
        let q = build_query(
            Some("AWS::EC2::Subnet"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},vpcId,cidrBlock,availabilityZone,availableIpAddressCount FROM"
        )));
    }

    #[test]
    fn resource_type_and_account_filter_combined() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            Some(&["123456789012".to_string()]),
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &HashMap::new(),
        );
        assert!(q.contains("query_table('ec2_instance')"));
        assert!(
            q.contains("WHERE accountId IN ('123456789012') OR accountName IN ('123456789012')")
        );
    }

    #[test]
    fn config_extra_columns_override_built_in_for_known_type() {
        let extra = HashMap::from([(
            "iam_role".to_string(),
            vec!["roleName".to_string(), "arn".to_string()],
        )]);
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &extra,
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},roleName,arn FROM"
        )));
    }

    #[test]
    fn config_extra_columns_used_for_type_with_no_built_in_columns() {
        let extra = HashMap::from([("ec2_instance".to_string(), vec!["instanceType".to_string()])]);
        let q = build_query(
            Some("AWS::EC2::Instance"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &extra,
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},instanceType FROM"
        )));
    }

    #[test]
    fn absent_from_config_falls_back_to_built_in_columns() {
        let extra = HashMap::new();
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions::default(),
            &extra,
        );
        assert!(q.contains(&format!("SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},arn FROM")));
    }

    #[test]
    fn tagname_column_appended_when_present() {
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions {
                has_tagname: true,
                ..Default::default()
            },
            &HashMap::new(),
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},tagName,arn FROM"
        )));
    }

    #[test]
    fn tagname_column_appended_with_extra_columns() {
        let extra = HashMap::from([(
            "iam_role".to_string(),
            vec!["roleName".to_string(), "arn".to_string()],
        )]);
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
            ColumnOptions {
                has_tagname: true,
                ..Default::default()
            },
            &extra,
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},tagName,roleName,arn FROM"
        )));
    }
}
