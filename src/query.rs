// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::process::Command;

use aws_sdk_config::types::ResourceType;

use crate::config::Config;
use crate::util;

enum FieldSelection {
    All,
    Default(Option<ResourceType>),
    Custom(Vec<String>),
}

fn build_query(
    resource_type: Option<&str>,
    account: Option<&str>,
    fields: Option<Vec<String>>,
    all_fields: bool,
    user_query: &str,
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
    let columns = column_selection(field_selection);
    let account_clause = account.map_or_else(String::new, |account| {
        format!("WHERE accountId == {account}")
    });
    format!(
        "CREATE OR REPLACE TEMPORARY VIEW input AS
            SELECT {columns} FROM query_table('{table}') {account_clause};
        {user_query};",
    )
}

/// Query the database
///
/// Calls the duckdb CLI instead of using the SDK so we don't need to implement TSV formatting here
pub fn query(
    resource_type: Option<&str>,
    account: Option<&str>,
    fields: Option<Vec<String>>,
    all_fields: bool,
    query: &str,
) -> anyhow::Result<()> {
    let final_query = build_query(resource_type, account, fields, all_fields, query);

    Command::new("duckdb")
        .args([
            "-readonly",
            "-safe",
            "-cmd",
            ".mode tabs",
            &Config::get().db_path().to_string_lossy(),
            &final_query,
        ])
        .spawn()?
        .wait()?;

    Ok(())
}

const DEFAULT_COLUMNS: &str = "resourceType, accountId, awsRegion, resourceId, resourceName";
const BASE_RESOURCE_DEFAULT_COLUMNS: &str = "accountId, awsRegion, resourceId";

fn column_selection(fields: FieldSelection) -> String {
    match fields {
        FieldSelection::All => "*".to_string(),
        FieldSelection::Default(None) => DEFAULT_COLUMNS.to_string(),
        FieldSelection::Default(Some(resource_type)) => resource_default_columns(&resource_type),
        FieldSelection::Custom(fields) => fields.join(","),
    }
}

fn resource_default_columns(resource_type: &ResourceType) -> String {
    BASE_RESOURCE_DEFAULT_COLUMNS.to_string()
        + match resource_type {
            ResourceType::Role => ",arn",
            ResourceType::Vpc => ",tags['Name'],cidrBlock",
            ResourceType::Subnet => {
                ",vpcId,tags['Name'],cidrBlock,availabilityZone,availableIpAddressCount"
            }
            _ => "",
        }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_table_and_columns() {
        let q = build_query(None, None, None, true, "SELECT * FROM input");
        assert!(q.contains("query_table('resources')"));
        assert!(q.contains("SELECT * FROM"));
    }

    #[test]
    fn resource_type_maps_to_table_name() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            None,
            None,
            true,
            "SELECT * FROM input",
        );
        assert!(q.contains("query_table('ec2_instance')"));
    }

    #[test]
    fn explicit_fields_joined() {
        let fields = vec!["resourceId".to_string(), "awsRegion".to_string()];
        let q = build_query(None, None, Some(fields), false, "SELECT * FROM input");
        assert!(q.contains("SELECT resourceId,awsRegion FROM"));
    }

    #[test]
    fn account_filter_generates_where_clause() {
        let q = build_query(
            None,
            Some("123456789012"),
            None,
            true,
            "SELECT * FROM input",
        );
        assert!(q.contains("WHERE accountId == 123456789012"));
    }

    #[test]
    fn no_account_filter_no_where_clause() {
        let q = build_query(None, None, None, true, "SELECT * FROM input");
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn user_query_included() {
        let q = build_query(None, None, None, true, "SELECT count(*) FROM input");
        assert!(q.contains("SELECT count(*) FROM input;"));
    }

    #[test]
    fn default_columns_no_resource_type() {
        let q = build_query(None, None, None, false, "SELECT * FROM input");
        assert!(q.contains(&format!("SELECT {DEFAULT_COLUMNS} FROM")));
    }

    #[test]
    fn default_columns_with_known_resource_type() {
        let q = build_query(
            Some("AWS::IAM::Role"),
            None,
            None,
            false,
            "SELECT * FROM input",
        );
        assert!(q.contains(&format!("SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},arn FROM")));
    }

    #[test]
    fn default_columns_with_unknown_resource_type() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            None,
            None,
            false,
            "SELECT * FROM input",
        );
        assert!(q.contains(&format!("SELECT {BASE_RESOURCE_DEFAULT_COLUMNS} FROM")));
        // Should not have any extra columns appended
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS} FROM query_table"
        )));
    }

    #[test]
    fn resource_type_and_account_combined() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            Some("123456789012"),
            None,
            true,
            "SELECT * FROM input",
        );
        assert!(q.contains("query_table('ec2_instance')"));
        assert!(q.contains("WHERE accountId == 123456789012"));
    }
}
