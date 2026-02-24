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
    include_account_name: bool,
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
    let columns = column_selection(field_selection, include_account_name);
    let account_clause = account.map_or_else(String::new, |account| {
        format!("WHERE accountId == {account}")
    });
    format!(
        "CREATE OR REPLACE TEMPORARY VIEW input AS
            SELECT {columns} FROM query_table('{table}')
            LEFT JOIN accounts USING (accountId)
            {account_clause};
        {user_query};",
    )
}

fn has_account_names() -> bool {
    let Ok(db_conn) = duckdb::Connection::open(Config::get().db_path()) else {
        return false;
    };
    db_conn
        .query_row(
            "SELECT count(*) FROM accounts WHERE accountName IS NOT NULL;",
            [],
            |row| row.get(0),
        )
        .unwrap_or(0i64)
        > 0
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
    let final_query = build_query(
        resource_type,
        account,
        fields,
        all_fields,
        query,
        has_account_names(),
    );

    Command::new("duckdb")
        .args(["-readonly", "-safe", "-cmd", ".mode tabs"])
        .arg(Config::get().db_path())
        .arg(&final_query)
        .spawn()?
        .wait()?;

    Ok(())
}

const DEFAULT_COLUMNS: &str = "resourceType, accountId, awsRegion, resourceId, resourceName";
const BASE_RESOURCE_DEFAULT_COLUMNS: &str = "accountId, awsRegion, resourceId";

fn column_selection(fields: FieldSelection, include_account_name: bool) -> String {
    match fields {
        FieldSelection::All => "*".to_string(),
        FieldSelection::Custom(fields) => fields.join(","),
        FieldSelection::Default(None) => DEFAULT_COLUMNS.replacen(
            "accountId",
            "accountId, accountName",
            usize::from(include_account_name),
        ),
        FieldSelection::Default(Some(resource_type)) => resource_default_columns(&resource_type)
            .replacen(
                "accountId",
                "accountId, accountName",
                usize::from(include_account_name),
            ),
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
    fn no_resource_type_uses_resources_table() {
        let q = build_query(None, None, None, false, "SELECT * FROM input", false);
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
            false,
        );
        assert!(q.contains("query_table('ec2_instance')"));
    }

    #[test]
    fn account_join_always_present() {
        let q = build_query(None, None, None, false, "SELECT * FROM input", false);
        assert!(q.contains("LEFT JOIN accounts USING (accountId)"));
    }

    #[test]
    fn account_filter_generates_where_clause() {
        let q = build_query(
            None,
            Some("123456789012"),
            None,
            false,
            "SELECT * FROM input",
            false,
        );
        assert!(q.contains("WHERE accountId == 123456789012"));
    }

    #[test]
    fn no_account_filter_no_where_clause() {
        let q = build_query(None, None, None, false, "SELECT * FROM input", false);
        assert!(!q.contains("WHERE"));
    }

    #[test]
    fn user_query_included() {
        let q = build_query(None, None, None, false, "SELECT count(*) FROM input", false);
        assert!(q.contains("SELECT count(*) FROM input;"));
    }

    #[test]
    fn all_fields_selects_star() {
        let q = build_query(None, None, None, true, "SELECT * FROM input", false);
        assert!(q.contains("SELECT * FROM query_table("));
    }

    #[test]
    fn all_fields_include_account_name_has_no_effect() {
        let a = build_query(None, None, None, true, "SELECT * FROM input", true);
        let b = build_query(None, None, None, true, "SELECT * FROM input", false);
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
            false,
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
            false,
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
            true,
        );
        let b = build_query(
            None,
            None,
            Some(fields),
            false,
            "SELECT * FROM input",
            false,
        );
        assert_eq!(a, b);
    }

    #[test]
    fn default_columns_no_resource_type() {
        let q = build_query(None, None, None, false, "SELECT * FROM input", false);
        assert!(q.contains(&format!("SELECT {DEFAULT_COLUMNS} FROM")));
    }

    #[test]
    fn default_columns_no_resource_type_with_account_name() {
        let q = build_query(None, None, None, false, "SELECT * FROM input", true);
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
            false,
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
            true,
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
            false,
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
            true,
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
            false,
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},tags['Name'],cidrBlock FROM"
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
            false,
        );
        assert!(q.contains(&format!(
            "SELECT {BASE_RESOURCE_DEFAULT_COLUMNS},vpcId,tags['Name'],cidrBlock,availabilityZone,availableIpAddressCount FROM"
        )));
    }

    #[test]
    fn resource_type_and_account_filter_combined() {
        let q = build_query(
            Some("AWS::EC2::Instance"),
            Some("123456789012"),
            None,
            false,
            "SELECT * FROM input",
            false,
        );
        assert!(q.contains("query_table('ec2_instance')"));
        assert!(q.contains("WHERE accountId == 123456789012"));
    }
}
