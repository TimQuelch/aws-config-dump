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

    let final_query = format!(
        "CREATE OR REPLACE TEMPORARY VIEW input AS
            SELECT {columns} FROM query_table('{table}') {account_clause};
        {query};",
    );

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
