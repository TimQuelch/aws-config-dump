// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::process::Command;

use crate::config::Config;
use crate::util;

/// Query the database
///
/// Calls the duckdb CLI instead of using the SDK so we don't need to implement TSV formatting here
pub fn query(
    resource_type: Option<&str>,
    account: Option<&str>,
    fields: Option<Vec<String>>,
    query: &str,
) -> anyhow::Result<()> {
    let table = resource_type.map_or_else(|| "resources".to_string(), util::resource_table_name);
    let columns = fields.map_or_else(|| "*".to_string(), |v| v.join(","));
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
