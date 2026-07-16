// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{collections::HashMap, fmt::Write, os::unix::process::CommandExt, process::Command};

use crate::cli::OutputFormat;
use crate::config::Config;
use crate::db;
use crate::util;

enum FieldSelection {
    All,
    Default(Option<String>),
    Custom(Vec<String>),
}

pub struct QueryParams {
    pub resource_type: Option<String>,
    pub accounts: Option<Vec<String>>,
    pub fields: Option<Vec<String>>,
    pub all_fields: bool,
    pub where_clauses: Option<Vec<(String, String)>>,
    pub where_raw_clauses: Option<Vec<String>>,
    pub ids: Option<Vec<String>>,
    pub names: Option<Vec<String>>,
    pub exclude_fields: Option<Vec<String>>,
    pub query: String,
    pub sort: Option<Vec<String>>,
    pub reverse: bool,
    pub format: OutputFormat,
}

fn build_account_clause(accounts: &[String]) -> String {
    if accounts.is_empty() {
        return String::new();
    }
    let values = accounts
        .iter()
        .map(|a| format!("'{a}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("AND (accountId IN ({values}) OR accountName IN ({values}))")
}

fn build_where_clause(clauses: &[(String, String)]) -> String {
    clauses.iter().fold(String::new(), |mut acc, (k, v)| {
        write!(acc, " AND (\"{k}\" = '{v}')").expect("write to String cannot fail");
        acc
    })
}

fn build_where_raw_clause(clauses: &[String]) -> String {
    clauses.iter().fold(String::new(), |mut acc, w| {
        write!(acc, " AND ({w})").expect("write to String cannot fail");
        acc
    })
}

fn build_regex_clause(column: &str, patterns: &[String]) -> String {
    if patterns.is_empty() {
        return String::new();
    }
    let matches = patterns
        .iter()
        .map(|p| format!("regexp_matches(\"{column}\", '{p}')"))
        .collect::<Vec<_>>()
        .join(" OR ");
    format!(" AND ({matches})")
}

pub struct Filters<'a> {
    pub accounts: &'a [String],
    pub where_clauses: &'a [(String, String)],
    pub where_raw: &'a [String],
    pub ids: &'a [String],
    pub names: &'a [String],
}

pub fn build_filter_predicates(filters: &Filters) -> String {
    format!(
        "{}{}{}{}{}",
        build_account_clause(filters.accounts),
        build_where_clause(filters.where_clauses),
        build_where_raw_clause(filters.where_raw),
        build_regex_clause("resourceId", filters.ids),
        build_regex_clause("resourceName", filters.names),
    )
}

fn build_order_by_clause(fields: &[String], reverse: bool) -> String {
    if fields.is_empty() {
        return String::new();
    }
    let first = fields.first().expect("already checked not-empty");
    let direction = if reverse { " DESC" } else { "" };
    fields
        .iter()
        .skip(1)
        .fold(format!("ORDER BY {first}{direction}"), |mut acc, f| {
            write!(acc, ", {f}{direction}").expect("write to String cannot fail");
            acc
        })
}

fn build_query(
    params: QueryParams,
    has_account_name: bool,
    extra_columns: &HashMap<String, Vec<String>>,
) -> String {
    let table = params
        .resource_type
        .as_deref()
        .map_or_else(|| "resources".to_string(), util::resource_table_name);
    let field_selection = if params.all_fields {
        FieldSelection::All
    } else {
        params.fields.map_or_else(
            || FieldSelection::Default(params.resource_type),
            FieldSelection::Custom,
        )
    };
    let columns = column_selection(
        field_selection,
        has_account_name,
        extra_columns,
        &params.exclude_fields.unwrap_or_default(),
    );
    let accounts = params.accounts.unwrap_or_default();
    let where_clauses = params.where_clauses.unwrap_or_default();
    let where_raw = params.where_raw_clauses.unwrap_or_default();
    let ids = params.ids.unwrap_or_default();
    let names = params.names.unwrap_or_default();
    let predicates = build_filter_predicates(&Filters {
        accounts: &accounts,
        where_clauses: &where_clauses,
        where_raw: &where_raw,
        ids: &ids,
        names: &names,
    });
    let order_by_clause = build_order_by_clause(&params.sort.unwrap_or_default(), params.reverse);
    format!(
        "CREATE OR REPLACE TEMPORARY VIEW input AS
            SELECT {columns} FROM query_table('{table}')
            LEFT JOIN accounts USING (accountId)
            WHERE true
            {predicates}
            {order_by_clause};
        {user_query};",
        user_query = params.query,
    )
}

async fn detect_has_account_name(db_path: &std::path::Path) -> bool {
    if let Ok(pool) = db::connect_to_db(db_path).await
        && let Ok(conn) = pool.get().await
    {
        return conn
            .with_conn(|c| {
                c.query_row(
                    "SELECT count(*) > 0 FROM accounts WHERE accountName IS NOT NULL;",
                    [],
                    |row| row.get(0),
                )
            })
            .await
            .unwrap_or(false);
    }
    false
}

/// Query the database
///
/// Calls the duckdb CLI instead of using the SDK so we don't need to implement output formatting here
pub async fn query(config: &Config, params: QueryParams) -> anyhow::Result<()> {
    let has_account_name = detect_has_account_name(&config.db_path).await;
    let mode = match params.format {
        OutputFormat::Tsv => "tabs",
        OutputFormat::Json => "json",
        OutputFormat::Csv => "csv",
        OutputFormat::Ndjson => "jsonlines",
    };
    let final_query = build_query(params, has_account_name, &config.query_extra_columns);

    Command::new("duckdb")
        .args(["-readonly", "-safe", "-cmd", &format!(".mode {mode}")])
        .arg(&config.db_path)
        .arg(&final_query)
        .spawn()?
        .wait()?;

    Ok(())
}

/// Open an interactive `DuckDB` REPL against the local database
pub fn repl(config: &Config) -> anyhow::Result<()> {
    let err = Command::new("duckdb").arg(&config.db_path).exec();
    Err(anyhow::Error::from(err))
}

const DEFAULT_COLUMNS: &[&str] = &[
    "resourceType",
    "accountId",
    "awsRegion",
    "resourceId",
    "resourceName",
];
const BASE_RESOURCE_DEFAULT_COLUMNS: &[&str] =
    &["accountId", "awsRegion", "resourceId", "resourceName"];

fn column_selection(
    fields: FieldSelection,
    has_account_name: bool,
    extra_columns: &HashMap<String, Vec<String>>,
    exclude_fields: &[String],
) -> String {
    match fields {
        FieldSelection::All => {
            if exclude_fields.is_empty() {
                "*".to_string()
            } else {
                format!("* EXCLUDE ({})", exclude_fields.join(", "))
            }
        }
        FieldSelection::Custom(fields) => fields
            .into_iter()
            .filter(|f| !exclude_fields.iter().any(|e| e.eq_ignore_ascii_case(f)))
            .collect::<Vec<_>>()
            .join(", "),
        FieldSelection::Default(resource_type_opt) => {
            let base: Vec<String> = resource_type_opt.map_or_else(
                || DEFAULT_COLUMNS.iter().copied().map(String::from).collect(),
                |rt| resource_default_columns(&rt, extra_columns),
            );
            base.into_iter()
                .flat_map(|f| {
                    if has_account_name && f == "accountId" {
                        vec![f, "accountName".to_string()]
                    } else {
                        vec![f]
                    }
                })
                .filter(|f| !exclude_fields.iter().any(|e| e.eq_ignore_ascii_case(f)))
                .collect::<Vec<_>>()
                .join(", ")
        }
    }
}

fn resource_default_columns(
    resource_type: &str,
    extra_columns: &HashMap<String, Vec<String>>,
) -> Vec<String> {
    let table_name = util::resource_table_name(resource_type);
    let mut base: Vec<String> = BASE_RESOURCE_DEFAULT_COLUMNS
        .iter()
        .copied()
        .map(String::from)
        .collect();
    if let Some(cols) = extra_columns.get(&table_name) {
        base.extend(cols.iter().cloned());
        return base;
    }
    let built_in: &[&str] = match resource_type {
        "AWS::IAM::Role" => &["arn"],
        "AWS::EC2::VPC" => &["cidrBlock"],
        "AWS::EC2::Subnet" => &[
            "vpcId",
            "cidrBlock",
            "availabilityZone",
            "availableIpAddressCount",
        ],
        _ => &[],
    };
    base.extend(built_in.iter().copied().map(String::from));
    base
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn default_params() -> QueryParams {
        QueryParams {
            resource_type: None,
            accounts: None,
            fields: None,
            all_fields: false,
            where_clauses: None,
            where_raw_clauses: None,
            ids: None,
            names: None,
            exclude_fields: Some(vec![]),
            query: "SELECT * FROM input".to_string(),
            sort: None,
            reverse: false,
            format: OutputFormat::Tsv,
        }
    }

    #[test]
    fn build_account_clause_empty_slice_is_empty() {
        assert!(build_account_clause(&[]).is_empty());
    }

    #[test]
    fn build_account_clause_single_account() {
        let clause = build_account_clause(&["123456789012".to_string()]);
        assert_eq!(
            clause,
            "AND (accountId IN ('123456789012') OR accountName IN ('123456789012'))"
        );
    }

    #[test]
    fn build_account_clause_multiple_accounts() {
        let clause = build_account_clause(&["123456789012".to_string(), "prod".to_string()]);
        assert_eq!(
            clause,
            "AND (accountId IN ('123456789012', 'prod') OR accountName IN ('123456789012', 'prod'))"
        );
    }

    #[test]
    fn build_where_clause_empty_is_empty() {
        assert!(build_where_clause(&[]).is_empty());
    }

    #[test]
    fn build_where_clause_single_clause() {
        let clause =
            build_where_clause(&[("resourceType".to_string(), "AWS::EC2::Instance".to_string())]);
        assert_eq!(clause, " AND (\"resourceType\" = 'AWS::EC2::Instance')");
    }

    #[test]
    fn build_where_clause_multiple_clauses() {
        let clause = build_where_clause(&[
            ("resourceType".to_string(), "AWS::EC2::Instance".to_string()),
            ("awsRegion".to_string(), "us-east-1".to_string()),
        ]);
        assert_eq!(
            clause,
            " AND (\"resourceType\" = 'AWS::EC2::Instance') AND (\"awsRegion\" = 'us-east-1')"
        );
    }

    #[test]
    fn build_regex_clause_empty_is_empty() {
        assert!(build_regex_clause("resourceId", &[]).is_empty());
    }

    #[test]
    fn build_regex_clause_single_pattern() {
        let clause = build_regex_clause("resourceId", &["someid".to_string()]);
        assert_eq!(clause, " AND (regexp_matches(\"resourceId\", 'someid'))");
    }

    #[test]
    fn build_regex_clause_multiple_patterns_are_ored() {
        let clause = build_regex_clause("resourceName", &["a".to_string(), "b".to_string()]);
        assert_eq!(
            clause,
            " AND (regexp_matches(\"resourceName\", 'a') OR regexp_matches(\"resourceName\", 'b'))"
        );
    }

    #[test]
    fn build_where_raw_clause_empty_is_empty() {
        assert!(build_where_raw_clause(&[]).is_empty());
    }

    #[test]
    fn build_where_raw_clause_single_clause() {
        let clause = build_where_raw_clause(&["resourceId LIKE 'arn:%'".to_string()]);
        assert_eq!(clause, " AND (resourceId LIKE 'arn:%')");
    }

    #[test]
    fn build_where_raw_clause_multiple_clauses() {
        let clause = build_where_raw_clause(&[
            "resourceId LIKE 'arn:%'".to_string(),
            "awsRegion != 'us-east-1'".to_string(),
        ]);
        assert_eq!(
            clause,
            " AND (resourceId LIKE 'arn:%') AND (awsRegion != 'us-east-1')"
        );
    }

    #[test]
    fn filter_predicates_empty_when_no_filters() {
        let filters = Filters {
            accounts: &[],
            where_clauses: &[],
            where_raw: &[],
            ids: &[],
            names: &[],
        };
        assert!(build_filter_predicates(&filters).is_empty());
    }

    #[test]
    fn filter_predicates_combine_all_families() {
        let filters = Filters {
            accounts: &["prod".to_string()],
            where_clauses: &[("awsRegion".to_string(), "us-east-1".to_string())],
            where_raw: &["x > 1".to_string()],
            ids: &["myid".to_string()],
            names: &["myname".to_string()],
        };
        let p = build_filter_predicates(&filters);
        assert!(p.contains("accountId IN ('prod')"));
        assert!(p.contains("(\"awsRegion\" = 'us-east-1')"));
        assert!(p.contains("(x > 1)"));
        assert!(p.contains("regexp_matches(\"resourceId\", 'myid')"));
        assert!(p.contains("regexp_matches(\"resourceName\", 'myname')"));
    }

    #[test]
    fn filter_predicates_account_where_boundary_exact() {
        // account contributes no leading space, where contributes one, so the
        // boundary between them must be a single space.
        let filters = Filters {
            accounts: &["prod".to_string()],
            where_clauses: &[("awsRegion".to_string(), "us-east-1".to_string())],
            where_raw: &[],
            ids: &[],
            names: &[],
        };
        assert_eq!(
            build_filter_predicates(&filters),
            "AND (accountId IN ('prod') OR accountName IN ('prod')) AND (\"awsRegion\" = 'us-east-1')"
        );
    }

    #[test]
    fn id_filter_generates_regexp_matches() {
        let q = build_query(
            QueryParams {
                ids: Some(vec!["myid".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("regexp_matches(\"resourceId\", 'myid')"));
    }

    #[test]
    fn name_filter_generates_regexp_matches() {
        let q = build_query(
            QueryParams {
                names: Some(vec!["myname".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("regexp_matches(\"resourceName\", 'myname')"));
    }

    #[test]
    fn id_filter_combines_with_account_filter() {
        let q = build_query(
            QueryParams {
                accounts: Some(vec!["123456789012".to_string()]),
                ids: Some(vec!["myid".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("accountId IN ('123456789012')"));
        assert!(q.contains("regexp_matches(\"resourceId\", 'myid')"));
    }

    #[test]
    fn no_resource_type_uses_resources_table() {
        let q = build_query(default_params(), false, &HashMap::new());
        assert!(q.contains("query_table('resources')"));
    }

    #[test]
    fn resource_type_maps_to_table_name() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::Instance".to_string()),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("query_table('ec2_instance')"));
    }

    #[test]
    fn account_join_always_present() {
        let q = build_query(default_params(), false, &HashMap::new());
        assert!(q.contains("LEFT JOIN accounts USING (accountId)"));
    }

    #[test]
    fn account_filter_generates_where_clause() {
        let q = build_query(
            QueryParams {
                accounts: Some(vec!["123456789012".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("(accountId IN ('123456789012') OR accountName IN ('123456789012'))"));
    }

    #[test]
    fn multiple_account_filters_generate_where_clause() {
        let q = build_query(
            QueryParams {
                accounts: Some(vec!["123456789012".to_string(), "prod".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains(
            "(accountId IN ('123456789012', 'prod') OR accountName IN ('123456789012', 'prod'))"
        ));
    }

    #[test]
    fn user_query_included() {
        let q = build_query(
            QueryParams {
                query: "SELECT count(*) FROM input".to_string(),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT count(*) FROM input;"));
    }

    #[test]
    fn all_fields_selects_star() {
        let q = build_query(
            QueryParams {
                all_fields: true,
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT * FROM query_table("));
    }

    #[test]
    fn all_fields_include_account_name_has_no_effect() {
        let a = build_query(
            QueryParams {
                all_fields: true,
                ..default_params()
            },
            true,
            &HashMap::new(),
        );
        let b = build_query(
            QueryParams {
                all_fields: true,
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn custom_fields_as_select_list() {
        let q = build_query(
            QueryParams {
                fields: Some(vec!["resourceId".to_string(), "awsRegion".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT resourceId, awsRegion FROM"));
    }

    #[test]
    fn custom_fields_can_include_account_name() {
        let q = build_query(
            QueryParams {
                fields: Some(vec!["resourceId".to_string(), "accountName".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT resourceId, accountName FROM"));
    }

    #[test]
    fn custom_fields_include_account_name_no_effect() {
        let fields = vec!["resourceId".to_string()];
        let a = build_query(
            QueryParams {
                fields: Some(fields.clone()),
                ..default_params()
            },
            true,
            &HashMap::new(),
        );
        let b = build_query(
            QueryParams {
                fields: Some(fields),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert_eq!(a, b);
    }

    #[test]
    fn default_columns_no_resource_type() {
        let q = build_query(default_params(), false, &HashMap::new());
        assert!(q.contains(&format!("SELECT {} FROM", DEFAULT_COLUMNS.join(", "))));
    }

    #[test]
    fn default_columns_no_resource_type_with_account_name() {
        let q = build_query(default_params(), true, &HashMap::new());
        assert!(q.contains(
            "SELECT resourceType, accountId, accountName, awsRegion, resourceId, resourceName FROM",
        ));
    }

    #[test]
    fn default_columns_unknown_resource_type() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::Instance".to_string()),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains(&format!(
            "SELECT {} FROM query_table",
            BASE_RESOURCE_DEFAULT_COLUMNS.join(", ")
        )));
    }

    #[test]
    fn default_columns_unknown_resource_type_with_account_name() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::Instance".to_string()),
                ..default_params()
            },
            true,
            &HashMap::new(),
        );
        assert!(
            q.contains("SELECT accountId, accountName, awsRegion, resourceId, resourceName FROM")
        );
    }

    #[test]
    fn default_columns_role() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::IAM::Role".to_string()),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, awsRegion, resourceId, resourceName, arn FROM"));
    }

    #[test]
    fn default_columns_role_with_account_name() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::IAM::Role".to_string()),
                ..default_params()
            },
            true,
            &HashMap::new(),
        );
        assert!(q.contains(
            "SELECT accountId, accountName, awsRegion, resourceId, resourceName, arn FROM"
        ));
    }

    #[test]
    fn default_columns_vpc() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::VPC".to_string()),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(
            q.contains("SELECT accountId, awsRegion, resourceId, resourceName, cidrBlock FROM")
        );
    }

    #[test]
    fn default_columns_subnet() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::Subnet".to_string()),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, awsRegion, resourceId, resourceName, vpcId, cidrBlock, availabilityZone, availableIpAddressCount FROM"));
    }

    #[test]
    fn resource_type_and_account_filter_combined() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::Instance".to_string()),
                accounts: Some(vec!["123456789012".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("query_table('ec2_instance')"));
        assert!(q.contains("(accountId IN ('123456789012') OR accountName IN ('123456789012'))"));
    }

    #[test]
    fn config_extra_columns_override_built_in_for_known_type() {
        let extra = HashMap::from([(
            "iam_role".to_string(),
            vec!["roleName".to_string(), "arn".to_string()],
        )]);
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::IAM::Role".to_string()),
                ..default_params()
            },
            false,
            &extra,
        );
        assert!(
            q.contains("SELECT accountId, awsRegion, resourceId, resourceName, roleName, arn FROM")
        );
    }

    #[test]
    fn config_extra_columns_used_for_type_with_no_built_in_columns() {
        let extra = HashMap::from([("ec2_instance".to_string(), vec!["instanceType".to_string()])]);
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::EC2::Instance".to_string()),
                ..default_params()
            },
            false,
            &extra,
        );
        assert!(
            q.contains("SELECT accountId, awsRegion, resourceId, resourceName, instanceType FROM")
        );
    }

    #[test]
    fn absent_from_config_falls_back_to_built_in_columns() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::IAM::Role".to_string()),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, awsRegion, resourceId, resourceName, arn FROM"));
    }

    #[test]
    fn exclude_fields_removes_from_default_columns() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::IAM::Role".to_string()),
                exclude_fields: Some(vec!["arn".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, awsRegion, resourceId, resourceName FROM"));
    }

    #[test]
    fn exclude_fields_removes_from_custom_fields() {
        let q = build_query(
            QueryParams {
                fields: Some(vec![
                    "resourceId".to_string(),
                    "awsRegion".to_string(),
                    "resourceName".to_string(),
                ]),
                exclude_fields: Some(vec!["resourceName".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT resourceId, awsRegion FROM"));
    }

    #[test]
    fn exclude_fields_with_all_fields_uses_exclude_syntax() {
        let q = build_query(
            QueryParams {
                all_fields: true,
                exclude_fields: Some(vec!["resourceName".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT * EXCLUDE (resourceName) FROM"));
    }

    #[test]
    fn exclude_fields_multiple_with_all_fields() {
        let q = build_query(
            QueryParams {
                all_fields: true,
                exclude_fields: Some(vec!["resourceName".to_string(), "awsRegion".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT * EXCLUDE (resourceName, awsRegion) FROM"));
    }

    #[test]
    fn exclude_fields_case_insensitive() {
        let q = build_query(
            QueryParams {
                resource_type: Some("AWS::IAM::Role".to_string()),
                exclude_fields: Some(vec!["resourcename".to_string(), "ARN".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("SELECT accountId, awsRegion, resourceId FROM"));
    }

    #[test]
    fn empty_exclude_fields_has_no_effect() {
        let with_empty = build_query(
            QueryParams {
                all_fields: true,
                exclude_fields: Some(vec![]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        let without = build_query(
            QueryParams {
                all_fields: true,
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert_eq!(with_empty, without);
    }

    #[test]
    fn order_by_empty_is_empty() {
        assert!(build_order_by_clause(&[], false).is_empty());
    }

    #[test]
    fn order_by_single_field() {
        assert_eq!(
            build_order_by_clause(&["resourceName".to_string()], false),
            "ORDER BY resourceName"
        );
    }

    #[test]
    fn order_by_multiple_fields() {
        assert_eq!(
            build_order_by_clause(
                &["resourceName".to_string(), "awsRegion".to_string()],
                false
            ),
            "ORDER BY resourceName, awsRegion"
        );
    }

    #[test]
    fn order_by_single_field_reversed() {
        assert_eq!(
            build_order_by_clause(&["resourceName".to_string()], true),
            "ORDER BY resourceName DESC"
        );
    }

    #[test]
    fn order_by_multiple_fields_reversed() {
        assert_eq!(
            build_order_by_clause(&["resourceName".to_string(), "awsRegion".to_string()], true),
            "ORDER BY resourceName DESC, awsRegion DESC"
        );
    }

    #[test]
    fn order_by_reverse_with_empty_fields_is_empty() {
        assert!(build_order_by_clause(&[], true).is_empty());
    }

    #[test]
    fn sort_fields_adds_order_by() {
        let q = build_query(
            QueryParams {
                sort: Some(vec!["resourceName".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("ORDER BY resourceName"));
    }

    #[test]
    fn sort_multiple_fields_order_preserved() {
        let q = build_query(
            QueryParams {
                sort: Some(vec!["awsRegion".to_string(), "resourceName".to_string()]),
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("ORDER BY awsRegion, resourceName"));
    }

    #[test]
    fn sort_with_reverse_adds_desc() {
        let q = build_query(
            QueryParams {
                sort: Some(vec!["resourceName".to_string()]),
                reverse: true,
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(q.contains("ORDER BY resourceName DESC"));
    }

    #[test]
    fn no_sort_fields_no_order_by() {
        let q = build_query(default_params(), false, &HashMap::new());
        assert!(!q.contains("ORDER BY"));
    }

    #[test]
    fn reverse_without_sort_fields_no_order_by() {
        let q = build_query(
            QueryParams {
                reverse: true,
                ..default_params()
            },
            false,
            &HashMap::new(),
        );
        assert!(!q.contains("ORDER BY"));
    }
}
