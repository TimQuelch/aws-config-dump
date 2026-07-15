// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::path::PathBuf;
use std::{convert::Into, sync::LazyLock};

use clap::{ArgMatches, CommandFactory};
use clap_complete::CompletionCandidate;
use tokio::sync::oneshot;
use tokio::task;

use crate::cli::Cli;
use crate::config::{Config, ConfigFile};
use crate::query;
use crate::util;

static PARSED_ARGS: LazyLock<ArgMatches> = LazyLock::new(|| {
    // The args of the candidate command are passed to the binary for completion.
    // In a completion context the args look like:
    // ["acd" "--" "acd" ... <the rest>]
    // Skip the first two, and parse the candidate cli for completion
    Cli::command()
        .ignore_errors(true)
        .get_matches_from(std::env::args_os().skip(2))
});

static CONFIG: LazyLock<Config> = LazyLock::new(|| {
    let config = PARSED_ARGS.get_one::<PathBuf>("config").cloned();
    let db = PARSED_ARGS.get_one::<String>("db").cloned();

    let (tx, rx) = oneshot::channel();
    tokio::spawn(async move { tx.send(ConfigFile::load(config.as_deref()).await) });
    let config_file = task::block_in_place(|| rx.blocking_recv().unwrap()).unwrap();

    Config::load(config_file, db.as_deref()).unwrap()
});

fn db_conn() -> duckdb::Connection {
    duckdb::Connection::open(&CONFIG.db_path).expect("failed to connect to database")
}

/// Run a given query and return the results as completion candidates. First returned column is the
/// value and second returned column, if any, is the help text
fn query_results_candidates(query: &str) -> Vec<clap_complete::CompletionCandidate> {
    db_conn()
        .prepare(query)
        .unwrap()
        .query_map([], |row| Ok((row.get(0)?, row.get(1).ok())))
        .unwrap()
        .filter_map(|r| {
            r.ok().map(|(c, h): (String, Option<String>)| {
                CompletionCandidate::new(c).help(h.map(Into::into))
            })
        })
        .collect()
}

pub fn resource_type_candidates() -> Vec<clap_complete::CompletionCandidate> {
    query_results_candidates("SELECT resourceType FROM resourceTypes;")
}

pub fn account_candidates() -> Vec<clap_complete::CompletionCandidate> {
    let mut candidates = query_results_candidates("SELECT accountId, accountName FROM accounts;");
    let flipped: Vec<_> = candidates
        .iter()
        .filter_map(|c| {
            c.get_help().map(|h| {
                CompletionCandidate::new(h.to_string())
                    .help(Some(c.get_value().to_string_lossy().to_string().into()))
            })
        })
        .collect();
    candidates.extend(flipped);
    candidates
}

pub fn field_candidates() -> Vec<clap_complete::CompletionCandidate> {
    let table = resource_table();
    query_results_candidates(
        format!(
            "FROM information_schema.columns SELECT column_name, data_type
                 WHERE table_name = '{table}'
                 UNION SELECT 'accountName', 'VARCHAR';"
        )
        .as_str(),
    )
}

fn resource_table() -> String {
    PARSED_ARGS
        .subcommand_matches("query")
        .and_then(|m| m.get_one::<String>("resource_type"))
        .map_or_else(|| "resources".to_string(), util::resource_table_name)
}

fn filter_predicates() -> String {
    // Turn every filter already on the command line into the same SQL predicates the main query
    // uses, so completion candidates are narrowed by the other args. ID and Name filters are not
    // included in the completion filter query because these are 'OR'd together.
    let m = PARSED_ARGS.subcommand_matches("query");
    let strings = |id: &str| -> Vec<String> {
        m.and_then(|m| m.get_many::<String>(id))
            .map(|vals| vals.cloned().collect())
            .unwrap_or_default()
    };
    let where_clauses: Vec<(String, String)> = m
        .and_then(|m| m.get_many::<(String, String)>("where"))
        .map(|vals| vals.cloned().collect())
        .unwrap_or_default();
    let accounts = strings("accounts");
    let where_raw = strings("where_raw");
    query::build_filter_predicates(&query::Filters {
        accounts: &accounts,
        where_clauses: &where_clauses,
        where_raw: &where_raw,
        ids: &[],
        names: &[],
    })
}

/// SQL that lists distinct values of `column` from `table`, narrowed by
/// `predicates` (from [`filter_predicates`]) and the `prefix` being completed.
fn column_candidates_sql(table: &str, column: &str, predicates: &str, prefix: &str) -> String {
    format!(
        r#"SELECT DISTINCT "{column}" FROM query_table('{table}')
             LEFT JOIN accounts USING (accountId)
             WHERE true {predicates} AND "{column}" LIKE '{prefix}%'
             ORDER BY "{column}";"#
    )
}

fn column_value_candidates(
    column: &str,
    current: &std::ffi::OsStr,
) -> Vec<clap_complete::CompletionCandidate> {
    let sql = column_candidates_sql(
        &resource_table(),
        column,
        &filter_predicates(),
        &current.to_string_lossy(),
    );
    query_results_candidates(&sql)
}

pub fn id_candidates(current: &std::ffi::OsStr) -> Vec<clap_complete::CompletionCandidate> {
    column_value_candidates("resourceId", current)
}

pub fn name_candidates(current: &std::ffi::OsStr) -> Vec<clap_complete::CompletionCandidate> {
    column_value_candidates("resourceName", current)
}

pub fn where_clause_completer(
    current: &std::ffi::OsStr,
) -> Vec<clap_complete::CompletionCandidate> {
    let table = resource_table();

    if let Some((key, prefix)) = current.to_string_lossy().split_once('=') {
        let key_prefix = format!("{key}=");
        let sql = column_candidates_sql(&table, key, &filter_predicates(), prefix);
        query_results_candidates(&sql)
            .into_iter()
            .map(|c| c.add_prefix(&key_prefix))
            .collect()
    } else {
        query_results_candidates(
            format!(
                "FROM information_schema.columns SELECT column_name || '=' WHERE table_name = '{table}' AND data_type = 'VARCHAR';"
            )
            .as_str(),
        )
    }
}
