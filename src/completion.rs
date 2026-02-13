// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::sync::LazyLock;

use clap::{ArgMatches, CommandFactory};
use clap_complete::{CompletionCandidate, engine::ValueCandidates};

use crate::cli::Cli;
use crate::config::Config;
use crate::util;

static PARSED_ARGS: LazyLock<ArgMatches> = LazyLock::new(|| {
    // The args of the candidate command are passed to the binary for completion.
    // In a completion context the args look like:
    // ["aws-config-dump" "--" "aws-config-dump" ... <the rest>]
    // Skip the first two, and parse the candidate cli for completion
    Cli::command()
        .ignore_errors(true)
        .get_matches_from(std::env::args_os().skip(2))
});

fn db_conn() -> duckdb::Connection {
    let db_name = PARSED_ARGS.get_one::<String>("db_name").unwrap();
    Config::init(db_name);
    duckdb::Connection::open(Config::get().db_path()).unwrap()
}

fn query_results_candidates(query: &str) -> Vec<clap_complete::CompletionCandidate> {
    db_conn()
        .prepare(query)
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .filter_map(|r| r.ok().map(|c: String| CompletionCandidate::new(c)))
        .collect()
}

pub struct ResourceTypeCandidates {}

impl ResourceTypeCandidates {
    pub fn new() -> Self {
        Self {}
    }
}

impl ValueCandidates for ResourceTypeCandidates {
    fn candidates(&self) -> Vec<clap_complete::CompletionCandidate> {
        query_results_candidates("SELECT resourceType FROM resourceTypes;")
    }
}

pub struct AccountCandidates {}

impl AccountCandidates {
    pub fn new() -> Self {
        Self {}
    }
}

impl ValueCandidates for AccountCandidates {
    fn candidates(&self) -> Vec<clap_complete::CompletionCandidate> {
        query_results_candidates("SELECT accountId FROM accounts;")
    }
}

pub struct FieldCandidates {}

impl FieldCandidates {
    pub fn new() -> Self {
        Self {}
    }
}

impl ValueCandidates for FieldCandidates {
    fn candidates(&self) -> Vec<clap_complete::CompletionCandidate> {
        let table = PARSED_ARGS
            .subcommand_matches("query")
            .and_then(|x| x.get_one::<String>("resource_type"))
            .map_or_else(|| "resources".to_string(), util::resource_table_name);

        query_results_candidates(
            format!(
                "FROM information_schema.columns SELECT column_name WHERE table_name = '{table}';"
            )
            .as_str(),
        )
    }
}
