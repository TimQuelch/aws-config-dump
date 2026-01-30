use clap::CommandFactory;
use clap_complete::{CompletionCandidate, engine::ValueCandidates};

use crate::cli::Cli;
use crate::config::Config;

fn query_results_candidates(query: &str) -> Vec<clap_complete::CompletionCandidate> {
    // The args of the candidate command are passed to the binary for completion.
    // In a completion context the args look like:
    // ["aws-config-dump" "--" "aws-config-dump" ... <the rest>]
    // Skip the first two, and parse the candidate cli for completion
    let arg_matches = Cli::command().get_matches_from(std::env::args_os().skip(2));
    let db_name = arg_matches.get_one::<String>("db_name").unwrap();

    duckdb::Connection::open(Config::init(db_name).db_path())
        .unwrap()
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
