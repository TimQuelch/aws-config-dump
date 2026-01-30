use clap_complete::{CompletionCandidate, engine::ValueCandidates};

use crate::config::Config;

fn query_results_candidates(query: &str) -> Vec<clap_complete::CompletionCandidate> {
    // TODO: Config won't behave as expected because we must pass in a value that is derived from
    // the original flags of the command. Flags from the parent command are not passed to the
    // completer binary. Might need to implement custom completer instead of the default clap
    // dynamic completer (although dynamic completion isn't working right now anyway so I'm not
    // going to bother thinking too much about this for now)
    duckdb::Connection::open(Config::init("db").db_path())
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
