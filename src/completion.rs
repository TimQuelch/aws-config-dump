use clap_complete::{CompletionCandidate, engine::ValueCandidates};

fn query_results_candidates(query: &str) -> Vec<clap_complete::CompletionCandidate> {
    duckdb::Connection::open("./db.duckdb")
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
