// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

//! Fetchers shipped with `acd`.

use crate::fetcher::Fetcher;

pub mod organizational_units;

/// Every fetcher `acd` ships with.
///
/// The single registration point: adding a fetcher means adding a line here,
/// not another copy of the spawn-and-land wiring in [`crate::build`]. Enabling
/// and profile selection are applied per-fetcher by the build from config, so
/// this list is unconditional.
pub fn all() -> Vec<Box<dyn Fetcher>> {
    vec![Box::new(organizational_units::OrganizationalUnitsFetcher)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    /// Names key the config's `[fetchers.<name>]` tables, so a duplicate would
    /// make one fetcher's `enabled`/`aws_profile` silently govern another.
    #[test]
    fn fetcher_names_are_unique() {
        let mut seen = HashSet::new();
        for f in all() {
            assert!(
                seen.insert(f.name()),
                "duplicate fetcher name: {}",
                f.name()
            );
        }
    }

    /// Two fetchers claiming one type would fight: each lands `Authoritative`
    /// separately, so whichever runs second deletes the first's rows.
    #[test]
    fn fetchers_do_not_claim_the_same_resource_type() {
        let mut seen = HashSet::new();
        for f in all() {
            for t in f.resource_types() {
                assert!(
                    seen.insert((*t).to_owned()),
                    "resource type {t} is claimed by more than one fetcher"
                );
            }
        }
    }
}
