// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::collections::{HashMap, HashSet};

use aws_client::{
    config_client::{ConfigFetchClient, ConfigResourceType, DispatchingClient},
    org_client, snapshot,
};
use chrono::{DateTime, Utc};
use indicatif::MultiProgress;
use tempfile::TempPath;
use tokio::task;
use tracing::{debug, error, info};

use crate::builtin_fetchers;
use crate::db;
use crate::fetcher::{self, Fetcher};
use crate::schema_alterations;
use crate::util::temp_file_writer;
use crate::{config::Config, util};

/// A running fetcher: `Ok(None)` means it failed and nothing should land.
type FetcherHandle = task::JoinHandle<anyhow::Result<Option<fetcher::FetchOutput>>>;

pub enum FetchSource {
    Api,
    Snapshots,
    Skip,
}

impl FetchSource {
    /// Whether this build fetches at all.
    ///
    /// `--no-fetch` is documented as "Don't fetch data, only build the resource
    /// tables", so fetchers stay off under it. Snapshots contain no
    /// fetcher-sourced resources either, so those builds still need them.
    fn fetches(&self) -> bool {
        match self {
            FetchSource::Api | FetchSource::Snapshots => true,
            FetchSource::Skip => false,
        }
    }
}

/// Resource types owned by a fetcher rather than by AWS Config.
///
/// Used so that functionality querying only the Config provided resource types can scope its
/// queries correctly.
///
/// A disabled fetcher claims nothing, which hands its previously stored rows back to Config's
/// delete to be reaped. That is deliberate, and differs from `--no-fetch`: skipping a fetch is
/// transient and keeps the claim so the rows survive, while disabling is a durable statement that
/// this database doesn't want them.
fn fetcher_resource_types(config: &Config, fetchers: &[Box<dyn Fetcher>]) -> Vec<String> {
    fetchers
        .iter()
        .filter(|f| config.fetcher_enabled(f.name()))
        .flat_map(|f| f.resource_types().iter().map(|t| (*t).to_owned()))
        .collect()
}

/// Start every enabled fetcher, each on its own task.
///
/// Fetching runs concurrently with the Config fetch; landing happens afterwards in
/// [`land_fetcher_output`], once the `resources` table is settled.
fn spawn_fetchers(
    config: &Config,
    fetch_source: &FetchSource,
    db_pool: &db_client::ConnectionPool,
    progress: &MultiProgress,
    fetchers: Vec<Box<dyn Fetcher>>,
) -> Vec<FetcherHandle> {
    if !fetch_source.fetches() {
        return Vec::new();
    }

    fetchers
        .into_iter()
        .filter(|f| config.fetcher_enabled(f.name()))
        .map(|f| {
            // Not necessarily the database's profile: the Organizations API, for
            // one, has to be called against the management account.
            let aws_profile = config.fetcher_profile(f.name());
            let db_pool = db_pool.clone();
            let progress = progress.clone();
            task::spawn(async move {
                fetcher::fetch_rows(f.as_ref(), &db_pool, aws_profile, &progress).await
            })
        })
        .collect()
}

pub async fn build_database(
    config: &Config,
    aggregator: Option<String>,
    fetch_source: FetchSource,
    should_rebuild: bool,
    fetch_org_accounts: bool,
) -> anyhow::Result<()> {
    if should_rebuild {
        db::delete_db(&config.db_path).await?;
    }

    let progress = MultiProgress::new();

    let db_pool = db::connect_to_db(&config.db_path).await?;
    debug!(db = %config.db_path.display(), "connected to database");

    let fetchers = builtin_fetchers::all();
    let fetcher_types = fetcher_resource_types(config, &fetchers);

    // Fetch concurrently with Config, but land afterwards: the snapshot path
    // replaces the whole resources table, and the API path merges into it.
    let fetcher_handles = spawn_fetchers(config, &fetch_source, &db_pool, &progress, fetchers);

    let org_accounts_handle = {
        let aws_profile = config.aws_profile.clone();
        task::spawn(async move {
            if fetch_org_accounts {
                Some(org_client::fetch_org_accounts(aws_profile).await)
            } else {
                None
            }
        })
    };

    let managed_policy_handle = {
        let aws_profile = config.aws_profile.clone();
        let progress = progress.clone();
        let db_pool = db_pool.clone();
        task::spawn(async move {
            let all_policies =
                aws_client::iam_client::list_aws_managed_policies(aws_profile.clone())
                    .await
                    .inspect_err(|e| error!(%e, "failed to list AWS managed policies"))
                    .unwrap_or_default();
            db::build_aws_managed_policies_table(
                &db_pool,
                all_policies,
                aws_profile,
                progress.clone(),
            )
            .await
        })
    };

    match fetch_source {
        FetchSource::Snapshots => {
            let mut dir = snapshot::get_snapshots(config.aws_profile.clone()).await?;
            dir.disable_cleanup(true);
            db::build_resources_table_from_snapshots(&db_pool, &dir).await?;
        }
        FetchSource::Api => {
            let cutoff = if should_rebuild {
                None
            } else {
                db::get_timestamp_cutoff(&db_pool, &fetcher_types).await?
            };

            if let Some(cutoff_timestamp) = cutoff {
                info!(%cutoff_timestamp, "fetching updated resources");
            } else {
                info!("fetching all resources");
            }

            let (resources_path, identifiers_path) = fetch_resources(
                aggregator,
                config.aws_profile.clone(),
                cutoff,
                progress.clone(),
                &fetcher_types,
            )
            .await?;
            db::build_resources_table(&db_pool, &resources_path, &identifiers_path, &fetcher_types)
                .await?;
        }
        FetchSource::Skip => {
            debug!("skipping fetch, using existing data");
        }
    }

    land_fetcher_output(&db_pool, fetcher_handles).await?;

    let org_accounts = org_accounts_handle.await?.transpose()?;
    debug!(
        fetched = org_accounts.is_some(),
        "org accounts fetch complete"
    );

    managed_policy_handle.await??;

    db::build_derived_tables(&db_pool, org_accounts, progress.clone()).await?;
    schema_alterations::apply_schema_alterations(config, &db_pool, progress.clone()).await?;
    db::build_custom_tables(&db_pool, config.custom_tables(), progress).await?;
    Ok(())
}

/// Apply each fetcher's rows, once the Config data is settled.
///
/// A fetcher failure is logged and skipped rather than failing the build: an OU
/// fetch that 403s shouldn't cost you the whole resource table, nor should it
/// stop the other fetchers landing. Nothing lands in that case, so the failed
/// fetcher's previous rows stay put.
async fn land_fetcher_output(
    db_pool: &db_client::ConnectionPool,
    handles: Vec<FetcherHandle>,
) -> anyhow::Result<()> {
    for handle in handles {
        match handle.await? {
            Ok(Some(output)) => db::land_fetched_rows(db_pool, &output).await?,
            Ok(None) => debug!("fetcher produced no output, keeping existing rows"),
            Err(error) => error!(%error, "failed to run fetcher"),
        }
    }

    Ok(())
}

async fn fetch_resources(
    aggregator: Option<String>,
    profile: Option<String>,
    cutoff: Option<DateTime<Utc>>,
    progress: MultiProgress,
    fetcher_types: &[String],
) -> anyhow::Result<(TempPath, TempPath)> {
    let config_client = DispatchingClient::new(aggregator.clone(), profile).await?;

    let type_counts = config_client.get_resource_counts().await?;
    let all_types: HashSet<_> = type_counts.keys().cloned().collect();
    debug!(
        total_types = all_types.len(),
        "got total resource type counts"
    );

    // If Config has started recording a type a fetcher claims, stop: landing the
    // fetcher's rows would delete Config's own data for that type.
    let claimed: Vec<&str> = fetcher_types.iter().map(String::as_str).collect();
    fetcher::check_claimed_types_free(all_types.iter().map(ConfigResourceType::as_str), &claimed)?;

    let selectable_type_counts = config_client.get_resource_counts_with_select().await?;
    let selectable_types: HashSet<_> = selectable_type_counts.keys().cloned().collect();
    let unselectable_types: HashSet<_> = all_types.difference(&selectable_types).cloned().collect();
    debug!(
        selectable = selectable_types.len(),
        unselectable = unselectable_types.len(),
        "computed selectable/unselectable type split"
    );

    let modified_type_counts = if let Some(cutoff) = cutoff {
        config_client
            .get_resource_counts_modified_since_cutoff(cutoff)
            .await?
    } else {
        HashMap::new()
    };

    info!(?modified_type_counts, "modified counts");

    let (resources_tx, resources_handle) = temp_file_writer();
    let (identifiers_tx, identifiers_handle) = temp_file_writer();

    info!("fetching modified resource configs for selectable types");
    {
        let c = config_client.clone();
        let resources_tx = resources_tx.clone();
        let bar = progress.add(util::progress_bar(
            "fetching modified resources",
            if cutoff.is_some() {
                modified_type_counts.values().sum::<i64>()
            } else {
                selectable_type_counts.values().sum()
            }
            .try_into()
            .expect("failed to cast to unsigned"),
        ));
        task::spawn(async move {
            c.get_resource_configs_with_select(resources_tx, cutoff, bar)
                .await;
        });
    }

    info!("fetching all resource identifiers for selectable types");
    {
        let c = config_client.clone();
        let identifiers_tx = identifiers_tx.clone();
        let bar = progress.add(util::progress_bar(
            "fetching resource identifers",
            selectable_type_counts
                .values()
                .sum::<i64>()
                .try_into()
                .expect("failed to cast to unsigned"),
        ));
        task::spawn(async move {
            c.get_resource_identifiers_with_select(identifiers_tx.clone(), bar)
                .await;
        });
    }

    info!("fetching all resource configs and identifiers for non-selectable types");

    {
        let c = config_client.clone();
        let bar = progress.add(util::progress_bar(
            "fetching remaining resources",
            type_counts
                .iter()
                .filter_map(|(resource_type, count)| {
                    unselectable_types.contains(resource_type).then_some(count)
                })
                .sum::<i64>()
                .try_into()
                .expect("failed to cast to unsigned"),
        ));
        task::spawn(async move {
            c.get_resource_configs_and_identifiers_with_batch(
                resources_tx,
                identifiers_tx,
                unselectable_types.iter().cloned(),
                cutoff,
                bar,
            )
            .await;
        });
    }

    let resources_path = resources_handle.await??;
    let identifiers_path = identifiers_handle.await??;

    Ok((resources_path, identifiers_path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetcher::Semantics;
    use crate::fetcher::test_support::FakeFetcher;
    use std::sync::atomic::Ordering;

    const OU_TYPE: &str = "AWS::Organizations::OrganizationalUnit";
    const OTHER_TYPE: &str = "Test::Second::Fetcher";

    fn config_from(toml_str: &str) -> Config {
        Config::load(Some(toml::from_str(toml_str).unwrap()), Some("test")).unwrap()
    }

    /// An enabled fetcher claims its types, which is what shields its rows from
    /// Config's identifier-driven delete.
    #[test]
    fn enabled_fetcher_claims_its_resource_types() {
        let config = config_from(
            r#"
[databases.test]
path = "/tmp/test.duckdb"
"#,
        );

        assert_eq!(
            fetcher_resource_types(&config, &builtin_fetchers::all()),
            vec![OU_TYPE.to_owned()]
        );
    }

    /// A disabled fetcher claims nothing, so Config's delete reaps its rows on
    /// the next API build rather than leaving them to rot unrefreshable.
    #[test]
    fn disabled_fetcher_claims_no_resource_types() {
        let config = config_from(
            r#"
[databases.test]
path = "/tmp/test.duckdb"

[databases.test.fetchers.organizational_units]
enabled = false
"#,
        );

        assert!(
            fetcher_resource_types(&config, &builtin_fetchers::all()).is_empty(),
            "a disabled fetcher must release its claim so its rows can be cleaned up"
        );
    }

    /// The claim depends on whether a fetcher is *configured to exist*, never on
    /// whether this particular build fetches. Gating it on `FetchSource` too
    /// would look like a tidy simplification and would quietly make `--no-fetch`
    /// delete every fetched row — the distinction the two tests below protect.
    #[test]
    fn claim_is_independent_of_whether_this_build_fetches() {
        let config = config_from(
            r#"
[databases.test]
path = "/tmp/test.duckdb"
"#,
        );

        // fetcher_resource_types deliberately takes no FetchSource.
        let claimed = fetcher_resource_types(&config, &builtin_fetchers::all());
        assert_eq!(
            claimed,
            vec![OU_TYPE.to_owned()],
            "an enabled fetcher must keep its claim even on a --no-fetch build"
        );
        assert!(
            !FetchSource::Skip.fetches(),
            "and that build fetches nothing"
        );
    }

    /// `--no-fetch` is documented as not fetching, so fetchers must stay off.
    /// Snapshots contain no OUs either, so those builds still need them.
    #[test]
    fn fetchers_run_when_the_build_fetches() {
        assert!(FetchSource::Api.fetches());
        assert!(FetchSource::Snapshots.fetches());
        assert!(
            !FetchSource::Skip.fetches(),
            "--no-fetch must not make fetcher network calls"
        );
    }

    #[tokio::test]
    async fn no_fetch_build_never_runs_a_fetcher() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        let fetcher = FakeFetcher::new(OU_TYPE, Semantics::Authoritative);
        let ran = fetcher.ran_flag();

        // The real gate, not a hand-copy of it.
        let handles = spawn_fetchers(
            &config_from("[databases.test]\npath = \"/tmp/test.duckdb\"\n"),
            &FetchSource::Skip,
            &pool,
            &MultiProgress::new(),
            vec![Box::new(fetcher)],
        );
        assert!(handles.is_empty(), "nothing should be spawned under Skip");
        land_fetcher_output(&pool, handles).await.unwrap();

        assert!(
            !ran.load(Ordering::SeqCst),
            "fetcher ran despite --no-fetch"
        );
    }

    /// A build that runs no fetcher must not be read as "the fetcher returned
    /// nothing" — that would let `--no-fetch` quietly delete every fetched row
    /// under Authoritative semantics.
    #[tokio::test]
    async fn no_fetch_build_leaves_previously_fetched_rows_intact() {
        let pool = db::connect_to_db_in_memory().await.unwrap();

        // A previous build's rows.
        let fetcher = FakeFetcher::new(OU_TYPE, Semantics::Authoritative)
            .with_rows(vec![fake_row(OU_TYPE, "ou-1")]);
        let output = fetcher::fetch_rows(&fetcher, &pool, None, &MultiProgress::new())
            .await
            .unwrap()
            .unwrap();
        db::land_fetched_rows(&pool, &output).await.unwrap();

        // Now a --no-fetch build: no fetcher, so nothing to land.
        land_fetcher_output(&pool, Vec::new()).await.unwrap();

        let count: i64 = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| c.query_row("SELECT count(*) FROM resources", [], |row| row.get(0)))
            .await
            .unwrap();
        assert_eq!(
            count, 1,
            "a --no-fetch build deleted previously fetched rows"
        );
    }

    /// Disabling is per-fetcher, so the gate has to filter the registry rather
    /// than switch the whole set off.
    #[tokio::test]
    async fn disabled_fetcher_is_not_spawned_but_its_neighbours_are() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        let config = config_from(
            r#"
[databases.test]
path = "/tmp/test.duckdb"

[databases.test.fetchers.off]
enabled = false
"#,
        );

        let mut disabled = FakeFetcher::new(OU_TYPE, Semantics::Authoritative);
        disabled.name = "off";
        let mut enabled = FakeFetcher::new(OTHER_TYPE, Semantics::Authoritative);
        enabled.name = "on";
        let (off_ran, on_ran) = (disabled.ran_flag(), enabled.ran_flag());

        let handles = spawn_fetchers(
            &config,
            &FetchSource::Api,
            &pool,
            &MultiProgress::new(),
            vec![Box::new(disabled), Box::new(enabled)],
        );
        assert_eq!(
            handles.len(),
            1,
            "only the enabled fetcher should be spawned"
        );
        land_fetcher_output(&pool, handles).await.unwrap();

        assert!(!off_ran.load(Ordering::SeqCst), "disabled fetcher ran");
        assert!(on_ran.load(Ordering::SeqCst), "enabled fetcher did not run");
    }

    /// Fetchers are independent. One failing must not stop a later one landing,
    /// or a single flaky API would take the rest of the registry down with it.
    #[tokio::test]
    async fn a_failing_fetcher_does_not_block_the_others() {
        let pool = db::connect_to_db_in_memory().await.unwrap();

        let failing = FakeFetcher::new(OU_TYPE, Semantics::Authoritative).failing();
        let working = FakeFetcher::new(OTHER_TYPE, Semantics::Authoritative)
            .with_rows(vec![fake_row(OTHER_TYPE, "i-1")]);

        let handles = spawn_fetchers(
            &config_from("[databases.test]\npath = \"/tmp/test.duckdb\"\n"),
            &FetchSource::Api,
            &pool,
            &MultiProgress::new(),
            vec![Box::new(failing), Box::new(working)],
        );
        land_fetcher_output(&pool, handles)
            .await
            .expect("a fetcher failure must not fail the build");

        let ids: Vec<String> = pool
            .get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.prepare("SELECT resourceId FROM resources ORDER BY resourceId")?
                    .query_map([], |row| row.get(0))?
                    .collect::<Result<_, _>>()
            })
            .await
            .unwrap();
        assert_eq!(
            ids,
            vec!["i-1".to_owned()],
            "the working fetcher's rows should have landed regardless"
        );
    }

    fn fake_row(resource_type: &str, resource_id: &str) -> fetcher::Row {
        fetcher::Row {
            arn: format!("arn:test:{resource_id}"),
            account_id: "111111111111".to_owned(),
            aws_region: "global".to_owned(),
            resource_type: resource_type.to_owned(),
            resource_id: resource_id.to_owned(),
            resource_name: Some(resource_id.to_owned()),
            captured_at: "2026-07-17T00:00:00Z".parse().expect("valid timestamp"),
            tags: Vec::new(),
            configuration: serde_json::json!({}),
        }
    }
}
