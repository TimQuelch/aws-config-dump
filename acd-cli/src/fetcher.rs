// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

//! Acquiring resource data from sources other than AWS Config.
//!
//! AWS Config doesn't record every resource type. A [`Fetcher`] pulls resources
//! from some other API and emits them as JSON lines shaped like the `resources`
//! table, so they land on the same spine as Config data and get derived tables,
//! `acd query` support and completions for free.
//!
//! Fetching and landing are deliberately separate phases. [`fetch_rows`] runs a
//! fetcher into a temporary file and can proceed concurrently with the Config
//! fetch; [`crate::db::land_fetched_rows`] applies the result afterwards, once
//! the `resources` table is known to exist and won't be replaced underneath it.

use std::future::Future;
use std::pin::Pin;

use aws_client::Tag;
use chrono::{DateTime, Utc};
use db_client::ConnectionPool;
use indicatif::MultiProgress;
use tempfile::TempPath;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{error, info};

use crate::util;

/// How a fetcher's emitted rows relate to the rows already stored for its
/// claimed types.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Semantics {
    /// The emitted rows are the complete set for the claimed types: anything
    /// stored for those types and not emitted is removed.
    Authoritative,
    /// The emitted rows are inserted or updated; rows not emitted are left
    /// alone. For fetchers that build a cache up over successive builds.
    //
    // No builtin fetcher needs this yet — it's required by the fetcher contract
    // and covered by tests, and exists so the AWS managed policy fetch (which
    // caches expensive per-policy documents across builds) can move onto this
    // seam without reshaping it. See openspec design notes.
    #[allow(dead_code)]
    Merge,
}

/// Everything a fetcher needs to do its job.
pub struct FetchContext {
    /// AWS profile to authenticate with, if any.
    pub profile: Option<String>,
    /// The database, for fetchers that decide what to fetch based on what is
    /// already stored (e.g. diffing version identifiers to avoid refetching
    /// unchanged documents).
    //
    // Unused by the OU fetcher, which needs nothing but a profile. Part of the
    // contract regardless: a fetcher that can't read the database can't express
    // the managed policy fetch, and finding that out after the trait is
    // load-bearing would be expensive. Exercised by tests.
    #[allow(dead_code)]
    pub db: ConnectionPool,
    /// Sink for the rows a fetcher emits.
    pub rows: mpsc::Sender<Row>,
    pub progress: MultiProgress,
}

/// One resource, as a fetcher describes it.
///
/// Carries only what a fetcher can know. Every other column of the `resources`
/// table is supplied by [`Row`]'s conversion into a spine JSON line, so a
/// fetcher cannot get those fields wrong by omitting or misshaping them --
/// several of them have constraints that fail silently rather than loudly. See
/// the conversion for what they are and why.
#[derive(Clone, Debug)]
pub struct Row {
    pub arn: String,
    pub account_id: String,
    pub aws_region: String,
    pub resource_type: String,
    pub resource_id: String,
    pub resource_name: Option<String>,
    /// When the fetcher observed this resource.
    ///
    /// Passed in rather than read from the clock during conversion, so a row's
    /// recorded time is the time of the fetch and stays determinable in tests.
    /// Stored in the column AWS Config uses for its own capture time; for a
    /// fetched row there is no equivalent of Config's recording time, so this
    /// is the only time available.
    pub captured_at: DateTime<Utc>,
    /// Key/value entries, in the order the source API returned them. Not a map:
    /// nothing between the API and storage looks a tag up by key.
    pub tags: Vec<Tag>,
    /// Type-specific detail, shaped however the resource type warrants.
    pub configuration: serde_json::Value,
}

/// Serialise a row into one JSON line shaped like the `resources` table.
///
/// The single place the spine-wide fields are decided. Each of the constants
/// below is load-bearing:
///
/// - `tags` serialise as `{key, value}` entries because
///   [`crate::db::land_fetched_rows`] stages the column as
///   `STRUCT(key, value)[]` and converts it with `map_from_entries`. A JSON
///   object here fails the `COPY`.
/// - `supplementaryConfiguration` is an empty object, not null.
///   `build_resource_derived_table` unnests this column, and UNNEST rejects a
///   JSON-typed all-null column -- a row with a null here lands on the spine
///   fine but silently loses its derived table. Config's own rows always carry
///   an object here, so this matches.
/// - The Config-only columns are null rather than a sentinel, so nothing
///   downstream reads a placeholder as a real value.
/// - The line is newline-terminated, since rows are appended to a spool file
///   that is read back one row per line.
impl From<Row> for String {
    fn from(row: Row) -> Self {
        let mut line = serde_json::json!({
            "arn": row.arn,
            "accountId": row.account_id,
            "awsRegion": row.aws_region,
            "resourceType": row.resource_type,
            "resourceId": row.resource_id,
            "resourceName": row.resource_name,
            "configurationItemCaptureTime": row.captured_at.to_rfc3339(),
            "tags": row.tags,
            "configuration": row.configuration,
            // Config-only concepts with no equivalent for a fetched resource.
            "availabilityZone": null,
            "resourceCreationTime": null,
            "configurationItemStatus": null,
            "configurationStateId": null,
            // No fetcher expresses relationships yet.
            "relationships": [],
            "supplementaryConfiguration": {},
        })
        .to_string();
        line.push('\n');
        line
    }
}

/// The future returned by [`Fetcher::fetch`].
///
/// Boxed rather than `impl Future` so the trait stays dyn-compatible: builds
/// hold their fetchers as a `Vec<Box<dyn Fetcher>>`, which `-> impl Future`
/// would forbid. One allocation per fetch is nothing next to the API calls it
/// wraps.
pub type FetchFuture<'a> = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send + 'a>>;

/// A source of resource rows that AWS Config does not provide.
pub trait Fetcher: Send + Sync {
    /// Stable identifier, used in logs and progress bars.
    fn name(&self) -> &'static str;

    /// The `resourceType` values this fetcher owns on the spine.
    ///
    /// Declared up front, before any fetching, so Config's identifier-driven
    /// delete and its incremental cutoff can both be scoped to exclude them.
    fn resource_types(&self) -> &[&str];

    fn semantics(&self) -> Semantics;

    /// Emit rows into `ctx.rows`.
    ///
    /// # Errors
    /// If the underlying source can't be read. An error means nothing lands and
    /// previously stored rows are kept — an error is never mistaken for "this
    /// type is now empty".
    fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a>;
}

/// Rows produced by a fetcher, waiting to be landed on the spine.
pub struct FetchOutput {
    pub name: String,
    pub resource_types: Vec<String>,
    pub semantics: Semantics,
    /// JSON lines shaped like the `resources` table. May be empty, which under
    /// [`Semantics::Authoritative`] legitimately means "this type has no rows".
    pub path: TempPath,
}

/// Run a fetcher, collecting its rows into a temporary file.
///
/// Generic over the fetcher so tests can drive it with a fake.
///
/// Returns `Ok(None)` when the fetcher itself failed: the failure is logged and
/// nothing is landed, so existing rows survive. This is the difference between
/// "the fetch broke" and "the fetch found nothing", which
/// [`Semantics::Authoritative`] would otherwise conflate into deleting
/// everything.
///
/// # Errors
/// If the temporary file backing the rows can't be written.
pub async fn fetch_rows<F: Fetcher + ?Sized>(
    fetcher: &F,
    pool: &ConnectionPool,
    profile: Option<String>,
    progress: &MultiProgress,
) -> anyhow::Result<Option<FetchOutput>> {
    let (rows, file_handle) = row_file_adapter();

    let ctx = FetchContext {
        profile,
        db: pool.clone(),
        rows,
        progress: progress.clone(),
    };

    info!(fetcher = fetcher.name(), "fetching");
    let result = fetcher.fetch(&ctx).await;

    // Drop the sender so the writer task can finish and hand back the path.
    drop(ctx);
    let path = file_handle.await??;

    match result {
        Ok(()) => Ok(Some(FetchOutput {
            name: fetcher.name().to_owned(),
            resource_types: fetcher
                .resource_types()
                .iter()
                .map(|t| (*t).to_owned())
                .collect(),
            semantics: fetcher.semantics(),
            path,
        })),
        Err(error) => {
            error!(
                fetcher = fetcher.name(),
                %error,
                "fetcher failed; keeping existing rows for its resource types"
            );
            Ok(None)
        }
    }
}

/// A `Row` sink that serialises into the spool file.
///
/// The spool stays a `String` sink -- it is shared with the Config fetch, which
/// has no use for `Row` -- so this forwards between the two, serialising on the
/// way.
///
/// The forwarding task's outcome is folded into the returned handle rather than
/// discarded. A dropped line is a truncated row set, and under
/// [`Semantics::Authoritative`] a truncated row set is indistinguishable from a
/// shrunken one: landing it would delete every row the truncation lost. This
/// preserves [`util::temp_file_writer`]'s all-or-nothing guarantee rather than
/// quietly opening a hole in it.
fn row_file_adapter() -> (mpsc::Sender<Row>, JoinHandle<anyhow::Result<TempPath>>) {
    let (lines, file_handle) = util::temp_file_writer();
    let (tx, mut rx) = mpsc::channel::<Row>(8);

    let handle = tokio::spawn(async move {
        // Keep draining after a failure, so a still-sending fetcher never
        // blocks on a full channel waiting for a forwarder that has given up.
        let mut result = Ok(());
        while let Some(row) = rx.recv().await {
            if result.is_ok() && lines.send(row.into()).await.is_err() {
                result = Err(anyhow::anyhow!("row spool stopped accepting rows"));
            }
        }

        // The writer only finishes once every sender is gone.
        drop(lines);

        // A write or flush failure is the more specific error, so it wins.
        let path = file_handle.await??;
        result.map(|()| path)
    });

    (tx, handle)
}

/// Fail if AWS Config has started recording a type a fetcher claims.
///
/// Config gaining support for a claimed type would otherwise be silent data
/// loss: Config would write rows, and the fetcher's [`Semantics::Authoritative`]
/// replace would delete them, with no code change on our side to notice.
///
/// # Errors
/// If any claimed type appears in `config_types`.
pub fn check_claimed_types_free<'a>(
    config_types: impl IntoIterator<Item = &'a str>,
    claimed: &[&str],
) -> anyhow::Result<()> {
    let collisions: Vec<&str> = config_types
        .into_iter()
        .filter(|t| claimed.contains(t))
        .collect();

    anyhow::ensure!(
        collisions.is_empty(),
        "AWS Config now records resource type(s) claimed by a fetcher: {}. \
         Landing fetched rows would delete Config's own data for those types. \
         Remove the claim, or give the fetcher a distinct resource type.",
        collisions.join(", ")
    );

    Ok(())
}

#[cfg(test)]
pub mod test_support {
    use super::{FetchContext, FetchFuture, Fetcher, Row, Semantics};
    use chrono::Utc;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    /// A minimal [`Row`], identified by `resource_id`.
    ///
    /// Everything else is filler: these tests are about the seam carrying rows,
    /// not about what any particular row says.
    pub fn test_row(resource_id: &str) -> Row {
        Row {
            arn: format!("arn:test:::{resource_id}"),
            account_id: "111111111111".to_owned(),
            aws_region: "global".to_owned(),
            resource_type: "Test::Thing::Widget".to_owned(),
            resource_id: resource_id.to_owned(),
            resource_name: None,
            captured_at: Utc::now(),
            tags: Vec::new(),
            configuration: serde_json::json!({}),
        }
    }

    /// A [`Fetcher`] that emits canned rows without touching the network.
    pub struct FakeFetcher {
        pub name: &'static str,
        pub resource_types: Vec<&'static str>,
        pub semantics: Semantics,
        /// Rows to emit before considering `fail`.
        pub rows: Vec<Row>,
        /// Emit `rows`, then fail — the shape of a fetcher that dies partway.
        pub fail: bool,
        ran: Arc<AtomicBool>,
    }

    impl FakeFetcher {
        pub fn new(resource_type: &'static str, semantics: Semantics) -> Self {
            Self {
                name: "fake",
                resource_types: vec![resource_type],
                semantics,
                rows: Vec::new(),
                fail: false,
                ran: Arc::new(AtomicBool::new(false)),
            }
        }

        #[must_use]
        pub fn with_rows(mut self, rows: Vec<Row>) -> Self {
            self.rows = rows;
            self
        }

        #[must_use]
        pub fn failing(mut self) -> Self {
            self.fail = true;
            self
        }

        /// Handle that reports whether [`Fetcher::fetch`] was ever entered.
        pub fn ran_flag(&self) -> Arc<AtomicBool> {
            self.ran.clone()
        }
    }

    impl Fetcher for FakeFetcher {
        fn name(&self) -> &'static str {
            self.name
        }

        fn resource_types(&self) -> &[&str] {
            &self.resource_types
        }

        fn semantics(&self) -> Semantics {
            self.semantics
        }

        fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a> {
            Box::pin(async move {
                self.ran.store(true, Ordering::SeqCst);
                for row in &self.rows {
                    ctx.rows.send(row.clone()).await?;
                }
                anyhow::ensure!(!self.fail, "fake fetcher failure");
                Ok(())
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{FakeFetcher, test_row};
    use super::*;
    use crate::db;
    use std::sync::atomic::Ordering;

    const TEST_TYPE: &str = "Test::Thing::Widget";

    async fn read_rows(output: &FetchOutput) -> String {
        tokio::fs::read_to_string(&output.path).await.unwrap()
    }

    // --- Row serialisation ---------------------------------------------------
    //
    // These cover the fields a fetcher no longer writes. Each constraint here
    // fails silently rather than loudly when broken, which is why they are
    // asserted directly on the conversion rather than only through a fetcher.

    fn serialise(row: Row) -> serde_json::Value {
        let line = String::from(row);
        assert!(line.ends_with('\n'), "rows must be newline delimited");
        serde_json::from_str(&line).expect("emitted row should be valid JSON")
    }

    #[test]
    fn emits_one_json_object_per_line() {
        let line = String::from(test_row("a"));

        assert_eq!(line.matches('\n').count(), 1, "got {line:?}");
        assert!(
            serde_json::from_str::<serde_json::Value>(&line)
                .unwrap()
                .is_object()
        );
    }

    #[test]
    fn config_only_fields_are_null_not_sentinels() {
        let row = serialise(test_row("a"));

        for field in [
            "configurationItemStatus",
            "configurationStateId",
            "resourceCreationTime",
            "availabilityZone",
        ] {
            assert!(
                row[field].is_null(),
                "{field} should be null, got {}",
                row[field]
            );
        }
    }

    /// Regression: a null here breaks `build_resource_derived_table`, which
    /// unnests the column. The rows still land, so the only visible symptom is
    /// a missing derived table and a swallowed error in the log.
    #[test]
    fn supplementary_configuration_is_an_object_not_null() {
        let row = serialise(test_row("a"));

        assert!(
            row["supplementaryConfiguration"].is_object(),
            "must be an object for the derived-table unnest to work, got {}",
            row["supplementaryConfiguration"]
        );
    }

    /// Regression: an object here fails the `COPY` in `land_fetched_rows`,
    /// which stages `tags` as `STRUCT(key, value)[]`.
    #[test]
    fn tags_are_emitted_as_key_value_entries() {
        let mut source = test_row("a");
        source.tags.push(Tag {
            key: "Environment".to_owned(),
            value: "prod".to_owned(),
        });

        let row = serialise(source);

        let tags = row["tags"].as_array().expect("tags must be an array");
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["key"], "Environment");
        assert_eq!(tags[0]["value"], "prod");
    }

    #[test]
    fn untagged_row_emits_empty_tags_not_null() {
        let row = serialise(test_row("a"));

        assert_eq!(
            row["tags"].as_array().expect("tags must be an array").len(),
            0
        );
    }

    #[test]
    fn capture_time_is_the_observation_time() {
        let captured_at = DateTime::parse_from_rfc3339("2026-07-17T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut source = test_row("a");
        source.captured_at = captured_at;

        let row = serialise(source);

        let emitted: DateTime<Utc> = row["configurationItemCaptureTime"]
            .as_str()
            .unwrap()
            .parse()
            .unwrap();
        assert_eq!(emitted, captured_at);
    }

    #[test]
    fn serialisation_does_not_read_the_clock() {
        let row = test_row("a");

        assert_eq!(
            serialise(row.clone())["configurationItemCaptureTime"],
            serialise(row)["configurationItemCaptureTime"]
        );
    }

    #[test]
    fn identity_fields_map_onto_their_columns() {
        let row = serialise(test_row("a"));

        assert_eq!(row["resourceId"], "a");
        assert_eq!(row["arn"], "arn:test:::a");
        assert_eq!(row["accountId"], "111111111111");
        assert_eq!(row["awsRegion"], "global");
        assert_eq!(row["resourceType"], TEST_TYPE);
    }

    #[tokio::test]
    async fn successful_fetch_yields_its_rows() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        let fetcher =
            FakeFetcher::new(TEST_TYPE, Semantics::Authoritative).with_rows(vec![test_row("a")]);

        let output = fetch_rows(&fetcher, &pool, None, &MultiProgress::new())
            .await
            .unwrap()
            .expect("a successful fetch should produce output");

        assert_eq!(output.resource_types, vec![TEST_TYPE.to_owned()]);
        assert_eq!(output.semantics, Semantics::Authoritative);
        assert!(read_rows(&output).await.contains(r#""resourceId":"a""#));
    }

    /// The soft-fail guarantee. A fetcher that dies must not be mistaken for one
    /// that found nothing, or `Authoritative` semantics would delete every row
    /// for its types on a transient API error.
    #[tokio::test]
    async fn failed_fetch_yields_no_output_to_land() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        let fetcher = FakeFetcher::new(TEST_TYPE, Semantics::Authoritative)
            .with_rows(vec![test_row("a")])
            .failing();

        let output = fetch_rows(&fetcher, &pool, None, &MultiProgress::new())
            .await
            .unwrap();

        assert!(
            output.is_none(),
            "a failed fetch must land nothing, even though it emitted rows first"
        );
    }

    /// A fetch that genuinely finds nothing is *not* a failure: it produces
    /// output, and landing it clears the claimed types.
    #[tokio::test]
    async fn successful_empty_fetch_still_yields_output() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        let fetcher = FakeFetcher::new(TEST_TYPE, Semantics::Authoritative);

        let output = fetch_rows(&fetcher, &pool, None, &MultiProgress::new())
            .await
            .unwrap();

        assert!(
            output.is_some(),
            "finding nothing is not the same as failing"
        );
        assert!(read_rows(&output.unwrap()).await.is_empty());
    }

    #[tokio::test]
    async fn fetcher_runs_when_driven() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        let fetcher = FakeFetcher::new(TEST_TYPE, Semantics::Merge);
        let ran = fetcher.ran_flag();

        assert!(!ran.load(Ordering::SeqCst));
        fetch_rows(&fetcher, &pool, None, &MultiProgress::new())
            .await
            .unwrap();
        assert!(ran.load(Ordering::SeqCst));
    }

    /// Decides what to emit by querying what's already stored — a miniature of
    /// the managed-policy fetch, where a cheap listing is diffed against stored
    /// versions before the expensive per-item fetch.
    struct DbReadingFetcher;

    impl Fetcher for DbReadingFetcher {
        fn name(&self) -> &'static str {
            "db-reading"
        }
        fn resource_types(&self) -> &[&str] {
            &[TEST_TYPE]
        }
        fn semantics(&self) -> Semantics {
            Semantics::Merge
        }
        fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a> {
            Box::pin(async move {
                let stale: Vec<String> = ctx
                    .db
                    .get()
                    .await?
                    .with_conn(|c| {
                        Ok(c.prepare("SELECT id FROM stored WHERE version = 'v1'")?
                            .query_map([], |row| row.get::<_, String>(0))?
                            .filter_map(Result::ok)
                            .collect())
                    })
                    .await?;

                for id in stale {
                    ctx.rows.send(test_support::test_row(&id)).await?;
                }
                Ok(())
            })
        }
    }

    /// The context must give a fetcher database access, so it can base what it
    /// fetches on what is already stored.
    #[tokio::test]
    async fn fetcher_can_read_the_database_while_fetching() {
        let pool = db::connect_to_db_in_memory().await.unwrap();
        pool.get()
            .await
            .unwrap()
            .with_conn(|c| {
                c.execute_batch(
                    "CREATE TABLE stored (id VARCHAR, version VARCHAR);
                     INSERT INTO stored VALUES ('a', 'v1'), ('b', 'v1');",
                )
            })
            .await
            .unwrap();

        let output = fetch_rows(&DbReadingFetcher, &pool, None, &MultiProgress::new())
            .await
            .unwrap()
            .expect("fetch should succeed");

        let rows = read_rows(&output).await;
        assert!(rows.contains(r#""resourceId":"a""#));
        assert!(rows.contains(r#""resourceId":"b""#));
    }

    #[test]
    fn claimed_types_free_when_config_records_none_of_them() {
        let result = check_claimed_types_free(
            ["AWS::EC2::Instance", "AWS::Organizations::Policy"],
            &["AWS::Organizations::OrganizationalUnit"],
        );
        assert!(result.is_ok());
    }

    #[test]
    fn claimed_type_recorded_by_config_is_rejected() {
        let result = check_claimed_types_free(
            [
                "AWS::EC2::Instance",
                "AWS::Organizations::OrganizationalUnit",
            ],
            &["AWS::Organizations::OrganizationalUnit"],
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("AWS::Organizations::OrganizationalUnit"),
            "error should name the colliding type, got: {err}"
        );
    }

    #[test]
    fn no_claims_never_collides() {
        assert!(check_claimed_types_free(["AWS::EC2::Instance"], &[]).is_ok());
    }
}
