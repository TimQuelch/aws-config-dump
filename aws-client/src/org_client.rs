// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use aws_smithy_types_convert::stream::PaginationStreamExt;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use indicatif::ProgressBar;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{Tag, sdk_config};

/// In-flight Organizations requests. Organizations throttles hard and per-org
/// OU counts are modest, so this is kept low enough to stay clear of the
/// service's rate limits while still removing the serial round-trip cost.
const CONCURRENCY: usize = 8;

/// An Organizational Unit, flattened out of the AWS Organizations tree.
///
/// The tree structure is carried by `parent_id` rather than by nesting, so that
/// callers can store OUs as flat rows and rebuild the hierarchy with a recursive
/// query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrganizationalUnit {
    pub id: String,
    pub arn: String,
    pub name: String,
    /// The parent OU or root this OU sits directly beneath.
    pub parent_id: String,
    pub tags: Vec<Tag>,
}

/// A handle onto AWS Organizations.
///
/// Holds the SDK client so a caller can make several calls against one client,
/// which the OU pipeline needs — the tree walk and the tag pass are separate
/// stages sharing a client rather than one call building its own.
#[derive(Clone, Debug)]
pub struct OrgClient {
    client: aws_sdk_organizations::Client,
}

impl OrgClient {
    /// Build a client for `profile`, or the default credential chain.
    pub async fn new(profile: Option<String>) -> Self {
        let config = sdk_config::load_config(profile).await;
        Self::from_client(aws_sdk_organizations::Client::new(&config))
    }

    /// The mock seam: tests construct a handle around a mocked SDK client.
    fn from_client(client: aws_sdk_organizations::Client) -> Self {
        Self { client }
    }

    /// Get the AWS Organization's management account ID.
    ///
    /// # Errors
    /// If the API query fails, or the organization has no management account ID
    pub async fn fetch_management_account_id(&self) -> anyhow::Result<String> {
        self.client
            .describe_organization()
            .send()
            .await?
            .organization
            .and_then(|o| o.master_account_id)
            .ok_or_else(|| anyhow::anyhow!("organization has no management account id"))
    }

    /// Stream every Organizational Unit in the organization, at every depth,
    /// each carrying its own tags.
    ///
    /// `progress`'s *length* is raised as the walk discovers each OU, so a
    /// caller can report a total for an API that offers no count endpoint. The
    /// caller raises the position as it consumes; discovery runs eagerly in its
    /// own task, so the total leads rather than tracking the position.
    ///
    /// Errors arrive as `Err` items rather than ending the stream: under
    /// `Authoritative` semantics a silently truncated tree reads as a mass
    /// delete, so a failed walk must never look like a small organization.
    pub fn organizational_units(
        &self,
        progress: ProgressBar,
    ) -> impl Stream<Item = anyhow::Result<OrganizationalUnit>> + Send + 'static {
        let (tx, rx) = mpsc::channel(8);
        let discovery = tokio::spawn(discover_organizational_units(
            self.client.clone(),
            tx,
            progress,
        ));

        let client = self.client.clone();
        let tagged = ReceiverStream::new(rx)
            .map(move |discovered| {
                let client = client.clone();
                async move {
                    let mut ou = discovered?;
                    ou.tags = list_tags_for_resource(&client, &ou.id).await?;
                    Ok(ou)
                }
            })
            .buffer_unordered(CONCURRENCY);

        // A panicking producer drops its sender, which closes the channel and
        // looks exactly like a completed walk. Joining the task after the items
        // are drained turns that back into an error.
        let join = stream::once(async move {
            match discovery.await {
                Ok(()) => None,
                Err(error) => Some(Err(anyhow::anyhow!(
                    "organizational unit discovery task failed: {error}"
                ))),
            }
        })
        .filter_map(|outcome| async move { outcome });

        tagged.chain(join)
    }
}

/// Walk the OU tree, pushing each OU out as it is discovered.
async fn discover_organizational_units(
    client: aws_sdk_organizations::Client,
    tx: mpsc::Sender<anyhow::Result<OrganizationalUnit>>,
    progress: ProgressBar,
) {
    if let Err(error) = walk_organizational_units(&client, &tx, &progress).await {
        // If this fails the consumer is already gone, so there is nobody left
        // to tell about the failure.
        let _ = tx.send(Err(error)).await;
    }
}

async fn walk_organizational_units(
    client: &aws_sdk_organizations::Client,
    tx: &mpsc::Sender<anyhow::Result<OrganizationalUnit>>,
    progress: &ProgressBar,
) -> anyhow::Result<()> {
    let roots: Vec<_> = client
        .list_roots()
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_err(anyhow::Error::from)
        .map_ok(|page| {
            page.roots
                .map(|rs| rs.into_iter().filter_map(|r| r.id).collect())
                .unwrap_or_default()
        })
        .try_concat()
        .await?;

    // Breadth-first: an OU's children are only discoverable once we know its id,
    // so each level's ids seed the next level's queries. Within a level the
    // listings are independent, so they run concurrently — a serial walk costs
    // one round trip per OU, which dominates the fetch on any real org, and
    // could not outrun 8-wide enrichment.
    let mut frontier = roots;

    while !frontier.is_empty() {
        let mut next_frontier = Vec::new();
        let mut listings = stream::iter(frontier)
            .map(|parent_id| {
                let client = client.clone();
                async move { list_organizational_units_for_parent(&client, &parent_id).await }
            })
            .buffer_unordered(CONCURRENCY);

        while let Some(children) = listings.next().await {
            for ou in children? {
                next_frontier.push(ou.id.clone());
                // Raised on discovery, before the send: the total has to grow
                // as work is found rather than being known up front.
                progress.inc_length(1);
                if tx.send(Ok(ou)).await.is_err() {
                    // Consumer dropped the stream; nothing left to walk for.
                    return Ok(());
                }
            }
        }

        frontier = next_frontier;
    }

    Ok(())
}

async fn list_organizational_units_for_parent(
    client: &aws_sdk_organizations::Client,
    parent_id: &str,
) -> anyhow::Result<Vec<OrganizationalUnit>> {
    client
        .list_organizational_units_for_parent()
        .parent_id(parent_id)
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_err(anyhow::Error::from)
        .map_ok(|page| {
            page.organizational_units
                .map(|ous| {
                    ous.into_iter()
                        .map(|ou| OrganizationalUnit {
                            id: ou.id.expect("id must be defined"),
                            arn: ou.arn.unwrap_or_default(),
                            name: ou.name.unwrap_or_default(),
                            parent_id: parent_id.to_string(),
                            tags: Vec::new(),
                        })
                        .collect()
                })
                .unwrap_or_default()
        })
        .try_concat()
        .await
}

async fn list_tags_for_resource(
    client: &aws_sdk_organizations::Client,
    resource_id: &str,
) -> anyhow::Result<Vec<Tag>> {
    client
        .list_tags_for_resource()
        .resource_id(resource_id)
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_err(anyhow::Error::from)
        .map_ok(|page| {
            page.tags
                .map(|tags| tags.into_iter().map(Into::into).collect())
                .unwrap_or_default()
        })
        .try_concat()
        .await
}

/// Get AWS Organizations account information
///
/// # Errors
/// If any API query fails
pub async fn fetch_org_accounts(
    profile: Option<String>,
) -> anyhow::Result<Vec<(String, Option<String>)>> {
    let config = sdk_config::load_config(profile).await;
    fetch_org_accounts_inner(aws_sdk_organizations::Client::new(&config)).await
}

async fn fetch_org_accounts_inner(
    client: aws_sdk_organizations::Client,
) -> anyhow::Result<Vec<(String, Option<String>)>> {
    client
        .list_accounts()
        .into_paginator()
        .send()
        .into_stream_03x()
        .map_err(anyhow::Error::from)
        .map_ok(|page| {
            page.accounts
                .map(|accounts| {
                    accounts
                        .into_iter()
                        .filter_map(|a| a.id.map(|id| (id, a.name)))
                        .collect()
                })
                .unwrap_or_default()
        })
        .try_concat()
        .await
}

#[cfg(test)]
mod tests {
    use aws_sdk_organizations::{
        Client,
        operation::{
            describe_organization::DescribeOrganizationOutput, list_accounts::ListAccountsOutput,
            list_organizational_units_for_parent::ListOrganizationalUnitsForParentOutput,
            list_roots::ListRootsOutput, list_tags_for_resource::ListTagsForResourceOutput,
        },
        types::{Account, Organization, OrganizationalUnit as SdkOu, Root, Tag},
    };
    use aws_smithy_mocks::{RuleMode, mock, mock_client};

    use super::*;

    fn sdk_ou(id: &str) -> SdkOu {
        SdkOu::builder()
            .id(id)
            .arn(format!("arn:aws:organizations::111111111111:ou/o-org/{id}"))
            .name(format!("name-of-{id}"))
            .build()
    }

    /// A rule serving `children` when asked for `parent_id`'s OUs.
    fn children_of(parent_id: &'static str, children: Vec<&'static str>) -> aws_smithy_mocks::Rule {
        mock!(Client::list_organizational_units_for_parent)
            .match_requests(move |req| req.parent_id() == Some(parent_id))
            .then_output(move || {
                let mut builder = ListOrganizationalUnitsForParentOutput::builder();
                for child in &children {
                    builder = builder.organizational_units(sdk_ou(child));
                }
                builder.build()
            })
    }

    fn roots_rule(root_id: &'static str) -> aws_smithy_mocks::Rule {
        mock!(Client::list_roots).then_output(move || {
            ListRootsOutput::builder()
                .roots(Root::builder().id(root_id).build())
                .build()
        })
    }

    /// Look one tag up by key.
    ///
    /// Tags are a list rather than a map because nothing in the production path
    /// looks one up; only assertions do, so the lookup lives here.
    fn tag_value<'a>(tags: &'a [crate::Tag], key: &str) -> Option<&'a str> {
        tags.iter().find(|t| t.key == key).map(|t| t.value.as_str())
    }

    fn no_tags_rule() -> aws_smithy_mocks::Rule {
        mock!(Client::list_tags_for_resource)
            .then_output(|| ListTagsForResourceOutput::builder().build())
    }

    /// One rule per OU, each serving `Owner=owner-of-<id>` only for that OU.
    ///
    /// Uniform tag fixtures cannot detect misattribution: if the concurrent tag
    /// pass zipped results onto the wrong OUs, assertions against a single
    /// shared tag value would still pass. These are per-OU on purpose.
    fn distinct_tags_rules(ids: &[&'static str]) -> Vec<aws_smithy_mocks::Rule> {
        ids.iter()
            .map(|id| {
                let id = *id;
                mock!(Client::list_tags_for_resource)
                    .match_requests(move |req| req.resource_id() == Some(id))
                    .then_output(move || {
                        ListTagsForResourceOutput::builder()
                            .tags(
                                Tag::builder()
                                    .key("Owner")
                                    .value(format!("owner-of-{id}"))
                                    .build()
                                    .expect("valid tag"),
                            )
                            .build()
                    })
            })
            .collect()
    }

    /// A bar that records length and position without drawing anything.
    ///
    /// Not `ProgressBar::hidden()`, whose length starts at `u64::MAX` — these
    /// tests assert on the length the stream grows.
    fn silent_bar() -> ProgressBar {
        ProgressBar::with_draw_target(Some(0), indicatif::ProgressDrawTarget::hidden())
    }

    /// Drain the OU stream into a `Vec`, as the pre-streaming walk returned one.
    ///
    /// Lets the scenarios below assert the same facts they did before the
    /// refactor: what is emitted is unchanged, only how it arrives.
    async fn collect_ous(client: Client) -> anyhow::Result<Vec<OrganizationalUnit>> {
        OrgClient::from_client(client)
            .organizational_units(silent_bar())
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn fetch_management_account_id_returns_master_account() {
        let rule = mock!(Client::describe_organization).then_output(|| {
            DescribeOrganizationOutput::builder()
                .organization(
                    Organization::builder()
                        .id("o-exampleorgid")
                        .master_account_id("111111111111")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_organizations, [&rule]);
        let result = OrgClient::from_client(client)
            .fetch_management_account_id()
            .await
            .unwrap();
        assert_eq!(result, "111111111111");
    }

    /// The whole point of the recursive walk: OUs nested below the root's direct
    /// children must still be found.
    #[tokio::test]
    async fn fetch_organizational_units_returns_deeply_nested_ous() {
        let roots = roots_rule("r-root");
        // r-root > ou-depth1 > ou-depth2 > ou-depth3
        let level1 = children_of("r-root", vec!["ou-depth1"]);
        let level2 = children_of("ou-depth1", vec!["ou-depth2"]);
        let level3 = children_of("ou-depth2", vec!["ou-depth3"]);
        let level4 = children_of("ou-depth3", vec![]);
        let tags = no_tags_rule();

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &level2, &level3, &level4, &tags]
        );
        let result = collect_ous(client).await.unwrap();

        let ids: Vec<&str> = result.iter().map(|ou| ou.id.as_str()).collect();
        assert_eq!(ids.len(), 3, "expected every nesting level, got {ids:?}");
        assert!(ids.contains(&"ou-depth1"));
        assert!(ids.contains(&"ou-depth2"), "missed a grandchild OU");
        assert!(ids.contains(&"ou-depth3"), "missed a great-grandchild OU");
    }

    #[tokio::test]
    async fn fetch_organizational_units_records_parent_of_each_ou() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-parent"]);
        let level2 = children_of("ou-parent", vec!["ou-child"]);
        let level3 = children_of("ou-child", vec![]);
        let tags = no_tags_rule();

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &level2, &level3, &tags]
        );
        let result = collect_ous(client).await.unwrap();

        let parent_of = |id: &str| {
            result
                .iter()
                .find(|ou| ou.id == id)
                .unwrap_or_else(|| panic!("{id} not fetched"))
                .parent_id
                .clone()
        };
        assert_eq!(parent_of("ou-parent"), "r-root");
        assert_eq!(parent_of("ou-child"), "ou-parent");
    }

    /// Pagination is mocked at a *nested* level, not at the root. A walk that
    /// only paginates the first level would pass a root-only test while
    /// silently truncating children deeper in the tree.
    #[tokio::test]
    async fn fetch_organizational_units_paginates_at_nested_level() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-parent"]);

        let nested_pages = mock!(Client::list_organizational_units_for_parent)
            .match_requests(|req| req.parent_id() == Some("ou-parent"))
            .sequence()
            .output(|| {
                ListOrganizationalUnitsForParentOutput::builder()
                    .organizational_units(sdk_ou("ou-page1"))
                    .next_token("page2-token")
                    .build()
            })
            .output(|| {
                ListOrganizationalUnitsForParentOutput::builder()
                    .organizational_units(sdk_ou("ou-page2"))
                    .build()
            })
            .build();

        let leaves = mock!(Client::list_organizational_units_for_parent)
            .match_requests(|req| matches!(req.parent_id(), Some("ou-page1" | "ou-page2")))
            .then_output(|| ListOrganizationalUnitsForParentOutput::builder().build());
        let tags = no_tags_rule();

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &nested_pages, &leaves, &tags]
        );
        let result = collect_ous(client).await.unwrap();

        let ids: Vec<&str> = result.iter().map(|ou| ou.id.as_str()).collect();
        assert!(
            ids.contains(&"ou-page1") && ids.contains(&"ou-page2"),
            "second page of a nested OU listing was dropped, got {ids:?}"
        );
    }

    #[tokio::test]
    async fn fetch_organizational_units_attaches_tags() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-tagged"]);
        let level2 = children_of("ou-tagged", vec![]);
        let tags = mock!(Client::list_tags_for_resource).then_output(|| {
            ListTagsForResourceOutput::builder()
                .tags(
                    Tag::builder()
                        .key("Environment")
                        .value("prod")
                        .build()
                        .expect("valid tag"),
                )
                .build()
        });

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &level2, &tags]
        );
        let result = collect_ous(client).await.unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(tag_value(&result[0].tags, "Environment"), Some("prod"));
    }

    /// Every OU must receive *its own* tags. The tag pass runs concurrently, so
    /// results come back out of order; they are zipped onto the OU list by
    /// position, and an unordered combinator here would silently give each OU
    /// some other OU's tags. More OUs than the concurrency bound, so the
    /// scheduler genuinely interleaves them.
    #[tokio::test]
    async fn fetch_organizational_units_gives_each_ou_its_own_tags() {
        const IDS: [&str; 12] = [
            "ou-00", "ou-01", "ou-02", "ou-03", "ou-04", "ou-05", "ou-06", "ou-07", "ou-08",
            "ou-09", "ou-10", "ou-11",
        ];
        assert!(
            IDS.len() > CONCURRENCY,
            "fixture must exceed the concurrency bound to exercise interleaving"
        );

        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", IDS.to_vec());
        let leaves: Vec<_> = IDS.iter().map(|id| children_of(id, vec![])).collect();
        let tags = distinct_tags_rules(&IDS);

        let mut rules: Vec<&aws_smithy_mocks::Rule> = vec![&roots, &level1];
        rules.extend(leaves.iter());
        rules.extend(tags.iter());

        let client = mock_client!(aws_sdk_organizations, RuleMode::MatchAny, &rules);
        let result = collect_ous(client).await.unwrap();

        assert_eq!(result.len(), IDS.len());
        for ou in &result {
            assert_eq!(
                tag_value(&ou.tags, "Owner"),
                Some(format!("owner-of-{}", ou.id).as_str()),
                "{} got another OU's tags: {:?}",
                ou.id,
                ou.tags
            );
        }
    }

    /// A failed listing must not look like a smaller organization. Truncating
    /// silently would hand `Authoritative` semantics a short row set, which it
    /// reads as "these OUs were deleted".
    #[tokio::test]
    async fn fetch_organizational_units_propagates_a_listing_failure() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-ok", "ou-broken"]);
        let ok = children_of("ou-ok", vec![]);
        let broken = mock!(Client::list_organizational_units_for_parent)
            .match_requests(|req| req.parent_id() == Some("ou-broken"))
            .then_error(|| {
                aws_sdk_organizations::operation::list_organizational_units_for_parent::ListOrganizationalUnitsForParentError::unhandled("boom")
            });
        let tags = no_tags_rule();

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &ok, &broken, &tags]
        );

        assert!(
            collect_ous(client).await.is_err(),
            "a failed child listing must surface as an error, not a truncated tree"
        );
    }

    /// Same guarantee for the tag pass: a tag call that fails must fail the
    /// fetch rather than yielding an OU with silently empty tags.
    #[tokio::test]
    async fn fetch_organizational_units_propagates_a_tag_failure() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-a"]);
        let leaf = children_of("ou-a", vec![]);
        let tags = mock!(Client::list_tags_for_resource).then_error(|| {
            aws_sdk_organizations::operation::list_tags_for_resource::ListTagsForResourceError::unhandled("boom")
        });

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &leaf, &tags]
        );

        assert!(
            collect_ous(client).await.is_err(),
            "a failed tag call must surface as an error, not an untagged OU"
        );
    }

    /// An organization that has roots but no OUs is normal, not an error.
    #[tokio::test]
    async fn fetch_organizational_units_empty_organization_is_not_an_error() {
        let roots = roots_rule("r-root");
        let empty = children_of("r-root", vec![]);
        let tags = no_tags_rule();

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &empty, &tags]
        );
        let result = collect_ous(client).await.unwrap();

        assert!(result.is_empty());
    }

    /// The defect this change exists to fix: the bar used to be created only
    /// once every call had returned, so it read `0/N` for the whole fetch.
    /// Discovery now runs ahead of enrichment, so the total leads the position
    /// while the fetch is in flight and the two meet at the end.
    #[tokio::test]
    async fn discovery_leads_completion() {
        const IDS: [&str; 12] = [
            "ou-00", "ou-01", "ou-02", "ou-03", "ou-04", "ou-05", "ou-06", "ou-07", "ou-08",
            "ou-09", "ou-10", "ou-11",
        ];

        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", IDS.to_vec());
        let leaves: Vec<_> = IDS.iter().map(|id| children_of(id, vec![])).collect();
        let tags = no_tags_rule();

        let mut rules: Vec<&aws_smithy_mocks::Rule> = vec![&roots, &level1];
        rules.extend(leaves.iter());
        rules.push(&tags);

        let client = mock_client!(aws_sdk_organizations, RuleMode::MatchAny, &rules);

        let bar = silent_bar();
        assert_eq!(bar.length(), Some(0), "no total is known before the walk");

        let stream = OrgClient::from_client(client).organizational_units(bar.clone());
        tokio::pin!(stream);

        // The consumer stands in for the fetcher: it raises the position as it
        // takes each tagged OU, exactly as `OrganizationalUnitsFetcher` does.
        let mut length_after_first = None;
        let mut received = 0u64;
        while let Some(ou) = stream.next().await {
            ou.expect("no rule fails in this fixture");
            received += 1;
            bar.inc(1);
            if received == 1 {
                length_after_first = bar.length();
            }
        }

        // A lazily pulled walk would have discovered only what this one item
        // demanded, leaving the total a step ahead of the position at most. The
        // eager producer has the whole level queued before the first item is
        // handed over.
        assert!(
            length_after_first.is_some_and(|len| len > 1),
            "after the first OU the total was {length_after_first:?}; discovery is not \
             running ahead of consumption, so the bar would sit pinned at 100%"
        );
        assert_eq!(received, IDS.len() as u64);
        assert_eq!(
            bar.length(),
            Some(IDS.len() as u64),
            "the total should have grown to every discovered OU"
        );
        assert_eq!(
            bar.position(),
            IDS.len() as u64,
            "each OU is counted exactly once"
        );
    }

    /// A truncated stream and a failed one are indistinguishable to a consumer
    /// unless the failure is delivered as an item. Under `Authoritative`
    /// semantics, mistaking one for the other deletes every missing OU.
    #[tokio::test]
    async fn a_failure_arrives_as_an_err_item_not_a_silent_end() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-ok", "ou-broken"]);
        let ok = children_of("ou-ok", vec![]);
        let broken = mock!(Client::list_organizational_units_for_parent)
            .match_requests(|req| req.parent_id() == Some("ou-broken"))
            .then_error(|| {
                aws_sdk_organizations::operation::list_organizational_units_for_parent::ListOrganizationalUnitsForParentError::unhandled("boom")
            });
        let tags = no_tags_rule();

        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::MatchAny,
            [&roots, &level1, &ok, &broken, &tags]
        );

        let items: Vec<anyhow::Result<OrganizationalUnit>> = OrgClient::from_client(client)
            .organizational_units(silent_bar())
            .collect()
            .await;

        assert!(
            items.iter().any(std::result::Result::is_err),
            "the failed listing ended the stream cleanly instead of reporting an error"
        );
    }

    /// The refactor changed how OUs arrive, not what they are. Pins the full
    /// emitted shape so a regression in any field shows up here.
    #[tokio::test]
    async fn emitted_ous_are_unchanged_by_streaming() {
        let roots = roots_rule("r-root");
        let level1 = children_of("r-root", vec!["ou-parent"]);
        let level2 = children_of("ou-parent", vec!["ou-child"]);
        let level3 = children_of("ou-child", vec![]);
        let tags = distinct_tags_rules(&["ou-parent", "ou-child"]);

        let mut rules: Vec<&aws_smithy_mocks::Rule> = vec![&roots, &level1, &level2, &level3];
        rules.extend(tags.iter());

        let client = mock_client!(aws_sdk_organizations, RuleMode::MatchAny, &rules);
        let mut result = collect_ous(client).await.unwrap();
        result.sort_by(|a, b| a.id.cmp(&b.id));

        let expected = |id: &str, parent_id: &str| OrganizationalUnit {
            id: id.to_owned(),
            arn: format!("arn:aws:organizations::111111111111:ou/o-org/{id}"),
            name: format!("name-of-{id}"),
            parent_id: parent_id.to_owned(),
            tags: vec![crate::Tag {
                key: "Owner".to_owned(),
                value: format!("owner-of-{id}"),
            }],
        };

        assert_eq!(
            result,
            vec![
                expected("ou-child", "ou-parent"),
                expected("ou-parent", "r-root"),
            ]
        );
    }

    #[tokio::test]
    async fn fetch_org_accounts_returns_accounts() {
        let rule = mock!(Client::list_accounts).then_output(|| {
            ListAccountsOutput::builder()
                .accounts(
                    Account::builder()
                        .id("111111111111")
                        .name("account-one")
                        .build(),
                )
                .accounts(
                    Account::builder()
                        .id("222222222222")
                        .name("account-two")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(aws_sdk_organizations, [&rule]);
        let result = fetch_org_accounts_inner(client).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("111111111111".to_string(), Some("account-one".to_string()))));
        assert!(result.contains(&("222222222222".to_string(), Some("account-two".to_string()))));
    }

    #[tokio::test]
    async fn fetch_org_accounts_multi_page() {
        let rule1 = mock!(Client::list_accounts).then_output(|| {
            ListAccountsOutput::builder()
                .accounts(
                    Account::builder()
                        .id("111111111111")
                        .name("account-one")
                        .build(),
                )
                .next_token("page2-token")
                .build()
        });
        let rule2 = mock!(Client::list_accounts).then_output(|| {
            ListAccountsOutput::builder()
                .accounts(
                    Account::builder()
                        .id("222222222222")
                        .name("account-two")
                        .build(),
                )
                .build()
        });
        let client = mock_client!(
            aws_sdk_organizations,
            RuleMode::Sequential,
            [&rule1, &rule2]
        );
        let result = fetch_org_accounts_inner(client).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&("111111111111".to_string(), Some("account-one".to_string()))));
        assert!(result.contains(&("222222222222".to_string(), Some("account-two".to_string()))));
    }
}
