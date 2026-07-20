// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

//! Fetches AWS Organizations OUs, which AWS Config does not record.
//!
//! The AWS calls live in [`aws_client::org_client`], where they can be mocked.
//! This module only maps the returned values onto the `resources` schema.

use aws_client::org_client::{OrgClient, OrganizationalUnit};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use serde_json::json;
use tracing::debug;

use crate::fetcher::{FetchContext, FetchFuture, Fetcher, Row, Semantics};
use crate::util;

/// AWS Config records `AWS::Organizations::Policy` but not OUs, so this type is
/// ours to claim. [`crate::fetcher::check_claimed_types_free`] guards against
/// that changing.
pub const RESOURCE_TYPE: &str = "AWS::Organizations::OrganizationalUnit";

/// Organizations is a global service, and `global` is what AWS Config already
/// stores for other global resource types.
const REGION: &str = "global";

pub struct OrganizationalUnitsFetcher;

impl Fetcher for OrganizationalUnitsFetcher {
    fn name(&self) -> &'static str {
        "organizational_units"
    }

    fn resource_types(&self) -> &[&str] {
        &[RESOURCE_TYPE]
    }

    fn semantics(&self) -> Semantics {
        // An organization's OUs are small enough to refetch whole every build,
        // so what we fetch is the complete set: anything missing was deleted.
        Semantics::Authoritative
    }

    fn fetch<'a>(&'a self, ctx: &'a FetchContext) -> FetchFuture<'a> {
        Box::pin(async move {
            let client = OrgClient::new(ctx.profile.clone()).await;
            let management_account_id = client.fetch_management_account_id().await?;

            // Organizations has no count API, so the bar starts empty and the
            // stream grows its total as the walk discovers OUs. This fetcher
            // only advances the position, once per row it emits.
            let bar = ctx
                .progress
                .add(util::progress_bar("fetching organizational units", 0));

            let captured_at = Utc::now();
            let management_account_id = &management_account_id;
            let bar_ref = &bar;

            client
                .organizational_units(bar.clone())
                .try_for_each(|ou| async move {
                    ctx.rows
                        .send(ou_to_row(ou, management_account_id, captured_at))
                        .await?;
                    bar_ref.inc(1);
                    Ok(())
                })
                .await?;

            debug!(count = bar.position(), "fetched organizational units");

            Ok(())
        })
    }
}

/// Map one OU onto a [`Row`].
///
/// Only the fields specific to an OU: the rest of the `resources` shape belongs
/// to [`Row`]'s conversion, not here.
///
/// `captured_at` is when `acd` observed the OU, not when AWS recorded it —
/// Organizations has no equivalent of Config's capture time. It's passed in
/// rather than read from the clock so this stays deterministic under test.
fn ou_to_row(
    ou: OrganizationalUnit,
    management_account_id: &str,
    captured_at: DateTime<Utc>,
) -> Row {
    // The tree is flattened: `ParentId` is what a recursive query walks to
    // rebuild the hierarchy. The identity fields are mirrored here too, as
    // Config's own rows repeat them in `configuration`.
    let configuration = json!({
        "ParentId": ou.parent_id,
    });

    Row {
        arn: ou.arn,
        account_id: management_account_id.to_owned(),
        aws_region: REGION.to_owned(),
        resource_type: RESOURCE_TYPE.to_owned(),
        resource_id: ou.id,
        resource_name: Some(ou.name),
        captured_at,
        tags: ou.tags,
        configuration,
    }
}

#[cfg(test)]
mod tests {
    use aws_client::Tag;

    use super::*;

    fn ou(id: &str, parent_id: &str) -> OrganizationalUnit {
        OrganizationalUnit {
            id: id.to_owned(),
            arn: format!("arn:aws:organizations::111111111111:ou/o-exampleorgid/{id}"),
            name: format!("name-of-{id}"),
            parent_id: parent_id.to_owned(),
            tags: Vec::new(),
        }
    }

    #[test]
    fn maps_identity_fields_onto_resource_columns() {
        let row = ou_to_row(ou("ou-abc", "r-root"), "111111111111", Utc::now());

        assert_eq!(row.resource_type, RESOURCE_TYPE);
        assert_eq!(row.resource_id, "ou-abc");
        assert_eq!(row.resource_name.as_deref(), Some("name-of-ou-abc"));
        assert_eq!(
            row.arn,
            "arn:aws:organizations::111111111111:ou/o-exampleorgid/ou-abc"
        );
    }

    #[test]
    fn attributes_to_management_account_and_global_region() {
        let row = ou_to_row(ou("ou-abc", "r-root"), "999999999999", Utc::now());

        assert_eq!(row.account_id, "999999999999");
        assert_eq!(row.aws_region, "global");
    }

    #[test]
    fn records_parent_for_hierarchy_reconstruction() {
        let row = ou_to_row(ou("ou-child", "ou-parent"), "111111111111", Utc::now());

        assert_eq!(row.configuration["ParentId"], "ou-parent");
    }

    #[test]
    fn capture_time_is_the_observation_time() {
        let captured_at = DateTime::parse_from_rfc3339("2026-07-17T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let row = ou_to_row(ou("ou-abc", "r-root"), "111111111111", captured_at);

        assert_eq!(row.captured_at, captured_at);
    }

    #[test]
    fn tags_are_carried_through_from_the_ou() {
        let mut with_tags = ou("ou-abc", "r-root");
        with_tags.tags.push(Tag {
            key: "Environment".to_owned(),
            value: "prod".to_owned(),
        });

        let row = ou_to_row(with_tags, "111111111111", Utc::now());

        assert_eq!(row.tags.len(), 1);
        assert_eq!(row.tags[0].key, "Environment");
        assert_eq!(row.tags[0].value, "prod");
    }

    /// The whole emitted object for one OU, pinned.
    ///
    /// This fetcher used to build the JSON by hand; the fields now come from
    /// [`Row`]'s conversion. Nothing about the stored shape was meant to move in
    /// that change, and only a golden assertion can show that it didn't.
    #[test]
    fn emitted_row_shape_is_unchanged() {
        let captured_at = DateTime::parse_from_rfc3339("2026-07-17T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut with_tags = ou("ou-abc", "r-root");
        with_tags.tags.push(Tag {
            key: "Environment".to_owned(),
            value: "prod".to_owned(),
        });

        let line = String::from(ou_to_row(with_tags, "111111111111", captured_at));

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&line).unwrap(),
            json!({
                "arn": "arn:aws:organizations::111111111111:ou/o-exampleorgid/ou-abc",
                "accountId": "111111111111",
                "awsRegion": "global",
                "resourceType": RESOURCE_TYPE,
                "resourceId": "ou-abc",
                "resourceName": "name-of-ou-abc",
                "availabilityZone": null,
                "resourceCreationTime": null,
                "configurationItemCaptureTime": "2026-07-17T12:00:00+00:00",
                "configurationItemStatus": null,
                "configurationStateId": null,
                "tags": [{ "key": "Environment", "value": "prod" }],
                "relationships": [],
                "configuration": {
                    "ParentId": "r-root",
                },
                "supplementaryConfiguration": {},
            })
        );
    }

    #[test]
    fn untagged_ou_carries_no_tags() {
        let row = ou_to_row(ou("ou-abc", "r-root"), "111111111111", Utc::now());

        assert!(row.tags.is_empty());
    }
}
