// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::borrow::Cow;

use indicatif::{ProgressBar, ProgressFinish, ProgressStyle};

pub fn resource_table_name(resource_type: impl AsRef<str>) -> String {
    resource_type
        .as_ref()
        .trim_start_matches("AWS::")
        .replace("::", "_")
        .to_lowercase()
}

pub fn progress_bar(message: impl Into<Cow<'static, str>>, total: u64) -> ProgressBar {
    let style = ProgressStyle::default_bar()
        .template("{msg:30} [{human_pos:>6}/{human_len:<6}] [{bar:40.cyan/blue}] ({per_sec})")
        .expect("valid template")
        .progress_chars("=> ");

    ProgressBar::new(total)
        .with_finish(ProgressFinish::AndLeave)
        .with_style(style)
        .with_message(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ec2_instance() {
        assert_eq!(resource_table_name("AWS::EC2::Instance"), "ec2_instance");
    }

    #[test]
    fn lambda_function() {
        assert_eq!(
            resource_table_name("AWS::Lambda::Function"),
            "lambda_function"
        );
    }

    #[test]
    fn no_aws_prefix() {
        assert_eq!(resource_table_name("EC2::Instance"), "ec2_instance");
    }

    #[test]
    fn multi_segment() {
        assert_eq!(
            resource_table_name("AWS::Route53Resolver::ResolverRule"),
            "route53resolver_resolverrule"
        );
    }
}
