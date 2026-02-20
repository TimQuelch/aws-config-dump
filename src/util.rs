// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

pub fn resource_table_name(resource_type: impl AsRef<str>) -> String {
    resource_type
        .as_ref()
        .trim_start_matches("AWS::")
        .replace("::", "_")
        .to_lowercase()
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
