// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

impl From<aws_sdk_organizations::types::Tag> for Tag {
    fn from(value: aws_sdk_organizations::types::Tag) -> Self {
        Self {
            key: value.key,
            value: value.value,
        }
    }
}
