// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::sync::LazyLock;

use serde::Deserialize;

use crate::config::{GlobalSchemaAlteration, SchemaAlteration};

#[derive(Deserialize)]
struct BuiltinsFile {
    #[serde(default)]
    schema_alterations: Vec<SchemaAlteration>,
    #[serde(default)]
    global_schema_alterations: Vec<GlobalSchemaAlteration>,
}

static BUILTINS: LazyLock<BuiltinsFile> = LazyLock::new(|| {
    toml::from_str(include_str!("builtin_alterations.toml"))
        .expect("built-in alterations TOML is invalid")
});

pub fn alterations() -> &'static [SchemaAlteration] {
    &BUILTINS.schema_alterations
}

pub fn global_alterations() -> &'static [GlobalSchemaAlteration] {
    &BUILTINS.global_schema_alterations
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_toml_parses_and_contains_some_expected_names() {
        let schema_names: Vec<&str> = alterations().iter().map(|a| a.name.as_str()).collect();
        assert!(schema_names.contains(&"ec2_instance_state"));
        assert!(schema_names.contains(&"ssm_managedinstanceinventory"));
        assert!(schema_names.contains(&"ssm_patchcompliance"));

        let global_names: Vec<&str> = global_alterations()
            .iter()
            .map(|a| a.name.as_str())
            .collect();
        assert!(global_names.contains(&"name_from_tags"));
        assert!(global_names.contains(&"drop_tags_1"));
        assert!(global_names.contains(&"drop_arn_1"));
    }
}
