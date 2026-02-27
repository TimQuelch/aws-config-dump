// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::Result;
use directories::ProjectDirs;
use serde::Deserialize;
use tokio::fs;

use crate::builtin_alterations;

#[derive(Debug, Deserialize)]
pub struct ConfigSchemaAlteration {
    pub description: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub condition: Option<String>,
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct ConfigGlobalSchemaAlteration {
    pub description: Option<String>,
    pub condition: Option<String>,
    pub sql: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct Config {
    pub aggregator_name: Option<String>,
    #[serde(default)]
    pub query_extra_columns: HashMap<String, Vec<String>>,
    #[serde(default)]
    pub schema_alterations: Vec<ConfigSchemaAlteration>,
    #[serde(default)]
    pub global_schema_alterations: Vec<ConfigGlobalSchemaAlteration>,
}

impl Config {
    pub fn alterations_iter(&self) -> impl Iterator<Item = &ConfigSchemaAlteration> {
        builtin_alterations::ALTERATIONS
            .iter()
            .chain(&self.schema_alterations)
    }

    pub fn global_alterations_iter(&self) -> impl Iterator<Item = &ConfigGlobalSchemaAlteration> {
        builtin_alterations::GLOBAL_ALTERATIONS
            .iter()
            .chain(&self.global_schema_alterations)
    }
}

async fn load_from_path(path: &Path) -> Result<Config> {
    let bytes = fs::read(path).await?;
    Ok(toml::from_slice(bytes.as_slice())?)
}

async fn load_or_default(path: &Path) -> Result<Config> {
    match fs::read(path).await {
        Ok(bytes) => Ok(toml::from_slice(bytes.as_slice())?),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(Config::default()),
        Err(e) => Err(e.into()),
    }
}

pub async fn load(path: Option<&Path>) -> Result<Config> {
    if let Some(p) = path {
        load_from_path(p).await
    } else {
        load_or_default(&project_dirs().config_dir().join("config.toml")).await
    }
}

pub async fn db_path() -> PathBuf {
    let dir = project_dirs().data_dir().to_path_buf();
    fs::create_dir_all(&dir)
        .await
        .expect("Error while creating data dir");
    dir.join("db").with_added_extension("duckdb")
}

fn project_dirs() -> ProjectDirs {
    ProjectDirs::from("com", "tquelch", "aws-config-dump").expect("failed to get project dirs")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin_alterations;

    #[test]
    fn empty_toml_deserializes_to_defaults() {
        let config: Config = toml::from_str("").unwrap();
        assert!(config.aggregator_name.is_none());
        assert!(config.query_extra_columns.is_empty());
        assert!(config.schema_alterations.is_empty());
        assert!(config.global_schema_alterations.is_empty());
    }

    #[test]
    fn full_config_deserializes_correctly() {
        let toml_str = r#"
aggregator_name = "my-aggregator"

[query_extra_columns]
ec2_vpc = ["tags['Name']", "cidrBlock"]

[[schema_alterations]]
description = "Add custom field"
dependencies = ["ec2_instance"]
condition = "SELECT true"
sql = "ALTER TABLE ec2_instance ADD COLUMN foo VARCHAR"

[[global_schema_alterations]]
description = "Global field"
condition = 'SELECT count(*) > 0 FROM "{table}"'
sql = 'ALTER TABLE "{table}" ADD COLUMN bar VARCHAR'
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.aggregator_name.as_deref(), Some("my-aggregator"));
        assert_eq!(
            config.query_extra_columns["ec2_vpc"],
            vec!["tags['Name']", "cidrBlock"]
        );
        let sa = &config.schema_alterations[0];
        assert_eq!(sa.description.as_deref(), Some("Add custom field"));
        assert_eq!(sa.dependencies, vec!["ec2_instance"]);
        assert_eq!(sa.condition.as_deref(), Some("SELECT true"));
        let ga = &config.global_schema_alterations[0];
        assert_eq!(ga.description.as_deref(), Some("Global field"));
        assert!(ga.condition.is_some());
    }

    #[test]
    fn alteration_without_description_and_condition_is_valid() {
        let toml_str = r#"
[[schema_alterations]]
dependencies = []
sql = "SELECT 1"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let sa = &config.schema_alterations[0];
        assert!(sa.description.is_none());
        assert!(sa.condition.is_none());
        assert_eq!(sa.sql, "SELECT 1");
    }

    #[test]
    fn table_placeholder_preserved_in_global_sql() {
        let toml_str = r#"
[[global_schema_alterations]]
sql = "ALTER TABLE {table} ADD COLUMN x VARCHAR"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.global_schema_alterations[0].sql,
            "ALTER TABLE {table} ADD COLUMN x VARCHAR"
        );
    }

    #[tokio::test]
    async fn load_from_path_missing_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let result = load_from_path(&path).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_or_default_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let config = load_or_default(&path).await.unwrap();
        assert!(config.aggregator_name.is_none());
        assert!(config.query_extra_columns.is_empty());
    }

    #[tokio::test]
    async fn load_or_default_existing_file_returns_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        tokio::fs::write(&path, r#"aggregator_name = "from-file""#)
            .await
            .unwrap();
        let config = load_or_default(&path).await.unwrap();
        assert_eq!(config.aggregator_name.as_deref(), Some("from-file"));
    }

    #[test]
    fn alterations_iter_chains_builtins_and_config() {
        let toml_str = r#"
[[schema_alterations]]
dependencies = []
sql = "SELECT 42"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let items: Vec<_> = config.alterations_iter().collect();
        assert_eq!(items.len(), builtin_alterations::ALTERATIONS.len() + 1);
        assert_eq!(items.last().unwrap().sql, "SELECT 42");
    }

    #[test]
    fn global_alterations_iter_chains_builtins_and_config() {
        let toml_str = r#"
[[global_schema_alterations]]
sql = "ALTER TABLE {table} ADD COLUMN x VARCHAR"
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        let items: Vec<_> = config.global_alterations_iter().collect();
        assert_eq!(
            items.len(),
            builtin_alterations::GLOBAL_ALTERATIONS.len() + 1
        );
        assert_eq!(
            items.last().unwrap().sql,
            "ALTER TABLE {table} ADD COLUMN x VARCHAR"
        );
    }

    #[test]
    fn aggregator_name_comes_from_file() {
        let config: Config = toml::from_str(r#"aggregator_name = "my-aggregator""#).unwrap();
        assert_eq!(config.aggregator_name.as_deref(), Some("my-aggregator"));
    }

    #[test]
    fn no_aggregator_name_when_absent_from_file() {
        let config: Config = toml::from_str("").unwrap();
        assert!(config.aggregator_name.is_none());
    }

    #[test]
    fn query_extra_columns_come_from_file() {
        let toml_str = r#"
[query_extra_columns]
ec2_vpc = ["cidrBlock"]
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.query_extra_columns["ec2_vpc"],
            vec!["cidrBlock".to_string()]
        );
    }
}
