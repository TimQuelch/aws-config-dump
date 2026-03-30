// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::HashMap,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use directories::ProjectDirs;
use serde::Deserialize;
use tokio::fs;

use crate::builtin_alterations;

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct DbConfig {
    aggregator_name: Option<String>,
    path: Option<PathBuf>,
}

impl DbConfig {
    fn resolve(&self, name: &str) -> (PathBuf, Option<String>) {
        (
            self.path.clone().unwrap_or_else(|| {
                project_dirs()
                    .data_dir()
                    .join(name)
                    .with_added_extension("duckdb")
            }),
            self.aggregator_name.clone(),
        )
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SchemaAlteration {
    pub description: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub condition: Option<String>,
    pub sql: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlobalSchemaAlteration {
    pub description: Option<String>,
    pub condition: Option<String>,
    pub sql: String,
}

#[derive(Debug, Deserialize)]
pub struct ConfigFile {
    default_database: Option<String>,
    #[serde(default)]
    databases: HashMap<String, DbConfig>,
    #[serde(default)]
    query_extra_columns: HashMap<String, Vec<String>>,
    #[serde(default)]
    schema_alterations: Vec<SchemaAlteration>,
    #[serde(default)]
    global_schema_alterations: Vec<GlobalSchemaAlteration>,
}

impl ConfigFile {
    pub async fn load(path: Option<&Path>) -> Result<Option<ConfigFile>> {
        let file_contents = if let Some(path) = path {
            Some(fs::read(path).await?)
        } else {
            read_or_default(&project_dirs().config_dir().join("config.toml")).await?
        };
        Ok(file_contents
            .map(|x| toml::from_slice(x.as_slice()))
            .transpose()?)
    }
}

async fn read_or_default(path: &Path) -> Result<Option<Vec<u8>>> {
    match fs::read(path).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub struct Config {
    pub db_path: PathBuf,
    pub aggregator_name: Option<String>,
    pub query_extra_columns: HashMap<String, Vec<String>>,
    pub schema_alterations: Vec<SchemaAlteration>,
    pub global_schema_alterations: Vec<GlobalSchemaAlteration>,
}

impl Config {
    pub fn load(config_file: Option<ConfigFile>, db_name: Option<&str>) -> Result<Config> {
        let (db_path, aggregator_name) = Config::resolve_db(config_file.as_ref(), db_name)?;

        let (query_extra_columns, schema_alterations, global_schema_alterations) = config_file
            .map(|x| {
                (
                    x.query_extra_columns,
                    x.schema_alterations,
                    x.global_schema_alterations,
                )
            })
            .unwrap_or_default();

        Ok(Self {
            db_path,
            aggregator_name,
            query_extra_columns,
            schema_alterations,
            global_schema_alterations,
        })
    }

    fn resolve_db(
        config_file: Option<&ConfigFile>,
        db: Option<&str>,
    ) -> Result<(PathBuf, Option<String>)> {
        match (config_file, db) {
            (Some(file), Some(db)) => {
                let Some(db_conf) = file.databases.get(db) else {
                    bail!("database '{db}' not found in config")
                };
                Ok(db_conf.resolve(db))
            }
            (Some(file), None) if file.default_database.is_some() => {
                let default = file
                    .default_database
                    .as_ref()
                    .expect("default_db should be defined");
                let Some(db_conf) = file.databases.get(default) else {
                    bail!("default database '{default}' not found in config")
                };
                Ok(db_conf.resolve(default))
            }
            (Some(file), None) if file.databases.len() == 1 => Ok(file
                .databases
                .iter()
                .next()
                .map(|(name, db_conf)| db_conf.resolve(name))
                .expect("expected exactly 1 item")),
            (Some(file), None) if file.databases.is_empty() => {
                Ok(DbConfig::default().resolve("db"))
            }
            (Some(_), None) => bail!(
                "multiple databases configured. configure default_database or specify --db flag"
            ),
            (None, Some(db)) => Ok(DbConfig::default().resolve(db)),
            (None, None) => Ok(DbConfig::default().resolve("db")),
        }
    }

    pub fn alterations(&self) -> impl IntoIterator<Item = &SchemaAlteration> {
        builtin_alterations::ALTERATIONS
            .iter()
            .chain(&self.schema_alterations)
    }

    pub fn alterations_count(&self) -> usize {
        builtin_alterations::ALTERATIONS.len() + self.schema_alterations.len()
    }

    pub fn global_alterations(&self) -> impl IntoIterator<Item = &GlobalSchemaAlteration> {
        builtin_alterations::GLOBAL_ALTERATIONS
            .iter()
            .chain(&self.global_schema_alterations)
    }

    pub fn global_alterations_count(&self) -> usize {
        builtin_alterations::GLOBAL_ALTERATIONS.len() + self.global_schema_alterations.len()
    }
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
        let config: ConfigFile = toml::from_str("").unwrap();
        assert!(config.default_database.is_none());
        assert!(config.databases.is_empty());
        assert!(config.query_extra_columns.is_empty());
        assert!(config.schema_alterations.is_empty());
        assert!(config.global_schema_alterations.is_empty());
    }

    #[test]
    fn full_config_deserializes_correctly() {
        let toml_str = r#"
default_database = "default"

[databases.default]
aggregator_name = "my-aggregator"

[databases.other]
path = "/path/to/db.otherext"

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
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.default_database.as_deref(), Some("default"));
        assert_eq!(
            config.databases,
            HashMap::from_iter(vec![
                (
                    "default".to_owned(),
                    DbConfig {
                        path: None,
                        aggregator_name: Some("my-aggregator".to_owned())
                    }
                ),
                (
                    "other".to_owned(),
                    DbConfig {
                        path: Some(PathBuf::from("/path/to/db.otherext")),
                        aggregator_name: None
                    }
                ),
            ])
        );
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
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
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
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.global_schema_alterations[0].sql,
            "ALTER TABLE {table} ADD COLUMN x VARCHAR"
        );
    }

    #[tokio::test]
    async fn load_from_path_missing_returns_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let result = ConfigFile::load(Some(&path)).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn read_or_default_missing_file_returns_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let buf = read_or_default(&path).await.unwrap();
        assert!(buf.is_none());
    }

    #[tokio::test]
    async fn read_or_default_existing_file_returns_config() {
        let path = tempfile::Builder::new()
            .tempfile()
            .unwrap()
            .into_temp_path();
        tokio::fs::write(&path, "hello").await.unwrap();
        let contents = read_or_default(&path).await.unwrap();
        assert_eq!(
            contents.and_then(|x| String::from_utf8(x).ok()).as_deref(),
            Some("hello")
        );
    }

    #[test]
    fn alterations_iter_chains_builtins_and_config() {
        let config: Config = Config::load(
            Some(ConfigFile {
                default_database: None,
                databases: HashMap::new(),
                query_extra_columns: HashMap::new(),
                schema_alterations: vec![SchemaAlteration {
                    dependencies: vec![],
                    condition: None,
                    description: None,
                    sql: "SELECT 42".to_owned(),
                }],
                global_schema_alterations: vec![],
            }),
            None,
        )
        .unwrap();
        let items: Vec<_> = config.alterations().into_iter().collect();
        assert_eq!(items.len(), builtin_alterations::ALTERATIONS.len() + 1);
        assert_eq!(items.last().unwrap().sql, "SELECT 42");
    }

    #[test]
    fn global_alterations_iter_chains_builtins_and_config() {
        let config: Config = Config::load(
            Some(ConfigFile {
                default_database: None,
                databases: HashMap::new(),
                query_extra_columns: HashMap::new(),
                schema_alterations: vec![],
                global_schema_alterations: vec![GlobalSchemaAlteration {
                    condition: None,
                    description: None,
                    sql: "ALTER TABLE {table} ADD COLUMN x VARCHAR".to_owned(),
                }],
            }),
            None,
        )
        .unwrap();

        let items: Vec<_> = config.global_alterations().into_iter().collect();
        assert_eq!(
            items.len(),
            builtin_alterations::GLOBAL_ALTERATIONS.len() + 1
        );
        assert_eq!(
            items.last().unwrap().sql,
            "ALTER TABLE {table} ADD COLUMN x VARCHAR"
        );
    }
}
