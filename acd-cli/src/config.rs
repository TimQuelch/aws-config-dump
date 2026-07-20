// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{
    collections::{HashMap, HashSet},
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Result, bail};
use directories::ProjectDirs;
use serde::Deserialize;
use tokio::fs;
use tracing::warn;

use crate::builtin_alterations;

// The types below are the config file format. Changing them changes the
// interface users write against, so keep the documentation in step:
//
// - `example-config.toml` in the repository root is the reference example, and
//   is loaded by a test in this file. That test will fail if it goes stale.
// - The README config section covers the common cases in prose. Nothing checks
//   it, so it needs updating by hand.

#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct DbConfig {
    aggregator_name: Option<String>,
    path: Option<PathBuf>,
    aws_profile: Option<String>,
}

struct ResolvedDbConfig {
    path: PathBuf,
    aggregator_name: Option<String>,
    aws_profile: Option<String>,
}

impl DbConfig {
    fn resolve(&self, name: &str) -> ResolvedDbConfig {
        ResolvedDbConfig {
            path: self.path.clone().unwrap_or_else(|| {
                project_dirs()
                    .data_dir()
                    .join(name)
                    .with_added_extension("duckdb")
            }),
            aggregator_name: self.aggregator_name.clone(),
            aws_profile: self.aws_profile.clone(),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct SchemaAlteration {
    pub name: String,
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub condition: Option<String>,
    pub sql: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GlobalSchemaAlteration {
    pub name: String,
    pub condition: Option<String>,
    pub sql: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct BuiltinAlterationsConfig {
    #[serde(default)]
    pub disable_all: bool,
    #[serde(default)]
    pub disabled: HashSet<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct CustomTable {
    pub name: String,
    pub description: Option<String>,
    #[serde(default)]
    pub dependencies: Vec<String>,
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
    #[serde(default)]
    custom_tables: Vec<CustomTable>,
    #[serde(default)]
    builtin_alterations: BuiltinAlterationsConfig,
}

impl ConfigFile {
    pub async fn load(path: Option<&Path>) -> Result<Option<ConfigFile>> {
        let file_contents = if let Some(path) = path {
            Some(fs::read(path).await?)
        } else {
            read_or_default(&project_dirs().config_dir().join("config.toml")).await?
        };
        file_contents
            .map(|bytes| ConfigFile::parse(std::str::from_utf8(&bytes)?))
            .transpose()
    }

    /// Deserialize a config file, warning about any fields we don't recognise.
    ///
    /// An unknown field is almost always a typo or a field that has been
    /// renamed. Ignoring it silently means the user's setting has no effect
    /// with nothing to indicate why, so warn instead. This is deliberately not
    /// an error: an ignored field never changes behaviour, so a stale line
    /// shouldn't stop the config loading.
    fn parse(contents: &str) -> Result<ConfigFile> {
        let deserializer = toml::Deserializer::parse(contents)?;
        Ok(serde_ignored::deserialize(deserializer, |path| {
            warn!("Ignoring unknown config field '{path}'");
        })?)
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
    pub aws_profile: Option<String>,
    pub aggregator_name: Option<String>,
    pub query_extra_columns: HashMap<String, Vec<String>>,
    pub schema_alterations: Vec<SchemaAlteration>,
    pub global_schema_alterations: Vec<GlobalSchemaAlteration>,
    pub custom_tables: Vec<CustomTable>,
    builtin_filter: BuiltinAlterationsConfig,
}

impl Config {
    pub fn load(config_file: Option<ConfigFile>, db_name: Option<&str>) -> Result<Config> {
        let ResolvedDbConfig {
            path,
            aws_profile,
            aggregator_name,
        } = Config::resolve_db(config_file.as_ref(), db_name)?;

        let (
            query_extra_columns,
            schema_alterations,
            global_schema_alterations,
            custom_tables,
            builtin_filter,
        ) = config_file
            .map(|x| {
                (
                    x.query_extra_columns,
                    x.schema_alterations,
                    x.global_schema_alterations,
                    x.custom_tables,
                    x.builtin_alterations,
                )
            })
            .unwrap_or_default();

        Ok(Self {
            db_path: path,
            aws_profile,
            aggregator_name,
            query_extra_columns,
            schema_alterations,
            global_schema_alterations,
            custom_tables,
            builtin_filter,
        })
    }

    fn resolve_db(config_file: Option<&ConfigFile>, db: Option<&str>) -> Result<ResolvedDbConfig> {
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
        let builtins: &[SchemaAlteration] = if self.builtin_filter.disable_all {
            &[]
        } else {
            builtin_alterations::alterations()
        };
        builtins
            .iter()
            .filter(|a| !self.builtin_filter.disabled.contains(&a.name))
            .chain(&self.schema_alterations)
    }

    pub fn alterations_count(&self) -> usize {
        let builtin_count = if self.builtin_filter.disable_all {
            0
        } else {
            builtin_alterations::alterations()
                .iter()
                .filter(|a| !self.builtin_filter.disabled.contains(&a.name))
                .count()
        };
        builtin_count + self.schema_alterations.len()
    }

    pub fn global_alterations(&self) -> impl IntoIterator<Item = &GlobalSchemaAlteration> {
        let builtins: &[GlobalSchemaAlteration] = if self.builtin_filter.disable_all {
            &[]
        } else {
            builtin_alterations::global_alterations()
        };
        builtins
            .iter()
            .filter(|a| !self.builtin_filter.disabled.contains(&a.name))
            .chain(&self.global_schema_alterations)
    }

    pub fn global_alterations_count(&self) -> usize {
        let builtin_count = if self.builtin_filter.disable_all {
            0
        } else {
            builtin_alterations::global_alterations()
                .iter()
                .filter(|a| !self.builtin_filter.disabled.contains(&a.name))
                .count()
        };
        builtin_count + self.global_schema_alterations.len()
    }

    pub fn custom_tables(&self) -> &[CustomTable] {
        &self.custom_tables
    }
}

fn project_dirs() -> ProjectDirs {
    ProjectDirs::from("com", "tquelch", "acd").expect("failed to get project dirs")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin_alterations;

    /// The reference example shipped in the repository root. Loaded at compile
    /// time so the test makes no assumptions about the filesystem at runtime.
    const EXAMPLE_CONFIG: &str = include_str!("../../example-config.toml");

    #[test]
    fn example_config_deserializes() {
        let config = ConfigFile::parse(EXAMPLE_CONFIG).unwrap();

        // Assert on content, not just that it parsed, so an example emptied of
        // everything interesting doesn't quietly keep passing.
        assert_eq!(config.default_database.as_deref(), Some("prod"));

        let prod = config.databases.get("prod").expect("prod database");
        assert_eq!(prod.aggregator_name.as_deref(), Some("my-aggregator"));
        let dev = config.databases.get("dev").expect("dev database");
        assert!(
            dev.aggregator_name.is_none(),
            "dev is the non-aggregator example"
        );

        assert!(config.query_extra_columns.contains_key("ec2_subnet"));
        assert!(
            config
                .builtin_alterations
                .disabled
                .contains("name_from_tags")
        );

        let names: Vec<&str> = config
            .schema_alterations
            .iter()
            .map(|a| a.name.as_str())
            .collect();
        assert!(names.contains(&"ec2_instance_short_id"));
        assert!(
            config
                .schema_alterations
                .iter()
                .any(|a| a.condition.is_some()),
            "example should show a conditional alteration"
        );

        assert!(!config.global_schema_alterations.is_empty());
        assert!(
            config
                .custom_tables
                .iter()
                .any(|t| t.name == "instances_by_account")
        );
    }

    #[test]
    fn unknown_field_is_ignored_not_rejected() {
        // Fields we don't recognise warn but must not stop the config loading.
        let config = ConfigFile::parse(
            r#"
            default_database = "prod"
            not_a_real_field = "whatever"

            [[schema_alterations]]
            name = "example"
            sql = "SELECT 1"
            description = "removed field, now ignored"
            "#,
        )
        .expect("unknown fields should not be an error");

        assert_eq!(config.default_database.as_deref(), Some("prod"));
        assert_eq!(config.schema_alterations.len(), 1);
    }

    #[test]
    fn missing_required_field_is_still_an_error() {
        // Warning about unknown fields must not soften genuine errors. This is
        // the failure the README example used to hit: `description` given
        // where `name` was required.
        let err = ConfigFile::parse(
            r#"
            [[schema_alterations]]
            description = "no name field"
            sql = "SELECT 1"
            "#,
        )
        .expect_err("missing required field should fail");

        assert!(
            err.to_string().contains("name"),
            "error should name the missing field, got: {err}"
        );
    }

    #[test]
    fn empty_toml_deserializes_to_defaults() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert!(config.default_database.is_none());
        assert!(config.databases.is_empty());
        assert!(config.query_extra_columns.is_empty());
        assert!(config.schema_alterations.is_empty());
        assert!(config.global_schema_alterations.is_empty());
        assert!(config.custom_tables.is_empty());
        assert!(!config.builtin_alterations.disable_all);
        assert!(config.builtin_alterations.disabled.is_empty());
    }

    #[test]
    fn full_config_deserializes_correctly() {
        let toml_str = r#"
default_database = "default"

[databases.default]
aggregator_name = "my-aggregator"

[databases.other]
path = "/path/to/db.otherext"
aws_profile = "some_profile"

[query_extra_columns]
ec2_vpc = ["tags['Name']", "cidrBlock"]

[[schema_alterations]]
name = "custom_field"
dependencies = ["ec2_instance"]
condition = "SELECT true"
sql = "ALTER TABLE ec2_instance ADD COLUMN foo VARCHAR"

[[global_schema_alterations]]
name = "global_field"
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
                        aggregator_name: Some("my-aggregator".to_owned()),
                        aws_profile: None,
                    }
                ),
                (
                    "other".to_owned(),
                    DbConfig {
                        path: Some(PathBuf::from("/path/to/db.otherext")),
                        aggregator_name: None,
                        aws_profile: Some("some_profile".to_string()),
                    }
                ),
            ])
        );
        assert_eq!(
            config.query_extra_columns["ec2_vpc"],
            vec!["tags['Name']", "cidrBlock"]
        );
        let sa = &config.schema_alterations[0];
        assert_eq!(sa.name, "custom_field");
        assert_eq!(sa.dependencies, vec!["ec2_instance"]);
        assert_eq!(sa.condition.as_deref(), Some("SELECT true"));
        let ga = &config.global_schema_alterations[0];
        assert_eq!(ga.name, "global_field");
        assert!(ga.condition.is_some());
    }

    #[test]
    fn alteration_without_condition_is_valid() {
        let toml_str = r#"
[[schema_alterations]]
name = "test"
dependencies = []
sql = "SELECT 1"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        let sa = &config.schema_alterations[0];
        assert_eq!(sa.name, "test");
        assert!(sa.condition.is_none());
        assert_eq!(sa.sql, "SELECT 1");
    }

    #[test]
    fn table_placeholder_preserved_in_global_sql() {
        let toml_str = r#"
[[global_schema_alterations]]
name = "test"
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
                    name: "test".to_string(),
                    dependencies: vec![],
                    condition: None,
                    sql: "SELECT 42".to_owned(),
                }],
                global_schema_alterations: vec![],
                custom_tables: vec![],
                builtin_alterations: BuiltinAlterationsConfig::default(),
            }),
            None,
        )
        .unwrap();
        let items: Vec<_> = config.alterations().into_iter().collect();
        assert_eq!(items.len(), builtin_alterations::alterations().len() + 1);
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
                    name: "test".to_string(),
                    condition: None,
                    sql: "ALTER TABLE {table} ADD COLUMN x VARCHAR".to_owned(),
                }],
                custom_tables: vec![],
                builtin_alterations: BuiltinAlterationsConfig::default(),
            }),
            None,
        )
        .unwrap();

        let items: Vec<_> = config.global_alterations().into_iter().collect();
        assert_eq!(
            items.len(),
            builtin_alterations::global_alterations().len() + 1
        );
        assert_eq!(
            items.last().unwrap().sql,
            "ALTER TABLE {table} ADD COLUMN x VARCHAR"
        );
    }

    #[test]
    fn custom_table_deserializes_correctly() {
        let toml_str = r#"
[[custom_tables]]
name = "Custom::EC2::Route"
description = "Routes from route tables"
dependencies = ["ec2_routetable"]
sql = "SELECT accountId FROM ec2_routetable"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert_eq!(config.custom_tables.len(), 1);
        let ct = &config.custom_tables[0];
        assert_eq!(ct.name, "Custom::EC2::Route");
        assert_eq!(ct.description.as_deref(), Some("Routes from route tables"));
        assert_eq!(ct.dependencies, vec!["ec2_routetable"]);
        assert_eq!(ct.sql, "SELECT accountId FROM ec2_routetable");
    }

    #[test]
    fn custom_table_without_optional_fields_is_valid() {
        let toml_str = r#"
[[custom_tables]]
name = "Custom::EC2::Route"
sql = "SELECT 1"
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        let ct = &config.custom_tables[0];
        assert!(ct.description.is_none());
        assert!(ct.dependencies.is_empty());
    }

    #[test]
    fn disable_all_builtins_suppresses_all_builtin_alterations() {
        let config = Config::load(
            Some(ConfigFile {
                default_database: None,
                databases: HashMap::new(),
                query_extra_columns: HashMap::new(),
                schema_alterations: vec![],
                global_schema_alterations: vec![],
                custom_tables: vec![],
                builtin_alterations: BuiltinAlterationsConfig {
                    disable_all: true,
                    disabled: [].into(),
                },
            }),
            None,
        )
        .unwrap();
        assert_eq!(config.alterations().into_iter().count(), 0);
        assert_eq!(config.global_alterations().into_iter().count(), 0);
    }

    #[test]
    fn disabled_list_suppresses_named_builtin() {
        let config = Config::load(
            Some(ConfigFile {
                default_database: None,
                databases: HashMap::new(),
                query_extra_columns: HashMap::new(),
                schema_alterations: vec![],
                global_schema_alterations: vec![],
                custom_tables: vec![],
                builtin_alterations: BuiltinAlterationsConfig {
                    disable_all: false,
                    disabled: ["ssm_patchcompliance".to_owned()].into(),
                },
            }),
            None,
        )
        .unwrap();
        let names: Vec<&str> = config
            .alterations()
            .into_iter()
            .map(|a| a.name.as_str())
            .collect();
        assert!(!names.contains(&"ssm_patchcompliance"));
        assert!(names.contains(&"ec2_instance_state"));
    }

    #[test]
    fn disable_all_does_not_suppress_user_alterations() {
        let config = Config::load(
            Some(ConfigFile {
                default_database: None,
                databases: HashMap::new(),
                query_extra_columns: HashMap::new(),
                schema_alterations: vec![SchemaAlteration {
                    name: "my_custom".to_string(),
                    dependencies: vec![],
                    condition: None,
                    sql: "SELECT 1".to_owned(),
                }],
                global_schema_alterations: vec![],
                custom_tables: vec![],
                builtin_alterations: BuiltinAlterationsConfig {
                    disable_all: true,
                    disabled: [].into(),
                },
            }),
            None,
        )
        .unwrap();
        let names: Vec<&str> = config
            .alterations()
            .into_iter()
            .map(|a| a.name.as_str())
            .collect();
        assert_eq!(names, vec!["my_custom"]);
    }

    #[test]
    fn empty_toml_builtin_alterations_section_defaults_to_all_enabled() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert!(!config.builtin_alterations.disable_all);
        assert!(config.builtin_alterations.disabled.is_empty());
    }

    #[test]
    fn builtin_alterations_config_deserializes_from_toml() {
        let toml_str = r#"
[builtin_alterations]
disable_all = true
disabled = ["ssm_patchcompliance", "sns_topic"]
"#;
        let config: ConfigFile = toml::from_str(toml_str).unwrap();
        assert!(config.builtin_alterations.disable_all);
        assert_eq!(
            config.builtin_alterations.disabled,
            ["ssm_patchcompliance".to_owned(), "sns_topic".to_owned()].into()
        );
    }

    #[test]
    fn custom_tables_accessor_returns_config_tables() {
        let config = Config::load(
            Some(ConfigFile {
                default_database: None,
                databases: HashMap::new(),
                query_extra_columns: HashMap::new(),
                schema_alterations: vec![],
                global_schema_alterations: vec![],
                custom_tables: vec![CustomTable {
                    name: "Custom::EC2::Route".to_owned(),
                    description: None,
                    dependencies: vec![],
                    sql: "SELECT 1".to_owned(),
                }],
                builtin_alterations: BuiltinAlterationsConfig::default(),
            }),
            None,
        )
        .unwrap();
        assert_eq!(config.custom_tables().len(), 1);
        assert_eq!(config.custom_tables()[0].name, "Custom::EC2::Route");
    }
}
