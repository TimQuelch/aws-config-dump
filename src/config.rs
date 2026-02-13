// SPDX-FileCopyrightText: 2026 Tim Quelch <tim@tquelch.com>
//
// SPDX-License-Identifier: GPL-3.0-only

use std::{path::PathBuf, sync::OnceLock};

use directories::ProjectDirs;

static CONFIG: OnceLock<Config> = OnceLock::new();

pub struct Config {
    db_name: String,
    dirs: ProjectDirs,
}

impl Config {
    pub fn init(db_name: impl Into<String>) -> &'static Self {
        CONFIG.get_or_init(|| {
            // TODO: handle failures better. I'm not entirely sure in what cases this will fail
            let dirs = ProjectDirs::from("com", "tquelch", "aws-config-dump")
                .expect("unable to generate project paths");

            Self {
                db_name: db_name.into(),
                dirs,
            }
        })
    }

    pub fn get() -> &'static Self {
        CONFIG
            .get()
            .expect("config retrieved before initialisation")
    }

    pub fn db_path(&self) -> PathBuf {
        let dir = self.dirs.data_dir();
        std::fs::create_dir_all(dir).expect("Error while creating data dir");
        let path = dir.join(&self.db_name).with_added_extension("duckdb");

        assert!(
            path.parent()
                .and_then(|parent| parent.canonicalize().ok().map(|parent| parent == dir))
                .unwrap_or(false),
            "Specified database name ('{}') results in invalid file path",
            self.db_name
        );

        path
    }
}
