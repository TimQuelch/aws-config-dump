use std::collections::{HashMap, hash_map::Entry};

use futures::future::join_all;
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::error;

pub trait Output {
    async fn send(&self, resource_type: String, value: String) -> anyhow::Result<()>;

    async fn close(&mut self) -> anyhow::Result<()>;
}

pub struct StdoutOutput {
    sender: Option<Sender<String>>,
    worker_handle: Option<JoinHandle<()>>,
}

impl StdoutOutput {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::channel::<String>(8);
        let worker_handle = tokio::task::spawn(async move {
            let mut stdout = tokio::io::stdout();
            while let Some(value) = receiver.recv().await {
                stdout
                    .write(value.as_bytes())
                    .await
                    .inspect_err(|e| error!(error = %e, "failed to write to stdout"))
                    .unwrap();
            }
        });
        Self {
            sender: Some(sender),
            worker_handle: Some(worker_handle),
        }
    }
}

impl Output for StdoutOutput {
    async fn send(&self, _: String, value: String) -> anyhow::Result<()> {
        Ok(self.sender.clone().unwrap().send(value).await?)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.sender.take();

        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }

        Ok(())
    }
}

pub struct JsonFileOutput {
    sender: Option<Sender<(String, String)>>,
    worker_handle: Option<JoinHandle<()>>,
}

impl JsonFileOutput {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);

        let worker_handle = tokio::spawn(Self::fan_out_writers(receiver));

        Self {
            sender: Some(sender),
            worker_handle: Some(worker_handle),
        }
    }

    async fn fan_out_writers(mut receiver: Receiver<(String, String)>) {
        let mut file_writers = HashMap::<_, (Sender<_>, JoinHandle<()>)>::new();
        while let Some((resource_type, value)) = receiver.recv().await {
            let writer = match file_writers.entry(resource_type.clone()) {
                Entry::Occupied(entry) => entry.get().0.clone(),
                Entry::Vacant(entry) => {
                    let (writer, handle) = Self::file_writer(resource_type).await;
                    entry.insert((writer.clone(), handle));
                    writer
                }
            };
            writer
                .send(value)
                .await
                .inspect_err(|e| error!(error = %e, "failed to send to file writer"))
                .unwrap();
        }

        // Drop all the senders so the writer loops will terminate. Then
        let handles: Vec<_> = file_writers
            .drain()
            .map(|(_, (_, handle))| handle)
            .collect();

        // Join all the handles to wait for files to flush
        join_all(handles.into_iter()).await;
    }

    async fn file_writer(resource_type: String) -> (Sender<String>, JoinHandle<()>) {
        let (sender, mut receiver) = mpsc::channel::<String>(8);
        let mut writer = BufWriter::new(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(format!("{}.json", resource_type.as_str()))
                .await
                .inspect_err(|e| error!(error = %e, "failed to open file"))
                .unwrap(),
        );

        let handle = tokio::task::spawn(async move {
            while let Some(value) = receiver.recv().await {
                writer
                    .write(value.as_bytes())
                    .await
                    .inspect_err(|e| error!(error = %e, "failed to write to file"))
                    .unwrap();
            }
            writer
                .flush()
                .await
                .inspect_err(|e| error!(error = %e, "failed to flush buffered file"))
                .unwrap();
        });

        (sender, handle)
    }
}

impl Output for JsonFileOutput {
    async fn send(&self, resource_type: String, value: String) -> anyhow::Result<()> {
        Ok(self
            .sender
            .clone()
            .unwrap()
            .send((resource_type, value))
            .await?)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.sender.take();

        if let Some(handle) = self.worker_handle.take() {
            handle.await?;
        }

        Ok(())
    }
}

pub struct DuckDbOutput {
    file_output: JsonFileOutput,
}

impl DuckDbOutput {
    pub fn new() -> Self {
        Self {
            file_output: JsonFileOutput::new(),
        }
    }
}

impl Output for DuckDbOutput {
    async fn send(&self, resource_type: String, value: String) -> anyhow::Result<()> {
        self.file_output.send(resource_type, value).await
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.file_output.close().await?;

        let db_conn = duckdb::Connection::open("./db.duckdb")
            .inspect_err(|e| error!(error = %e, "failed open duckdb database"))
            .unwrap();

        let json_files = std::fs::read_dir(".")
            .inspect_err(|e| error!(error = %e, "failed list directory"))
            .unwrap()
            .filter_map(|x| {
                x.ok().map(|x| x.path()).take_if(|path| {
                    path.extension()
                        .is_some_and(|ext| ext.eq_ignore_ascii_case("json"))
                })
            });

        for path in json_files {
            let table_name = path
                .file_prefix()
                .unwrap()
                .to_str()
                .unwrap()
                .trim_start_matches("AWS::")
                .replace("::", "_")
                .to_lowercase();

            db_conn.execute(
                format!(concat!(
                    "CREATE TABLE {} AS SELECT ",
                    "COLUMNS(c -> c not in ['configuration', 'supplementaryConfiguration']), ",
                    "UNNEST(COLUMNS(c -> c in ['configuration', 'supplementaryConfiguration'])) ",
                    "FROM read_json(?);",
                ), table_name).as_str(),
                [path.to_str().unwrap()],
            )?;
        }

        Ok(())
    }
}
