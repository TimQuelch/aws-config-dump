use std::collections::{HashMap, hash_map::Entry};

use aws_sdk_config::types::ResourceType;
use futures::future::join_all;
use tokio::{
    fs::OpenOptions,
    io::{AsyncWriteExt, BufWriter},
    sync::mpsc::{self, Receiver, Sender},
    task::JoinHandle,
};
use tracing::error;

pub trait Output {
    async fn send(
        &self,
        resource_type: ResourceType,
        value: serde_json::Value,
    ) -> anyhow::Result<()>;

    async fn close(&mut self) -> anyhow::Result<()>;
}

pub struct StdoutOutput {
    sender: Sender<serde_json::Value>,
}

impl StdoutOutput {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::channel(8);
        tokio::task::spawn(async move {
            let mut stdout = tokio::io::stdout();
            while let Some(value) = receiver.recv().await {
                stdout
                    .write(
                        serde_json::to_vec(&value)
                            .inspect_err(|e| error!(error = %e, "failed serialise json"))
                            .unwrap()
                            .as_slice(),
                    )
                    .await
                    .inspect_err(|e| error!(error = %e, "failed to write to stdout"))
                    .unwrap();
            }
        });
        Self { sender }
    }
}

impl Output for StdoutOutput {
    async fn send(&self, _: ResourceType, value: serde_json::Value) -> anyhow::Result<()> {
        Ok(self.sender.send(value).await?)
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

pub struct JsonFileOutput {
    sender: Option<Sender<(ResourceType, serde_json::Value)>>,
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

    async fn fan_out_writers(mut receiver: Receiver<(ResourceType, serde_json::Value)>) {
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

    async fn file_writer(
        resource_type: ResourceType,
    ) -> (Sender<serde_json::Value>, JoinHandle<()>) {
        let (sender, mut receiver) = mpsc::channel::<serde_json::Value>(8);
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
                    .write(
                        serde_json::to_vec(&value)
                            .inspect_err(|e| error!(error = %e, "failed serialise json"))
                            .unwrap()
                            .as_slice(),
                    )
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
    async fn send(
        &self,
        resource_type: ResourceType,
        value: serde_json::Value,
    ) -> anyhow::Result<()> {
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
