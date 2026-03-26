use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use bb8::ManageConnection;
use duckdb::Connection as SyncConnection;
use tokio::{
    sync::{mpsc, oneshot},
    task,
};

type Command = Box<dyn FnOnce(&mut SyncConnection) + Send + Sync>;

pub type ConnectionPool = bb8::Pool<ConnectionManager>;

pub struct Connection {
    cmd_queue: mpsc::Sender<Command>,
}

impl Connection {
    fn from_sync_conn(conn: SyncConnection) -> Self {
        let (tx, mut rx) = mpsc::channel::<Command>(8);

        task::spawn_blocking(move || {
            let mut conn = conn;
            while let Some(cmd) = rx.blocking_recv() {
                cmd(&mut conn);
            }
        });

        Self { cmd_queue: tx }
    }

    /// Run a arbitrary command with a synchronous connection
    ///
    /// # Errors
    ///
    /// This function will return an error if there is a failure sending the message or the
    /// underlying command fails.
    ///
    /// # Panics
    ///
    /// This function will panic if there is a failure in sending thee command or receiving the result
    pub async fn with_conn<F, T>(&self, f: F) -> Result<T, duckdb::Error>
    where
        F: FnOnce(&mut SyncConnection) -> Result<T, duckdb::Error> + Send + Sync + 'static,
        T: Send + 'static,
    {
        let (result_tx, result_rx) = oneshot::channel();
        self.cmd_queue
            .send(Box::new(|conn| {
                let _ = result_tx.send(f(conn));
            }))
            .await
            .unwrap();
        result_rx.await.unwrap()
    }
}

pub struct ConnectionManager {
    inner: Arc<Mutex<SyncConnection>>,
}

impl ConnectionManager {
    /// Creates a connection manager with an underlying duckdb conneection
    ///
    /// # Errors
    ///
    /// This function will return an error if the underlying connection fails to open.
    pub fn open(
        path: Option<impl AsRef<Path>>,
        config: Option<duckdb::Config>,
    ) -> Result<Self, duckdb::Error> {
        let sync_conn = match (path, config) {
            (None, None) => SyncConnection::open_in_memory(),
            (Some(path), None) => SyncConnection::open(path),
            (None, Some(config)) => SyncConnection::open_in_memory_with_flags(config),
            (Some(path), Some(config)) => SyncConnection::open_with_flags(path, config),
        }?;

        Ok(sync_conn.into())
    }
}

impl From<SyncConnection> for ConnectionManager {
    fn from(value: SyncConnection) -> Self {
        Self {
            inner: Arc::new(Mutex::new(value)),
        }
    }
}

impl ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = duckdb::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let inner_conn = self.inner.clone();
        let sync_conn = task::spawn_blocking(move || inner_conn.lock().unwrap().try_clone())
            .await
            .unwrap()?;
        Ok(Connection::from_sync_conn(sync_conn))
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.with_conn(|c| c.execute_batch("")).await
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
