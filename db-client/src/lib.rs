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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use bb8::ManageConnection as _;

    use super::*;

    fn make_manager() -> ConnectionManager {
        ConnectionManager::open(None::<&PathBuf>, None).unwrap()
    }

    async fn make_pool() -> ConnectionPool {
        bb8::Pool::builder()
            .max_size(4)
            .build(make_manager())
            .await
            .unwrap()
    }

    #[test]
    fn connection_manager_open_in_memory_succeeds() {
        let result = ConnectionManager::open(None::<&PathBuf>, None);
        assert!(result.is_ok());
    }

    #[test]
    fn connection_manager_open_with_config_succeeds() {
        let config = duckdb::Config::default()
            .with("preserve_insertion_order", "false")
            .unwrap();
        let result = ConnectionManager::open(None::<&PathBuf>, Some(config));
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn with_conn_executes_ddl() {
        let manager = make_manager();
        let conn = manager.connect().await.unwrap();
        let result = conn
            .with_conn(|c| c.execute_batch("CREATE TABLE t (id INTEGER);"))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn with_conn_returns_queried_value() {
        let manager = make_manager();
        let conn = manager.connect().await.unwrap();
        conn.with_conn(|c| {
            c.execute_batch("CREATE TABLE nums (n INTEGER); INSERT INTO nums VALUES (42);")
        })
        .await
        .unwrap();

        let value: i32 = conn
            .with_conn(|c| c.query_row("SELECT n FROM nums;", [], |row| row.get(0)))
            .await
            .unwrap();
        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn with_conn_returns_error_on_invalid_sql() {
        let manager = make_manager();
        let conn = manager.connect().await.unwrap();
        let result = conn.with_conn(|c| c.execute_batch("THIS IS NOT SQL")).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn with_conn_multiple_calls_share_connection_state() {
        // Validates that commands are serialised through the same background
        // thread, preserving DuckDB session state (tables, inserted rows) between calls.
        let manager = make_manager();
        let conn = manager.connect().await.unwrap();
        conn.with_conn(|c| c.execute_batch("CREATE TEMPORARY TABLE counter (val INTEGER);"))
            .await
            .unwrap();
        for _ in 0..5 {
            conn.with_conn(|c| c.execute("INSERT INTO counter VALUES (1);", []))
                .await
                .unwrap();
        }
        let count: i64 = conn
            .with_conn(|c| c.query_row("SELECT count(*) FROM counter;", [], |row| row.get(0)))
            .await
            .unwrap();
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn manage_connection_connect_returns_working_connection() {
        let manager = make_manager();
        let conn = manager.connect().await.unwrap();
        let result = conn
            .with_conn(|c| c.query_row("SELECT 1;", [], |row| row.get::<_, i32>(0)))
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn manage_connection_is_valid_returns_ok_on_healthy_connection() {
        let manager = make_manager();
        let mut conn = manager.connect().await.unwrap();
        let result = manager.is_valid(&mut conn).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn manage_connection_has_broken_returns_false() {
        let manager = make_manager();
        let mut conn = manager.connect().await.unwrap();
        assert!(!manager.has_broken(&mut conn));
    }

    #[tokio::test]
    async fn pool_get_and_execute_query_succeeds() {
        let pool = make_pool().await;
        let conn = pool.get().await.unwrap();
        let result: i32 = conn
            .with_conn(|c| c.query_row("SELECT 21 + 21;", [], |row| row.get(0)))
            .await
            .unwrap();
        assert_eq!(result, 42);
    }
}
