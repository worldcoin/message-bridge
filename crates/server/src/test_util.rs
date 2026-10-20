//! A local test server backed by the production router and a caller-provided Redis.
//!
//! No data is flushed: use unique request ids, or a dedicated Redis instance. Analytics
//! and app overrides are disabled. Stored messages retain the production TTL.

use std::{error::Error, io, sync::Arc, time::Duration};

use redis::aio::ConnectionManagerConfig;
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

/// A running bridge on an ephemeral loopback port. Dropping it aborts the server;
/// call [`Self::shutdown`] to observe server failures and wait for graceful shutdown.
pub struct TestServer {
    base_url: String,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<io::Result<()>>,
}

impl TestServer {
    /// Connects to Redis and binds the real bridge application on `127.0.0.1:0`.
    ///
    /// # Errors
    /// Returns an error if Redis cannot be reached within five seconds or binding fails.
    pub async fn start(redis_url: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let config = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(5)))
            .set_response_timeout(Some(Duration::from_secs(5)));
        let redis = tokio::time::timeout(
            Duration::from_secs(5),
            redis::Client::open(redis_url)?.get_connection_manager_with_config(config),
        )
        .await??;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("http://{}", listener.local_addr()?);
        let router = crate::app(redis, Arc::default(), None);
        let (stop, stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = stopped.await;
                })
                .await
        });
        Ok(Self {
            base_url,
            stop: Some(stop),
            task,
        })
    }

    /// The HTTP base URL, available as soon as the listener is bound.
    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Stops accepting requests and waits up to five seconds for in-flight requests.
    ///
    /// # Errors
    /// Returns server failures, task panics or a shutdown timeout. On timeout, the server
    /// is aborted when this handle is dropped.
    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            // An already exited server is reported by awaiting its task below.
            let _ = stop.send(());
        }
        tokio::time::timeout(Duration::from_secs(5), &mut self.task)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bridge shutdown timed out"))?
            .map_err(io::Error::other)?
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
