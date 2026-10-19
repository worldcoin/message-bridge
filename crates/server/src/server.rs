use std::{env, net::SocketAddr, sync::Arc};

use redis::aio::ConnectionManager;
use tokio::{net::TcpListener, signal};

use crate::{analytics::Analytics, utils::AppOverrides};

/// Bind the configured address and serve the bridge until a shutdown signal.
///
/// # Panics
///
/// Panics if `PORT` is set but not parseable as a port, if binding the TCP
/// listener fails, or if the server exits with an error.
pub async fn start(
    redis: ConnectionManager,
    app_overrides: Arc<AppOverrides>,
    analytics: Option<Arc<Analytics>>,
) {
    let router = crate::app(redis, app_overrides, analytics);

    let address = SocketAddr::from((
        [0, 0, 0, 0],
        env::var("PORT").map_or(8000, |p| p.parse().unwrap()),
    ));
    let listener = TcpListener::bind(&address)
        .await
        .expect("Failed to bind address");

    println!("🔛💬 Message Bridge started on http://{address}");

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("Failed to start server");
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            tracing::info!("👋 Received Ctrl+C, shutting down gracefully...");
        },
        () = terminate => {
            tracing::info!("👋Received SIGTERM, shutting down gracefully...");
        },
    }
}
