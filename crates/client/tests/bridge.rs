#![cfg(not(target_arch = "wasm32"))]

use message_bridge::test_util::TestServer;
use message_bridge_client::{BridgeClient, BridgeError, EncryptedPayload, ResponseState};

#[tokio::test]
async fn client_exercises_production_single_use_routes() {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let server = TestServer::start(&redis_url).await.unwrap();
    let client = BridgeClient::new(server.base_url().parse().unwrap()).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let payload = EncryptedPayload {
        iv: "aXY=".to_owned(),
        payload: "Y2lwaGVydGV4dA==".to_owned(),
    };

    assert!(client.fetch_response(&id).await.unwrap().is_none());
    client.publish_request(&id, &payload).await.unwrap();
    assert!(matches!(
        client.fetch_response(&id).await.unwrap(),
        Some(ResponseState::Initialized)
    ));
    let taken = client.take_request(&id).await.unwrap().unwrap();
    assert!(taken == payload);
    assert!(client.take_request(&id).await.unwrap().is_none());
    assert!(matches!(
        client.fetch_response(&id).await.unwrap(),
        Some(ResponseState::Retrieved)
    ));
    client.put_response(&id, &payload).await.unwrap();
    // Completion removes the status key, so a duplicate write returns 400.
    assert!(
        matches!(client.put_response(&id, &payload).await, Err(BridgeError::UnexpectedStatus(status)) if status.as_u16() == 400)
    );
    assert!(
        matches!(client.fetch_response(&id).await.unwrap(), Some(ResponseState::Completed(response)) if response == payload)
    );
    assert!(client.fetch_response(&id).await.unwrap().is_none());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_invalid_responses_and_unsupported_base_urls() {
    for url in [
        "file:///tmp/bridge",
        "https://user:secret@example.com",
        "https://example.com/?secret=1",
        "https://example.com/#fragment",
    ] {
        assert!(matches!(
            BridgeClient::new(url.parse().unwrap()),
            Err(BridgeError::InvalidUrl)
        ));
    }
    let app = axum::Router::new().route(
        "/request/:id",
        axum::routing::get(
            |axum::extract::Path(id): axum::extract::Path<String>| async move {
                match id.as_str() {
                    "malformed" => (axum::http::StatusCode::OK, "not JSON".to_owned()),
                    "oversized" => (axum::http::StatusCode::OK, "x".repeat(5 * 1024 * 1024 + 1)),
                    _ => (axum::http::StatusCode::FORBIDDEN, String::new()),
                }
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let client = BridgeClient::new(url.parse().unwrap()).unwrap();
    assert!(matches!(
        client.take_request("malformed").await,
        Err(BridgeError::Json(_))
    ));
    assert!(matches!(
        client.take_request("oversized").await,
        Err(BridgeError::TooLarge)
    ));
    assert!(
        matches!(client.take_request("forbidden").await, Err(BridgeError::UnexpectedStatus(status)) if status.as_u16() == 403)
    );
    stop.send(()).unwrap();
    task.await.unwrap();
}
