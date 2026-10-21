#![cfg(not(target_arch = "wasm32"))]

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use message_bridge::test_util::TestServer;
use message_bridge_client::{BridgeClient, BridgeError, EncryptedPayload, ResponseState};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

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
        "/request/{id}",
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

#[tokio::test]
async fn lost_consuming_read_reply_remains_a_transport_error() {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let server = TestServer::start(&redis_url).await.unwrap();
    let direct = BridgeClient::new(server.base_url().parse().unwrap()).unwrap();
    let payload = EncryptedPayload {
        iv: "aXY=".to_owned(),
        payload: "Y2lwaGVydGV4dA==".to_owned(),
    };
    for route in ["request", "response"] {
        let id = uuid::Uuid::new_v4().to_string();
        direct.publish_request(&id, &payload).await.unwrap();
        if route == "response" {
            direct.take_request(&id).await.unwrap().unwrap();
            direct.put_response(&id, &payload).await.unwrap();
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let upstream = format!("{}/{route}/{id}", server.base_url());
        let attempts = Arc::new(AtomicUsize::new(0));
        let received = attempts.clone();
        let proxy = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut headers = Vec::new();
                while !headers.ends_with(b"\r\n\r\n") {
                    headers.push(socket.read_u8().await.unwrap());
                }
                if received.fetch_add(1, Ordering::SeqCst) == 0 {
                    let reply = reqwest::get(&upstream).await.unwrap();
                    assert!(reply.status().is_success());
                    reply.bytes().await.unwrap();
                    // The production GET consumed the payload, but its reply is lost.
                    socket.shutdown().await.unwrap();
                } else {
                    socket
                        .write_all(b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                        .await
                        .unwrap();
                }
            }
        });
        let client = BridgeClient::new(url.parse().unwrap()).unwrap();
        let failed = if route == "request" {
            matches!(
                client.take_request(&id).await,
                Err(BridgeError::Transport(_))
            )
        } else {
            matches!(
                client.fetch_response(&id).await,
                Err(BridgeError::Transport(_))
            )
        };
        proxy.abort();
        assert!(proxy.await.unwrap_err().is_cancelled());
        assert!(failed, "lost {route} reply must remain a transport error");
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        if route == "request" {
            assert!(direct.take_request(&id).await.unwrap().is_none());
        } else {
            assert!(direct.fetch_response(&id).await.unwrap().is_none());
        }
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn response_write_rejections_remain_errors_after_retries() {
    for status in [
        axum::http::StatusCode::BAD_REQUEST,
        axum::http::StatusCode::CONFLICT,
    ] {
        for retry in [false, true] {
            let attempts = Arc::new(AtomicUsize::new(0));
            let received = attempts.clone();
            let app = axum::Router::new().route(
                "/response/{id}",
                axum::routing::put(move || {
                    let attempt = received.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if retry && attempt == 0 {
                            axum::http::StatusCode::INTERNAL_SERVER_ERROR
                        } else {
                            status
                        }
                    }
                }),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            let client = BridgeClient::new(url.parse().unwrap()).unwrap();
            let result = client
                .put_response(
                    "id",
                    &EncryptedPayload {
                        iv: "aXY=".to_owned(),
                        payload: "Y2lwaGVydGV4dA==".to_owned(),
                    },
                )
                .await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(
                matches!(result, Err(BridgeError::UnexpectedStatus(actual)) if actual == status)
            );
            assert_eq!(attempts.load(Ordering::SeqCst), if retry { 2 } else { 1 });
        }
    }
}
