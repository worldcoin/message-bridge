use axum::{
    body::Body,
    http::{Method, Request},
};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use redis::AsyncCommands;
use serde_json::{json, Value};
use tower::ServiceExt;
mod common;

async fn create(app: &axum::Router, supplied: bool, protected: bool) -> (String, Value) {
    let mut body = json!({"iv":"iv","payload":"opaque","require_token_to_read_response":protected});
    if supplied {
        body["request_id"] = json!(uuid::Uuid::new_v4().to_string());
    }
    let (status, body) = common::post(app, "/request", &body).await;
    assert_eq!(status, 200);
    let body: Value = serde_json::from_str(&body).unwrap();
    (body["request_id"].as_str().unwrap().to_owned(), body)
}

async fn poll(app: &axum::Router, id: &str, token: &str) -> (u16, String) {
    common::get_with_header(
        app,
        &format!("/response/{id}"),
        "authorization",
        &format!("Bearer {token}"),
    )
    .await
}

async fn hash(id: &str) -> std::collections::BTreeMap<String, Vec<u8>> {
    common::redis_connection()
        .await
        .hgetall(format!("session:{id}"))
        .await
        .unwrap()
}

async fn deadline(id: &str) -> i64 {
    redis::cmd("PEXPIRETIME")
        .arg(format!("session:{id}"))
        .query_async(&mut common::redis_connection().await)
        .await
        .unwrap()
}

#[tokio::test]
async fn authorization_preserves_single_use_and_reservation() {
    let app = common::test_app().await;
    let (id, created) = create(&app, true, true).await;
    let token = created["response_read_token"].as_str().unwrap();
    let (other_id, other) = create(&app, true, true).await;
    let other_token = other["response_read_token"].as_str().unwrap();
    assert_ne!(token, other_token);
    assert_eq!(token.len(), 44);
    assert_eq!(STANDARD.decode(token).unwrap().len(), 32);
    assert_eq!(STANDARD.encode(STANDARD.decode(token).unwrap()), token);
    let initial = hash(&id).await;
    assert_eq!(
        initial["response_read_token_digest"],
        ring::digest::digest(&ring::digest::SHA256, token.as_bytes()).as_ref()
    );
    assert!(!initial.values().any(|v| v == token.as_bytes()));
    let expires = deadline(&id).await;
    let response = json!({"iv":"iv","payload":"proof"});

    for completed in [false, true] {
        if completed {
            assert_eq!(common::get(&app, &format!("/request/{id}")).await.0, 200);
            assert_eq!(
                common::put(&app, &format!("/response/{id}"), &response)
                    .await
                    .0,
                201
            );
        }
        let before = hash(&id).await;
        assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 401);
        for invalid in ["", "bad", &"F".repeat(64), other_token] {
            assert_eq!(poll(&app, &id, invalid).await.0, 401);
        }
        assert_eq!(hash(&id).await, before);
        assert_eq!(deadline(&id).await, expires);
        if !completed {
            assert_eq!(poll(&app, &id, token).await.0, 200);
        }
    }
    let (a, b, c) = tokio::join!(
        poll(&app, &id, token),
        poll(&app, &id, token),
        poll(&app, &id, other_token)
    );
    assert!(matches!((a.0, b.0), (200, 404) | (404, 200)));
    assert_eq!(c.0, 401);
    let success = if a.0 == 200 { a.1 } else { b.1 };
    assert_eq!(
        serde_json::from_str::<Value>(&success).unwrap()["response"],
        response
    );
    assert_eq!(deadline(&id).await, expires);
    let after = hash(&id).await;
    assert_eq!(
        after["response_read_token_digest"],
        initial["response_read_token_digest"]
    );
    let (status, collision) = common::post(&app, "/request", &json!({
        "request_id":id,"iv":"replacement","payload":"replacement","require_token_to_read_response":true
    })).await;
    assert_eq!(status, 409);
    assert!(!collision.contains(token));
    assert_eq!(hash(&id).await, after);

    // A fresh reservation gets a fresh token, and the old credential cannot read it.
    common::redis_connection()
        .await
        .del::<_, ()>(format!("session:{id}"))
        .await
        .unwrap();
    let (_, replacement) = common::post(
        &app,
        "/request",
        &json!({
            "request_id":id,"iv":"new","payload":"new","require_token_to_read_response":true
        }),
    )
    .await;
    let replacement: Value = serde_json::from_str(&replacement).unwrap();
    let new_token = replacement["response_read_token"].as_str().unwrap();
    assert_ne!(new_token, token);
    assert_eq!(poll(&app, &id, token).await.0, 401);
    assert_eq!(poll(&app, &id, new_token).await.0, 200);
    assert_eq!(poll(&app, &other_id, other_token).await.0, 200);
}

#[tokio::test]
async fn creation_disables_caching_and_polling_supports_authorization_preflight() {
    let app = common::test_app().await;
    let request = Request::builder()
        .method(Method::POST)
        .uri("/request")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"iv":"iv","payload":"opaque","require_token_to_read_response":true}"#,
        ))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let request = Request::builder()
        .method(Method::OPTIONS)
        .uri("/response/00000000-0000-0000-0000-000000000000")
        .header("origin", "https://example.com")
        .header("access-control-request-method", "GET")
        .header("access-control-request-headers", "authorization")
        .body(Body::empty())
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
    assert_eq!(
        response.headers()["access-control-allow-headers"],
        "authorization"
    );
}

#[tokio::test]
async fn staging_reset_retains_credentials() {
    if !std::env::var("ENVIRONMENT").is_ok_and(|v| v.eq_ignore_ascii_case("staging")) {
        return;
    }
    let app = common::test_app().await;
    let (id, created) = create(&app, true, true).await;
    let before = hash(&id).await;
    assert_eq!(
        common::put(
            &app,
            &format!("/request/{id}"),
            &json!({"iv":"new","payload":"new"})
        )
        .await
        .0,
        201
    );
    let after = hash(&id).await;
    assert_eq!(
        before["response_read_token_digest"],
        after["response_read_token_digest"]
    );
    assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 401);
    assert_eq!(
        poll(&app, &id, created["response_read_token"].as_str().unwrap())
            .await
            .0,
        200
    );
}

#[tokio::test]
async fn typed_authorization_preserves_session_policy() {
    let app = common::test_app().await;
    for protected in [false, true] {
        let (id, created) = create(&app, false, protected).await;
        let url = format!("/response/{id}");
        for malformed in ["Basic dXNlcjpwYXNz", "Bearer", "Digest invalid", ""] {
            let (status, _) = common::get_with_header(&app, &url, "authorization", malformed).await;
            assert_eq!(status, if protected { 401 } else { 200 });
        }
        if protected {
            let token = created["response_read_token"].as_str().unwrap();
            for scheme in ["Bearer", "bearer", "BEARER"] {
                let (status, body) = common::get_with_header(
                    &app,
                    &url,
                    "authorization",
                    &format!("{scheme} {token}"),
                )
                .await;
                assert_eq!(status, 200);
                assert_eq!(
                    serde_json::from_str::<Value>(&body).unwrap()["status"],
                    "initialized"
                );
            }
        }
    }
}
