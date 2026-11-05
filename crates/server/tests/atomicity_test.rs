use std::sync::Arc;

use axum::{
    body::Body,
    http::{header::WWW_AUTHENTICATE, Request},
};
use redis::AsyncCommands;
use ring::digest;
use serde_json::{json, Value};
use tokio::sync::Barrier;
use tower::ServiceExt;
use uuid::Uuid;

mod common;

fn payload(label: &str) -> Value {
    json!({"iv": label, "payload": format!("{label}-ciphertext")})
}

async fn create(app: &axum::Router, id: &str, protected: bool) -> Value {
    let mut body = payload("request");
    body["request_id"] = json!(id);
    body["require_token_to_read_response"] = json!(protected);
    let (status, body) = common::post(app, "/request", &body).await;
    assert_eq!(status, 200);
    serde_json::from_str(&body).unwrap()
}

fn authorization(created: &Value) -> String {
    format!(
        "Bearer {}",
        created["response_read_token"].as_str().unwrap()
    )
}

async fn poll(app: &axum::Router, id: &str, created: &Value) -> (u16, String) {
    common::get_with_header(
        app,
        &format!("/response/{id}"),
        "Authorization",
        &authorization(created),
    )
    .await
}

#[tokio::test]
async fn concurrent_mixed_creators_publish_one_complete_session() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let barrier = Arc::new(Barrier::new(24));
    let mut tasks = Vec::new();
    for index in 0..24 {
        let app = app.clone();
        let id = id.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let protected = index % 2 == 0;
            let mut request = payload(&format!("creator-{index}"));
            request["request_id"] = json!(id);
            request["require_token_to_read_response"] = json!(protected);
            barrier.wait().await;
            (
                protected,
                request.clone(),
                common::post(&app, "/request", &request).await,
            )
        }));
    }
    let mut winner = None;
    for task in tasks {
        let (protected, request, (status, body)) = task.await.unwrap();
        if status == 200 {
            assert!(winner.is_none(), "only one creation may succeed");
            winner = Some((
                protected,
                request,
                serde_json::from_str::<Value>(&body).unwrap(),
            ));
        } else {
            assert_eq!(status, 409);
        }
    }
    let (protected, expected, created) = winner.expect("one creation must succeed");
    let mut redis = common::redis_connection().await;
    let stored: Vec<u8> = redis.get(format!("req:{id}")).await.unwrap();
    let stored: Value = serde_json::from_slice(&stored).unwrap();
    assert_eq!(
        stored,
        json!({"iv": expected["iv"], "payload": expected["payload"]})
    );
    let status: String = redis.get(format!("req:status:{id}")).await.unwrap();
    assert_eq!(status, "initialized");
    let head_allowed: String = redis
        .get(format!("existence_check_allowed:{id}"))
        .await
        .unwrap();
    assert_eq!(head_allowed, "false");
    let token_hash: Option<Vec<u8>> = redis.get(format!("res:token:{id}")).await.unwrap();
    if protected {
        let token = created["response_read_token"].as_str().unwrap();
        assert_eq!(
            token_hash.as_deref(),
            Some(digest::digest(&digest::SHA256, token.as_bytes()).as_ref())
        );
        assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 401);
        assert_eq!(poll(&app, &id, &created).await.0, 200);
    } else {
        assert!(created.get("response_read_token").is_none());
        assert!(token_hash.is_none());
        assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 200);
    }
}

#[tokio::test]
async fn every_reserved_key_blocks_creation_without_partial_writes() {
    let app = common::test_app().await;
    let prefixes = [
        "req:",
        "req:status:",
        "res:",
        "flow:",
        "res:token:",
        "existence_check_allowed:",
    ];
    let mut redis = common::redis_connection().await;
    for reserved in prefixes {
        let id = Uuid::new_v4().to_string();
        let key = format!("{reserved}{id}");
        redis
            .set_ex::<_, _, ()>(&key, "reserved", 60)
            .await
            .unwrap();
        let body = json!({"request_id": id, "iv": "iv", "payload": "ciphertext", "require_token_to_read_response": true});
        assert_eq!(common::post(&app, "/request", &body).await.0, 409);
        for prefix in prefixes {
            let exists: bool = redis.exists(format!("{prefix}{id}")).await.unwrap();
            assert_eq!(exists, prefix == reserved);
        }
        let value: String = redis.get(&key).await.unwrap();
        assert_eq!(value, "reserved");
    }
}

#[tokio::test]
async fn early_response_consumption_retains_authorization_for_later_wallet_response() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let created = create(&app, &id, true).await;
    let response_url = format!("/response/{id}");
    assert_eq!(
        common::put(&app, &response_url, &payload("early")).await.0,
        201
    );
    assert_eq!(poll(&app, &id, &created).await.0, 200);
    let mut redis = common::redis_connection().await;
    assert!(redis.exists::<_, bool>(format!("req:{id}")).await.unwrap());
    assert!(redis
        .exists::<_, bool>(format!("res:token:{id}"))
        .await
        .unwrap());
    assert_eq!(common::get(&app, &response_url).await.0, 404);
    assert_eq!(common::get(&app, &format!("/request/{id}")).await.0, 200);
    assert_eq!(
        common::put(&app, &response_url, &payload("wallet")).await.0,
        201
    );
    let denied = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(&response_url)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(denied.status(), 401);
    assert_eq!(denied.headers()[WWW_AUTHENTICATE], "Bearer");
    let (status, body) = poll(&app, &id, &created).await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["response"],
        payload("wallet")
    );
    assert_eq!(poll(&app, &id, &created).await.0, 404);
    let body = json!({"request_id": id, "iv": "iv", "payload": "ciphertext"});
    assert_eq!(common::post(&app, "/request", &body).await.0, 409);
}

// Drive the production Lua operation with the credential observation from
// before a concurrent create. This deterministically exercises the TOCTOU guard.
async fn consume_observed(id: &str, present: bool, hash: &[u8], authenticated: bool) -> u16 {
    let mut redis = common::redis_connection().await;
    let (result, _): (u16, redis::Value) = redis::cmd("EVAL")
        .arg(include_str!("../src/routes/response/consume.lua"))
        .arg(6)
        .arg(format!("res:token:{id}"))
        .arg(format!("req:status:{id}"))
        .arg(format!("res:{id}"))
        .arg(format!("flow:{id}"))
        .arg(format!("supports_flow_telemetry:{id}"))
        .arg(format!("client_name:{id}"))
        .arg(present)
        .arg(hash)
        .arg(authenticated)
        .query_async(&mut redis)
        .await
        .unwrap();
    result
}

#[tokio::test]
async fn stale_authorization_observation_cannot_consume_a_new_response() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let created = create(&app, &id, true).await;
    let response_url = format!("/response/{id}");
    assert_eq!(
        common::put(&app, &response_url, &payload("wallet")).await.0,
        201
    );
    assert_eq!(consume_observed(&id, false, &[], false).await, 0);
    assert_eq!(consume_observed(&id, true, &[0; 32], true).await, 0);
    let (status, body) = poll(&app, &id, &created).await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["response"],
        payload("wallet")
    );
}

#[tokio::test]
async fn concurrent_readers_deliver_once_and_denied_readers_never_consume() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let created = create(&app, &id, true).await;
    let response_url = format!("/response/{id}");
    assert_eq!(common::get(&app, &format!("/request/{id}")).await.0, 200);
    assert_eq!(
        common::put(&app, &response_url, &payload("wallet")).await.0,
        201
    );
    let barrier = Arc::new(Barrier::new(24));
    let mut tasks = Vec::new();
    for index in 0..24 {
        let (app, id, created, barrier) =
            (app.clone(), id.clone(), created.clone(), barrier.clone());
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            let authorized = index % 2 == 0;
            let result = if authorized {
                poll(&app, &id, &created).await
            } else {
                common::get(&app, &format!("/response/{id}")).await
            };
            (authorized, result)
        }));
    }
    let mut deliveries = 0;
    for task in tasks {
        let (authorized, (status, body)) = task.await.unwrap();
        if status == 200 {
            assert!(authorized);
            assert_eq!(
                serde_json::from_str::<Value>(&body).unwrap()["response"],
                payload("wallet")
            );
            deliveries += 1;
        } else if authorized {
            assert_eq!(status, 404);
        } else {
            assert!(status == 401 || status == 404);
        }
    }
    assert_eq!(deliveries, 1);
}

#[tokio::test]
async fn malformed_credentials_fail_closed_and_orphans_remain_not_found() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let created = create(&app, &id, true).await;
    let route = format!("/response/{id}");
    assert_eq!(common::put(&app, &route, &payload("wallet")).await.0, 201);
    for value in [
        "Basic abc",
        "Bearer short",
        &format!("Bearer {}", "A".repeat(44)),
    ] {
        assert_eq!(
            common::get_with_header(&app, &route, "Authorization", value)
                .await
                .0,
            401
        );
    }
    let auth = authorization(&created);
    assert_eq!(
        common::get_with_headers(
            &app,
            &route,
            &[("Authorization", &auth), ("Authorization", &auth)]
        )
        .await
        .0,
        401
    );
    let mut redis = common::redis_connection().await;
    let key = format!("res:token:{id}");
    let saved: Vec<u8> = redis.get(&key).await.unwrap();
    redis
        .set_ex::<_, _, ()>(&key, &[] as &[u8], 1200)
        .await
        .unwrap();
    assert_eq!(common::get(&app, &route).await.0, 500);
    assert!(redis.exists::<_, bool>(format!("res:{id}")).await.unwrap());
    redis.set_ex::<_, _, ()>(&key, saved, 1200).await.unwrap();
    assert_eq!(poll(&app, &id, &created).await.0, 200);
    assert_eq!(common::get(&app, &route).await.0, 404);
}

#[tokio::test]
async fn staging_upsert_preserves_response_authorization() {
    if !std::env::var("ENVIRONMENT").is_ok_and(|v| v.trim().eq_ignore_ascii_case("staging")) {
        return;
    }
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let created = create(&app, &id, true).await;
    let route = format!("/response/{id}");
    assert_eq!(common::put(&app, &route, &payload("first")).await.0, 201);
    assert_eq!(poll(&app, &id, &created).await.0, 200);
    assert_eq!(
        common::put(&app, &format!("/request/{id}"), &payload("upsert"))
            .await
            .0,
        201
    );
    assert_eq!(common::get(&app, &route).await.0, 401);
    assert_eq!(common::put(&app, &route, &payload("second")).await.0, 201);
    assert_eq!(poll(&app, &id, &created).await.0, 200);
}
