//! Session hashes reserve IDs independently of single-use payload storage.
use redis::AsyncCommands;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use uuid::Uuid;
mod common;

async fn session(id: &str) -> BTreeMap<String, String> {
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
async fn assert_conflict(app: &axum::Router, id: &str) {
    let before = session(id).await;
    let expires = deadline(id).await;
    assert_eq!(
        common::post(
            app,
            "/request",
            &json!({"request_id":id.to_uppercase(),"iv":"other","payload":"other"})
        )
        .await
        .0,
        409
    );
    assert_eq!(session(id).await, before);
    assert_eq!(deadline(id).await, expires);
}
#[tokio::test]
async fn fixed_reservation_survives_redemption_upload_and_consumption() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let request = json!({"request_id":id,"iv":"iv","payload":"request"});
    let response = json!({"iv":"iv","payload":"response"});
    assert_eq!(common::post(&app, "/request", &request).await.0, 200);
    let initial = session(&id).await;
    assert_eq!(initial["reserved"], "1");
    assert!(initial["idkit_flow_id"].starts_with("idkitflow_"));
    let mut redis = common::redis_connection().await;
    let ttl: i64 = redis.ttl(format!("session:{id}")).await.unwrap();
    assert!((2699..=2700).contains(&ttl));
    let req_ttl: i64 = redis.ttl(format!("req:{id}")).await.unwrap();
    assert!((899..=900).contains(&req_ttl));
    // Shorten the remaining reservation to make accidental refresh observable.
    redis
        .expire::<_, ()>(format!("session:{id}"), 1800)
        .await
        .unwrap();
    let expires = deadline(&id).await;
    assert_conflict(&app, &id).await;
    assert_eq!(
        common::get_with_headers(
            &app,
            &format!("/request/{id}"),
            &[
                ("supports-flow-telemetry", "true"),
                ("client-name", "android-id")
            ]
        )
        .await
        .0,
        200
    );
    assert_conflict(&app, &id).await;
    assert_eq!(deadline(&id).await, expires);
    let metadata = session(&id).await;
    assert_eq!(metadata["client_name"], "android-id");
    assert_eq!(metadata["supports_flow_telemetry"], "true");
    for _ in 0..3 {
        assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 200);
    }
    assert_eq!(
        common::put(&app, &format!("/response/{id}"), &response)
            .await
            .0,
        201
    );
    assert_conflict(&app, &id).await;
    assert_eq!(deadline(&id).await, expires);
    assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 200);
    assert_eq!(deadline(&id).await, expires);
    assert_eq!(
        session(&id).await,
        BTreeMap::from([("reserved".to_owned(), "1".to_owned())])
    );
    assert_conflict(&app, &id).await;
    assert_eq!(
        common::put(&app, &format!("/response/{id}"), &response)
            .await
            .0,
        400
    );
    assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 404);
    for prefix in [
        "req:",
        "req:status:",
        "res:",
        "flow:",
        "client_name:",
        "supports_flow_telemetry:",
    ] {
        assert!(!redis
            .exists::<_, bool>(format!("{prefix}{id}"))
            .await
            .unwrap());
    }
    redis
        .pexpire::<_, ()>(format!("session:{id}"), 0)
        .await
        .unwrap();
    assert_eq!(common::post(&app, "/request", &request).await.0, 200);
    assert_ne!(
        session(&id).await["idkit_flow_id"],
        initial["idkit_flow_id"]
    );
    assert!(!session(&id).await.contains_key("client_name"));
}
#[tokio::test]
async fn concurrent_creation_reserves_once() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let body = json!({"request_id":id,"iv":"iv","payload":"request"});
    let (a, b) = tokio::join!(
        common::post(&app, "/request", &body),
        common::post(&app, "/request", &body)
    );
    assert!(matches!((a.0, b.0), (200, 409) | (409, 200)));
    let url = format!("/request/{id}");
    let (a, b) = tokio::join!(common::get(&app, &url), common::get(&app, &url));
    assert!(matches!((a.0, b.0), (200, 404) | (404, 200)));
}
#[tokio::test]
async fn empty_reservation_blocks_creation() {
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    let mut redis = common::redis_connection().await;
    redis::pipe()
        .atomic()
        .hset(format!("session:{id}"), "reserved", "1")
        .ignore()
        .expire(format!("session:{id}"), 2700)
        .ignore()
        .query_async::<()>(&mut redis)
        .await
        .unwrap();
    assert_conflict(&app, &id).await;
    assert_eq!(common::get(&app, &format!("/request/{id}")).await.0, 404);
}
#[tokio::test]
async fn legacy_keys_allow_creation_and_metadata_remains_readable() {
    let app = common::test_app().await;
    let mut redis = common::redis_connection().await;
    for prefix in [
        "req:",
        "req:status:",
        "res:",
        "flow:",
        "client_name:",
        "supports_flow_telemetry:",
    ] {
        let id = Uuid::new_v4().to_string();
        redis
            .set_ex::<_, _, ()>(format!("{prefix}{id}"), "legacy", 900)
            .await
            .unwrap();
        assert_eq!(
            common::post(
                &app,
                "/request",
                &json!({"request_id": id, "iv": "iv", "payload": "new"})
            )
            .await
            .0,
            200
        );
        assert!(!session(&id).await.is_empty());
    }
    let id = Uuid::new_v4().to_string();
    let flow = format!("idkitflow_{}", Uuid::new_v4());
    redis
        .set_ex::<_, _, ()>(format!("flow:{id}"), &flow, 2700)
        .await
        .unwrap();
    redis
        .set_ex::<_, _, ()>(
            format!("req:{id}"),
            json!({"iv":"iv","payload":"legacy"}).to_string(),
            900,
        )
        .await
        .unwrap();
    let (status, body) = common::get_with_headers(
        &app,
        &format!("/request/{id}"),
        &[("accept-idkit-flow-id", "true")],
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["idkit_flow_id"],
        flow
    );
    assert_eq!(session(&id).await["idkit_flow_id"], flow);
    assert_conflict(&app, &id).await;
}
#[tokio::test]
async fn staging_reset_replaces_metadata_and_payload() {
    if !std::env::var("ENVIRONMENT").is_ok_and(|v| v.eq_ignore_ascii_case("staging")) {
        return;
    }
    let app = common::test_app().await;
    let id = Uuid::new_v4().to_string();
    assert_eq!(
        common::post(
            &app,
            "/request",
            &json!({"request_id":id,"iv":"old","payload":"old"})
        )
        .await
        .0,
        200
    );
    assert_eq!(
        common::get_with_headers(&app, &format!("/request/{id}"), &[("client-name", "ios")])
            .await
            .0,
        200
    );
    let old = session(&id).await;
    assert_eq!(
        common::put(
            &app,
            &format!("/response/{id}"),
            &json!({"iv":"old","payload":"old"})
        )
        .await
        .0,
        201
    );
    let replacement = json!({"iv":"new","payload":"new"});
    assert_eq!(
        common::put(&app, &format!("/request/{id}"), &replacement)
            .await
            .0,
        201
    );
    let new = session(&id).await;
    assert_ne!(new["idkit_flow_id"], old["idkit_flow_id"]);
    assert!(!new.contains_key("client_name"));
    assert_eq!(common::get(&app, &format!("/response/{id}")).await.0, 200);
    let body = common::get(&app, &format!("/request/{id}")).await.1;
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap(), replacement);
    assert_conflict(&app, &id).await;
}
