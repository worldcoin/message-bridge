//! In-process test server for the message bridge.
//!
//! Integration tests build the real application router via [`message_bridge::app`]

#![allow(dead_code, reason = "used in integration tests")]

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request};
use http_body_util::BodyExt;
use message_bridge::analytics::Analytics;
use message_bridge::app;
use message_bridge::utils::{AppOverride, AppOverrides};
use redis::aio::ConnectionManager;
use serde_json::Value;
use tower::ServiceExt;

/// App-override fixture the override tests assert against. Injected directly
/// into the router by the harness, so those tests need no `APP_URL_OVERRIDES`
/// env var.
pub const FIXTURE_APP_ID: &str = "app_integration_override_fixture";
pub const FIXTURE_APP_CLIP_BUNDLE_ID: &str = "org.example.integration.Clip";
pub const FIXTURE_VERIFY_URL: &str = "https://world.org/verify";

fn fixture_overrides() -> AppOverrides {
    let mut overrides = AppOverrides::new();
    overrides.insert(
        FIXTURE_APP_ID.to_string(),
        AppOverride {
            app_clip_bundle_id: Some(FIXTURE_APP_CLIP_BUNDLE_ID.to_string()),
            verify_url: Some(FIXTURE_VERIFY_URL.to_string()),
        },
    );
    overrides
}

pub async fn redis_connection() -> ConnectionManager {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let client = redis::Client::open(url).expect("REDIS_URL must be a valid Redis URL");
    ConnectionManager::new(client).await.expect(
        "integration tests need a running Redis — start one with \
         `docker-compose -f docker-compose.test.yml up -d` (see README \"Testing\")",
    )
}

/// Build the real bridge router, wired to a local Redis and the override fixture.
pub async fn test_app() -> axum::Router {
    app(
        redis_connection().await,
        Arc::new(fixture_overrides()),
        None,
    )
}

pub async fn test_app_with_analytics(callback_url: &str) -> axum::Router {
    let url = callback_url
        .parse()
        .expect("test analytics URL must be valid");
    let analytics = Analytics::new(url).expect("test analytics client must build");

    app(
        redis_connection().await,
        Arc::new(fixture_overrides()),
        Some(Arc::new(analytics)),
    )
}

async fn send(
    app: &axum::Router,
    method: Method,
    route: &str,
    body: Option<&Value>,
    headers: &[(&str, &str)],
) -> (u16, String) {
    let mut builder = Request::builder().uri(route).method(method);
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    let request = match body {
        Some(json) => builder
            .header("Content-Type", "application/json")
            .body(Body::from(json.to_string())),
        None => builder.body(Body::empty()),
    }
    .expect("failed to build request");

    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("router service is infallible");
    let status = response.status().as_u16();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("failed to read response body")
        .to_bytes();
    (
        status,
        String::from_utf8(bytes.to_vec()).unwrap_or_default(),
    )
}

pub async fn get(app: &axum::Router, route: &str) -> (u16, String) {
    send(app, Method::GET, route, None, &[]).await
}

pub async fn get_with_header(
    app: &axum::Router,
    route: &str,
    name: &str,
    value: &str,
) -> (u16, String) {
    send(app, Method::GET, route, None, &[(name, value)]).await
}

pub async fn get_with_headers(
    app: &axum::Router,
    route: &str,
    headers: &[(&str, &str)],
) -> (u16, String) {
    send(app, Method::GET, route, None, headers).await
}

pub async fn post(app: &axum::Router, route: &str, body: &Value) -> (u16, String) {
    send(app, Method::POST, route, Some(body), &[]).await
}

pub async fn put(app: &axum::Router, route: &str, body: &Value) -> (u16, String) {
    send(app, Method::PUT, route, Some(body), &[]).await
}
