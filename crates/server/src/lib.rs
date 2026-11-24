#![deny(clippy::all, clippy::pedantic, clippy::nursery)]

use std::sync::Arc;

use aide::openapi::{Info, License, OpenApi};
use axum::{extract::DefaultBodyLimit, Extension};
use redis::aio::ConnectionManager;

pub mod analytics;
use crate::{analytics::Analytics, utils::AppOverrides};

pub(crate) mod observability;
pub mod routes;
pub mod server;
pub mod utils;

#[cfg(feature = "test-util")]
pub mod test_util;

/// The largest request body the bridge accepts, unless a route sets its own limit.
pub const DEFAULT_BODY_LIMIT: usize = 5 * 1024 * 1024;

/// Assemble the fully-wired application router.
///
/// `response_body_limit` is the largest `PUT /response/:id` body in bytes. Every other route
/// accepts at most [`DEFAULT_BODY_LIMIT`].
///
/// Shared by the binary (via [`server::start`]) and the integration tests so
/// both exercise the exact same routes, middleware stack, and `OpenAPI` document.
/// Tests drive the returned router in-process with `tower::ServiceExt::oneshot`,
/// so no separately-running bridge is required.
pub fn app(
    redis: ConnectionManager,
    app_overrides: Arc<AppOverrides>,
    analytics: Option<Arc<Analytics>>,
    response_body_limit: usize,
) -> axum::Router {
    let mut openapi = OpenApi {
        info: Info {
            title: "Message Bridge".to_string(),
            summary: Some(
                "A dumb, environment and client agnostic relay of arbitrary messages. It lets two parties share an arbitrary message where parties can gossip a symmetric key off-band.".to_string(),
            ),
            license: Some(License {
                name: "MIT".to_string(),
                identifier: Some("MIT".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        },
        ..Default::default()
    };

    routes::handler(response_body_limit)
        .finish_api(&mut openapi)
        .layer(Extension(redis))
        .layer(Extension(app_overrides))
        .layer(Extension(analytics))
        .layer(Extension(openapi))
        .layer(DefaultBodyLimit::max(DEFAULT_BODY_LIMIT))
}
