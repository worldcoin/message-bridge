#![deny(clippy::all, clippy::pedantic, clippy::nursery)]

use std::sync::Arc;

use aide::openapi::{Info, License, OpenApi, SecurityScheme};
use axum::{extract::DefaultBodyLimit, Extension};
use redis::aio::ConnectionManager;

pub mod analytics;
use crate::{analytics::Analytics, utils::AppOverrides};

mod existence_check;
pub(crate) mod observability;
mod response_token;
pub mod routes;
pub mod server;
pub mod utils;

#[cfg(feature = "test-util")]
pub mod test_util;

/// Assemble the fully-wired application router.
///
/// Shared by the binary (via [`server::start`]) and the integration tests so
/// both exercise the exact same routes, middleware stack, and `OpenAPI` document.
/// Tests drive the returned router in-process with `tower::ServiceExt::oneshot`,
/// so no separately-running bridge is required.
pub fn app(
    redis: ConnectionManager,
    app_overrides: Arc<AppOverrides>,
    analytics: Option<Arc<Analytics>>,
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

    routes::handler()
        .finish_api_with(&mut openapi, |api| {
            // Declare the reusable OpenAPI scheme, without a global requirement.
            // Only GET /response references it; enforcement lives in its handler.
            api.security_scheme("response_read_token", SecurityScheme::Http {
                scheme: "bearer".to_string(),
                bearer_format: Some("Base64".to_string()),
                description: Some("The response_read_token returned by POST /request when require_token_to_read_response is true.".to_string()),
                extensions: std::iter::empty().collect(),
            })
        })
        .layer(Extension(redis))
        .layer(Extension(app_overrides))
        .layer(Extension(analytics))
        .layer(Extension(openapi))
        .layer(DefaultBodyLimit::max(5 * 1024 * 1024))
}
