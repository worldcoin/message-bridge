use aide::axum::{
    routing::{get, post},
    ApiRouter,
};
use axum::http::Method;
use schemars::JsonSchema;
use tower_http::cors::{AllowHeaders, Any, CorsLayer};

use crate::utils::RequestPayload;

mod get;
mod head;
mod post;
mod put;

pub(super) const RES_PREFIX: &str = "res:";

#[derive(Debug, serde::Deserialize, serde::Serialize, JsonSchema)]
pub(super) struct StoredResponse {
    #[serde(flatten)]
    pub(super) payload: RequestPayload,
    /// Opaque analytics receipt supplied by the response producer. Stored
    /// temporarily, but never returned to the response consumer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) tracking_receipt: Option<String>,
}

pub fn handler() -> ApiRouter {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(AllowHeaders::any())
        .allow_methods([Method::GET, Method::PUT, Method::POST]); //TODO: PUT is required by the simulator but should not be included

    ApiRouter::new()
        .api_route(
            "/response/:request_id",
            get(get::handler)
                .head(head::handler)
                .put(put::handler)
                .layer(cors.clone()),
        )
        .api_route("/response", post(post::handler).layer(cors))
}
