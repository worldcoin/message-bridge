use std::env;

use aide::axum::{
    routing::{head as head_route, post as post_route, put as put_route},
    ApiRouter,
};
use axum::http::Method;
use tower_http::cors::{AllowHeaders, Any, CorsLayer};

mod get;
mod head;
mod post;
mod put;

pub(super) const REQ_PREFIX: &str = "req:";

pub fn handler() -> ApiRouter {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(AllowHeaders::any())
        .allow_methods([Method::GET, Method::POST, Method::HEAD, Method::PUT]);

    let environment = env::var("ENVIRONMENT")
        .unwrap_or_else(|_| "unknown".to_string())
        .trim()
        .to_lowercase();

    let mut router = ApiRouter::new()
        .api_route("/request", post_route(post::handler))
        .api_route(
            "/request/:request_id",
            head_route(head::handler).get(get::handler),
        )
        .layer(cors);

    if environment == "staging" {
        router = router.api_route("/request/:request_id", put_route(put::handler));
    }

    router
}
