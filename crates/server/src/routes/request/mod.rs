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

/// Deployment environment, as far as the request routes care.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Environment {
    /// Enables the staging-only `PUT /request/{request_id}`.
    Staging,
    Other,
}

impl Environment {
    fn from_env() -> Self {
        let environment = env::var("ENVIRONMENT").unwrap_or_default();
        if environment.trim().eq_ignore_ascii_case("staging") {
            Self::Staging
        } else {
            Self::Other
        }
    }
}

pub fn handler() -> ApiRouter {
    router(Environment::from_env())
}

fn router(environment: Environment) -> ApiRouter {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(AllowHeaders::any())
        .allow_methods([Method::GET, Method::POST, Method::HEAD, Method::PUT]);

    let mut router = ApiRouter::new()
        .api_route("/request", post_route(post::handler))
        .api_route(
            "/request/{request_id}",
            head_route(head::handler).get(get::handler),
        );

    if environment == Environment::Staging {
        router = router.api_route("/request/{request_id}", put_route(put::handler));
    }

    // Layer after every route is added: merging an unlayered method router into
    // a layered path replaces its fallback, which answers CORS preflights.
    router.layer(cors)
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{header, Method, Request, StatusCode},
        Router,
    };
    use tower::ServiceExt;

    use super::{router, Environment};

    const ORIGIN: &str = "http://127.0.0.1:4173";

    async fn send(environment: Environment, request: Request<Body>) -> axum::response::Response {
        Router::from(router(environment))
            .oneshot(request)
            .await
            .expect("router service is infallible")
    }

    fn preflight(method: &Method) -> Request<Body> {
        Request::builder()
            .method(Method::OPTIONS)
            .uri("/request/c5f4b1a6-7a3e-4c1e-9d47-2f0b8e6a9c11")
            .header(header::ORIGIN, ORIGIN)
            .header(header::ACCESS_CONTROL_REQUEST_METHOD, method.as_str())
            .body(Body::empty())
            .expect("valid preflight request")
    }

    #[tokio::test]
    async fn request_by_id_answers_cors_preflight() {
        for environment in [Environment::Other, Environment::Staging] {
            for method in [Method::GET, Method::HEAD, Method::PUT] {
                let response = send(environment, preflight(&method)).await;

                assert_eq!(
                    response.status(),
                    StatusCode::OK,
                    "preflight {method} ({environment:?})"
                );
                assert_eq!(
                    response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN],
                    "*",
                    "preflight {method} ({environment:?})"
                );
            }
        }
    }

    #[tokio::test]
    async fn staging_put_response_carries_cors_headers() {
        let request = Request::builder()
            .method(Method::PUT)
            .uri("/request/c5f4b1a6-7a3e-4c1e-9d47-2f0b8e6a9c11")
            .header(header::ORIGIN, ORIGIN)
            .body(Body::empty())
            .expect("valid PUT request");

        // The handler rejects this bare request; only the CORS headers matter.
        let response = send(Environment::Staging, request).await;

        assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
    }
}
