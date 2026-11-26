use aide::axum::ApiRouter;

mod request;
mod response;
mod system;

pub fn handler(response_body_limit: usize) -> ApiRouter {
    ApiRouter::new()
        .merge(system::handler())
        .merge(request::handler())
        .merge(response::handler(response_body_limit))
}
