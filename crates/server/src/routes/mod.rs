use aide::axum::ApiRouter;

mod request;
mod response;
mod system;

pub fn handler() -> ApiRouter {
    ApiRouter::new()
        .merge(system::handler())
        .merge(request::handler())
        .merge(response::handler())
}
