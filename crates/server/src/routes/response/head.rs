use axum::{extract::Path, http::StatusCode, Extension};
use redis::aio::ConnectionManager;

use crate::{
    existence_check,
    utils::{validate_request_id, REQ_STATUS_PREFIX},
};

pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
) -> StatusCode {
    let request_id = request_id.to_lowercase();
    if validate_request_id(&request_id).is_err() {
        return StatusCode::BAD_REQUEST;
    }

    match existence_check::exists(&mut redis, &request_id, REQ_STATUS_PREFIX).await {
        Ok(true) => StatusCode::OK,
        Ok(false) => StatusCode::NOT_FOUND,
        Err(status) => status,
    }
}
