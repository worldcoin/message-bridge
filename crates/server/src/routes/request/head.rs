use axum::{extract::Path, http::StatusCode, Extension};
use redis::{aio::ConnectionManager, AsyncCommands};

use crate::utils::validate_request_id;

use super::REQ_PREFIX;

pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
) -> StatusCode {
    let request_id = request_id.to_lowercase();
    if validate_request_id(&request_id).is_err() {
        return StatusCode::BAD_REQUEST;
    }

    let Ok(exists) = redis
        .exists::<_, bool>(format!("{REQ_PREFIX}{request_id}"))
        .await
    else {
        return StatusCode::INTERNAL_SERVER_ERROR;
    };

    if exists {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}
