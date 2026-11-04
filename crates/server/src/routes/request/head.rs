use axum::{extract::Path, http::StatusCode, Extension};
use redis::aio::ConnectionManager;

use crate::utils::validate_request_id;

use crate::utils::REQ_PREFIX;

pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
) -> StatusCode {
    let request_id = request_id.to_lowercase();
    if validate_request_id(&request_id).is_err() {
        return StatusCode::BAD_REQUEST;
    }

    let Ok(exists) = crate::session::exists_if_allowed(&mut redis, &request_id, REQ_PREFIX).await
    else {
        return StatusCode::INTERNAL_SERVER_ERROR;
    };

    if exists {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}
