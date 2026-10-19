use axum::{extract::Path, http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::{aio::ConnectionManager, AsyncCommands};

use crate::utils::{
    handle_redis_error, validate_request_id, RequestPayload, RequestStatus, EXPIRE_AFTER_SECONDS,
    REQ_STATUS_PREFIX,
};

use super::REQ_PREFIX;

/// Create a new request by ID idempotently — retries succeed, even if the
/// request exists. Only enabled in staging.
pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
    Json(request): Json<RequestPayload>,
) -> Result<StatusCode, StatusCode> {
    let request_id = request_id.to_lowercase();
    validate_request_id(&request_id)?;

    redis
        .set_ex::<_, _, ()>(
            format!("{REQ_STATUS_PREFIX}{request_id}"),
            RequestStatus::Initialized.to_string(),
            EXPIRE_AFTER_SECONDS,
        )
        .await
        .map_err(handle_redis_error)?;

    redis
        .set_ex::<_, _, ()>(
            format!("{REQ_PREFIX}{request_id}"),
            serde_json::to_vec(&request).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            EXPIRE_AFTER_SECONDS,
        )
        .await
        .map_err(handle_redis_error)?;

    Ok(StatusCode::CREATED)
}
