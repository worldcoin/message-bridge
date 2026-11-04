use axum::{extract::Path, http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::aio::ConnectionManager;

use crate::{
    observability::IdkitFlowId,
    session,
    utils::{
        handle_redis_error, validate_request_id, RequestPayload, RequestStatus,
        EXPIRE_AFTER_SECONDS, REQ_PREFIX, REQ_STATUS_PREFIX,
    },
};

/// Reset an entire session, intentionally bypassing reservations. Staging only.
pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
    Json(request): Json<RequestPayload>,
) -> Result<StatusCode, StatusCode> {
    let request_id = request_id.to_lowercase();
    validate_request_id(&request_id)?;

    let payload = serde_json::to_vec(&request).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let idkit_flow_id = IdkitFlowId::new();
    let mut pipe = redis::pipe();
    session::reset(&mut pipe, &request_id, &idkit_flow_id);
    pipe.set_ex(
        format!("{REQ_PREFIX}{request_id}"),
        &payload,
        EXPIRE_AFTER_SECONDS,
    )
    .ignore()
    .set_ex(
        format!("{REQ_STATUS_PREFIX}{request_id}"),
        RequestStatus::Initialized.to_string(),
        EXPIRE_AFTER_SECONDS,
    )
    .ignore()
    .query_async::<()>(&mut redis)
    .await
    .map_err(handle_redis_error)?;

    Ok(StatusCode::CREATED)
}
