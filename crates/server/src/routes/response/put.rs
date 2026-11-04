use std::str::FromStr;

use axum::{extract::Path, http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::{aio::ConnectionManager, AsyncCommands, ExistenceCheck, SetExpiry, SetOptions};

use crate::{
    observability, session,
    utils::{
        handle_redis_error, validate_request_id, RequestStatus, EXPIRE_AFTER_SECONDS,
        REQ_STATUS_PREFIX,
    },
};

use super::StoredResponse;
use crate::utils::RES_PREFIX;

#[tracing::instrument(
    parent = None,
    name = "message_bridge.response.create",
    skip_all,
    fields(
        idkit_flow_id = tracing::field::Empty,
        http.route = "/response/:request_id",
    )
)]
pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
    Json(request): Json<StoredResponse>,
) -> Result<StatusCode, StatusCode> {
    let request_id = request_id.to_lowercase();
    validate_request_id(&request_id)?;

    let payload = serde_json::to_vec(&request).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let status: Option<String> = redis
        .get(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .await
        .map_err(handle_redis_error)?;
    if status
        .and_then(|value| RequestStatus::from_str(&value).ok())
        .is_none()
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let metadata = session::metadata(&mut redis, &request_id)
        .await
        .map_err(handle_redis_error)?;
    let options = SetOptions::default()
        .conditional_set(ExistenceCheck::NX)
        .with_expiration(SetExpiry::EX(EXPIRE_AFTER_SECONDS));
    let stored: Option<String> = redis
        .set_options(format!("{RES_PREFIX}{request_id}"), &payload, options)
        .await
        .map_err(handle_redis_error)?;
    if stored.is_none() {
        return Err(StatusCode::CONFLICT);
    }
    redis
        .del::<_, ()>(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .await
        .map_err(handle_redis_error)?;
    observability::record_response_handoff(metadata.idkit_flow_id.as_deref());

    telemetry_batteries::reexports::metrics::counter!(
        "message_bridge.response_created",
        "supports_flow_telemetry" => observability::supports_flow_telemetry_tag_from_stored(metadata.supports_flow_telemetry.as_deref()),
        "client_name" => observability::client_name_tag_from_stored(metadata.client_name.as_deref())
    )
    .increment(1);

    Ok(StatusCode::CREATED)
}
