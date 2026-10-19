use std::str::FromStr;

use axum::{extract::Path, http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::{aio::ConnectionManager, AsyncCommands, ExistenceCheck, SetExpiry, SetOptions};

use crate::{
    observability,
    utils::{
        handle_redis_error, validate_request_id, RequestStatus, EXPIRE_AFTER_SECONDS,
        REQ_STATUS_PREFIX,
    },
};

use super::{StoredResponse, RES_PREFIX};

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

    let mut pipe = redis::pipe();
    pipe.atomic()
        .get(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .get(observability::flow_key(&request_id))
        .get(observability::supports_flow_telemetry_key(&request_id))
        .get(observability::client_name_key(&request_id));
    let (status, flow, supports_flow_telemetry, client_name): (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = pipe
        .query_async(&mut redis)
        .await
        .map_err(handle_redis_error)?;

    let current_status = status.and_then(|status| RequestStatus::from_str(&status).ok());
    if current_status.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }

    observability::record_response_handoff(flow.as_deref());

    let options = SetOptions::default()
        .conditional_set(ExistenceCheck::NX)
        .with_expiration(SetExpiry::EX(EXPIRE_AFTER_SECONDS));

    let set_ok: Option<String> = redis
        .set_options(
            format!("{RES_PREFIX}{request_id}"),
            serde_json::to_vec(&request).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            options,
        )
        .await
        .map_err(handle_redis_error)?;

    if set_ok.is_none() {
        return Err(StatusCode::CONFLICT);
    }

    // The response now represents completion, so the request status is no longer needed.
    redis
        .del::<_, ()>(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .await
        .map_err(handle_redis_error)?;

    telemetry_batteries::reexports::metrics::counter!(
        "message_bridge.response_created",
        "supports_flow_telemetry" => observability::supports_flow_telemetry_tag_from_stored(supports_flow_telemetry.as_deref()),
        "client_name" => observability::client_name_tag_from_stored(client_name.as_deref())
    )
    .increment(1);

    Ok(StatusCode::CREATED)
}
