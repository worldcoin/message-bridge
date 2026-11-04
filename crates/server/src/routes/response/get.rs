use std::{str::FromStr, sync::Arc};

use axum::{extract::Path, http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::aio::ConnectionManager;
use schemars::JsonSchema;

use crate::{
    analytics::Analytics,
    observability, session,
    utils::{
        handle_redis_error, validate_request_id, RequestPayload, RequestStatus, REQ_PREFIX,
        REQ_STATUS_PREFIX, RES_PREFIX,
    },
};

use super::StoredResponse;

#[derive(Debug, serde::Deserialize, serde::Serialize, JsonSchema)]
pub(super) struct Response {
    status: RequestStatus,
    response: Option<RequestPayload>,
}

#[tracing::instrument(
    parent = None,
    name = "message_bridge.response.consume",
    skip_all,
    fields(
        idkit_flow_id = tracing::field::Empty,
        http.route = "/response/:request_id",
    )
)]
pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
    Extension(analytics): Extension<Option<Arc<Analytics>>>,
) -> Result<Json<Response>, StatusCode> {
    let request_id = request_id.to_lowercase();
    validate_request_id(&request_id)?;

    let metadata = session::metadata(&mut redis, &request_id)
        .await
        .map_err(handle_redis_error)?;
    let (status, value): (Option<String>, Option<Vec<u8>>) = redis::pipe()
        .atomic()
        .get(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .get_del(format!("{RES_PREFIX}{request_id}"))
        .query_async(&mut redis)
        .await
        .map_err(handle_redis_error)?;
    observability::record_response_handoff(metadata.idkit_flow_id.as_deref());

    if let Some(value) = value {
        let stored_response: StoredResponse =
            serde_json::from_slice(&value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut cleanup = redis::pipe();
        cleanup
            .atomic()
            .del(&[
                format!("{REQ_PREFIX}{request_id}"),
                format!("{REQ_STATUS_PREFIX}{request_id}"),
            ])
            .ignore();
        session::clear_metadata(&mut cleanup, &request_id);
        if let Err(error) = cleanup.query_async::<()>(&mut redis).await {
            tracing::warn!("Failed to clean up consumed response state: {error}");
        }

        telemetry_batteries::reexports::metrics::counter!(
            "message_bridge.response_consumed",
            "supports_flow_telemetry" => observability::supports_flow_telemetry_tag_from_stored(metadata.supports_flow_telemetry.as_deref()),
            "client_name" => observability::client_name_tag_from_stored(metadata.client_name.as_deref())
        )
        .increment(1);

        if let (Some(analytics), Some(tracking_receipt)) =
            (analytics, stored_response.tracking_receipt)
        {
            analytics.send_response_fetched_event(tracking_receipt);
        }

        return Ok(Json(Response {
            response: Some(stored_response.payload),
            status: RequestStatus::Completed,
        }));
    }

    let status = status.ok_or(StatusCode::NOT_FOUND)?;
    let status = RequestStatus::from_str(&status).map_err(|error| {
        tracing::error!("Failed to parse status: {error}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    Ok(Json(Response {
        status,
        response: None,
    }))
}
