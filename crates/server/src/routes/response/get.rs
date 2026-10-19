use std::{str::FromStr, sync::Arc};

use axum::{extract::Path, http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::aio::ConnectionManager;
use schemars::JsonSchema;

use crate::{
    analytics::Analytics,
    observability,
    utils::{
        handle_redis_error, validate_request_id, RequestPayload, RequestStatus, REQ_STATUS_PREFIX,
    },
};

use super::{StoredResponse, RES_PREFIX};

#[derive(Debug, serde::Deserialize, serde::Serialize, JsonSchema)]
pub(super) struct Response {
    status: RequestStatus,
    response: Option<RequestPayload>,
}

/// (status, response payload, flow ID, `supports_flow_telemetry` flag,
/// `client_name`), as read from Redis.
type PollResult = (
    Option<String>,
    Option<Vec<u8>>,
    Option<String>,
    Option<String>,
    Option<String>,
);

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

    // Read the correlation key without consuming it while the response is
    // still being polled. Successful response delivery cleans it up below.
    let mut pipe = redis::pipe();
    pipe.atomic()
        .get(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .get_del(format!("{RES_PREFIX}{request_id}"))
        .get(observability::flow_key(&request_id))
        .get(observability::supports_flow_telemetry_key(&request_id))
        .get(observability::client_name_key(&request_id));

    let (status, value, flow, supports_flow_telemetry, client_name): PollResult = pipe
        .query_async(&mut redis)
        .await
        .map_err(handle_redis_error)?;

    observability::record_response_handoff(flow.as_deref());

    if let Some(value) = value {
        let stored_response: StoredResponse =
            serde_json::from_slice(&value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Cleanup is best effort after GETDEL: the correlation key has a
        // fixed expiry and response delivery must not fail because of it.
        let mut cleanup = redis::pipe();
        cleanup
            .atomic()
            .del(format!("{REQ_STATUS_PREFIX}{request_id}"))
            .del(observability::flow_key(&request_id))
            .del(observability::supports_flow_telemetry_key(&request_id))
            .del(observability::client_name_key(&request_id));
        if cleanup
            .query_async::<(u64, u64, u64, u64)>(&mut redis)
            .await
            .is_err()
        {
            tracing::warn!(
                outcome = "flow_id_cleanup_failed",
                operation = "response_handoff",
                "Failed to clean up IDKit flow ID"
            );
        }

        telemetry_batteries::reexports::metrics::counter!(
            "message_bridge.response_consumed",
            "supports_flow_telemetry" => observability::supports_flow_telemetry_tag_from_stored(supports_flow_telemetry.as_deref()),
            "client_name" => observability::client_name_tag_from_stored(client_name.as_deref())
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
