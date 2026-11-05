use std::{str::FromStr, sync::Arc};

use aide::{openapi::SecurityRequirement, transform::TransformOperation};
use axum::{
    extract::Path,
    http::{header::WWW_AUTHENTICATE, HeaderMap, StatusCode},
    response::{IntoResponse as _, Response as HttpResponse},
    Extension,
};
use axum_jsonschema::Json;
use redis::{aio::ConnectionManager, AsyncCommands as _};
use schemars::JsonSchema;

use crate::{
    analytics::Analytics,
    observability, response_token,
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

pub(super) fn documentation(operation: TransformOperation<'_>) -> TransformOperation<'_> {
    let mut operation = operation
        .description("Poll status or consume the response once. Every poll requires Authorization: Bearer <response_read_token> when POST /request opted in with require_token_to_read_response.")
        .security_requirement("response_read_token")
        .response::<200, Json<Response>>()
        .response_with::<401, (), _>(|response| response.description("Missing, malformed, or incorrect response token. WWW-Authenticate: Bearer."));
    operation
        .inner_mut()
        .security
        .push(SecurityRequirement::default());
    operation
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
    Extension(redis): Extension<ConnectionManager>,
    Extension(analytics): Extension<Option<Arc<Analytics>>>,
    headers: HeaderMap,
) -> HttpResponse {
    match poll(request_id, redis, analytics, headers).await {
        Ok(response) => response.into_response(),
        Err(StatusCode::UNAUTHORIZED) => {
            (StatusCode::UNAUTHORIZED, [(WWW_AUTHENTICATE, "Bearer")]).into_response()
        }
        Err(status) => status.into_response(),
    }
}

async fn poll(
    request_id: String,
    mut redis: ConnectionManager,
    analytics: Option<Arc<Analytics>>,
    headers: HeaderMap,
) -> Result<Json<Response>, StatusCode> {
    let request_id = request_id.to_lowercase();
    validate_request_id(&request_id)?;

    // Keep credential parsing and constant-time verification in Rust. Redis
    // consumes only if the exact credential state we checked is still current.
    let (status, value, flow, supports_flow_telemetry, client_name) = loop {
        let token_hash: Option<Vec<u8>> = redis
            .get(response_token::key(&request_id))
            .await
            .map_err(handle_redis_error)?;
        let authenticated = token_hash
            .as_deref()
            .is_some_and(|hash| response_token::authenticates(&headers, hash));
        let (result, values): (u16, Option<PollResult>) = redis::cmd("EVAL")
            .arg(include_str!("consume.lua"))
            .arg(6)
            .arg(response_token::key(&request_id))
            .arg(format!("{REQ_STATUS_PREFIX}{request_id}"))
            .arg(format!("{RES_PREFIX}{request_id}"))
            .arg(observability::flow_key(&request_id))
            .arg(observability::supports_flow_telemetry_key(&request_id))
            .arg(observability::client_name_key(&request_id))
            .arg(token_hash.is_some())
            .arg(token_hash.as_deref().unwrap_or_default())
            .arg(authenticated)
            .query_async(&mut redis)
            .await
            .map_err(handle_redis_error)?;
        match result {
            0 => {}
            200 => break values.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
            code => {
                return Err(StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
            }
        }
    };

    observability::record_response_handoff(flow.as_deref());

    if let Some(value) = value {
        let stored_response: StoredResponse =
            serde_json::from_slice(&value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
