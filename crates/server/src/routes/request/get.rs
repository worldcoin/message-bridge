use axum::{
    extract::Path,
    http::{HeaderMap, StatusCode},
    Extension,
};
use axum_jsonschema::Json;
use redis::{aio::ConnectionManager, AsyncCommands};
use schemars::JsonSchema;

use crate::{
    observability::{self, IdkitFlowId},
    utils::{
        handle_redis_error, validate_request_id, RequestPayload, RequestStatus,
        EXPIRE_AFTER_SECONDS, REQ_STATUS_PREFIX,
    },
};

use super::REQ_PREFIX;

/// Capability-negotiation header for backwards-compatible flow correlation.
///
/// When this header is present with the value `true`, `GET /request/:request_id`
/// includes `idkit_flow_id`. Callers that omit it retain the legacy
/// `{iv, payload}` response shape so clients that reject unknown fields remain
/// compatible.
const ACCEPT_IDKIT_FLOW_ID_HEADER: &str = "accept-idkit-flow-id";

/// Self-reported by the fetching client — the sole source of the
/// `supports_flow_telemetry`/`client_name` tags, since `POST /request`
/// can't know who, if anyone, will fetch its request.
const CLIENT_NAME_HEADER: &str = "client-name";
const SUPPORTS_FLOW_TELEMETRY_HEADER: &str = "supports-flow-telemetry";

#[derive(Debug, serde::Serialize, JsonSchema)]
pub(super) struct RequestResponse {
    /// The initialization vector for the encrypted payload.
    iv: String,
    /// The encrypted payload.
    payload: String,
    /// Only present when the client explicitly opts in via
    /// `accept-idkit-flow-id: true`.
    #[serde(skip_serializing_if = "Option::is_none")]
    idkit_flow_id: Option<IdkitFlowId>,
}

#[tracing::instrument(
    parent = None,
    name = "message_bridge.request.consume",
    skip_all,
    fields(
        idkit_flow_id = tracing::field::Empty,
        http.route = "/request/:request_id",
    )
)]
pub(super) async fn handler(
    Path(request_id): Path<String>,
    Extension(mut redis): Extension<ConnectionManager>,
    headers: HeaderMap,
) -> Result<Json<RequestResponse>, StatusCode> {
    let request_id = request_id.to_lowercase();
    validate_request_id(&request_id)?;

    let mut pipe = redis::pipe();
    pipe.atomic()
        .get_del(format!("{REQ_PREFIX}{request_id}"))
        .get(observability::flow_key(&request_id));

    let (value, idkit_flow_id): (Option<Vec<u8>>, Option<String>) = pipe
        .query_async(&mut redis)
        .await
        .map_err(handle_redis_error)?;

    let value = value.ok_or(StatusCode::NOT_FOUND)?;
    let payload: RequestPayload =
        serde_json::from_slice(&value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    redis
        .set_ex::<_, _, ()>(
            format!("{REQ_STATUS_PREFIX}{request_id}"),
            RequestStatus::Retrieved.to_string(),
            EXPIRE_AFTER_SECONDS,
        )
        .await
        .map_err(handle_redis_error)?;

    let idkit_flow_id = observability::record_request_handoff(idkit_flow_id.as_deref());

    let supports_flow_telemetry =
        header_bool(&headers, SUPPORTS_FLOW_TELEMETRY_HEADER).unwrap_or(false);
    let client_name = header_str(&headers, CLIENT_NAME_HEADER);

    observability::store_supports_flow_telemetry_flag(
        &mut redis,
        &request_id,
        supports_flow_telemetry,
    )
    .await;
    observability::store_client_name(&mut redis, &request_id, client_name).await;

    telemetry_batteries::reexports::metrics::counter!(
        "message_bridge.request_consumed",
        "supports_flow_telemetry" => observability::supports_flow_telemetry_tag(supports_flow_telemetry),
        "client_name" => observability::client_name_tag(client_name)
    )
    .increment(1);

    Ok(Json(RequestResponse {
        iv: payload.iv,
        payload: payload.payload,
        idkit_flow_id: accepts_idkit_flow_id(&headers)
            .then_some(idkit_flow_id)
            .flatten(),
    }))
}

fn accepts_idkit_flow_id(headers: &HeaderMap) -> bool {
    headers
        .get(ACCEPT_IDKIT_FLOW_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true"))
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name)?.to_str().ok()
}

/// `None` only when the header is missing/non-UTF-8; present-but-not-`"true"`
/// is a deliberate `Some(false)`.
fn header_bool(headers: &HeaderMap, name: &str) -> Option<bool> {
    header_str(headers, name).map(|value| value.eq_ignore_ascii_case("true"))
}
