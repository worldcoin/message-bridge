use std::sync::Arc;

use axum::{
    http::{header, HeaderMap, HeaderValue, StatusCode},
    Extension,
};
use axum_jsonschema::Json;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use redis::aio::ConnectionManager;
use schemars::JsonSchema;
use uuid::Uuid;

use crate::{
    observability, session,
    utils::{handle_redis_error, validate_request_id, AppOverrides, RequestPayload},
};

#[derive(Debug, serde::Deserialize, JsonSchema)]
pub(super) struct CreateRequestBody {
    /// The initialization vector for the encrypted payload (opaque to the bridge).
    iv: String,
    /// The encrypted payload (opaque to the bridge).
    payload: String,
    /// Optional client-supplied `request_id`. When present, the bridge stores
    /// the request under this key with NX semantics (409 on collision); when
    /// absent, the bridge generates a UUID v4. Lets the RP address requests by
    /// any opaque identifier they choose (e.g. an HKDF output) so the bridge
    /// stays a generic content-addressable single-use store rather than baking
    /// in any specific application's flow.
    ///
    /// **Entropy is the RP's responsibility.** The bridge enforces length
    /// bounds and a charset (see `validate_request_id`) but does not check
    /// cryptographic randomness. A predictable identifier lets a third party
    /// GETDEL the request before the legitimate consumer does — confidentiality
    /// still holds because the payload is encrypted, but the single-use
    /// guarantee is broken. RPs should use a high-entropy source (UUID v4,
    /// HKDF output, etc.). TODO: enforce a stronger guarantee at the protocol
    /// layer — e.g. a server-mixed nonce — once we have a clear story for it.
    #[serde(default)]
    request_id: Option<String>,
    /// Opt-in capability flag. The bridge only surfaces `app_overrides` in the
    /// response when the client explicitly advertises that it can parse it.
    ///
    /// Adding a field to the response is **not** safe for clients that reject
    /// unknown JSON keys (e.g. a strict `kotlinx.serialization` decoder), and
    /// those clients are already shipped — they can't be retroactively fixed.
    /// Since `POST /request` carries no platform/version, gate the field behind
    /// an explicit flag instead: clients that predate the feature omit it and
    /// get the lean `{request_id}` response, so a server-side override rollout
    /// can never break them. Updated SDKs send `true` to receive overrides.
    #[serde(default)]
    supports_app_overrides: bool,
    /// Require a private bearer credential for response reads.
    #[serde(default)]
    require_token_to_read_response: bool,
}

#[derive(serde::Serialize, JsonSchema)]
pub(super) struct RequestCreatedPayload {
    /// The unique identifier for the request — the client-supplied value if
    /// one was provided, otherwise a server-generated UUID v4.
    request_id: String,
    /// Private response-read credential; returned only to the creating caller.
    #[serde(skip_serializing_if = "Option::is_none")]
    response_read_token: Option<String>,
    /// Temporary workaround for the World ID app rollout: lets the server
    /// configure deeplink values and app-specific overrides. Only populated for
    /// clients that opt in via `supports_app_overrides`; omitted entirely
    /// otherwise (empty map) so unaware clients see the legacy `{request_id}`
    /// response shape.
    #[serde(skip_serializing_if = "AppOverrides::is_empty")]
    app_overrides: AppOverrides,
}

/// Create a new request. Optionally accepts a client-supplied `request_id`
/// with NX semantics; otherwise generates a UUID v4.
#[tracing::instrument(
    parent = None,
    name = "message_bridge.request.create",
    skip_all,
    fields(
        idkit_flow_id = tracing::field::Empty,
        http.route = "/request",
    )
)]
pub(super) async fn handler(
    Extension(mut redis): Extension<ConnectionManager>,
    Extension(app_overrides): Extension<Arc<AppOverrides>>,
    Json(body): Json<CreateRequestBody>,
) -> Result<(HeaderMap, Json<RequestCreatedPayload>), StatusCode> {
    let CreateRequestBody {
        iv,
        payload,
        request_id,
        supports_app_overrides,
        require_token_to_read_response,
    } = body;

    let client_supplied_id = request_id.is_some();
    let request_id = match request_id {
        Some(id) => {
            let id = id.to_lowercase();
            validate_request_id(&id)?;
            id
        }
        None => Uuid::new_v4().to_string(),
    };

    let payload = RequestPayload::new(iv, payload);
    let payload_bytes =
        serde_json::to_vec(&payload).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let response_read_token = if require_token_to_read_response {
        Some(generate_response_read_token()?)
    } else {
        None
    };
    let token_digest = response_read_token.as_deref().map(session::token_digest);

    let idkit_flow_id = observability::IdkitFlowId::new();
    let request_id = create_request(
        &mut redis,
        request_id,
        client_supplied_id,
        &idkit_flow_id,
        &payload_bytes,
        token_digest.as_deref(),
    )
    .await?;

    observability::record_idkit_flow_id(&idkit_flow_id);

    // supports_flow_telemetry/client_name are reported on GET /request/:id
    // instead — the RP calling POST has no way to know either.
    telemetry_batteries::reexports::metrics::counter!("message_bridge.request_created")
        .increment(1);

    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok((
        headers,
        Json(RequestCreatedPayload {
            request_id,
            response_read_token,
            app_overrides: select_response_overrides(supports_app_overrides, &app_overrides),
        }),
    ))
}

fn generate_response_read_token() -> Result<String, StatusCode> {
    let mut bytes = [0_u8; 32];
    rustls::crypto::ring::default_provider()
        .secure_random
        .fill(&mut bytes)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(STANDARD.encode(bytes))
}

async fn create_request(
    redis: &mut ConnectionManager,
    mut request_id: String,
    client_supplied_id: bool,
    idkit_flow_id: &observability::IdkitFlowId,
    payload: &[u8],
    token_digest: Option<&[u8]>,
) -> Result<String, StatusCode> {
    const MAX_RESERVATION_ATTEMPTS: usize = 3;
    let allow_existence_check = !client_supplied_id;

    for attempt in 1..=MAX_RESERVATION_ATTEMPTS {
        if session::create_request(
            redis,
            &request_id,
            idkit_flow_id,
            payload,
            token_digest,
            allow_existence_check,
        )
        .await
        .map_err(handle_redis_error)?
        {
            return Ok(request_id);
        }
        if client_supplied_id {
            return Err(StatusCode::CONFLICT);
        }
        if attempt < MAX_RESERVATION_ATTEMPTS {
            request_id = Uuid::new_v4().to_string();
        }
    }
    Err(StatusCode::SERVICE_UNAVAILABLE)
}

/// Pick the overrides to echo back on `POST /request`: the configured map for
/// clients that opted in via `supports_app_overrides`, otherwise an empty map
/// (omitted from the response). Keeps the field invisible to clients that
/// predate the feature so a server-side override rollout can't break a strict,
/// unknown-key-rejecting JSON parser.
fn select_response_overrides(opted_in: bool, configured: &AppOverrides) -> AppOverrides {
    if opted_in {
        configured.clone()
    } else {
        AppOverrides::default()
    }
}
