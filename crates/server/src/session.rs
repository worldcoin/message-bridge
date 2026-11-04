//! Session reservation and telemetry are independent of single-use payload keys.

use crate::{
    observability::{self, IdkitFlowId},
    utils::{EXPIRE_AFTER_SECONDS, REQ_PREFIX, REQ_STATUS_PREFIX, RES_PREFIX},
};
use redis::{aio::ConnectionManager, RedisResult};

const SESSION_TTL_SECONDS: i64 = 2700;
const SESSION_PREFIX: &str = "session:";

fn session_key(request_id: &str) -> String {
    format!("{SESSION_PREFIX}{request_id}")
}

/// Build the legacy correlation key for existing sessions.
fn legacy_flow_key(request_id: &str) -> String {
    format!("flow:{request_id}")
}

fn legacy_supports_flow_telemetry_key(request_id: &str) -> String {
    format!("supports_flow_telemetry:{request_id}")
}

fn legacy_client_name_key(request_id: &str) -> String {
    format!("client_name:{request_id}")
}

// Keys used by atomic request creation and staging reset; only the session key blocks reuse.
fn keys(request_id: &str) -> [String; 7] {
    [
        session_key(request_id),
        format!("{REQ_PREFIX}{request_id}"),
        format!("{REQ_STATUS_PREFIX}{request_id}"),
        format!("{RES_PREFIX}{request_id}"),
        legacy_supports_flow_telemetry_key(request_id),
        legacy_client_name_key(request_id),
        legacy_flow_key(request_id),
    ]
}

pub async fn create_request(
    redis: &mut ConnectionManager,
    request_id: &str,
    idkit_flow_id: &IdkitFlowId,
    payload: &[u8],
    token_digest: Option<&[u8]>,
    allow_existence_check: bool,
) -> RedisResult<bool> {
    let keys = keys(request_id);
    redis::cmd("EVAL")
        .arg(include_str!("session/create_request.lua"))
        .arg(keys.len())
        .arg(&keys)
        .arg(SESSION_TTL_SECONDS)
        .arg(EXPIRE_AFTER_SECONDS)
        .arg(idkit_flow_id.as_str())
        .arg(payload)
        .arg(token_digest.unwrap_or_default())
        .arg(allow_existence_check.to_string())
        .query_async(redis)
        .await
}

/// Result of atomically authorizing a response read and consuming its payload.
pub struct ReadResponse {
    pub authorized: bool,
    pub status: Option<String>,
    pub response: Option<Vec<u8>>,
}

pub async fn read_response(
    redis: &mut ConnectionManager,
    request_id: &str,
    token: Option<&str>,
) -> RedisResult<ReadResponse> {
    let digest = token.map(token_digest).unwrap_or_default();
    let (authorized, status, response) = redis::cmd("EVAL")
        .arg(include_str!("session/read_response.lua"))
        .arg(3)
        .arg(session_key(request_id))
        .arg(format!("{REQ_STATUS_PREFIX}{request_id}"))
        .arg(format!("{RES_PREFIX}{request_id}"))
        .arg(digest)
        .query_async(redis)
        .await?;
    Ok(ReadResponse {
        authorized,
        status,
        response,
    })
}

/// Session telemetry, independent of request and response storage.
pub struct Metadata {
    pub idkit_flow_id: Option<String>,
    pub supports_flow_telemetry: Option<String>,
    pub client_name: Option<String>,
}

/// Read hash metadata with fallbacks for sessions created before the hash schema.
pub async fn metadata(redis: &mut ConnectionManager, request_id: &str) -> RedisResult<Metadata> {
    type Tags = (Option<String>, Option<String>, Option<String>);
    let (current, legacy): (Tags, Tags) = redis::pipe()
        .hmget(
            session_key(request_id),
            &["idkit_flow_id", "supports_flow_telemetry", "client_name"],
        )
        .mget(&[
            legacy_flow_key(request_id),
            legacy_supports_flow_telemetry_key(request_id),
            legacy_client_name_key(request_id),
        ])
        .query_async(redis)
        .await?;
    Ok(Metadata {
        idkit_flow_id: current.0.or(legacy.0),
        supports_flow_telemetry: current.1.or(legacy.1),
        client_name: current.2.or(legacy.2),
    })
}

/// Update client metadata without refreshing an existing reservation.
pub async fn store_client_metadata(
    redis: &mut ConnectionManager,
    request_id: &str,
    supports_flow_telemetry: bool,
    client_name: Option<&str>,
) -> RedisResult<Option<IdkitFlowId>> {
    let idkit_flow_id = metadata(redis, request_id).await?.idkit_flow_id;
    let key = session_key(request_id);

    let mut pipe = redis::pipe();
    pipe.atomic()
        .hset_multiple(
            &key,
            &[
                ("reserved", "1"),
                (
                    "supports_flow_telemetry",
                    observability::supports_flow_telemetry_tag(supports_flow_telemetry),
                ),
                ("client_name", observability::client_name_tag(client_name)),
            ],
        )
        .ignore();

    if let Some(idkit_flow_id) = &idkit_flow_id {
        pipe.hset_nx(&key, "idkit_flow_id", idkit_flow_id).ignore();
    }

    // NX only attaches a TTL to a newly created legacy-session hash.
    // Normal metadata updates preserve the original reservation deadline.
    pipe.cmd("EXPIRE")
        .arg(&key)
        .arg(SESSION_TTL_SECONDS)
        .arg("NX")
        .ignore();

    pipe.query_async::<()>(redis).await?;

    let parsed = idkit_flow_id.as_deref().and_then(IdkitFlowId::from_redis);
    if parsed.is_none() {
        let outcome = if idkit_flow_id.is_some() {
            "flow_id_invalid"
        } else {
            "flow_id_missing"
        };
        tracing::warn!(
            outcome,
            operation = "request_handoff",
            "Failed to observe IDKit flow ID"
        );
    }

    Ok(parsed)
}

/// Queue a staging reset in the same transaction as replacement payload writes.
pub fn reset(pipe: &mut redis::Pipeline, request_id: &str, idkit_flow_id: &IdkitFlowId) {
    let key = session_key(request_id);
    pipe.atomic();

    pipe.del(&keys(request_id)[1..]).ignore();

    pipe.hdel(
        &key,
        &["idkit_flow_id", "supports_flow_telemetry", "client_name"],
    )
    .ignore();

    pipe.hset_multiple(
        &key,
        &[("reserved", "1"), ("idkit_flow_id", idkit_flow_id.as_str())],
    )
    .ignore();

    pipe.cmd("EXPIRE")
        .arg(&key)
        .arg(SESSION_TTL_SECONDS)
        .arg("NX")
        .ignore();

    pipe.cmd("EXPIRE")
        .arg(&key)
        .arg(SESSION_TTL_SECONDS)
        .arg("GT")
        .ignore();
}

/// Clear telemetry after delivery without releasing or refreshing the reservation.
pub fn clear_metadata(pipe: &mut redis::Pipeline, request_id: &str) {
    pipe.hdel(
        session_key(request_id),
        &["idkit_flow_id", "supports_flow_telemetry", "client_name"],
    )
    .ignore();

    pipe.del(&[
        legacy_flow_key(request_id),
        legacy_supports_flow_telemetry_key(request_id),
        legacy_client_name_key(request_id),
    ])
    .ignore();
}

/// Hash a response-read credential before sending it to Redis.
pub fn token_digest(token: &str) -> Vec<u8> {
    ring::digest::digest(&ring::digest::SHA256, token.as_bytes())
        .as_ref()
        .to_vec()
}

/// Return whether the prefixed key exists and the session allows existence checks.
pub async fn exists_if_allowed(
    redis: &mut ConnectionManager,
    request_id: &str,
    prefix: &'static str,
) -> RedisResult<bool> {
    redis::cmd("EVAL")
        .arg(include_str!("session/exists_if_allowed.lua"))
        .arg(2)
        .arg(session_key(request_id))
        .arg(format!("{prefix}{request_id}"))
        .query_async(redis)
        .await
}
