//! Correlation helpers for tracing a message across bridge handoffs.
//!
//! Records request/response create and consume spans and counters. Flow
//! identifiers connect those spans so handoff latency can be measured. A
//! client-reported `supports_flow_telemetry` flag is relayed onto those
//! counters as a `supports_flow_telemetry:{bool}` tag, uninterpreted.

use std::fmt;

use redis::{aio::ConnectionManager, AsyncCommands};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{field, Span};
use uuid::Uuid;

use crate::utils::EXPIRE_AFTER_SECONDS;

/// Redis namespace used to carry a flow identifier to the response leg.
pub const FLOW_PREFIX: &str = "flow:";

const IDKIT_FLOW_ID_PREFIX: &str = "idkitflow_";

/// Covers the request wait, app processing, and response wait windows.
const FLOW_EXPIRE_AFTER_SECONDS: u64 = EXPIRE_AFTER_SECONDS * 3;

/// Opaque correlation identifier shared by the spans in one `IDKit` flow.
///
/// The textual prefix distinguishes this value from request IDs and other
/// UUID-shaped identifiers without attaching any client-specific meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct IdkitFlowId(String);

impl IdkitFlowId {
    /// Generate a new prefixed correlation identifier.
    fn new() -> Self {
        Self(format!("{IDKIT_FLOW_ID_PREFIX}{}", Uuid::new_v4()))
    }

    /// Return the serialized representation stored in Redis and JSON.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_redis(value: &str) -> Option<Self> {
        let uuid = value.strip_prefix(IDKIT_FLOW_ID_PREFIX)?;
        Uuid::parse_str(uuid).ok()?;
        Some(Self(value.to_string()))
    }
}

impl fmt::Display for IdkitFlowId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Build the Redis key used to carry a flow identifier between route legs.
#[must_use]
pub fn flow_key(request_id: &str) -> String {
    format!("{FLOW_PREFIX}{request_id}")
}

/// Redis namespace carrying the `supports_flow_telemetry` flag between legs.
const SUPPORTS_FLOW_TELEMETRY_PREFIX: &str = "supports_flow_telemetry:";

#[must_use]
pub fn supports_flow_telemetry_key(request_id: &str) -> String {
    format!("{SUPPORTS_FLOW_TELEMETRY_PREFIX}{request_id}")
}

/// Always overwrites, even when `false` — otherwise a stale `true` could
/// leak into a reused `request_id`. Best-effort on write failure.
pub async fn store_supports_flow_telemetry_flag(
    redis: &mut ConnectionManager,
    request_id: &str,
    in_cohort: bool,
) {
    if let Err(error) = redis
        .set_ex::<_, _, ()>(
            supports_flow_telemetry_key(request_id),
            supports_flow_telemetry_tag(in_cohort),
            FLOW_EXPIRE_AFTER_SECONDS,
        )
        .await
    {
        tracing::warn!(
            outcome = "supports_flow_telemetry_write_failed",
            operation = "request_handoff",
            "Failed to persist supports_flow_telemetry flag: {error}"
        );
    }
}

/// Render the `supports_flow_telemetry` tag value for a `message_bridge.*` counter.
#[must_use]
pub const fn supports_flow_telemetry_tag(in_cohort: bool) -> &'static str {
    if in_cohort {
        "true"
    } else {
        "false"
    }
}

/// Redis namespace carrying the client's `client_name` between legs.
const CLIENT_NAME_PREFIX: &str = "client_name:";

#[must_use]
pub fn client_name_key(request_id: &str) -> String {
    format!("{CLIENT_NAME_PREFIX}{request_id}")
}

/// Same always-overwrite policy as `store_supports_flow_telemetry_flag`.
pub async fn store_client_name(
    redis: &mut ConnectionManager,
    request_id: &str,
    client_name: Option<&str>,
) {
    if let Err(error) = redis
        .set_ex::<_, _, ()>(
            client_name_key(request_id),
            client_name_tag(client_name),
            FLOW_EXPIRE_AFTER_SECONDS,
        )
        .await
    {
        tracing::warn!(
            outcome = "client_name_write_failed",
            operation = "request_handoff",
            "Failed to persist client_name: {error}"
        );
    }
}

/// Bounds the tag to a fixed vocabulary instead of relaying the client's
/// string verbatim, which would let cardinality grow unbounded.
#[must_use]
pub fn client_name_tag(client_name: Option<&str>) -> &'static str {
    match client_name {
        Some("ios") => "ios",
        Some("android") => "android",
        Some("ios-id") => "ios-id",
        Some("android-id") => "android-id",
        Some("ios-money") => "ios-money",
        Some("android-money") => "android-money",
        Some(_) => "invalid",
        None => "unknown",
    }
}

/// Same tag, computed from a value already read back out of Redis.
#[must_use]
pub fn supports_flow_telemetry_tag_from_stored(value: Option<&str>) -> &'static str {
    supports_flow_telemetry_tag(value == Some("true"))
}

/// Matches the stored literal directly (rather than via `client_name_tag`,
/// which would fold a stored `"unknown"` into `"invalid"`).
#[must_use]
pub fn client_name_tag_from_stored(value: Option<&str>) -> &'static str {
    match value {
        Some("ios") => "ios",
        Some("android") => "android",
        Some("ios-id") => "ios-id",
        Some("android-id") => "android-id",
        Some("ios-money") => "ios-money",
        Some("android-money") => "android-money",
        Some("invalid") => "invalid",
        _ => "unknown",
    }
}

/// Mint and persist a flow identifier for a newly created request.
///
/// # Errors
///
/// Returns `flow_id_write_failed` when Redis cannot store the identifier.
pub async fn mint_and_store_idkit_flow_id(
    redis: &mut ConnectionManager,
    request_id: &str,
) -> Result<IdkitFlowId, &'static str> {
    let idkit_flow_id = IdkitFlowId::new();
    record_idkit_flow_id(&idkit_flow_id);

    redis
        .set_ex::<_, _, ()>(
            flow_key(request_id),
            idkit_flow_id.as_str(),
            FLOW_EXPIRE_AFTER_SECONDS,
        )
        .await
        .map_err(|_| "flow_id_write_failed")?;

    Ok(idkit_flow_id)
}

/// Record the flow identifier observed while consuming a request.
pub fn record_request_handoff(value: Option<&str>) -> Option<IdkitFlowId> {
    let Some(value) = value else {
        tracing::warn!(
            outcome = "flow_id_missing",
            operation = "request_handoff",
            "Failed to observe IDKit flow ID"
        );
        return None;
    };
    let Some(idkit_flow_id) = IdkitFlowId::from_redis(value) else {
        tracing::warn!(
            outcome = "flow_id_invalid",
            operation = "request_handoff",
            "Failed to observe IDKit flow ID"
        );
        return None;
    };
    record_idkit_flow_id(&idkit_flow_id);

    Some(idkit_flow_id)
}

/// Record a response-side flow identifier when one exists.
///
/// Missing identifiers are expected for standalone and legacy response flows.
/// Malformed identifiers are reported but never block payload delivery.
pub fn record_response_handoff(value: Option<&str>) {
    let Some(value) = value else {
        return;
    };
    let Some(idkit_flow_id) = IdkitFlowId::from_redis(value) else {
        tracing::warn!(
            outcome = "flow_id_invalid",
            operation = "response_handoff",
            "Failed to observe IDKit flow ID"
        );
        return;
    };
    record_idkit_flow_id(&idkit_flow_id);
}

fn record_idkit_flow_id(idkit_flow_id: &IdkitFlowId) {
    Span::current().record("idkit_flow_id", field::display(idkit_flow_id));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idkit_flow_id_has_a_recognizable_prefix_and_uuid_suffix() {
        let flow_id = IdkitFlowId::new();
        let uuid = flow_id
            .as_str()
            .strip_prefix(IDKIT_FLOW_ID_PREFIX)
            .expect("flow ID should use the IDKit prefix");

        assert!(Uuid::parse_str(uuid).is_ok());
        assert_eq!(
            IdkitFlowId::from_redis(flow_id.as_str()),
            Some(flow_id.clone())
        );
        assert!(IdkitFlowId::from_redis("not-a-flow-id").is_none());
    }

    #[test]
    fn supports_flow_telemetry_tag_reflects_cohort_membership() {
        assert_eq!(supports_flow_telemetry_tag(true), "true");
        assert_eq!(supports_flow_telemetry_tag(false), "false");
    }

    #[test]
    fn client_name_tag_recognizes_known_client_names() {
        assert_eq!(client_name_tag(Some("ios")), "ios");
        assert_eq!(client_name_tag(Some("android")), "android");
        assert_eq!(client_name_tag(Some("ios-id")), "ios-id");
        assert_eq!(client_name_tag(Some("android-id")), "android-id");
        assert_eq!(client_name_tag(Some("ios-money")), "ios-money");
        assert_eq!(client_name_tag(Some("android-money")), "android-money");
    }

    #[test]
    fn client_name_tag_falls_back_to_unknown_when_absent() {
        assert_eq!(client_name_tag(None), "unknown");
    }

    #[test]
    fn client_name_tag_bounds_unrecognized_values_to_invalid() {
        // Anything outside the fixed vocabulary must collapse to one value,
        // not be relayed verbatim — otherwise a buggy or hostile caller could
        // mint one metric time series per distinct string it sends.
        assert_eq!(client_name_tag(Some("iOS")), "invalid");
        assert_eq!(client_name_tag(Some("")), "invalid");
        assert_eq!(client_name_tag(Some("anything-else")), "invalid");
    }
}
