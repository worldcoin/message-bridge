//! Correlation helpers for tracing a message across bridge handoffs.
//!
//! Records request/response create and consume spans and counters. Flow
//! identifiers connect those spans so handoff latency can be measured. A
//! client-reported `supports_flow_telemetry` flag is relayed onto those
//! counters as a `supports_flow_telemetry:{bool}` tag, uninterpreted.

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tracing::{field, Span};
use uuid::Uuid;

const IDKIT_FLOW_ID_PREFIX: &str = "idkitflow_";

/// Opaque correlation identifier shared by the spans in one `IDKit` flow.
///
/// The textual prefix distinguishes this value from request IDs and other
/// UUID-shaped identifiers without attaching any client-specific meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct IdkitFlowId(String);

impl IdkitFlowId {
    /// Generate a new prefixed correlation identifier.
    pub(crate) fn new() -> Self {
        Self(format!("{IDKIT_FLOW_ID_PREFIX}{}", Uuid::new_v4()))
    }

    /// Return the serialized representation stored in Redis and JSON.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn from_redis(value: &str) -> Option<Self> {
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

/// Render the `supports_flow_telemetry` tag value for a `message_bridge.*` counter.
#[must_use]
pub const fn supports_flow_telemetry_tag(in_cohort: bool) -> &'static str {
    if in_cohort {
        "true"
    } else {
        "false"
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

/// Record a response-side flow identifier when one exists.
///
/// Missing identifiers are expected for legacy response flows.
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

pub fn record_idkit_flow_id(idkit_flow_id: &IdkitFlowId) {
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
