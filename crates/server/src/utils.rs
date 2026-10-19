use std::{collections::HashMap, fmt::Display, str::FromStr};

use axum::http::StatusCode;
use redis::RedisError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const EXPIRE_AFTER_SECONDS: u64 = 900; // Increasing to allow partner verifications.
pub const REQ_STATUS_PREFIX: &str = "req:status:";

/// Maximum length of a `request_id`.
///
/// Whether supplied by the client on `POST /request` or extracted from a route
/// path. Bounds Redis-key memory and keeps URLs reasonable; 256 is more than
/// enough for UUIDs (36), HKDF-derived hex (64), and base64-shaped identifiers.
pub const REQUEST_ID_MAX_LEN: usize = 256;

/// Minimum length of a `request_id`.
///
/// Anything shorter is almost certainly a mistake — UUIDs are 36 chars,
/// UUID-simple is 32, HKDF-derived hex is 64, and base64url of 12 random bytes
/// is 16. The bound doesn't enforce cryptographic entropy on its own (a 16-char
/// string of `aaaa…` passes), but it stops trivial typos and obvious attacks;
/// the RP is responsible for picking high-entropy identifiers.
pub const REQUEST_ID_MIN_LEN: usize = 16;

/// A per-`app_id` override, this is a temporary workaround that enables smooth rollout of our new World ID app.
///
/// - `app_clip_bundle_id` is the App Clip's bundle identifier (the `p`
///   parameter of an `appclip.apple.com/id` default link, e.g.
///   `org.worldcoin.insight.Clip`).
/// - `verify_url` is a *base* URL; the SDK appends its own per-request query
///   params (`t/i/k/b`).
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
// Reject unknown keys so a misspelled override field (e.g. the old
// `app_clip_url`) fails to parse and trips the startup fail-fast, rather than
// silently producing an empty override that looks healthy but does nothing.
#[serde(deny_unknown_fields)]
pub struct AppOverride {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_clip_bundle_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verify_url: Option<String>,
}

/// A map with app overrides, loaded from environment during startup
pub type AppOverrides = HashMap<String, AppOverride>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum RequestStatus {
    /// The request has been initiated by the client
    Initialized,
    /// The request has been retrieved
    Retrieved,
    /// The request has received a response
    Completed,
}

impl Display for RequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Retrieved => write!(f, "retrieved"),
            Self::Completed => write!(f, "completed"),
            Self::Initialized => write!(f, "initialized"),
        }
    }
}

impl FromStr for RequestStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "initialized" => Ok(Self::Initialized),
            "retrieved" => Ok(Self::Retrieved),
            "completed" => Ok(Self::Completed),
            _ => Err(format!("Invalid status: {s}")),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
pub struct RequestPayload {
    /// The initialization vector for the encrypted payload
    pub(crate) iv: String,
    /// The encrypted payload
    pub(crate) payload: String,
}

impl RequestPayload {
    #[must_use]
    pub const fn new(iv: String, payload: String) -> Self {
        Self { iv, payload }
    }
}

#[allow(clippy::needless_pass_by_value)]
pub fn handle_redis_error(e: RedisError) -> StatusCode {
    tracing::error!("Redis error: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
}

/// Validate a `request_id` (path param or client-supplied body field).
///
/// Length must be between `REQUEST_ID_MIN_LEN` and `REQUEST_ID_MAX_LEN`, and the
/// charset is limited to URL-path-safe ASCII (alphanumeric plus `-`, `_`, `.`).
///
/// `:` is intentionally excluded from the charset: an ID like `status:foo`
/// would round-trip into the Redis key `req:status:foo`, colliding with the
/// status-namespace key `req:status:foo` that the bridge writes for some
/// other request. Keeping the charset disjoint from the literal `:` prevents
/// that overlap by construction.
///
/// # Errors
///
/// Returns [`StatusCode::BAD_REQUEST`] if `id` is outside the length bounds or
/// contains characters outside the allowed charset.
pub fn validate_request_id(id: &str) -> Result<(), StatusCode> {
    if id.len() < REQUEST_ID_MIN_LEN || id.len() > REQUEST_ID_MAX_LEN {
        return Err(StatusCode::BAD_REQUEST);
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn app_overrides_parse_from_json() {
        let raw = r#"{
            "app_one": {"app_clip_bundle_id": "org.one.Clip", "verify_url": "https://world.org/verify"},
            "app_two": {"app_clip_bundle_id": "org.two.Clip"},
            "app_three": {"verify_url": "https://world.org/verify"}
        }"#;
        let map: AppOverrides = serde_json::from_str(raw).expect("valid override JSON");

        assert_eq!(map.len(), 3);
        assert_eq!(
            map["app_one"].verify_url.as_deref(),
            Some("https://world.org/verify")
        );
        assert_eq!(
            map["app_one"].app_clip_bundle_id.as_deref(),
            Some("org.one.Clip")
        );
        // Independently optional fields.
        assert!(map["app_two"].verify_url.is_none());
        assert!(map["app_three"].app_clip_bundle_id.is_none());
    }

    #[test]
    fn app_override_omits_absent_fields_when_serialized() {
        // `skip_serializing_if` keeps the POST /request response lean and lets
        // the SDK treat "absent" as "fall back to default".
        let empty = AppOverride {
            app_clip_bundle_id: None,
            verify_url: None,
        };
        assert_eq!(serde_json::to_string(&empty).unwrap(), "{}");
    }

    #[test]
    fn app_overrides_rejects_unknown_fields() {
        // A misspelled/unknown override key (e.g. the old name `app_clip_url`)
        // must fail to parse rather than silently producing an empty override
        // entry — so a bad rollout config fails fast at startup instead of
        // looking healthy while doing nothing.
        let raw = r#"{"app_one": {"app_clip_url": "org.one.Clip"}}"#;
        assert!(serde_json::from_str::<AppOverrides>(raw).is_err());
    }

    #[test]
    fn empty_json_object_is_valid_and_disables_feature() {
        let map: AppOverrides = serde_json::from_str("{}").expect("empty object is valid");
        assert!(map.is_empty());
    }
}
