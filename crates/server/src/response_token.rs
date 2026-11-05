//! Server-issued credentials for opt-in response reads. Never log credentials or hashes.

use axum::http::{header::AUTHORIZATION, HeaderMap, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use headers::{authorization::Bearer, Authorization, HeaderMapExt as _};
use redis::{aio::ConnectionManager, AsyncCommands};
use ring::{
    digest,
    rand::{SecureRandom as _, SystemRandom},
};
use subtle::ConstantTimeEq as _;

use crate::utils::{handle_redis_error, EXPIRE_AFTER_SECONDS};

// Deliberate exception to the payload TTL: the extra margin allows separate
// refreshes to keep protection alive throughout status and response retention.
pub const TOKEN_EXPIRE_AFTER_SECONDS: u64 = EXPIRE_AFTER_SECONDS + 300;

pub fn key(request_id: &str) -> String {
    format!("res:token:{request_id}")
}

pub fn generate() -> Result<String, StatusCode> {
    let mut bytes = [0_u8; 32];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(STANDARD.encode(bytes))
}

pub fn hash(token: &str) -> digest::Digest {
    digest::digest(&digest::SHA256, token.as_bytes())
}

pub fn authenticates(headers: &HeaderMap, expected: &[u8]) -> bool {
    if headers.get_all(AUTHORIZATION).iter().count() != 1 {
        return false;
    }
    let Some(authorization) = headers.typed_get::<Authorization<Bearer>>() else {
        return false;
    };
    let token = authorization.token();
    if token.len() != 44 {
        return false;
    }
    bool::from(hash(token).as_ref().ct_eq(expected))
}

/// EXPIRE is a no-op for sessions that never opted in. Refresh before extending
/// status/response retention so Redis failures do not extend it without protection.
pub async fn refresh(redis: &mut ConnectionManager, request_id: &str) -> Result<(), StatusCode> {
    redis
        .expire::<_, bool>(
            key(request_id),
            i64::try_from(TOKEN_EXPIRE_AFTER_SECONDS).expect("token TTL fits i64"),
        )
        .await
        .map_err(handle_redis_error)?;
    Ok(())
}
