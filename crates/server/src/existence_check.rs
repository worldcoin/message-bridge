//! HEAD visibility for requests created with client-supplied IDs.

use axum::http::StatusCode;
use redis::{aio::ConnectionManager, AsyncCommands as _};

use crate::utils::handle_redis_error;

// Explicit exception to the payload TTL: protection outlives retained data.
const EXPIRE_AFTER_SECONDS: u64 = 1_200;

pub fn key(request_id: &str) -> String {
    format!("existence_check_allowed:{request_id}")
}

/// Disable existence checks for a request created with a client-supplied ID.
pub async fn protect(redis: &mut ConnectionManager, request_id: &str) -> Result<(), StatusCode> {
    redis
        .set_ex::<_, _, ()>(key(request_id), "false", EXPIRE_AFTER_SECONDS)
        .await
        .map_err(handle_redis_error)
}

/// Refresh only existing protection before extending any retained data.
pub async fn refresh(redis: &mut ConnectionManager, request_id: &str) -> Result<(), StatusCode> {
    redis
        .expire::<_, bool>(
            key(request_id),
            i64::try_from(EXPIRE_AFTER_SECONDS).expect("existence check TTL fits i64"),
        )
        .await
        .map_err(handle_redis_error)?;
    Ok(())
}

/// Check resource existence and permission to check it in one Redis transaction.
pub async fn exists(
    redis: &mut ConnectionManager,
    request_id: &str,
    resource_prefix: &'static str,
) -> Result<bool, StatusCode> {
    let (allowed, exists): (Option<String>, bool) = redis::pipe()
        .atomic()
        .get(key(request_id))
        .exists(format!("{resource_prefix}{request_id}"))
        .query_async(redis)
        .await
        .map_err(handle_redis_error)?;
    Ok(exists && allowed.as_deref().is_none_or(|value| value == "true"))
}
