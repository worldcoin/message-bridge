use axum::{http::StatusCode, Extension};
use axum_jsonschema::Json;
use redis::{aio::ConnectionManager, AsyncCommands};
use schemars::JsonSchema;
use uuid::Uuid;

use crate::utils::{handle_redis_error, RequestStatus, EXPIRE_AFTER_SECONDS, REQ_STATUS_PREFIX};

use super::{StoredResponse, RES_PREFIX};

#[derive(Debug, serde::Serialize, JsonSchema)]
pub(super) struct ResponseCreatedPayload {
    /// The unique identifier for the response.
    request_id: String,
}

/// Create a standalone response without `IDKit` flow correlation.
pub(super) async fn handler(
    Extension(mut redis): Extension<ConnectionManager>,
    Json(request): Json<StoredResponse>,
) -> Result<(StatusCode, Json<ResponseCreatedPayload>), StatusCode> {
    let request_id = Uuid::new_v4().to_string();

    redis
        .set_ex::<_, _, ()>(
            format!("{REQ_STATUS_PREFIX}{request_id}"),
            RequestStatus::Initialized.to_string(),
            EXPIRE_AFTER_SECONDS,
        )
        .await
        .map_err(handle_redis_error)?;

    redis
        .set_ex::<_, _, ()>(
            format!("{RES_PREFIX}{request_id}"),
            serde_json::to_vec(&request).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
            EXPIRE_AFTER_SECONDS,
        )
        .await
        .map_err(handle_redis_error)?;

    Ok((
        StatusCode::CREATED,
        Json(ResponseCreatedPayload { request_id }),
    ))
}
