//! HTTP client for the message bridge's single-use request/response API.
//!
//! Payloads must already be encrypted by the caller. This crate handles transport only;
//! it does not implement application protocols or key management.

#![deny(clippy::all, clippy::pedantic, clippy::nursery)]

use std::{future::Future, time::Duration};

use backon::{ExponentialBuilder, Retryable as _, Sleeper as _};
use futures_util::future::{select, Either};
use futures_util::StreamExt as _;
use reqwest::{RequestBuilder, Response, StatusCode, Url};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// The message bridge's response body limit.
const MAX_BODY_SIZE: usize = 5 * 1024 * 1024;

/// The time budget of one operation, including its retries and reading the response.
const OPERATION_TIMEOUT: Duration = Duration::from_mins(1);

/// A client for one bridge deployment.
///
/// Transport failures, `429` and server errors are retried with bounded backoff and jitter.
/// Reading a request or a response is single-use, so a retry after a lost reply can find the
/// message gone.
#[derive(Clone, Debug)]
pub struct BridgeClient {
    http: reqwest::Client,
    base_url: Url,
}

/// Errors from talking to the bridge.
#[derive(Debug, thiserror::Error)]
pub enum BridgeError {
    /// The request failed in transit. The error carries no URL: the bridge routes contain the
    /// request id, which is the only key to the session.
    #[error("bridge request failed: {0}")]
    Transport(#[source] reqwest::Error),
    /// The bridge answered with a status the operation does not expect.
    #[error("bridge returned unexpected status {0}")]
    UnexpectedStatus(StatusCode),
    /// The bridge returned malformed JSON.
    #[error("invalid bridge response: {0}")]
    Json(#[from] serde_json::Error),
    /// The bridge response exceeded the bridge's body limit.
    #[error("bridge response exceeds the 5 MiB body limit")]
    TooLarge,
    /// The operation exhausted its time budget.
    #[error("bridge operation timed out")]
    Timeout,
    /// The bridge URL is not a supported HTTP(S) base URL.
    #[error("bridge URL must be HTTP(S), without credentials, query or fragment")]
    InvalidUrl,
}

/// An opaque, already encrypted bridge payload. Both fields are base64 strings.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptedPayload {
    /// Initialization vector chosen by the application encryption scheme.
    pub iv: String,
    /// Ciphertext, including any application authentication tag.
    pub payload: String,
}

/// The state of a response on the bridge (`GET /response/:id`).
#[derive(PartialEq, Eq, Deserialize)]
#[serde(tag = "status", content = "response", rename_all = "snake_case")]
pub enum ResponseState {
    /// The request has not been taken yet.
    Initialized,
    /// The request was taken; the response has not arrived yet.
    Retrieved,
    /// The response, which the read consumed.
    Completed(EncryptedPayload),
}

/// The body of `POST /request`.
#[derive(Serialize)]
struct PublishBody<'a> {
    request_id: String,
    #[serde(flatten)]
    request: &'a EncryptedPayload,
}

impl BridgeClient {
    /// Creates a client for a bridge's HTTP or HTTPS base URL.
    ///
    /// Native TLS uses bundled Mozilla roots; browsers use their own trust store.
    ///
    /// # Errors
    /// Returns an error for an unsupported URL or if the HTTP client cannot be built.
    pub fn new(base_url: Url) -> Result<Self, BridgeError> {
        if !matches!(base_url.scheme(), "http" | "https")
            || base_url.host_str().is_none()
            || !base_url.username().is_empty()
            || base_url.password().is_some()
            || base_url.query().is_some()
            || base_url.fragment().is_some()
        {
            return Err(BridgeError::InvalidUrl);
        }
        Ok(Self {
            http: http_client()?,
            base_url,
        })
    }

    /// Stores the encrypted request (`POST /request`). A `409` means that an earlier attempt,
    /// whose reply was lost, stored it. The caller must use a unique, high-entropy id and
    /// reuse it only for retries of the same payload.
    ///
    /// # Errors
    /// Returns a transport, timeout or unexpected-status error.
    pub async fn publish_request(
        &self,
        request_id: &str,
        request: &EncryptedPayload,
    ) -> Result<(), BridgeError> {
        let body = PublishBody {
            request_id: request_id.to_string(),
            request,
        };
        bounded(async {
            let response = self
                .send(self.http.post(self.url(&["request"])).json(&body))
                .await?;
            match response.status() {
                StatusCode::OK | StatusCode::CONFLICT => Ok(()),
                status => Err(BridgeError::UnexpectedStatus(status)),
            }
        })
        .await
    }

    /// Takes the encrypted request (`GET /request/:id`). Returns `None` if it expired or was
    /// already taken.
    ///
    /// # Errors
    /// Returns an error on transport failure, timeout, unexpected status or invalid JSON.
    pub async fn take_request(
        &self,
        request_id: &str,
    ) -> Result<Option<EncryptedPayload>, BridgeError> {
        self.read(self.url(&["request", request_id])).await
    }

    /// Reads the state of the response (`GET /response/:id`), consuming a completed one. Returns
    /// `None` if the session expired or its response was already consumed.
    ///
    /// # Errors
    /// Returns an error on transport failure, timeout, unexpected status or invalid JSON.
    pub async fn fetch_response(
        &self,
        request_id: &str,
    ) -> Result<Option<ResponseState>, BridgeError> {
        self.read(self.url(&["response", request_id])).await
    }

    /// Stores the encrypted response (`PUT /response/:id`). The bridge answers `201` once it
    /// stored it; a `400` or `409` after a failed attempt can mean that attempt was stored.
    ///
    /// # Errors
    /// Returns a transport, timeout or unexpected-status error.
    pub async fn put_response(
        &self,
        request_id: &str,
        response: &EncryptedPayload,
    ) -> Result<(), BridgeError> {
        let url = self.url(&["response", request_id]);
        bounded(async {
            let reply = self.send(self.http.put(url).json(response)).await?;
            match reply.status() {
                StatusCode::CREATED => Ok(()),
                status => Err(BridgeError::UnexpectedStatus(status)),
            }
        })
        .await
    }

    async fn read<T: DeserializeOwned>(&self, url: Url) -> Result<Option<T>, BridgeError> {
        bounded(async {
            let response = self.send(self.http.get(url)).await?;
            match response.status() {
                StatusCode::OK => read_json(response).await.map(Some),
                StatusCode::NOT_FOUND => Ok(None),
                status => Err(BridgeError::UnexpectedStatus(status)),
            }
        })
        .await
    }

    /// Sends `request`, retrying transient failures.
    ///
    /// Each attempt owns its request so the operation remains `Send`.
    async fn send(&self, request: RequestBuilder) -> Result<Response, BridgeError> {
        let request = request.build()?;
        let attempt = || {
            let http = self.http.clone();
            let request = request
                .try_clone()
                .expect("bridge requests have buffered bodies");
            async move {
                let response = http.execute(request).await?;
                let status = response.status();
                if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
                    return Err(BridgeError::UnexpectedStatus(status));
                }
                Ok(response)
            }
        };
        attempt
            .retry(retry_backoff())
            .when(BridgeError::is_transient)
            .await
    }

    fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.base_url.clone();
        url.path_segments_mut()
            .expect("`new` accepts only HTTP(S) URLs")
            .pop_if_empty()
            .extend(segments);
        url
    }
}

impl BridgeError {
    fn is_transient(&self) -> bool {
        match self {
            Self::Transport(error) => !error.is_builder(),
            Self::UnexpectedStatus(status) => {
                status.is_server_error() || *status == StatusCode::TOO_MANY_REQUESTS
            }
            Self::Json(_) | Self::TooLarge | Self::Timeout | Self::InvalidUrl => false,
        }
    }
}

impl From<reqwest::Error> for BridgeError {
    fn from(error: reqwest::Error) -> Self {
        Self::Transport(error.without_url())
    }
}

/// Builds the HTTP client for the bridge. Browsers decide connection and request timeouts
/// themselves.
fn http_client() -> Result<reqwest::Client, BridgeError> {
    let builder = reqwest::Client::builder();
    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder.tls_certs_only(
        webpki_root_certs::TLS_SERVER_ROOT_CERTS
            .iter()
            .map(|cert| reqwest::Certificate::from_der(cert).expect("bundled root is DER")),
    );
    #[cfg(not(target_arch = "wasm32"))]
    let builder = builder
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30));
    Ok(builder.build()?)
}

/// Runs a bridge operation within [`OPERATION_TIMEOUT`].
async fn bounded<T>(
    operation: impl Future<Output = Result<T, BridgeError>>,
) -> Result<T, BridgeError> {
    let timeout = backon::DefaultSleeper::default().sleep(OPERATION_TIMEOUT);
    match select(std::pin::pin!(operation), std::pin::pin!(timeout)).await {
        Either::Left((output, _)) => output,
        Either::Right(_) => Err(BridgeError::Timeout),
    }
}

fn retry_backoff() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(500))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(3)
        .with_jitter()
}

async fn read_json<T: DeserializeOwned>(response: Response) -> Result<T, BridgeError> {
    let mut chunks = std::pin::pin!(response.bytes_stream());
    let mut body = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        if body.len() + chunk.len() > MAX_BODY_SIZE {
            return Err(BridgeError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(serde_json::from_slice(&body)?)
}
