use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use reqwest::{Client, StatusCode, Url};
use serde::Serialize;
use telemetry_batteries::reexports::metrics::counter;
use tokio::sync::Semaphore;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_ATTEMPTS: u32 = 3;
const MAX_IN_FLIGHT: usize = 128;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub struct Analytics {
    client: Client,
    response_fetched_url: Url,
    in_flight: Arc<Semaphore>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ResponseFetchedEvent {
    tracking_receipt: String,
}

impl Analytics {
    /// Create an analytics client for the configured response-fetched URL.
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP client cannot be built.
    pub fn new(response_fetched_url: Url) -> Result<Self, reqwest::Error> {
        let _provider_install_result = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            // App Backend's WAF rejects requests without a User-Agent header.
            .user_agent("wallet-bridge")
            .connect_timeout(Duration::from_millis(500))
            .timeout(REQUEST_TIMEOUT)
            .build()?;

        Ok(Self {
            client,
            response_fetched_url,
            in_flight: Arc::new(Semaphore::new(MAX_IN_FLIGHT)),
        })
    }

    pub fn send_response_fetched_event(&self, tracking_receipt: String) {
        // Acquire before spawning: neither requests nor waiting tasks may grow unbounded.
        let Ok(permit) = Arc::clone(&self.in_flight).try_acquire_owned() else {
            record_outcome("capacity_exhausted");
            return;
        };
        let analytics = self.clone();
        let _handle = tokio::spawn(async move {
            let _permit = permit;
            analytics.deliver(tracking_receipt).await;
        });
    }

    async fn deliver(&self, tracking_receipt: String) {
        // All attempts use the same receipt; the receiver deduplicates receipt/event.
        let event = ResponseFetchedEvent { tracking_receipt };
        for attempt in 1..=MAX_ATTEMPTS {
            counter!("message_bridge.analytics_callback_attempted").increment(1);
            let result = self
                .client
                .post(self.response_fetched_url.clone())
                .json(&event)
                .send()
                .await;
            let retry_after = match result {
                Ok(response) if response.status().is_success() => {
                    record_outcome("accepted");
                    return;
                }
                Ok(response) => {
                    if !retryable_status(response.status()) {
                        record_outcome("permanent_failure");
                        tracing::warn!(status = %response.status(), "Analytics callback rejected");
                        return;
                    }
                    response
                        .headers()
                        .get(reqwest::header::RETRY_AFTER)
                        .and_then(|value| value.to_str().ok())
                        .and_then(parse_retry_after)
                }
                Err(error) => {
                    if !error.is_timeout()
                        && !error.is_connect()
                        && !error.is_request()
                        && !error.is_body()
                    {
                        record_outcome("permanent_failure");
                        return;
                    }
                    // Do not log the receipt or callback URL.
                    None
                }
            };
            if attempt == MAX_ATTEMPTS {
                record_outcome("retry_exhausted");
                return;
            }
            if retry_after.is_some_and(|delay| delay > MAX_RETRY_DELAY) {
                // Do not retry earlier than the server requested or retain tasks indefinitely.
                record_outcome("retry_after_exceeded");
                return;
            }
            counter!("message_bridge.analytics_callback_retried").increment(1);
            tokio::time::sleep(retry_after.unwrap_or_else(|| retry_delay(attempt))).await;
        }
    }
}

const fn retryable_status(status: StatusCode) -> bool {
    status.is_server_error() || matches!(status.as_u16(), 408 | 429)
}

fn parse_retry_after(value: &str) -> Option<Duration> {
    value.parse::<u64>().map(Duration::from_secs).ok().or_else(|| {
        httpdate::parse_http_date(value)
            .ok()
            .map(|date| date.duration_since(SystemTime::now()).unwrap_or_default())
    })
}

fn retry_delay(attempt: u32) -> Duration {
    let random = uuid::Uuid::new_v4();
    let bytes = random.as_bytes();
    let jitter = u64::from(u16::from_le_bytes([bytes[0], bytes[1]]));
    Duration::from_millis(jitter % (500 * (1_u64 << (attempt - 1)) + 1))
}

fn record_outcome(outcome: &'static str) {
    counter!("message_bridge.analytics_callback_outcome", "outcome" => outcome).increment(1);
}

#[cfg(test)]
mod tests {
    use super::{parse_retry_after, Analytics};
    use axum::{extract::State, routing::post, Json, Router};
    use reqwest::{StatusCode, Url};
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::{Duration, SystemTime},
    };

    type TestState = (Vec<StatusCode>, Arc<AtomicUsize>);

    async fn callback(
        State((statuses, calls)): State<TestState>,
        Json(body): Json<serde_json::Value>,
    ) -> StatusCode {
        assert_eq!(body["trackingReceipt"], "same-receipt");
        let index = calls.fetch_add(1, Ordering::SeqCst);
        statuses[index.min(statuses.len() - 1)]
    }

    async fn serve(
        statuses: Vec<StatusCode>,
    ) -> (Analytics, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = (statuses, Arc::clone(&calls));
        let app = Router::new().route("/", post(callback)).with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let handle = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (Analytics::new(url).unwrap(), calls, handle)
    }

    #[tokio::test]
    async fn transient_failure_retries_same_receipt() {
        let (analytics, calls, server) =
            serve(vec![StatusCode::SERVICE_UNAVAILABLE, StatusCode::NO_CONTENT]).await;
        analytics.deliver("same-receipt".into()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        server.abort();
    }

    #[tokio::test]
    async fn permanent_failure_is_not_retried() {
        let (analytics, calls, server) = serve(vec![StatusCode::BAD_REQUEST]).await;
        analytics.deliver("same-receipt".into()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test]
    async fn retries_are_bounded() {
        let (analytics, calls, server) = serve(vec![StatusCode::SERVICE_UNAVAILABLE]).await;
        analytics.deliver("same-receipt".into()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        server.abort();
    }

    #[test]
    fn retry_after_accepts_seconds_and_http_dates() {
        assert_eq!(parse_retry_after("2"), Some(Duration::from_secs(2)));
        let future = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(30));
        assert!(parse_retry_after(&future).unwrap() > Duration::from_secs(28));
        assert_eq!(parse_retry_after("invalid"), None);
    }
}
