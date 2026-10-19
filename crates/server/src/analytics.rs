use std::time::Duration;

use reqwest::{Client, Url};
use serde::Serialize;

const REQUEST_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub struct Analytics {
    client: Client,
    response_fetched_url: Url,
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
            .timeout(REQUEST_TIMEOUT)
            .build()?;

        Ok(Self {
            client,
            response_fetched_url,
        })
    }

    pub fn send_response_fetched_event(&self, tracking_receipt: String) {
        let client = self.client.clone();
        let url = self.response_fetched_url.clone();
        let _handle = tokio::spawn(async move {
            let result = client
                .post(url)
                .json(&ResponseFetchedEvent { tracking_receipt })
                .send()
                .await;

            match result {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => tracing::warn!(
                    status = %response.status(),
                    "Failed to send response-fetched analytics event"
                ),
                Err(error) => tracing::warn!(
                    error = %error,
                    "Failed to send response-fetched analytics event"
                ),
            }
        });
    }
}
