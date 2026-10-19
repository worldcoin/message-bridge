#![cfg(feature = "test-util")]

use std::time::Duration;

use message_bridge::test_util::TestServer;

#[tokio::test]
async fn serves_the_production_router_and_shuts_down() {
    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_owned());
    let server = TestServer::start(&redis_url).await.unwrap();
    let _provider_install_result = rustls::crypto::ring::default_provider().install_default();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let url = server.base_url().to_owned();
    let response = client.get(&url).send().await.unwrap();
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(
        body["repo_url"],
        "https://github.com/worldcoin/message-bridge"
    );
    server.shutdown().await.unwrap();
    assert!(client.get(url).send().await.is_err());
}
