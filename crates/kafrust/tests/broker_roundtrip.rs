#![allow(clippy::expect_used)]

use kafrust::ClientConfig;

#[tokio::test]
async fn api_versions_and_metadata_roundtrip_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping broker roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };

    let mut client = ClientConfig::new([bootstrap])
        .client_id("kafrust-integration")
        .connect()
        .await
        .expect("connect to Kafka broker");

    let api_versions = client
        .api_versions()
        .await
        .expect("ApiVersions roundtrip should succeed");
    assert!(!api_versions.api_keys.is_empty());

    let metadata = client
        .metadata(None)
        .await
        .expect("Metadata roundtrip should succeed");
    assert!(!metadata.brokers.is_empty());
}

#[tokio::test]
async fn find_group_coordinator_roundtrip_when_broker_is_configured() {
    let Some(bootstrap) = std::env::var("KAFRUST_BOOTSTRAP_SERVERS").ok() else {
        eprintln!("skipping group coordinator roundtrip; set KAFRUST_BOOTSTRAP_SERVERS to run it");
        return;
    };
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut client = ClientConfig::new([bootstrap])
        .client_id("kafrust-integration")
        .connect()
        .await
        .expect("connect to Kafka broker");

    let coordinator = client
        .find_group_coordinator(group_id)
        .await
        .expect("FindCoordinator roundtrip should succeed");

    assert!(coordinator.node_id >= 0);
    assert!(!coordinator.host.is_empty());
    assert!(coordinator.port > 0);
}
