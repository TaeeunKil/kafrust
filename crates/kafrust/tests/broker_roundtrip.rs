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
