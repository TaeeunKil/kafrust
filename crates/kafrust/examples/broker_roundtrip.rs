use kafrust::ClientConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".to_owned());

    let mut client = ClientConfig::new([bootstrap])
        .client_id("kafrust-roundtrip")
        .connect()
        .await?;

    let api_versions = client.api_versions().await?;
    println!("api_versions: {} APIs", api_versions.api_keys.len());

    let metadata = client.metadata(None).await?;
    println!(
        "metadata: {} brokers, {} topics, controller {}",
        metadata.brokers.len(),
        metadata.topics.len(),
        metadata.controller_id
    );

    Ok(())
}
