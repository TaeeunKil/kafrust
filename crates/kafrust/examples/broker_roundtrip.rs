mod common;

use kafrust::ClientConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();

    let mut client = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-roundtrip"),
    )?
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
