use kafrust::ClientConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut client = ClientConfig::new([bootstrap])
        .client_id("kafrust-find-coordinator")
        .connect()
        .await?;

    let coordinator = client.find_group_coordinator(group_id).await?;
    println!(
        "group coordinator: node {} at {}:{}",
        coordinator.node_id, coordinator.host, coordinator.port
    );

    Ok(())
}
