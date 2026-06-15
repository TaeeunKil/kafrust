mod common;

use kafrust::ClientConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut client = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-find-coordinator"),
    )?
    .connect()
    .await?;

    let coordinator = client.find_group_coordinator(group_id).await?;
    println!(
        "group coordinator: node {} at {}:{}",
        coordinator.node_id, coordinator.host, coordinator.port
    );

    Ok(())
}
