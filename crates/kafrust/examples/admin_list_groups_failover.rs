mod common;

use std::io::{self, Write};

use kafrust::{AdminClient, ClientConfig, ClientMetrics};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(16)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-list-groups-failover")
            .metrics(metrics.clone()),
    )?;
    // Broker restart recovery can exceed the short default retry budget.
    let admin = AdminClient::new(config).max_retries(12);
    let groups = admin.list_groups().await?;

    println!(
        "admin list groups failover completed groups={} retries={}",
        groups.len(),
        metrics.snapshot().retries,
    );
    io::stdout().flush().map_err(kafrust::Error::Io)?;
    Ok(())
}
