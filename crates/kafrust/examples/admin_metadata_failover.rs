mod common;

use std::io::{self, Write};

use kafrust::{AdminClient, ClientConfig, ClientMetrics, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(3)?;

    let operation =
        std::env::var("KAFRUST_METADATA_OPERATION").unwrap_or_else(|_| "cluster".to_owned());
    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(common::bootstrap_servers_from_env())
            .client_id("kafrust-admin-metadata-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config).max_retries(12);

    match operation.as_str() {
        "cluster" => {
            let cluster = admin.describe_cluster().await?;
            println!(
                "admin describe cluster failover completed brokers={} controller={} retries={}",
                cluster.brokers().len(),
                cluster.controller_id(),
                metrics.snapshot().retries,
            );
        }
        "topics" => {
            let topics = admin.list_topics().await?;
            println!(
                "admin list topics failover completed topics={} retries={}",
                topics.len(),
                metrics.snapshot().retries,
            );
        }
        _ => {
            return Err(Error::Unsupported(
                "KAFRUST_METADATA_OPERATION must be cluster or topics",
            ))
        }
    }

    io::stdout().flush().map_err(Error::Io)?;
    Ok(())
}
