mod common;

use std::io::{self, Write};

use kafrust::{AdminClient, ClientConfig, ClientMetrics, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(15)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let discovery_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-admin-describe-group-failover-discovery"),
    )?;
    let mut bootstrap = discovery_config.clone().connect().await?;
    let coordinator = bootstrap.find_group_coordinator(group_id.clone()).await?;
    if coordinator.error_code != 0 {
        return Err(Error::Broker {
            code: coordinator.error_code,
            context: format!("find consumer group coordinator for {group_id}"),
        });
    }
    println!("group coordinator node {}", coordinator.node_id);
    io::stdout().flush().map_err(Error::Io)?;

    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-describe-group-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);
    let descriptions = admin
        .describe_consumer_groups(std::slice::from_ref(&group_id))
        .await?;
    let description = descriptions
        .iter()
        .find(|candidate| candidate.group_id() == group_id)
        .ok_or_else(|| Error::MissingGroupDescription {
            group_id: group_id.clone(),
        })?;
    if !description.is_success() {
        return Err(Error::Broker {
            code: description.error_code(),
            context: format!("DescribeGroups for {group_id}"),
        });
    }

    println!(
        "admin describe group failover completed {group_id} state={} members={} retries={}",
        description.state(),
        description.members().len(),
        metrics.snapshot().retries,
    );
    Ok(())
}
