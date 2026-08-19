mod common;

use std::io::{self, Write};

use kafrust::{AdminClient, ClientConfig, ClientMetrics, ConsumerGroupOffset, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(8)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = parse_partition()?;
    let target_offset = parse_target_offset()?;

    let discovery_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-admin-group-offset-commit-failover-discovery"),
    )?;
    let mut bootstrap = discovery_config.connect().await?;
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
            .client_id("kafrust-admin-group-offset-commit-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);
    let altered = match admin
        .alter_consumer_group_offsets(
            &group_id,
            &[
                ConsumerGroupOffset::new(topic.clone(), partition, target_offset)
                    .metadata("kafrust-admin-offset-commit-failover"),
            ],
        )
        .await
    {
        Ok(altered) => altered,
        Err(Error::AdminMutationOutcomeUnknown {
            operation: "OffsetCommit",
        }) => {
            println!(
                "admin consumer group offset commit failover completed {group_id}/{topic}-{partition} outcome=unknown retries={}",
                metrics.snapshot().retries,
            );
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    if !altered.is_success() {
        let error_code = altered
            .topics()
            .iter()
            .flat_map(|topic| topic.partitions())
            .find(|partition| !partition.is_success())
            .map(|partition| partition.error_code())
            .unwrap_or(-1);
        return Err(Error::Broker {
            code: error_code,
            context: format!("commit offset for {group_id}/{topic}-{partition}"),
        });
    }

    println!(
        "admin consumer group offset commit failover completed {group_id}/{topic}-{partition} offset={target_offset} retries={}",
        metrics.snapshot().retries,
    );
    Ok(())
}

fn parse_partition() -> kafrust::Result<i32> {
    std::env::var("KAFRUST_PARTITION")
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be a partition index"))
        })
        .transpose()
        .map(|partition| partition.unwrap_or(0))
}

fn parse_target_offset() -> kafrust::Result<i64> {
    std::env::var("KAFRUST_ADMIN_OFFSET")
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_OFFSET must be an offset"))
        })
        .transpose()
        .map(|offset| offset.unwrap_or(0))
}
