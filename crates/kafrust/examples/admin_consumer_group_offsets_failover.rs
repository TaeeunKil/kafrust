mod common;

use std::io::{self, Write};

use kafrust::{AdminClient, ClientConfig, ClientMetrics, ConsumerGroupOffsetQuery, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(9)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = std::env::var("KAFRUST_PARTITION")
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be a partition index"))
        })
        .transpose()?
        .unwrap_or(0);

    let discovery_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-admin-group-offsets-failover-discovery"),
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
            .client_id("kafrust-admin-group-offsets-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);
    let query = [ConsumerGroupOffsetQuery::new(topic.clone(), [partition])];
    let result = admin
        .list_consumer_group_offsets(&group_id, Some(&query))
        .await?;
    if result.error_code() != 0 {
        return Err(Error::Broker {
            code: result.error_code(),
            context: format!("list committed offsets for {group_id}"),
        });
    }
    let topic_result = result
        .topics()
        .iter()
        .find(|candidate| candidate.topic() == topic)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition,
        })?;
    let partition_result = topic_result
        .partitions()
        .iter()
        .find(|candidate| candidate.partition_index() == partition)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition,
        })?;
    if !partition_result.is_success() {
        return Err(Error::Broker {
            code: partition_result.error_code(),
            context: format!("list committed offset for {group_id}/{topic}-{partition}"),
        });
    }

    println!(
        "admin consumer group offsets failover completed {group_id}/{topic}-{partition} offset={} retries={}",
        partition_result.committed_offset(),
        metrics.snapshot().retries,
    );
    Ok(())
}
