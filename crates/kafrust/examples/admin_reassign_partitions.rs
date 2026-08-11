mod common;

use kafrust::{
    AdminClient, ClientConfig, Error, PartitionReassignment, PartitionReassignmentOptions,
    PartitionReassignmentQuery,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_REASSIGNMENT_TOPIC")
        .or_else(|_| std::env::var("KAFRUST_TOPIC"))
        .unwrap_or_else(|_| "kafrust-smoke-multi".to_owned());
    let partition = parse_partition()?;
    let replicas = parse_replicas()?;
    if replicas.is_empty() {
        return Err(Error::Unsupported(
            "KAFRUST_REASSIGNMENT_REPLICAS must contain at least one broker ID",
        ));
    }

    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-reassignment-example"),
    )?;
    let admin = AdminClient::new(config);
    let options = PartitionReassignmentOptions::new().timeout(Duration::from_secs(30));
    let request = [PartitionReassignment::new(&topic).partition(partition, replicas.clone())];
    let altered = admin
        .alter_partition_reassignments(&request, options)
        .await?;
    if !altered.is_success() {
        let code = altered
            .topics()
            .iter()
            .flat_map(|topic| topic.partitions())
            .find(|partition| !partition.is_success())
            .map(|partition| partition.error_code())
            .unwrap_or(altered.error_code());
        return Err(Error::Broker {
            code,
            context: format!("alter partition reassignment for {topic}-{partition}"),
        });
    }
    println!("submitted partition reassignment for {topic}-{partition} to replicas {replicas:?}");

    let query = [PartitionReassignmentQuery::new(&topic).partition(partition)];
    for _ in 0..60 {
        let status = admin
            .list_partition_reassignments(Some(&query), options)
            .await?;
        if !status.is_success() {
            return Err(Error::Broker {
                code: status.error_code(),
                context: format!("list partition reassignment for {topic}-{partition}"),
            });
        }
        let still_running = status.topics().iter().any(|listed_topic| {
            listed_topic.name() == topic
                && listed_topic
                    .partitions()
                    .iter()
                    .any(|listed_partition| listed_partition.partition_index() == partition)
        });
        if !still_running {
            println!("partition reassignment for {topic}-{partition} completed");
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    Err(Error::Unsupported(
        "partition reassignment did not complete within the smoke-test deadline",
    ))
}

fn parse_partition() -> kafrust::Result<i32> {
    let Ok(value) = std::env::var("KAFRUST_REASSIGNMENT_PARTITION") else {
        return Ok(0);
    };
    value
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_REASSIGNMENT_PARTITION must be an integer"))
}

fn parse_replicas() -> kafrust::Result<Vec<i32>> {
    let value =
        std::env::var("KAFRUST_REASSIGNMENT_REPLICAS").unwrap_or_else(|_| "3,1,2".to_owned());
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse().map_err(|_| {
                Error::Unsupported("KAFRUST_REASSIGNMENT_REPLICAS must be CSV integers")
            })
        })
        .collect()
}
