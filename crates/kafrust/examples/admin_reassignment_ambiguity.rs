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
        .map_err(|_| Error::Unsupported("KAFRUST_REASSIGNMENT_TOPIC must be set"))?;
    let partition = parse_partition()?;
    let replicas = parse_replicas()?;

    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-reassignment-ambiguity-example"),
    )?;
    let admin = AdminClient::new(config);
    let options = PartitionReassignmentOptions::new().timeout(Duration::from_secs(30));
    let query = [PartitionReassignmentQuery::new(&topic).partition(partition)];

    wait_for_topic(&admin, &topic).await?;
    ensure_reassignment_idle(&admin, &query, options).await?;

    let request = [PartitionReassignment::new(&topic).partition(partition, replicas)];
    let error = match admin.alter_partition_reassignments(&request, options).await {
        Ok(result) if result.is_success() => {
            return Err(Error::Unsupported(
                "the response-drop proxy did not make AlterPartitionReassignments ambiguous",
            ))
        }
        Ok(_) => return Err(Error::Unsupported(
            "AlterPartitionReassignments returned a broker error before the response was dropped",
        )),
        Err(error) => error,
    };
    if !matches!(
        error,
        Error::AdminMutationOutcomeUnknown {
            operation: "AlterPartitionReassignments"
        }
    ) {
        return Err(error);
    }

    println!("AlterPartitionReassignments response was lost; outcome is explicitly unknown");
    wait_for_reassignment_idle(&admin, &query, options).await?;
    println!(
        "reconciled partition reassignment state for {topic}-{partition} through ListPartitionReassignments"
    );
    Ok(())
}

async fn wait_for_topic(admin: &AdminClient, topic: &str) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let topics = admin.list_topics().await?;
        if topics.iter().any(|listed| {
            listed.name() == topic && listed.is_success() && listed.partition_count() > 0
        }) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "reassignment topic did not become visible",
            ));
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn ensure_reassignment_idle(
    admin: &AdminClient,
    query: &[PartitionReassignmentQuery],
    options: PartitionReassignmentOptions,
) -> kafrust::Result<()> {
    let status = admin
        .list_partition_reassignments(Some(query), options)
        .await?;
    if !status.is_success() {
        return Err(Error::Broker {
            code: status.error_code(),
            context: "inspect initial partition reassignment state".to_owned(),
        });
    }
    if status
        .topics()
        .iter()
        .any(|topic| !topic.partitions().is_empty())
    {
        return Err(Error::Unsupported(
            "partition already has an ongoing reassignment",
        ));
    }
    Ok(())
}

async fn wait_for_reassignment_idle(
    admin: &AdminClient,
    query: &[PartitionReassignmentQuery],
    options: PartitionReassignmentOptions,
) -> kafrust::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
    loop {
        let status = admin
            .list_partition_reassignments(Some(query), options)
            .await?;
        if !status.is_success() {
            return Err(Error::Broker {
                code: status.error_code(),
                context: "reconcile partition reassignment state".to_owned(),
            });
        }
        if status
            .topics()
            .iter()
            .all(|topic| topic.partitions().is_empty())
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Unsupported(
                "partition reassignment did not complete before the reconciliation deadline",
            ));
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn parse_partition() -> kafrust::Result<i32> {
    let value = std::env::var("KAFRUST_REASSIGNMENT_PARTITION")
        .map_err(|_| Error::Unsupported("KAFRUST_REASSIGNMENT_PARTITION must be set"))?;
    value
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_REASSIGNMENT_PARTITION must be an integer"))
}

fn parse_replicas() -> kafrust::Result<Vec<i32>> {
    let value = std::env::var("KAFRUST_REASSIGNMENT_REPLICAS")
        .map_err(|_| Error::Unsupported("KAFRUST_REASSIGNMENT_REPLICAS must be set"))?;
    let replicas = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse().map_err(|_| {
                Error::Unsupported("KAFRUST_REASSIGNMENT_REPLICAS must be CSV integers")
            })
        })
        .collect::<kafrust::Result<Vec<i32>>>()?;
    if replicas.len() < 2 || replicas.windows(2).any(|window| window[0] == window[1]) {
        return Err(Error::Unsupported(
            "KAFRUST_REASSIGNMENT_REPLICAS must contain at least two distinct broker IDs",
        ));
    }
    Ok(replicas)
}
