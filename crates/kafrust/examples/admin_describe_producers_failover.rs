mod common;

use kafrust::{AdminClient, ClientConfig, ClientMetrics, DescribeProducersTopic, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(61)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
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
    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-describe-producers-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .describe_producers(&[DescribeProducersTopic::new(topic.clone()).partition(partition)])
        .await?;

    let topic_result = result
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
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
            context: format!("DescribeProducers for {topic}-{partition}"),
        });
    }

    println!(
        "admin describe producers failover completed {topic}-{partition} active_producers={} retries={}",
        partition_result.active_producers().len(),
        metrics.snapshot().retries,
    );
    Ok(())
}
