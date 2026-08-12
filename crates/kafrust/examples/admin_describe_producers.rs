mod common;

use kafrust::{AdminClient, ClientConfig, DescribeProducersTopic, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partitions = partitions_from_env()?;
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-describe-producers"),
    )?;
    let admin = AdminClient::new(config);
    let topic_target = partitions.iter().fold(
        DescribeProducersTopic::new(topic.clone()),
        |target, partition| target.partition(*partition),
    );
    let result = admin.describe_producers(&[topic_target]).await?;

    let described_topic = result
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .ok_or_else(|| Error::Broker {
            code: -1,
            context: format!("DescribeProducers omitted topic {topic}"),
        })?;
    for partition in described_topic.partitions() {
        if !partition.is_success() {
            return Err(Error::Broker {
                code: partition.error_code(),
                context: format!(
                    "DescribeProducers for {topic}-{}{}",
                    partition.partition_index(),
                    partition
                        .error_message()
                        .map(|message| format!(": {message}"))
                        .unwrap_or_default()
                ),
            });
        }
        println!(
            "described producer state for {topic}-{} active_producers={} throttle={:?}",
            partition.partition_index(),
            partition.active_producers().len(),
            result.throttle_time()
        );
        for producer in partition.active_producers() {
            println!(
                "  producer={} epoch={} last_sequence={} current_txn_start_offset={}",
                producer.producer_id(),
                producer.producer_epoch(),
                producer.last_sequence(),
                producer.current_txn_start_offset(),
            );
        }
    }
    Ok(())
}

fn partitions_from_env() -> kafrust::Result<Vec<i32>> {
    if let Ok(value) = std::env::var("KAFRUST_PARTITIONS") {
        let partitions: Vec<_> = value
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::parse)
            .collect::<Result<_, _>>()
            .map_err(|_| {
                Error::Unsupported("KAFRUST_PARTITIONS must be comma-separated integers")
            })?;
        if !partitions.is_empty() {
            return Ok(partitions);
        }
    }
    let partition = std::env::var("KAFRUST_PARTITION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    Ok(vec![partition])
}
