mod common;

use kafrust::{
    AdminClient, ClientConfig, ClientMetrics, DeleteRecordsOptions, DeleteRecordsTopic, Error,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(21)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = parse_i32("KAFRUST_PARTITION", 0)?;
    let offset = parse_i64("KAFRUST_ADMIN_OFFSET", 1)?;
    let request_timeout_ms = parse_u64("KAFRUST_REQUEST_TIMEOUT_MS", 30_000)?;
    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-delete-records-failover")
            .request_timeout_ms(request_timeout_ms)
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);

    println!("admin delete records failover target {topic}-{partition} offset={offset}");
    let result = admin
        .delete_records(
            &[DeleteRecordsTopic::new(topic.clone()).partition(partition, offset)],
            DeleteRecordsOptions::new(),
        )
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
            context: format!("DeleteRecords for {topic}-{partition}"),
        });
    }
    let retries = metrics.snapshot().retries;
    println!(
        "admin delete records failover completed {topic}-{partition} low_watermark={} retries={retries}",
        partition_result.low_watermark(),
    );
    Ok(())
}

fn parse_i32(name: &'static str, default: i32) -> kafrust::Result<i32> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_i64(name: &'static str, default: i64) -> kafrust::Result<i64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_u64(name: &'static str, default: u64) -> kafrust::Result<u64> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}
