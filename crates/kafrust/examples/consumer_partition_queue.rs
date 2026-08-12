mod common;

use kafrust::ConsumerConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = std::env::var("KAFRUST_PARTITION")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let offset = std::env::var("KAFRUST_OFFSET")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers)
            .client_id("kafrust-partition-queue-example")
            .partition_queue_capacity(1024),
    )?
    .build()
    .await?;
    consumer.assign(&topic, partition, offset);
    let mut queue = consumer.split_partition_queue(&topic, partition)?;

    consumer.poll().await?;
    while let Some(record) = queue.try_recv() {
        println!(
            "queued {}-{}@{} key={:?} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key().map(String::from_utf8_lossy),
            record.value().map(String::from_utf8_lossy)
        );
    }
    Ok(())
}
