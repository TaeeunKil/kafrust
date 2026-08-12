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
    let require_record = std::env::var("KAFRUST_PARTITION_QUEUE_REQUIRE_RECORD")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));

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
    let mut queued_count = 0;
    while let Some(record) = queue.try_recv() {
        queued_count += 1;
        println!(
            "queued {}-{}@{} key={:?} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key().map(String::from_utf8_lossy),
            record.value().map(String::from_utf8_lossy)
        );
    }
    if require_record && queued_count == 0 {
        return Err(kafrust::Error::Unsupported(
            "partition queue smoke expected at least one record",
        ));
    }
    println!("partition queue delivered {queued_count} records");
    Ok(())
}
