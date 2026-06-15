mod common;

use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let count = std::env::var("KAFRUST_BATCH_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers).client_id("kafrust-producer-batch-example"),
    )?
    .acks(Acks::Leader)
    .build()
    .await?;

    let records = (0..count)
        .map(|index| {
            ProducerRecord::to(topic.clone())
                .key(format!("kafrust-batch-key-{index}"))
                .value(format!("hello from kafrust batch {index}"))
        })
        .collect::<Vec<_>>();

    let metadata = producer.send_batch(records).await?;
    for metadata in metadata {
        println!(
            "produced {}-{}@{}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset()
        );
    }

    Ok(())
}
