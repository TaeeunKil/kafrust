use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let count = std::env::var("KAFRUST_BATCH_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);

    let mut producer = ProducerConfig::new([bootstrap])
        .client_id("kafrust-producer-batch-example")
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
