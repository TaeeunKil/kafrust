mod common;

use kafrust::{Acks, ConsumerConfig, Error, ProducerConfig, ProducerRecord, RecordMetadata};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let count = std::env::var("KAFRUST_BUFFERED_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3)
        .max(1);

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-buffered-producer-example"),
    )?
    .acks(Acks::Leader)
    .linger_ms(60_000)
    .max_records_per_batch(count)
    .max_batch_bytes(64 * 1024)
    .build_buffered()
    .await?;

    let mut expected = Vec::with_capacity(count);
    let mut deliveries = Vec::with_capacity(count);
    for index in 0..count {
        let key = format!("kafrust-buffered-key-{index}");
        let value = format!("hello from kafrust buffered {index}");
        let delivery = producer
            .send(
                ProducerRecord::to(topic.clone())
                    .key(key.clone())
                    .value(value.clone()),
            )
            .await?;
        expected.push((key, value));
        deliveries.push(delivery);
    }

    let mut metadata = Vec::with_capacity(count);
    for delivery in deliveries {
        metadata.push(delivery.await?);
    }
    producer.close().await?;

    for metadata in &metadata {
        println!(
            "buffered produced {}-{}@{}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset()
        );
    }

    fetch_buffered_records(&bootstrap_servers, &topic, &expected, &metadata).await?;

    Ok(())
}

async fn fetch_buffered_records(
    bootstrap_servers: &[String],
    topic: &str,
    expected: &[(String, String)],
    metadata: &[RecordMetadata],
) -> kafrust::Result<()> {
    let first = metadata.first().ok_or(Error::Unsupported(
        "buffered producer smoke missing metadata",
    ))?;
    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers.to_owned())
            .client_id("kafrust-buffered-consumer-example"),
    )?
    .max_wait_ms(500)
    .build()
    .await?;
    let fetched = consumer
        .fetch(topic.to_owned(), first.partition(), first.offset())
        .await?;

    for ((key, value), metadata) in expected.iter().zip(metadata) {
        let record = fetched
            .iter()
            .find(|record| record.offset() == metadata.offset())
            .ok_or(Error::Unsupported(
                "buffered producer smoke record not fetched",
            ))?;
        if record.partition() != metadata.partition()
            || record.key() != Some(key.as_bytes())
            || record.value() != Some(value.as_bytes())
        {
            return Err(Error::Unsupported(
                "buffered producer smoke record mismatch",
            ));
        }
        println!(
            "buffered fetched {}-{}@{} key={key:?} value={value:?}",
            record.topic(),
            record.partition(),
            record.offset()
        );
    }

    Ok(())
}
