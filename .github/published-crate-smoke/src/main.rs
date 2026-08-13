use std::env;

use kafrust::{Acks, ConsumerConfig, Error, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let topic =
        env::var("KAFRUST_TOPIC").map_err(|_| Error::Unsupported("KAFRUST_TOPIC is required"))?;
    let value =
        env::var("KAFRUST_VALUE").map_err(|_| Error::Unsupported("KAFRUST_VALUE is required"))?;

    let mut producer = ProducerConfig::new([bootstrap_servers.clone()])
        .client_id("kafrust-published-smoke-producer")
        .acks(Acks::Leader)
        .build()
        .await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(value.as_bytes()),
        )
        .await?;

    let mut consumer = ConsumerConfig::new([bootstrap_servers])
        .client_id("kafrust-published-smoke-consumer")
        .max_poll_records(10)
        .build()
        .await?;
    consumer.assign(&topic, metadata.partition(), metadata.offset());
    let records = consumer.poll().await?;
    if !records.iter().any(|record| {
        record.topic() == topic
            && record.partition() == metadata.partition()
            && record.offset() == metadata.offset()
            && record.value() == Some(value.as_bytes())
    }) {
        return Err(Error::Unsupported(
            "published crate consumer did not read the produced record",
        ));
    }

    println!(
        "published kafrust verified {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}
