mod common;

use kafrust::{Acks, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let key = std::env::var("KAFRUST_KEY").unwrap_or_else(|_| "kafrust-key".to_owned());
    let value = std::env::var("KAFRUST_VALUE").unwrap_or_else(|_| "hello from kafrust".to_owned());

    let mut producer = common::apply_security(
        ProducerConfig::new([bootstrap]).client_id("kafrust-producer-example"),
    )?
    .acks(Acks::Leader)
    .build()
    .await?;

    let metadata = producer
        .send(ProducerRecord::to(topic).key(key).value(value))
        .await?;

    println!(
        "produced {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );

    Ok(())
}
