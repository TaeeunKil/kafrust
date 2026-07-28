mod common;

use kafrust::{ConsumerConfig, Error};

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
        ConsumerConfig::new(bootstrap_servers).client_id("kafrust-consumer-example"),
    )?
    .build()
    .await?;

    let watermarks = consumer.fetch_watermarks(&topic, partition).await?;
    if watermarks.high() < watermarks.low() {
        return Err(Error::Unsupported(
            "partition high watermark is below its low watermark",
        ));
    }
    println!(
        "watermarks {}-{} low={} high={}",
        topic,
        partition,
        watermarks.low(),
        watermarks.high()
    );

    consumer.assign(topic, partition, offset);
    let records = consumer.poll().await?;
    for record in records {
        println!(
            "fetched {}-{}@{} key={:?} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key().map(String::from_utf8_lossy),
            record.value().map(String::from_utf8_lossy)
        );
    }

    Ok(())
}
