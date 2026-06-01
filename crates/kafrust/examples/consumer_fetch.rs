use kafrust::ConsumerConfig;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = std::env::var("KAFRUST_PARTITION")
        .ok()
        .and_then(|value| value.parse::<i32>().ok())
        .unwrap_or(0);
    let offset = std::env::var("KAFRUST_OFFSET")
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);

    let mut consumer = ConsumerConfig::new([bootstrap])
        .client_id("kafrust-consumer-example")
        .build()
        .await?;

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
