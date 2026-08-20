use kafrust::{ShareAcknowledgementType, ShareAcquireMode, ShareConsumerConfig};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers =
        std::env::var("KAFRUST_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = std::env::var("KAFRUST_SHARE_TOPIC")
        .unwrap_or_else(|_| "kafrust-published-share-runtime".to_owned());
    let group_id = std::env::var("KAFRUST_SHARE_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-published-share-runtime".to_owned());
    let expected_value = std::env::var("KAFRUST_SHARE_EXPECTED_VALUE")
        .unwrap_or_else(|_| "published-share-record".to_owned());

    let mut consumer = ShareConsumerConfig::new([bootstrap_servers], group_id)
        .subscribe(topic)
        .max_wait_ms(100)
        .max_records(1)
        .batch_size(1)
        .max_retries(10)
        .acquire_mode(ShareAcquireMode::RecordLimit)
        .build()
        .await?;
    consumer
        .spawn_heartbeat_task(Duration::from_secs(1))
        .await?;

    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let records = consumer.poll().await?;
        let Some(record) = records.into_iter().next() else {
            if Instant::now() >= deadline {
                return Err(kafrust::Error::RequestTimedOut { timeout_ms: 60_000 });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };

        if record.value() != Some(expected_value.as_bytes()) {
            return Err(kafrust::Error::InvalidConfiguration {
                field: "KAFRUST_SHARE_EXPECTED_VALUE",
                reason: "published ShareConsumer received an unexpected record value",
            });
        }

        let offset = record.offset();
        consumer.acknowledge(&record, ShareAcknowledgementType::Accept)?;
        consumer.commit().await?;
        consumer.stop_heartbeat_task().await?;
        consumer.close().await?;
        println!("published share runtime ok offset={offset}");
        return Ok(());
    }
}
