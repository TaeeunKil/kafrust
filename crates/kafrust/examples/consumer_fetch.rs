mod common;

use kafrust::{ConsumerConfig, Error};
use std::time::Duration;

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

    let mut config = ConsumerConfig::new(bootstrap_servers).client_id("kafrust-consumer-example");
    let rack_aware = std::env::var("KAFRUST_CLIENT_RACK").is_ok();
    if let Ok(client_rack) = std::env::var("KAFRUST_CLIENT_RACK") {
        config = config.client_rack(client_rack);
    }
    let require_rack_record = std::env::var("KAFRUST_RACK_REQUIRE_RECORD")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    if rack_aware {
        common::init_request_gate(kafrust::protocol::api::fetch::API_KEY)?;
    }
    let mut consumer = common::apply_security(config)?.build().await?;

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

    consumer.assign(&topic, partition, offset);
    let mut records = consumer.poll().await?;
    if rack_aware && require_rack_record && records.is_empty() {
        // A rack-aware fetch can race follower replication immediately after a write.
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(250)).await;
            records = consumer.fetch(&topic, partition, offset).await?;
            if !records.is_empty() {
                break;
            }
        }
    }
    for record in &records {
        println!(
            "fetched {}-{}@{} key={:?} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key().map(String::from_utf8_lossy),
            record.value().map(String::from_utf8_lossy)
        );
    }
    if require_rack_record && records.is_empty() {
        return Err(Error::Unsupported(
            "rack-aware consumer smoke expected at least one record",
        ));
    }

    if rack_aware {
        let follow_up = consumer.fetch(&topic, partition, offset).await?;
        println!("rack-aware follow-up fetched {} records", follow_up.len());
        if require_rack_record && follow_up.is_empty() {
            return Err(Error::Unsupported(
                "rack-aware follow-up expected at least one record",
            ));
        }
    }

    Ok(())
}
