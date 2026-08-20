use kafrust::{ShareAcknowledgementType, ShareAcquireMode, ShareConsumerConfig};
use std::collections::BTreeSet;
use std::time::{Duration, Instant};

fn required_env(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| kafrust::Error::InvalidConfiguration {
        field: name,
        reason: "published ShareConsumer soak environment variable is required",
    })
}

fn parse_cycles() -> kafrust::Result<usize> {
    let value = required_env("KAFRUST_SHARE_LONG_CYCLES")?;
    let cycles = value
        .parse::<usize>()
        .map_err(|_| kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_LONG_CYCLES",
            reason: "published ShareConsumer soak cycles must be a positive integer",
        })?;
    if cycles == 0 {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_LONG_CYCLES",
            reason: "published ShareConsumer soak cycles must be a positive integer",
        });
    }
    Ok(cycles)
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required_env("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let group_id = required_env("KAFRUST_SHARE_GROUP_ID")?;
    let prefix = required_env("KAFRUST_SHARE_LONG_PREFIX")?;
    let cycles = parse_cycles()?;

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

    let deadline = Instant::now() + Duration::from_secs(90);
    let mut accepted_values = BTreeSet::new();
    let mut accepted_offsets = BTreeSet::new();

    while accepted_values.len() < cycles {
        let records = consumer.poll().await?;
        let record_count = records.len();
        for record in records {
            let Some(value) = record.value() else {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_LONG_PREFIX",
                    reason: "published ShareConsumer soak received a null record value",
                });
            };
            if !value.starts_with(prefix.as_bytes()) {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_LONG_PREFIX",
                    reason: "published ShareConsumer soak received an unexpected record value",
                });
            }
            if !accepted_values.insert(value.to_vec()) {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_LONG_PREFIX",
                    reason: "published ShareConsumer soak redelivered a record before completion",
                });
            }
            accepted_offsets.insert(record.offset());
            consumer.acknowledge(&record, ShareAcknowledgementType::Accept)?;
            consumer.commit().await?;
        }

        if Instant::now() >= deadline {
            return Err(kafrust::Error::RequestTimedOut { timeout_ms: 90_000 });
        }
        if record_count == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    if accepted_offsets.len() != cycles {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_LONG_CYCLES",
            reason: "published ShareConsumer soak did not observe unique offsets",
        });
    }

    consumer.stop_heartbeat_task().await?;
    consumer.close().await?;
    println!(
        "published share acknowledgement soak ok cycles={} unique_offsets={}",
        accepted_values.len(),
        accepted_offsets.len()
    );
    Ok(())
}
