use kafrust::{
    ProducerConfig, ProducerRecord, ShareAcknowledgementType, ShareAcquireMode, ShareConsumerConfig,
};
use std::path::Path;
use std::time::{Duration, Instant};

fn required_env(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| kafrust::Error::InvalidConfiguration {
        field: name,
        reason: "published Share multi-broker environment variable is required",
    })
}

fn bootstrap_servers() -> kafrust::Result<Vec<String>> {
    let value = required_env("KAFRUST_BOOTSTRAP_SERVERS")?;
    let servers = value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_BOOTSTRAP_SERVERS",
            reason: "at least one bootstrap server is required",
        });
    }
    Ok(servers)
}

fn partition() -> kafrust::Result<i32> {
    required_env("KAFRUST_SHARE_PARTITION")?
        .parse::<i32>()
        .map_err(|_| kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_PARTITION",
            reason: "published Share multi-broker partition must be an integer",
        })
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    match required_env("KAFRUST_OPERATION")?.as_str() {
        "produce" => produce().await,
        "consume" => consume().await,
        "heartbeat-consume" => heartbeat_consume().await,
        _ => Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_OPERATION",
            reason: "published Share multi-broker operation must be produce, consume, or heartbeat-consume",
        }),
    }
}

async fn produce() -> kafrust::Result<()> {
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let value = required_env("KAFRUST_SHARE_VALUE")?;
    let partition = partition()?;
    let mut producer = ProducerConfig::new(bootstrap_servers()?)
        .client_id("kafrust-published-share-multi-broker-producer")
        .build()
        .await?;
    let metadata = producer
        .send(ProducerRecord::to(topic).partition(partition).value(value))
        .await?;
    if metadata.partition() != partition {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_PARTITION",
            reason: "published producer returned a different partition",
        });
    }
    println!(
        "published share multi-broker produced partition={} offset={}",
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}

async fn consume() -> kafrust::Result<()> {
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let expected_value = required_env("KAFRUST_SHARE_VALUE")?.into_bytes();
    let phase = required_env("KAFRUST_SHARE_PHASE")?;
    let partition = partition()?;
    let mut consumer = ShareConsumerConfig::new(
        bootstrap_servers()?,
        required_env("KAFRUST_SHARE_GROUP_ID")?,
    )
    .subscribe(topic.clone())
    .max_wait_ms(100)
    .max_records(1)
    .batch_size(1)
    .max_retries(10)
    .acquire_mode(ShareAcquireMode::RecordLimit)
    .build()
    .await?;
    consumer
        .spawn_heartbeat_task(Duration::from_millis(500))
        .await?;

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let records = consumer.poll().await?;
        if let Some(record) = records.iter().find(|record| {
            record.topic() == topic
                && record.partition() == partition
                && record.value() == Some(expected_value.as_slice())
        }) {
            let offset = record.offset();
            consumer.acknowledge(record, ShareAcknowledgementType::Accept)?;
            consumer.commit().await?;
            consumer.stop_heartbeat_task().await?;
            consumer.close().await?;
            println!(
                "published share multi-broker phase={} received partition={} offset={}",
                phase, partition, offset
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(kafrust::Error::RequestTimedOut { timeout_ms: 90_000 });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn positive_cycles() -> kafrust::Result<usize> {
    let value = required_env("KAFRUST_SHARE_HEARTBEAT_CYCLES")?;
    let cycles = value
        .parse::<usize>()
        .map_err(|_| kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_HEARTBEAT_CYCLES",
            reason: "published Share heartbeat cycles must be a positive integer",
        })?;
    if cycles == 0 {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_HEARTBEAT_CYCLES",
            reason: "published Share heartbeat cycles must be a positive integer",
        });
    }
    Ok(cycles)
}

async fn wait_for_marker(path: &Path, deadline: Instant) -> kafrust::Result<()> {
    while !path.exists() {
        if Instant::now() >= deadline {
            return Err(kafrust::Error::RequestTimedOut { timeout_ms: 90_000 });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

async fn heartbeat_consume() -> kafrust::Result<()> {
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let pre_value = required_env("KAFRUST_SHARE_PRE_VALUE")?.into_bytes();
    let group_id = required_env("KAFRUST_SHARE_GROUP_ID")?;
    let partition = partition()?;
    let cycles = positive_cycles()?;
    let value_prefix = required_env("KAFRUST_SHARE_HEARTBEAT_VALUE_PREFIX")?;
    let marker_base = required_env("KAFRUST_SHARE_HEARTBEAT_READY_FILE")?;
    let mut consumer = ShareConsumerConfig::new(bootstrap_servers()?, group_id)
        .subscribe(topic.clone())
        .max_wait_ms(100)
        .max_records(1)
        .batch_size(1)
        .max_retries(10)
        .acquire_mode(ShareAcquireMode::RecordLimit)
        .build()
        .await?;

    let pre_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let records = consumer.poll().await?;
        if let Some(record) = records.iter().find(|record| {
            record.topic() == topic
                && record.partition() == partition
                && record.value() == Some(pre_value.as_slice())
        }) {
            consumer.acknowledge(record, ShareAcknowledgementType::Accept)?;
            consumer.commit().await?;
            break;
        }
        if Instant::now() >= pre_deadline {
            return Err(kafrust::Error::RequestTimedOut { timeout_ms: 60_000 });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    consumer
        .spawn_heartbeat_task(Duration::from_millis(500))
        .await?;
    tokio::time::sleep(Duration::from_millis(750)).await;

    for cycle in 1..=cycles {
        let ready_path = format!("{marker_base}-{cycle}-ready");
        let recovered_path = format!("{marker_base}-{cycle}-recovered");
        let continue_path = format!("{marker_base}-{cycle}-continue");
        std::fs::write(&ready_path, b"heartbeat-running\n").map_err(kafrust::Error::Io)?;
        let expected_value = format!("{value_prefix}{cycle}").into_bytes();
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let records = consumer.poll().await?;
            if let Some(record) = records.iter().find(|record| {
                record.topic() == topic
                    && record.partition() == partition
                    && record.value() == Some(expected_value.as_slice())
            }) {
                consumer.acknowledge(record, ShareAcknowledgementType::Accept)?;
                consumer.commit().await?;
                if consumer.heartbeat_task_is_finished() {
                    return Err(kafrust::Error::InvalidConfiguration {
                        field: "KAFRUST_SHARE_HEARTBEAT_READY_FILE",
                        reason:
                            "published Share heartbeat task finished during coordinator failover",
                    });
                }
                std::fs::write(&recovered_path, b"acknowledged\n").map_err(kafrust::Error::Io)?;
                break;
            }
            if Instant::now() >= deadline {
                return Err(kafrust::Error::RequestTimedOut { timeout_ms: 90_000 });
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if cycle < cycles {
            wait_for_marker(Path::new(&continue_path), deadline).await?;
        }
    }

    consumer.stop_heartbeat_task().await?;
    consumer.close().await?;
    println!("published Share heartbeat failover ok cycles={cycles}");
    Ok(())
}
