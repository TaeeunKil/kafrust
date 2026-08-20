use kafrust::{
    ProducerConfig, ProducerRecord, ShareAcknowledgementType, ShareAcquireMode, ShareConsumerConfig,
};
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
        _ => Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_OPERATION",
            reason: "published Share multi-broker operation must be produce or consume",
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
