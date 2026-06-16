mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{Acks, Error, ProducerConfig, ProducerRecord, RecordMetadata};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = partition_from_env()?;
    let pause = pause_from_env()?;

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers).client_id("kafrust-producer-failover-example"),
    )?
    .acks(Acks::Leader)
    .max_retries(5)
    .build()
    .await?;

    let before = producer
        .send(failover_record(&topic, partition, "before"))
        .await?;
    print_metadata("failover before produced", &before)?;

    if !pause.is_zero() {
        println!("failover pause {}ms", pause.as_millis());
        flush_stdout()?;
        tokio::time::sleep(pause).await;
    }

    let after = producer
        .send(failover_record(&topic, partition, "after"))
        .await?;
    print_metadata("failover after produced", &after)?;

    Ok(())
}

fn failover_record(topic: &str, partition: i32, marker: &str) -> ProducerRecord {
    ProducerRecord::to(topic.to_owned())
        .partition(partition)
        .key(format!("kafrust-failover-{marker}-key"))
        .value(format!("hello from kafrust failover {marker}"))
}

fn print_metadata(label: &str, metadata: &RecordMetadata) -> kafrust::Result<()> {
    println!(
        "{label} {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    flush_stdout()
}

fn partition_from_env() -> kafrust::Result<i32> {
    std::env::var("KAFRUST_PARTITION")
        .map(|value| parse_partition(&value))
        .unwrap_or(Ok(0))
}

fn parse_partition(value: &str) -> kafrust::Result<i32> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be a partition index"))
}

fn pause_from_env() -> kafrust::Result<Duration> {
    std::env::var("KAFRUST_FAILOVER_PAUSE_MS")
        .map(|value| parse_pause(&value))
        .unwrap_or(Ok(Duration::from_millis(0)))
}

fn parse_pause(value: &str) -> kafrust::Result<Duration> {
    value
        .trim()
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| Error::Unsupported("KAFRUST_FAILOVER_PAUSE_MS must be milliseconds"))
}

fn flush_stdout() -> kafrust::Result<()> {
    io::stdout().flush().map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{parse_partition, parse_pause};

    #[test]
    fn parses_partition() {
        assert_eq!(parse_partition(" 2 ").expect("partition should parse"), 2);
    }

    #[test]
    fn rejects_invalid_partition() {
        assert!(parse_partition("not-a-partition").is_err());
    }

    #[test]
    fn parses_pause() {
        assert_eq!(
            parse_pause(" 1500 ").expect("pause should parse"),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn rejects_invalid_pause() {
        assert!(parse_pause("one second").is_err());
    }
}
