mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{ConsumerConfig, ConsumerRecord, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_tracing()?;
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = partition_from_env()?;
    let offset = offset_from_env()?;
    let pause = pause_from_env()?;

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers).client_id("kafrust-consumer-epoch-failover"),
    )?
    .max_retries(5)
    .build()
    .await?;
    consumer.assign(&topic, partition, offset);

    let before = consumer.poll().await?;
    ensure_records("failover before fetch", &before)?;
    print_records("failover before fetched", &before)?;

    if !pause.is_zero() {
        println!("failover pause {}ms", pause.as_millis());
        flush_stdout()?;
        tokio::time::sleep(pause).await;
    }

    let after = consumer.poll().await?;
    ensure_records("failover after fetch", &after)?;
    print_records("failover after fetched", &after)?;

    Ok(())
}

fn ensure_records(context: &'static str, records: &[ConsumerRecord]) -> kafrust::Result<()> {
    if records.is_empty() {
        return Err(Error::Unsupported(context));
    }
    Ok(())
}

fn print_records(label: &str, records: &[ConsumerRecord]) -> kafrust::Result<()> {
    println!("{label} count={}", records.len());
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

fn offset_from_env() -> kafrust::Result<i64> {
    std::env::var("KAFRUST_OFFSET")
        .map(|value| parse_offset(&value))
        .unwrap_or(Ok(0))
}

fn parse_offset(value: &str) -> kafrust::Result<i64> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| Error::Unsupported("KAFRUST_OFFSET must be a partition offset"))
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

    use super::{parse_offset, parse_partition, parse_pause};

    #[test]
    fn parses_partition() {
        assert_eq!(parse_partition(" 2 ").expect("partition should parse"), 2);
    }

    #[test]
    fn rejects_invalid_partition() {
        assert!(parse_partition("not-a-partition").is_err());
    }

    #[test]
    fn parses_offset() {
        assert_eq!(parse_offset(" 42 ").expect("offset should parse"), 42);
    }

    #[test]
    fn rejects_invalid_offset() {
        assert!(parse_offset("not-an-offset").is_err());
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
