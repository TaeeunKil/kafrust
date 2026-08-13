mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{ConsumerGroupConfig, ConsumerRecord, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_tracing()?;
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-consumer-group-epoch-failover".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = partition_from_env()?;
    let offset = offset_from_env()?;
    let pause = pause_from_env()?;

    let mut group = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-consumer-group-epoch-failover")
            .session_timeout_ms(30_000)
            .start_offset(offset)
            .max_retries(5)
            .subscribe(topic.clone()),
    )?
    .join()
    .await?;

    if !group
        .assignments()
        .iter()
        .any(|assignment| assignment.topic() == topic && assignment.partition() == partition)
    {
        return Err(Error::Unsupported(
            "consumer group did not receive the leader-failover partition",
        ));
    }

    println!(
        "group epoch failover joined group {group_id} member={} generation={} assignments={}",
        group.member_id(),
        group.generation_id(),
        group.assignments().len()
    );
    let before = group.poll().await?;
    if !before
        .iter()
        .any(|record| record.topic() == topic && record.partition() == partition)
    {
        return Err(Error::Unsupported(
            "consumer group did not fetch a record from the leader-failover partition",
        ));
    }
    print_records("group epoch failover before polled", &before)?;

    if !pause.is_zero() {
        println!("group epoch failover pause {}ms", pause.as_millis());
        flush_stdout()?;
        tokio::time::sleep(pause).await;
    }

    let after = group.poll().await?;
    print_records("group epoch failover after polled", &after)?;
    group.leave().await?;
    println!("group epoch failover left group");
    Ok(())
}

fn print_records(label: &str, records: &[ConsumerRecord]) -> kafrust::Result<()> {
    println!("{label} count={}", records.len());
    for record in records {
        println!(
            "fetched {}-{}@{}",
            record.topic(),
            record.partition(),
            record.offset()
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
        .parse()
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
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_OFFSET must be a partition offset"))
}

fn pause_from_env() -> kafrust::Result<Duration> {
    std::env::var("KAFRUST_FAILOVER_PAUSE_MS")
        .map(|value| parse_pause(&value))
        .unwrap_or(Ok(Duration::ZERO))
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
    fn parses_failover_values() {
        assert_eq!(parse_partition(" 2 ").unwrap(), 2);
        assert_eq!(parse_offset(" 42 ").unwrap(), 42);
        assert_eq!(parse_pause(" 1500 ").unwrap(), Duration::from_millis(1500));
    }

    #[test]
    fn rejects_invalid_failover_values() {
        assert!(parse_partition("not-a-partition").is_err());
        assert!(parse_offset("not-an-offset").is_err());
        assert!(parse_pause("one second").is_err());
    }
}
