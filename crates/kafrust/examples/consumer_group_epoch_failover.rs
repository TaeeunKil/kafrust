mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{ConsumerGroupConfig, ConsumerGroupProtocol, ConsumerRecord, Error};

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
    let protocol = group_protocol_from_env()?;
    let expected_value = std::env::var("KAFRUST_EXPECTED_VALUE").ok();

    let mut group = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-consumer-group-epoch-failover")
            .group_protocol(protocol)
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
    let mut heartbeat = group
        .spawn_heartbeat_task(Duration::from_millis(100))
        .await?;
    let before = group.poll_with_heartbeat(&mut heartbeat).await?;
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

    let after = group.poll_with_heartbeat(&mut heartbeat).await?;
    print_records("group epoch failover after polled", &after)?;
    if let Some(expected_value) = expected_value.as_deref() {
        if !after.iter().any(|record| {
            record.topic() == topic
                && record.partition() == partition
                && record.value() == Some(expected_value.as_bytes())
        }) {
            return Err(Error::Unsupported(
                "consumer group did not fetch the expected post-failover record",
            ));
        }
        println!("group epoch failover observed expected post-failover record");
        flush_stdout()?;
    }
    heartbeat.stop().await?;
    group.leave().await?;
    println!("group epoch failover left group");
    Ok(())
}

fn print_records(label: &str, records: &[ConsumerRecord]) -> kafrust::Result<()> {
    println!("{label} count={}", records.len());
    for record in records {
        println!(
            "fetched {}-{}@{} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
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

fn group_protocol_from_env() -> kafrust::Result<ConsumerGroupProtocol> {
    std::env::var("KAFRUST_GROUP_PROTOCOL")
        .map(|value| parse_group_protocol(&value))
        .unwrap_or(Ok(ConsumerGroupProtocol::Classic))
}

fn parse_group_protocol(value: &str) -> kafrust::Result<ConsumerGroupProtocol> {
    match value.trim().to_ascii_lowercase().as_str() {
        "classic" => Ok(ConsumerGroupProtocol::Classic),
        "consumer" | "kip-848" => Ok(ConsumerGroupProtocol::Consumer),
        _ => Err(Error::Unsupported(
            "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
        )),
    }
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

    use kafrust::ConsumerGroupProtocol;

    use super::{parse_group_protocol, parse_offset, parse_partition, parse_pause};

    #[test]
    fn parses_failover_values() {
        assert_eq!(parse_partition(" 2 ").unwrap(), 2);
        assert_eq!(parse_offset(" 42 ").unwrap(), 42);
        assert_eq!(parse_pause(" 1500 ").unwrap(), Duration::from_millis(1500));
        assert_eq!(
            parse_group_protocol(" KIP-848 ").unwrap(),
            ConsumerGroupProtocol::Consumer
        );
    }

    #[test]
    fn rejects_invalid_failover_values() {
        assert!(parse_partition("not-a-partition").is_err());
        assert!(parse_offset("not-an-offset").is_err());
        assert!(parse_pause("one second").is_err());
        assert!(parse_group_protocol("unknown").is_err());
    }
}
