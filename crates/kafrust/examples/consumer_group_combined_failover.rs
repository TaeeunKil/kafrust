mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{
    ConsumerGroupConfig, ConsumerGroupProtocol, ConsumerRecord, Error, OffsetResetPolicy,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_tracing()?;
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-combined-group-failover".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = parse_i32("KAFRUST_PARTITION", 0)?;
    let expected_value = std::env::var("KAFRUST_EXPECTED_VALUE")
        .unwrap_or_else(|_| "kafrust-combined-after".to_owned());
    let pause = parse_duration("KAFRUST_FAILOVER_PAUSE_MS", Duration::ZERO)?;
    let protocol = group_protocol_from_env()?;

    let mut group = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-consumer-group-combined-failover")
            .group_protocol(protocol)
            .session_timeout_ms(6_000)
            .rebalance_timeout_ms(10_000)
            .max_wait_ms(100)
            .max_poll_records(10)
            .max_retries(100)
            .offset_reset_policy(OffsetResetPolicy::Earliest)
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
            "combined group failover did not receive the target partition",
        ));
    }

    println!(
        "combined group failover joined group {group_id} member={} generation={} coordinator-partition={topic}-{partition}",
        group.member_id(),
        group.generation_id()
    );
    let before = group.poll().await?;
    if !contains_partition(&before, &topic, partition) {
        return Err(Error::Unsupported(
            "combined group failover did not fetch the pre-stop record",
        ));
    }
    print_records("combined group failover before polled", &before)?;

    if !pause.is_zero() {
        println!("combined group failover pause {}ms", pause.as_millis());
        flush_stdout()?;
        tokio::time::sleep(pause).await;
    }

    let mut after_records = Vec::new();
    for _ in 0..20 {
        let records = group.poll().await?;
        let found = contains_value(&records, &topic, partition, expected_value.as_bytes());
        after_records.extend(records);
        if found {
            break;
        }
    }
    if !contains_value(&after_records, &topic, partition, expected_value.as_bytes()) {
        return Err(Error::Unsupported(
            "combined group failover did not fetch the post-stop record",
        ));
    }
    print_records("combined group failover after polled", &after_records)?;
    group.leave().await?;
    println!("combined group failover left group");
    Ok(())
}

fn contains_partition(records: &[ConsumerRecord], topic: &str, partition: i32) -> bool {
    records
        .iter()
        .any(|record| record.topic() == topic && record.partition() == partition)
}

fn contains_value(
    records: &[ConsumerRecord],
    topic: &str,
    partition: i32,
    expected: &[u8],
) -> bool {
    records.iter().any(|record| {
        record.topic() == topic
            && record.partition() == partition
            && record.value() == Some(expected)
    })
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

fn parse_i32(name: &'static str, default: i32) -> kafrust::Result<i32> {
    std::env::var(name)
        .ok()
        .map(|value| value.parse().map_err(|_| Error::Unsupported(name)))
        .transpose()
        .map(|value| value.unwrap_or(default))
}

fn parse_duration(name: &'static str, default: Duration) -> kafrust::Result<Duration> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|_| Error::Unsupported(name))
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
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

fn flush_stdout() -> kafrust::Result<()> {
    io::stdout().flush().map_err(Error::from)
}

#[cfg(test)]
mod tests {
    use kafrust::ConsumerGroupProtocol;

    use super::parse_group_protocol;

    #[test]
    fn parses_classic_and_kip848_protocols() {
        assert_eq!(
            parse_group_protocol(" classic ").unwrap(),
            ConsumerGroupProtocol::Classic
        );
        assert_eq!(
            parse_group_protocol("KIP-848").unwrap(),
            ConsumerGroupProtocol::Consumer
        );
    }

    #[test]
    fn rejects_unknown_group_protocol() {
        assert!(parse_group_protocol("unknown").is_err());
    }
}
