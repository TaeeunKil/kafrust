mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{
    BrokerErrorKind, Client, ClientConfig, ConsumerGroupConfig, ConsumerGroupProtocol, Error,
};

const COORDINATOR_LOOKUP_MAX_RETRIES: u32 = 120;
const COORDINATOR_LOOKUP_BACKOFF: Duration = Duration::from_millis(250);

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id =
        std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-group-failover".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let protocol = group_protocol_from_env()?;
    let pause = pause_from_env()?;

    let mut bootstrap = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-group-failover-coordinator"),
    )?
    .connect()
    .await?;
    let coordinator = find_group_coordinator_with_retry(&mut bootstrap, &group_id).await?;
    let use_partition_queue = std::env::var("KAFRUST_PARTITION_QUEUE")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));

    let config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .group_protocol(protocol)
            .client_id("kafrust-group-failover-consumer")
            .session_timeout_ms(6_000)
            .rebalance_timeout_ms(10_000)
            .max_wait_ms(100)
            .max_retries(100)
            .subscribe(topic),
    )?;
    let mut group = config.join().await?;
    let mut partition_queue = if use_partition_queue {
        let assignment = group
            .assignments()
            .first()
            .ok_or(Error::Unsupported("consumer group has no assignment"))?;
        let assigned_topic = assignment.topic().to_owned();
        let assigned_partition = assignment.partition();
        Some(group.split_partition_queue(assigned_topic, assigned_partition)?)
    } else {
        None
    };
    println!(
        "consumer group failover joined member {} generation {} coordinator node {}",
        group.member_id(),
        group.generation_id(),
        coordinator.node_id
    );

    let before = group.poll().await?;
    let before_queued = drain_partition_queue(&mut partition_queue);
    println!(
        "consumer group failover before polled count={} queued count={}",
        before.len(),
        before_queued
    );
    flush_stdout()?;

    if !pause.is_zero() {
        println!("consumer group failover pause {}ms", pause.as_millis());
        flush_stdout()?;
        tokio::time::sleep(pause).await;
    }

    let after = group.poll().await?;
    let after_queued = drain_partition_queue(&mut partition_queue);
    println!(
        "consumer group failover after polled count={} queued count={}",
        after.len(),
        after_queued
    );
    group.leave().await?;
    println!("consumer group failover left group");
    Ok(())
}

fn drain_partition_queue(queue: &mut Option<kafrust::ConsumerPartitionQueue>) -> usize {
    queue
        .as_mut()
        .map(|queue| {
            let mut count = 0;
            while queue.try_recv().is_some() {
                count += 1;
            }
            count
        })
        .unwrap_or(0)
}

fn group_protocol_from_env() -> kafrust::Result<ConsumerGroupProtocol> {
    let value = std::env::var("KAFRUST_GROUP_PROTOCOL").unwrap_or_else(|_| "classic".to_owned());
    group_protocol_from_value(&value)
}

fn group_protocol_from_value(value: &str) -> kafrust::Result<ConsumerGroupProtocol> {
    match value.trim().to_ascii_lowercase().as_str() {
        "classic" => Ok(ConsumerGroupProtocol::Classic),
        "consumer" | "kip-848" => Ok(ConsumerGroupProtocol::Consumer),
        _ => Err(Error::Unsupported(
            "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
        )),
    }
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

async fn find_group_coordinator_with_retry(
    bootstrap: &mut Client,
    group_id: &str,
) -> kafrust::Result<kafrust::protocol::api::find_coordinator::FindCoordinatorResponseV1> {
    let mut retry_attempt = 0;
    loop {
        let response = bootstrap.find_group_coordinator(group_id).await?;
        if response.error_code == 0 {
            return Ok(response);
        }

        let retryable = matches!(
            BrokerErrorKind::from_code(response.error_code),
            BrokerErrorKind::CoordinatorLoadInProgress
                | BrokerErrorKind::CoordinatorNotAvailable
                | BrokerErrorKind::NotCoordinator
        );
        if !retryable || retry_attempt >= COORDINATOR_LOOKUP_MAX_RETRIES {
            return Err(Error::Broker {
                code: response.error_code,
                context: "find group failover coordinator".to_owned(),
            });
        }

        retry_attempt += 1;
        eprintln!(
            "waiting for group coordinator retry {} after broker error {}",
            retry_attempt, response.error_code
        );
        tokio::time::sleep(COORDINATOR_LOOKUP_BACKOFF).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use kafrust::ConsumerGroupProtocol;

    use super::{group_protocol_from_value, parse_pause};

    #[test]
    fn parses_group_protocol() {
        assert_eq!(
            group_protocol_from_value(" KIP-848 ").expect("protocol should parse"),
            ConsumerGroupProtocol::Consumer
        );
        assert_eq!(
            group_protocol_from_value("classic").expect("protocol should parse"),
            ConsumerGroupProtocol::Classic
        );
    }

    #[test]
    fn rejects_unknown_group_protocol() {
        assert!(group_protocol_from_value("unknown").is_err());
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
        assert!(parse_pause("later").is_err());
    }
}
