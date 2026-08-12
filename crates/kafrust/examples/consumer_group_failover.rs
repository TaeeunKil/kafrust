mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{BrokerErrorKind, Client, ClientConfig, ConsumerGroupConfig, Error};

const COORDINATOR_LOOKUP_MAX_RETRIES: u32 = 120;
const COORDINATOR_LOOKUP_BACKOFF: Duration = Duration::from_millis(250);

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id =
        std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-group-failover".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let pause = pause_from_env()?;

    let mut bootstrap = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-group-failover-coordinator"),
    )?
    .connect()
    .await?;
    let coordinator = find_group_coordinator_with_retry(&mut bootstrap, &group_id).await?;

    let config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .client_id("kafrust-group-failover-consumer")
            .session_timeout_ms(6_000)
            .rebalance_timeout_ms(10_000)
            .max_wait_ms(100)
            .max_retries(100)
            .subscribe(topic),
    )?;
    let mut group = config.join().await?;
    println!(
        "consumer group failover joined member {} generation {} coordinator node {}",
        group.member_id(),
        group.generation_id(),
        coordinator.node_id
    );

    let before = group.poll().await?;
    println!(
        "consumer group failover before polled count={}",
        before.len()
    );
    flush_stdout()?;

    if !pause.is_zero() {
        println!("consumer group failover pause {}ms", pause.as_millis());
        flush_stdout()?;
        tokio::time::sleep(pause).await;
    }

    let after = group.poll().await?;
    println!("consumer group failover after polled count={}", after.len());
    group.leave().await?;
    println!("consumer group failover left group");
    Ok(())
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

    use super::parse_pause;

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
