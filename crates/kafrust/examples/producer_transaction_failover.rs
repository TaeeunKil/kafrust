mod common;

use std::io::{self, Write};
use std::time::Duration;

use kafrust::{
    ClientConfig, ConsumerConfig, Error, IsolationLevel, ProducerConfig, ProducerRecord,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transaction-failover".to_owned());
    let pause = pause_from_env()?;
    let max_retries = max_retries_from_env()?;

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-transaction-failover-producer")
            .transactional_id(transactional_id.clone())
            .max_retries(max_retries),
    )?
    .build()
    .await?;
    let mut coordinator_client = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-transaction-failover-coordinator"),
    )?
    .connect()
    .await?;
    let coordinator = coordinator_client
        .find_transaction_coordinator(transactional_id)
        .await?;
    if coordinator.error_code != 0 {
        return Err(Error::Broker {
            code: coordinator.error_code,
            context: "find transaction failover coordinator".to_owned(),
        });
    }

    producer.begin_transaction()?;
    let produced = producer
        .send(
            ProducerRecord::to(topic.clone())
                .key("kafrust-transaction-failover")
                .value("committed after transaction coordinator failover"),
        )
        .await?;
    println!(
        "transaction failover produced {}-{}@{} coordinator node {}",
        produced.topic(),
        produced.partition(),
        produced.offset(),
        coordinator.node_id
    );
    flush_stdout()?;

    if !pause.is_zero() {
        tokio::time::sleep(pause).await;
    }

    producer.commit_transaction().await?;
    println!("transaction failover committed");

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers)
            .client_id("kafrust-transaction-failover-consumer")
            .isolation_level(IsolationLevel::ReadCommitted),
    )?
    .build()
    .await?;
    let records = consumer
        .fetch(&topic, produced.partition(), produced.offset())
        .await?;
    if !records
        .iter()
        .any(|record| record.value() == Some(b"committed after transaction coordinator failover"))
    {
        return Err(Error::Unsupported(
            "read_committed did not return the transaction failover record",
        ));
    }
    println!("transaction failover read_committed verified");
    Ok(())
}

fn pause_from_env() -> kafrust::Result<Duration> {
    std::env::var("KAFRUST_FAILOVER_PAUSE_MS")
        .map(|value| parse_pause(&value))
        .unwrap_or(Ok(Duration::ZERO))
}

fn max_retries_from_env() -> kafrust::Result<u32> {
    std::env::var("KAFRUST_FAILOVER_MAX_RETRIES")
        .map(|value| parse_max_retries(&value))
        .unwrap_or(Ok(300))
}

fn parse_max_retries(value: &str) -> kafrust::Result<u32> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| Error::Unsupported("KAFRUST_FAILOVER_MAX_RETRIES must be an integer"))
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

    use super::{parse_max_retries, parse_pause};

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

    #[test]
    fn parses_max_retries() {
        assert_eq!(parse_max_retries(" 1200 ").unwrap(), 1200);
    }

    #[test]
    fn rejects_invalid_max_retries() {
        assert!(parse_max_retries("later").is_err());
    }
}
