mod common;

use kafrust::{ConsumerConfig, Error, IsolationLevel, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transaction-failover".to_owned());
    let max_retries = max_retries_from_env()?;

    // Reusing the transactional ID fences the old producer and lets Kafka
    // finish the incomplete transaction before the new transaction starts.
    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-transaction-recovery-producer")
            .transactional_id(transactional_id)
            .max_retries(max_retries),
    )?
    .build()
    .await?;

    producer.begin_transaction()?;
    let produced = producer
        .send(
            ProducerRecord::to(topic.clone())
                .key("kafrust-transaction-recovery")
                .value("committed after producer reinitialization"),
        )
        .await?;
    producer.commit_transaction().await?;

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers)
            .client_id("kafrust-transaction-recovery-consumer")
            .isolation_level(IsolationLevel::ReadCommitted),
    )?
    .build()
    .await?;
    let records = consumer.fetch(&topic, produced.partition(), 0).await?;

    if records
        .iter()
        .any(|record| record.value() == Some(b"committed after transaction coordinator failover"))
    {
        return Err(Error::Unsupported(
            "read_committed exposed the incomplete transaction",
        ));
    }
    if !records
        .iter()
        .any(|record| record.value() == Some(b"committed after producer reinitialization"))
    {
        return Err(Error::Unsupported(
            "read_committed did not return the recovery transaction",
        ));
    }

    println!(
        "transaction recovery committed and read_committed verified at {}-{}@{}",
        produced.topic(),
        produced.partition(),
        produced.offset()
    );
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::parse_max_retries;

    #[test]
    fn parses_max_retries() {
        assert_eq!(parse_max_retries(" 1200 ").unwrap(), 1200);
    }

    #[test]
    fn rejects_invalid_max_retries() {
        assert!(parse_max_retries("later").is_err());
    }
}
