mod common;

use kafrust::{Acks, Error, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let key = std::env::var("KAFRUST_KEY").unwrap_or_else(|_| "kafrust-key".to_owned());
    let value = std::env::var("KAFRUST_VALUE").unwrap_or_else(|_| "hello from kafrust".to_owned());
    let partition = partition_from_env()?;
    let expected_partition = optional_partition_from_env("KAFRUST_EXPECT_PARTITION")?;
    let idempotence = common::idempotence_from_env()?;

    let config = common::apply_security(
        ProducerConfig::new(bootstrap_servers).client_id("kafrust-producer-example"),
    )?
    .acks(Acks::Leader)
    .compression(common::compression_from_env()?);
    let mut producer = config.enable_idempotence(idempotence).build().await?;

    let mut record = ProducerRecord::to(topic).key(key).value(value);
    if let Some(partition) = partition {
        record = record.partition(partition);
    }

    let metadata = producer.send(record).await?;
    if expected_partition.is_some_and(|expected| metadata.partition() != expected) {
        return Err(Error::Unsupported(
            "produced partition did not match KAFRUST_EXPECT_PARTITION",
        ));
    }

    println!(
        "produced {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );

    Ok(())
}

fn partition_from_env() -> kafrust::Result<Option<i32>> {
    optional_partition_from_env("KAFRUST_PARTITION")
}

fn optional_partition_from_env(name: &str) -> kafrust::Result<Option<i32>> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(None);
    };

    parse_partition(&value).map(Some)
}

fn parse_partition(value: &str) -> kafrust::Result<i32> {
    value
        .trim()
        .parse::<i32>()
        .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be a partition index"))
}

#[cfg(test)]
mod tests {
    use super::parse_partition;

    #[test]
    fn parses_partition() {
        assert_eq!(parse_partition(" 2 ").expect("partition should parse"), 2);
    }

    #[test]
    fn rejects_invalid_partition() {
        assert!(parse_partition("not-a-partition").is_err());
    }
}
