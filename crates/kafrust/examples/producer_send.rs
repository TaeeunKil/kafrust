mod common;

use kafrust::{Acks, Error, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let key = std::env::var("KAFRUST_KEY").unwrap_or_else(|_| "kafrust-key".to_owned());
    let value = std::env::var("KAFRUST_VALUE").unwrap_or_else(|_| "hello from kafrust".to_owned());
    let partition = partition_from_env()?;
    let idempotence = idempotence_from_env()?;

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

    println!(
        "produced {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );

    Ok(())
}

fn idempotence_from_env() -> kafrust::Result<bool> {
    let Ok(value) = std::env::var("KAFRUST_ENABLE_IDEMPOTENCE") else {
        return Ok(false);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "0" | "false" | "no" => Ok(false),
        "1" | "true" | "yes" => Ok(true),
        _ => Err(Error::Unsupported(
            "KAFRUST_ENABLE_IDEMPOTENCE must be true or false",
        )),
    }
}

fn partition_from_env() -> kafrust::Result<Option<i32>> {
    let Some(value) = std::env::var("KAFRUST_PARTITION").ok() else {
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
    use super::{idempotence_from_env, parse_partition};

    #[test]
    fn parses_partition() {
        assert_eq!(parse_partition(" 2 ").expect("partition should parse"), 2);
    }

    #[test]
    fn rejects_invalid_partition() {
        assert!(parse_partition("not-a-partition").is_err());
    }

    #[test]
    fn parses_idempotence_default() {
        std::env::remove_var("KAFRUST_ENABLE_IDEMPOTENCE");
        assert!(!idempotence_from_env().expect("default should parse"));
    }
}
