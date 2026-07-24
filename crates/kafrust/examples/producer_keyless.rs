mod common;

use kafrust::{Acks, Error, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let expected_partitions = expected_partitions_from_env()?;
    let count = expected_partitions.as_ref().map_or(2, Vec::len).max(1);

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers).client_id("kafrust-keyless-producer-example"),
    )?
    .acks(Acks::Leader)
    .build()
    .await?;

    let mut actual_partitions = Vec::with_capacity(count);
    for index in 0..count {
        let metadata = producer
            .send(
                ProducerRecord::to(topic.clone())
                    .value(format!("hello from kafrust keyless {index}")),
            )
            .await?;
        actual_partitions.push(metadata.partition());
        println!(
            "keyless produced {}-{}@{}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset()
        );
    }

    if expected_partitions
        .as_ref()
        .is_some_and(|expected| expected != &actual_partitions)
    {
        return Err(Error::Unsupported(
            "keyless partition sequence did not match KAFRUST_EXPECT_PARTITIONS",
        ));
    }

    Ok(())
}

fn expected_partitions_from_env() -> kafrust::Result<Option<Vec<i32>>> {
    std::env::var("KAFRUST_EXPECT_PARTITIONS")
        .ok()
        .map(|value| parse_partitions(&value))
        .transpose()
}

fn parse_partitions(value: &str) -> kafrust::Result<Vec<i32>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|partition| !partition.is_empty())
        .map(|partition| {
            partition.parse::<i32>().map_err(|_| {
                Error::Unsupported(
                    "KAFRUST_EXPECT_PARTITIONS must be comma-separated partition indexes",
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_expected_partitions() {
        assert_eq!(
            parse_partitions(" 0,1,,2 ").expect("partition list should parse"),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn rejects_invalid_expected_partition() {
        assert!(parse_partitions("0,invalid").is_err());
    }
}
