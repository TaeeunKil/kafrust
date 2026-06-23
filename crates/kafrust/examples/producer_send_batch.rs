mod common;

use kafrust::{Acks, Error, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let count = std::env::var("KAFRUST_BATCH_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3);
    let partitions = batch_partitions_from_env()?;

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers).client_id("kafrust-producer-batch-example"),
    )?
    .acks(Acks::Leader)
    .compression(common::compression_from_env()?)
    .build()
    .await?;

    let records = (0..count)
        .map(|index| {
            let record = ProducerRecord::to(topic.clone())
                .key(format!("kafrust-batch-key-{index}"))
                .value(format!("hello from kafrust batch {index}"));
            if partitions.is_empty() {
                record
            } else {
                record.partition(partitions[index % partitions.len()])
            }
        })
        .collect::<Vec<_>>();

    let metadata = producer.send_batch(records).await?;
    for metadata in metadata {
        println!(
            "produced {}-{}@{}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset()
        );
    }

    Ok(())
}

fn batch_partitions_from_env() -> kafrust::Result<Vec<i32>> {
    let Some(value) = std::env::var("KAFRUST_BATCH_PARTITIONS").ok() else {
        return Ok(Vec::new());
    };

    parse_batch_partitions(&value)
}

fn parse_batch_partitions(value: &str) -> kafrust::Result<Vec<i32>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|partition| !partition.is_empty())
        .map(|partition| {
            partition.parse::<i32>().map_err(|_| {
                Error::Unsupported(
                    "KAFRUST_BATCH_PARTITIONS must be comma-separated partition indexes",
                )
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::parse_batch_partitions;

    #[test]
    fn parses_batch_partitions() {
        assert_eq!(
            parse_batch_partitions(" 0,1,,2 ").expect("partition list should parse"),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn rejects_invalid_batch_partition() {
        assert!(parse_batch_partitions("0,not-a-partition").is_err());
    }
}
