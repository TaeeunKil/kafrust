use std::{collections::HashSet, env, error::Error, time::Duration, time::Instant};

use futures_util::future::join_all;
use kafrust::{Acks, ClientMetrics, ConsumerConfig, ProducerConfig, ProducerRecord};
use rdkafka::{
    consumer::{BaseConsumer, Consumer as RdkConsumer},
    producer::{FutureProducer, FutureRecord},
    topic_partition_list::{Offset, TopicPartitionList},
    util::Timeout,
    ClientConfig, Message,
};
use sha2::{Digest, Sha256};

const PARTITION: i32 = 0;
const CONSUME_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug)]
struct ResultRow {
    implementation: &'static str,
    repetition: usize,
    records: usize,
    unique_records: usize,
    duplicate_count: usize,
    loss_count: usize,
    payload_bytes: usize,
    batch_size: usize,
    payload_sha256: String,
    produce_seconds: f64,
    consume_seconds: f64,
    produce_records_per_second: f64,
    consume_records_per_second: f64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let implementation = required("KAFRUST_COMPARISON_IMPLEMENTATION")?;
    let repetition = usize_from_env("KAFRUST_COMPARISON_REPETITION", 1)?;
    let servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let records = usize_from_env("KAFRUST_COMPARISON_RECORDS", 20_000)?;
    let batch_size = usize_from_env("KAFRUST_COMPARISON_BATCH_SIZE", 200)?.max(1);
    let payload_bytes = usize_from_env("KAFRUST_COMPARISON_PAYLOAD_BYTES", 1_024)?;
    if records == 0 {
        return Err("record count must be greater than zero".into());
    }

    let row = match implementation.as_str() {
        "kafrust" => {
            run_kafrust(
                &servers,
                &topic,
                records,
                batch_size,
                payload_bytes,
                repetition,
            )
            .await?
        }
        "rdkafka" => {
            run_rdkafka(
                &servers,
                &topic,
                records,
                batch_size,
                payload_bytes,
                repetition,
            )
            .await?
        }
        _ => return Err("KAFRUST_COMPARISON_IMPLEMENTATION must be kafrust or rdkafka".into()),
    };
    println!(
        "{{\"implementation\":\"{}\",\"repetition\":{},\"records\":{},\"unique_records\":{},\"duplicate_count\":{},\"loss_count\":{},\"payload_bytes\":{},\"batch_size\":{},\"payload_sha256\":\"{}\",\"produce_seconds\":{:.6},\"consume_seconds\":{:.6},\"produce_records_per_second\":{:.2},\"consume_records_per_second\":{:.2}}}",
        row.implementation,
        row.repetition,
        row.records,
        row.unique_records,
        row.duplicate_count,
        row.loss_count,
        row.payload_bytes,
        row.batch_size,
        row.payload_sha256,
        row.produce_seconds,
        row.consume_seconds,
        row.produce_records_per_second,
        row.consume_records_per_second,
    );
    Ok(())
}

async fn run_kafrust(
    servers: &str,
    topic: &str,
    records: usize,
    batch_size: usize,
    payload_bytes: usize,
    repetition: usize,
) -> Result<ResultRow, Box<dyn Error + Send + Sync>> {
    let bootstrap_servers = servers.split(',').map(str::to_owned).collect::<Vec<_>>();
    let metrics = ClientMetrics::new();
    let mut producer = ProducerConfig::new(bootstrap_servers.clone())
        .client_id("kafrust-published-rdkafka-comparison-producer")
        .metrics(metrics.clone())
        .acks(Acks::Leader)
        .max_records_per_batch(batch_size)
        .max_batch_bytes(900 * 1024)
        .build()
        .await?;
    for _ in 0..3 {
        producer
            .send_batch(records_for(
                topic,
                payload_bytes,
                0,
                batch_size.min(records),
            ))
            .await?;
    }

    let mut consumer = ConsumerConfig::new(bootstrap_servers)
        .client_id("kafrust-published-rdkafka-comparison-consumer")
        .metrics(metrics)
        .max_wait_ms(100)
        .max_partition_bytes(
            payload_bytes
                .saturating_add(256)
                .saturating_mul(batch_size)
                .max(1_048_576)
                .min(i32::MAX as usize) as i32,
        )
        .build()
        .await?;
    let start_offset = consumer.fetch_watermarks(topic, PARTITION).await?.high();
    let produce_started = Instant::now();
    let mut produced = 0;
    while produced < records {
        let current = batch_size.min(records - produced);
        producer
            .send_batch(records_for(topic, payload_bytes, produced, current))
            .await?;
        produced += current;
    }
    let produce_elapsed = produce_started.elapsed();
    let consume_started = Instant::now();
    let consume_deadline = consume_started + CONSUME_TIMEOUT;
    let mut next_offset = start_offset;
    let mut consumed = 0;
    let mut seen = HashSet::with_capacity(records);
    let mut duplicate_count = 0;
    let mut digest = Sha256::new();
    while consumed < records {
        if Instant::now() >= consume_deadline {
            return Err("kafrust consumer timed out".into());
        }
        let fetched = consumer.fetch(topic, PARTITION, next_offset).await?;
        if fetched.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        for record in fetched.into_iter().take(records - consumed) {
            let value = record
                .value()
                .ok_or_else(|| invalid_record("kafrust returned a null migration value"))?;
            let sequence = sequence_from_value(value)?;
            if sequence >= records as u64 {
                return Err("kafrust returned an out-of-range migration record ID".into());
            }
            digest.update(value);
            if !seen.insert(sequence) {
                duplicate_count += 1;
            }
            next_offset = record.offset().saturating_add(1);
            consumed += 1;
        }
    }
    let consume_elapsed = consume_started.elapsed();
    let unique_records = seen.len();
    let loss_count = records.saturating_sub(unique_records);
    Ok(ResultRow {
        implementation: "kafrust",
        repetition,
        records,
        unique_records,
        duplicate_count,
        loss_count,
        payload_bytes,
        batch_size,
        payload_sha256: hex_digest(digest.finalize()),
        produce_seconds: produce_elapsed.as_secs_f64(),
        consume_seconds: consume_elapsed.as_secs_f64(),
        produce_records_per_second: rate(records, produce_elapsed),
        consume_records_per_second: rate(consumed, consume_elapsed),
    })
}

async fn run_rdkafka(
    servers: &str,
    topic: &str,
    records: usize,
    batch_size: usize,
    payload_bytes: usize,
    repetition: usize,
) -> Result<ResultRow, Box<dyn Error + Send + Sync>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .set("acks", "1")
        .set("linger.ms", "5")
        .set("batch.num.messages", &batch_size.to_string())
        .set("message.timeout.ms", "60000")
        .set("compression.type", "none")
        .create()?;
    for _ in 0..3 {
        produce_rdkafka_batch(&producer, topic, payload_bytes, 0, batch_size.min(records)).await?;
    }
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", servers)
        .set("group.id", "kafrust-published-rdkafka-comparison")
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "false")
        .set("auto.offset.reset", "earliest")
        .create()?;
    let start_offset = consumer
        .fetch_watermarks(topic, PARTITION, Timeout::After(Duration::from_secs(10)))?
        .1;
    let produce_started = Instant::now();
    let mut produced = 0;
    while produced < records {
        let current = batch_size.min(records - produced);
        produce_rdkafka_batch(&producer, topic, payload_bytes, produced, current).await?;
        produced += current;
    }
    let produce_elapsed = produce_started.elapsed();
    let mut assignment = TopicPartitionList::new();
    assignment.add_partition_offset(topic, PARTITION, Offset::Offset(start_offset))?;
    consumer.assign(&assignment)?;
    let consume_started = Instant::now();
    let consume_deadline = consume_started + CONSUME_TIMEOUT;
    let mut consumed = 0;
    let mut seen = HashSet::with_capacity(records);
    let mut duplicate_count = 0;
    let mut digest = Sha256::new();
    while consumed < records {
        if Instant::now() >= consume_deadline {
            return Err("rdkafka consumer timed out".into());
        }
        match consumer.poll(Duration::from_millis(100)) {
            Some(Ok(message)) => {
                if message.offset() >= start_offset {
                    let value = message.payload().ok_or_else(|| {
                        invalid_record("rust-rdkafka returned a null migration value")
                    })?;
                    let sequence = sequence_from_value(value)?;
                    if sequence >= records as u64 {
                        return Err(
                            "rust-rdkafka returned an out-of-range migration record ID".into()
                        );
                    }
                    digest.update(value);
                    if !seen.insert(sequence) {
                        duplicate_count += 1;
                    }
                    consumed += 1;
                }
            }
            Some(Err(error)) => return Err(error.into()),
            None => {}
        }
    }
    let consume_elapsed = consume_started.elapsed();
    let unique_records = seen.len();
    let loss_count = records.saturating_sub(unique_records);
    Ok(ResultRow {
        implementation: "rdkafka",
        repetition,
        records,
        unique_records,
        duplicate_count,
        loss_count,
        payload_bytes,
        batch_size,
        payload_sha256: hex_digest(digest.finalize()),
        produce_seconds: produce_elapsed.as_secs_f64(),
        consume_seconds: consume_elapsed.as_secs_f64(),
        produce_records_per_second: rate(records, produce_elapsed),
        consume_records_per_second: rate(consumed, consume_elapsed),
    })
}

async fn produce_rdkafka_batch(
    producer: &FutureProducer,
    topic: &str,
    payload_bytes: usize,
    start: usize,
    count: usize,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let payloads = (start..start.saturating_add(count))
        .map(|sequence| record_value(payload_bytes, sequence))
        .collect::<Vec<_>>();
    let deliveries = payloads
        .iter()
        .map(|payload| {
            producer.send(
                FutureRecord::<(), [u8]>::to(topic)
                    .partition(PARTITION)
                    .payload(payload),
                Timeout::Never,
            )
        })
        .collect::<Vec<_>>();
    for result in join_all(deliveries).await {
        result.map_err(|(error, _message)| error)?;
    }
    Ok(())
}

fn records_for(
    topic: &str,
    payload_bytes: usize,
    start: usize,
    count: usize,
) -> Vec<ProducerRecord> {
    (start..start.saturating_add(count))
        .map(|sequence| {
            ProducerRecord::to(topic.to_owned())
                .partition(PARTITION)
                .value(record_value(payload_bytes, sequence))
        })
        .collect()
}

fn record_value(payload_bytes: usize, sequence: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(8 + payload_bytes);
    value.extend_from_slice(&(sequence as u64).to_be_bytes());
    value.extend(std::iter::repeat(b'x').take(payload_bytes));
    value
}

fn sequence_from_value(value: &[u8]) -> Result<u64, Box<dyn Error + Send + Sync>> {
    let sequence = value
        .get(..8)
        .ok_or_else(|| invalid_record("migration record is missing its 8-byte business ID"))?;
    Ok(u64::from_be_bytes(sequence.try_into().map_err(|_| {
        invalid_record("invalid migration record ID")
    })?))
}

fn invalid_record(message: &'static str) -> Box<dyn Error + Send + Sync> {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn required(name: &'static str) -> Result<String, Box<dyn Error + Send + Sync>> {
    env::var(name).map_err(|_| format!("{name} is missing").into())
}

fn usize_from_env(
    name: &'static str,
    default: usize,
) -> Result<usize, Box<dyn Error + Send + Sync>> {
    env::var(name)
        .ok()
        .map_or(Ok(default), |value| Ok(value.parse()?))
}

fn rate(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}

#[cfg(test)]
mod tests {
    use super::{hex_digest, record_value, sequence_from_value};
    use sha2::{Digest, Sha256};

    #[test]
    fn migration_record_embeds_a_big_endian_business_id() {
        let value = record_value(3, 0x0102);

        assert_eq!(sequence_from_value(&value).expect("record ID"), 0x0102);
        assert_eq!(&value[8..], b"xxx");
    }

    #[test]
    fn migration_digest_is_stable_for_identical_values() {
        let value = record_value(3, 7);
        let mut digest = Sha256::new();
        digest.update(&value);

        assert_eq!(hex_digest(digest.finalize()).len(), 64);
        assert_eq!(
            hex_digest(Sha256::digest(&value)),
            hex_digest(Sha256::digest(&value))
        );
    }

    #[test]
    fn migration_record_requires_business_id_prefix() {
        assert!(sequence_from_value(b"short").is_err());
    }
}
