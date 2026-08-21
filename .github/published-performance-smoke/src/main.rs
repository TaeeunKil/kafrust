use std::time::{Duration, Instant};

use kafrust::{Acks, ClientMetrics, Compression, ConsumerConfig, ProducerConfig, ProducerRecord};

const DEFAULT_RECORDS: usize = 10_000;
const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const CONSUME_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let record_count = usize_from_env("KAFRUST_RECORDS", DEFAULT_RECORDS)?.max(1);
    let batch_size = usize_from_env("KAFRUST_BATCH_SIZE", DEFAULT_BATCH_SIZE)?.max(1);
    let payload_bytes = usize_from_env("KAFRUST_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES)?;
    let compression = compression_from_env()?;
    let metrics = ClientMetrics::new();
    let payload = vec![b'x'; payload_bytes];

    let mut producer = ProducerConfig::new([bootstrap_servers.clone()])
        .client_id("kafrust-published-performance-producer")
        .metrics(metrics.clone())
        .acks(Acks::Leader)
        .compression(compression)
        .max_records_per_batch(batch_size)
        .max_batch_bytes(900 * 1024)
        .build()
        .await?;

    let produce_started = Instant::now();
    let mut batch_latencies = Vec::new();
    let mut produced = 0;
    let mut first_offset = None;
    while produced < record_count {
        let current_batch = batch_size.min(record_count - produced);
        let batch_started = Instant::now();
        let metadata = producer
            .send_batch(records(&topic, &payload, current_batch))
            .await?;
        if first_offset.is_none() {
            first_offset = metadata.first().map(|item| item.offset());
        }
        produced += metadata.len();
        batch_latencies.push(batch_started.elapsed());
    }
    let produce_elapsed = produce_started.elapsed();
    let first_offset = first_offset.ok_or(kafrust::Error::Unsupported(
        "published performance producer returned no metadata",
    ))?;

    let max_partition_bytes = payload_bytes
        .saturating_add(256)
        .saturating_mul(batch_size)
        .max(1_048_576)
        .min(i32::MAX as usize) as i32;
    let mut consumer = ConsumerConfig::new([bootstrap_servers])
        .client_id("kafrust-published-performance-consumer")
        .metrics(metrics.clone())
        .max_wait_ms(100)
        .max_partition_bytes(max_partition_bytes)
        .build()
        .await?;

    let consume_started = Instant::now();
    let deadline = consume_started + CONSUME_TIMEOUT;
    let mut next_offset = first_offset;
    let mut consumed = 0;
    let mut consumed_bytes = 0usize;
    while consumed < record_count {
        if Instant::now() >= deadline {
            return Err(kafrust::Error::Unsupported(
                "published performance consumer timed out",
            ));
        }
        let fetched = consumer.fetch(topic.clone(), 0, next_offset).await?;
        if fetched.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        for record in fetched.into_iter().take(record_count - consumed) {
            if record.offset() != next_offset {
                return Err(kafrust::Error::Unsupported(
                    "published performance fetch returned a non-contiguous offset",
                ));
            }
            next_offset = record.offset().saturating_add(1);
            consumed_bytes = consumed_bytes.saturating_add(record.value().map_or(0, <[u8]>::len));
            consumed += 1;
        }
    }
    let consume_elapsed = consume_started.elapsed();
    let snapshot = metrics.snapshot();
    if snapshot.in_flight_requests != 0 || snapshot.buffered_records != 0 {
        return Err(kafrust::Error::Unsupported(
            "published performance finished with non-zero client gauges",
        ));
    }

    batch_latencies.sort_unstable();
    println!(
        concat!(
            "{{\"topic\":\"{}\",\"compression\":\"{}\",\"records\":{},",
            "\"batch_size\":{},\"payload_bytes\":{},\"produce_seconds\":{:.6},",
            "\"produce_records_per_second\":{:.2},\"consume_seconds\":{:.6},",
            "\"consume_records_per_second\":{:.2},\"consumed_bytes\":{},",
            "\"batch_p50_ms\":{:.3},\"batch_p95_ms\":{:.3},\"batch_p99_ms\":{:.3},",
            "\"requests_started\":{},\"retries\":{},\"in_flight_requests\":{},",
            "\"max_in_flight_requests\":{},\"buffered_records\":{},",
            "\"max_buffered_records\":{}}}"
        ),
        topic,
        compression_name(compression),
        record_count,
        batch_size,
        payload_bytes,
        produce_elapsed.as_secs_f64(),
        rate(produced, produce_elapsed),
        consume_elapsed.as_secs_f64(),
        rate(consumed, consume_elapsed),
        consumed_bytes,
        percentile_ms(&batch_latencies, 50),
        percentile_ms(&batch_latencies, 95),
        percentile_ms(&batch_latencies, 99),
        snapshot.requests_started,
        snapshot.retries,
        snapshot.in_flight_requests,
        snapshot.buffered_records,
        snapshot.max_in_flight_requests,
        snapshot.max_buffered_records,
    );
    Ok(())
}

fn records(topic: &str, payload: &[u8], count: usize) -> Vec<ProducerRecord> {
    (0..count)
        .map(|_| {
            ProducerRecord::to(topic.to_owned())
                .partition(0)
                .value(payload.to_vec())
        })
        .collect()
}

fn required(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| kafrust::Error::Unsupported("performance variable is missing"))
}

fn usize_from_env(name: &'static str, default: usize) -> kafrust::Result<usize> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| kafrust::Error::Unsupported("performance size variable is invalid"))
}

fn compression_from_env() -> kafrust::Result<Compression> {
    match std::env::var("KAFRUST_COMPRESSION").as_deref() {
        Ok("none") | Err(_) => Ok(Compression::None),
        Ok("zstd") => Ok(Compression::Zstd),
        _ => Err(kafrust::Error::Unsupported(
            "performance compression must be none or zstd",
        )),
    }
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Zstd => "zstd",
        _ => "other",
    }
}

fn rate(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}

fn percentile_ms(sorted: &[Duration], percentile: usize) -> f64 {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted.get(index).copied().unwrap_or_default().as_secs_f64() * 1_000.0
}
