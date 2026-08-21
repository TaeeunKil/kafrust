mod common;

use std::time::{Duration, Instant};

use kafrust::{Acks, ClientMetrics, Compression, ConsumerConfig, ProducerConfig, ProducerRecord};

const DEFAULT_RECORDS: usize = 20_000;
const DEFAULT_BATCH_SIZE: usize = 200;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const DEFAULT_WARMUP_BATCHES: usize = 3;
const DEFAULT_MAX_BATCH_BYTES: usize = 900 * 1024;
const CONSUME_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC")
        .unwrap_or_else(|_| "kafrust-throughput-benchmark".to_owned());
    let record_count = usize_from_env("KAFRUST_BENCH_RECORDS", DEFAULT_RECORDS)?;
    let batch_size = usize_from_env("KAFRUST_BENCH_BATCH_SIZE", DEFAULT_BATCH_SIZE)?.max(1);
    let payload_bytes = usize_from_env("KAFRUST_BENCH_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES)?;
    let warmup_batches = usize_from_env("KAFRUST_BENCH_WARMUP_BATCHES", DEFAULT_WARMUP_BATCHES)?;
    let max_batch_bytes =
        usize_from_env("KAFRUST_BENCH_MAX_BATCH_BYTES", DEFAULT_MAX_BATCH_BYTES)?.max(1);
    if record_count == 0 {
        return Err(kafrust::Error::Unsupported(
            "KAFRUST_BENCH_RECORDS must be greater than zero",
        ));
    }

    let compression = common::compression_from_env()?;
    let metrics = ClientMetrics::new();
    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-throughput-producer")
            .metrics(metrics.clone()),
    )?
    .acks(Acks::Leader)
    .compression(compression)
    .max_records_per_batch(batch_size)
    .max_batch_bytes(max_batch_bytes)
    .build()
    .await?;

    let payload = vec![b'x'; payload_bytes];
    for _ in 0..warmup_batches {
        producer
            .send_batch(records(&topic, &payload, batch_size))
            .await?;
    }

    let produce_started = Instant::now();
    let mut batch_latencies = Vec::new();
    let mut first_offset = None;
    let mut produced = 0;
    while produced < record_count {
        let current_batch = batch_size.min(record_count - produced);
        let batch_started = Instant::now();
        let metadata = producer
            .send_batch(records(&topic, &payload, current_batch))
            .await?;
        batch_latencies.push(batch_started.elapsed());
        if first_offset.is_none() {
            first_offset = metadata.first().map(|metadata| metadata.offset());
        }
        produced += metadata.len();
    }
    let produce_elapsed = produce_started.elapsed();
    let first_offset = first_offset.ok_or(kafrust::Error::Unsupported(
        "benchmark producer returned no record metadata",
    ))?;
    if first_offset < 0 {
        return Err(kafrust::Error::Unsupported(
            "benchmark requires concrete broker offsets",
        ));
    }

    let max_partition_bytes = payload_bytes
        .saturating_add(256)
        .saturating_mul(batch_size)
        .max(1_048_576)
        .min(i32::MAX as usize) as i32;
    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers)
            .client_id("kafrust-throughput-consumer")
            .metrics(metrics.clone()),
    )?
    .max_wait_ms(100)
    .max_partition_bytes(max_partition_bytes)
    .build()
    .await?;

    let consume_started = Instant::now();
    let consume_deadline = consume_started + CONSUME_TIMEOUT;
    let mut next_offset = first_offset;
    let mut consumed = 0;
    let mut consumed_bytes = 0usize;
    while consumed < record_count {
        if Instant::now() >= consume_deadline {
            return Err(kafrust::Error::Unsupported(
                "benchmark consumer timed out before reading every produced record",
            ));
        }
        let fetched = consumer.fetch(topic.clone(), 0, next_offset).await?;
        if fetched.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        for record in fetched.into_iter().take(record_count - consumed) {
            consumed_bytes = consumed_bytes.saturating_add(record.value().map_or(0, <[u8]>::len));
            next_offset = record.offset().saturating_add(1);
            consumed += 1;
        }
    }
    let consume_elapsed = consume_started.elapsed();
    let snapshot = metrics.snapshot();

    batch_latencies.sort_unstable();
    println!(
        concat!(
            "{{\"topic\":\"{}\",\"compression\":\"{}\",\"records\":{},",
            "\"batch_size\":{},\"max_batch_bytes\":{},\"payload_bytes\":{},",
            "\"produce_seconds\":{:.6},\"produce_records_per_second\":{:.2},",
            "\"produce_mib_per_second\":{:.2},\"batch_p50_ms\":{:.3},",
            "\"batch_p95_ms\":{:.3},\"batch_p99_ms\":{:.3},",
            "\"request_p50_ms\":{:.3},\"request_p95_ms\":{:.3},",
            "\"request_p99_ms\":{:.3},",
            "\"consume_seconds\":{:.6},\"consume_records_per_second\":{:.2},",
            "\"consume_mib_per_second\":{:.2},\"requests\":{},\"retries\":{},",
            "\"max_in_flight_requests\":{},\"max_buffered_records\":{}}}"
        ),
        topic,
        compression_name(compression),
        record_count,
        batch_size,
        max_batch_bytes,
        payload_bytes,
        produce_elapsed.as_secs_f64(),
        rate(record_count, produce_elapsed),
        mib_rate(record_count.saturating_mul(payload_bytes), produce_elapsed),
        percentile_ms(&batch_latencies, 50),
        percentile_ms(&batch_latencies, 95),
        percentile_ms(&batch_latencies, 99),
        request_percentile_ms(&snapshot, 50),
        request_percentile_ms(&snapshot, 95),
        request_percentile_ms(&snapshot, 99),
        consume_elapsed.as_secs_f64(),
        rate(consumed, consume_elapsed),
        mib_rate(consumed_bytes, consume_elapsed),
        snapshot.requests_started,
        snapshot.retries,
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

fn usize_from_env(name: &'static str, default: usize) -> kafrust::Result<usize> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| kafrust::Error::Unsupported("benchmark size variables must be integers"))
}

fn compression_name(compression: Compression) -> &'static str {
    match compression {
        Compression::None => "none",
        Compression::Gzip => "gzip",
        Compression::Snappy => "snappy",
        Compression::Lz4 => "lz4",
        Compression::Zstd => "zstd",
        _ => "unknown",
    }
}

fn rate(count: usize, elapsed: Duration) -> f64 {
    count as f64 / elapsed.as_secs_f64().max(f64::EPSILON)
}

fn mib_rate(bytes: usize, elapsed: Duration) -> f64 {
    rate(bytes, elapsed) / (1024.0 * 1024.0)
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

fn request_percentile_ms(snapshot: &kafrust::ClientMetricsSnapshot, percentile: u8) -> f64 {
    snapshot
        .latency_percentile(percentile)
        .map_or(0.0, |latency| latency.as_secs_f64() * 1_000.0)
}

#[cfg(test)]
mod tests {
    use super::{percentile_ms, rate, request_percentile_ms};
    use kafrust::ClientMetricsSnapshot;
    use std::time::Duration;

    #[test]
    fn computes_nearest_rank_percentiles() {
        let samples = [
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
        ];

        assert_eq!(percentile_ms(&samples, 50), 2.0);
        assert_eq!(percentile_ms(&samples, 95), 4.0);
    }

    #[test]
    fn computes_record_rate() {
        assert_eq!(rate(1_000, Duration::from_secs(2)), 500.0);
    }

    #[test]
    fn reports_request_percentiles_from_client_metrics() {
        let snapshot = ClientMetricsSnapshot {
            request_latency_buckets: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0],
            ..ClientMetricsSnapshot::default()
        };

        assert_eq!(request_percentile_ms(&snapshot, 50), 5_000.0);
        assert_eq!(request_percentile_ms(&snapshot, 99), 5_000.0);
    }
}
