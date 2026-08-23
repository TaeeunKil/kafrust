mod common;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use kafrust::{
    consumer::Consumer, producer::Producer, Acks, ClientMetrics, ClientMetricsSnapshot,
    Compression, ConsumerConfig, ProducerConfig, ProducerRecord,
};
use tokio::sync::Barrier;

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
    if let Some((warmup_seconds, measured_seconds, sample_seconds)) = campaign_config()? {
        let settings = CampaignSettings {
            batch_size,
            payload_bytes,
            max_batch_bytes,
            compression,
            warmup_seconds,
            measured_seconds,
            sample_seconds,
            workers: campaign_workers()?,
        };
        return run_campaign(bootstrap_servers, topic, settings).await;
    }

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

#[derive(Clone, Copy)]
struct CampaignSettings {
    batch_size: usize,
    payload_bytes: usize,
    max_batch_bytes: usize,
    compression: Compression,
    warmup_seconds: u64,
    measured_seconds: u64,
    sample_seconds: u64,
    workers: usize,
}

#[derive(Clone)]
struct CampaignBarriers {
    start: Arc<Barrier>,
    measurement: Arc<Barrier>,
}

struct BatchWork<'a> {
    topic: &'a str,
    partition: i32,
    payload: &'a [u8],
    batch_size: usize,
}

async fn run_campaign(
    bootstrap_servers: Vec<String>,
    topic: String,
    settings: CampaignSettings,
) -> kafrust::Result<()> {
    let max_partition_bytes = settings
        .payload_bytes
        .saturating_add(256)
        .saturating_mul(settings.batch_size)
        .max(1_048_576)
        .min(i32::MAX as usize) as i32;
    let metrics = ClientMetrics::new();
    let mut workers = Vec::with_capacity(settings.workers);
    for partition in 0..settings.workers {
        let partition = partition as i32;
        let producer = common::apply_security(
            ProducerConfig::new(bootstrap_servers.clone())
                .client_id(format!("kafrust-throughput-campaign-producer-{partition}"))
                .metrics(metrics.clone()),
        )?
        .acks(Acks::Leader)
        .compression(settings.compression)
        .max_records_per_batch(settings.batch_size)
        .max_batch_bytes(settings.max_batch_bytes)
        .build()
        .await?;
        let mut consumer = common::apply_security(
            ConsumerConfig::new(bootstrap_servers.clone())
                .client_id(format!("kafrust-throughput-campaign-consumer-{partition}"))
                .metrics(metrics.clone()),
        )?
        .max_wait_ms(100)
        .max_partition_bytes(max_partition_bytes)
        .build()
        .await?;
        let next_offset = consumer.fetch_watermarks(&topic, partition).await?.high();
        if next_offset < 0 {
            return Err(kafrust::Error::Unsupported(
                "benchmark campaign requires concrete broker offsets",
            ));
        }
        workers.push((producer, consumer, partition, next_offset));
    }

    let barriers = CampaignBarriers {
        start: Arc::new(Barrier::new(settings.workers + 1)),
        measurement: Arc::new(Barrier::new(settings.workers + 1)),
    };
    let mut handles = Vec::with_capacity(settings.workers);
    for (producer, consumer, partition, next_offset) in workers {
        handles.push(tokio::spawn(run_campaign_worker(
            producer,
            consumer,
            topic.clone(),
            partition,
            next_offset,
            settings,
            barriers.clone(),
        )));
    }
    wait_for_barrier(barriers.start.clone(), CONSUME_TIMEOUT).await?;
    let measurement_wait =
        Duration::from_secs(settings.warmup_seconds).saturating_add(CONSUME_TIMEOUT);
    wait_for_barrier(barriers.measurement.clone(), measurement_wait).await?;
    let baseline = metrics.snapshot();
    let profile =
        std::env::var("KAFRUST_BENCH_PROFILE").unwrap_or_else(|_| "throughput-campaign".to_owned());
    let measured_started = Instant::now();
    let measured_deadline = measured_started + Duration::from_secs(settings.measured_seconds);
    let mut window_start = measured_started;
    let mut sample_index = 0_u64;
    let mut measured_latency_buckets = ClientMetricsSnapshot::default().request_latency_buckets;
    let mut rss_samples = Vec::new();
    while window_start < measured_deadline {
        let window_end =
            (window_start + Duration::from_secs(settings.sample_seconds)).min(measured_deadline);
        let before = metrics.snapshot();
        let sleep_for = window_end.saturating_duration_since(Instant::now());
        if !sleep_for.is_zero() {
            tokio::time::sleep(sleep_for).await;
        }
        let after = metrics.snapshot();
        let elapsed_seconds = measured_started.elapsed().as_secs_f64();
        let delta = delta_snapshot(&before, &after);
        for (total, sample) in measured_latency_buckets
            .iter_mut()
            .zip(delta.request_latency_buckets)
        {
            *total = total.saturating_add(sample);
        }
        if let Some(rss_bytes) = resident_bytes() {
            rss_samples.push((elapsed_seconds, rss_bytes));
        }
        print_campaign_sample(
            &profile,
            sample_index,
            elapsed_seconds,
            (window_start - measured_started).as_secs_f64(),
            (window_end - measured_started).as_secs_f64(),
            &before,
            &after,
        );
        sample_index = sample_index.saturating_add(1);
        window_start = window_end;
    }

    for handle in handles {
        handle
            .await
            .map_err(|_| kafrust::Error::Unsupported("benchmark campaign worker panicked"))??;
    }
    let final_snapshot = metrics.snapshot();
    if final_snapshot.produced_records != final_snapshot.consumed_records {
        return Err(kafrust::Error::Unsupported(
            "benchmark campaign finished with unmatched produced and consumed records",
        ));
    }
    if final_snapshot.in_flight_requests != 0 || final_snapshot.buffered_records != 0 {
        return Err(kafrust::Error::Unsupported(
            "benchmark campaign detected a non-zero final client gauge",
        ));
    }
    let measured_metrics = ClientMetricsSnapshot {
        request_latency_buckets: measured_latency_buckets,
        ..ClientMetricsSnapshot::default()
    };
    let (rss_baseline, rss_terminal, rss_growth, rss_slope) = rss_summary(&rss_samples);
    println!(
        concat!(
            "{{\"mode\":\"campaign-final\",\"profile\":\"{}\",",
            "\"warmup_seconds\":{},\"measured_seconds\":{},\"sample_seconds\":{},\"workers\":{},",
            "\"batch_size\":{},\"payload_bytes\":{},\"compression\":\"{}\",",
            "\"produced_records\":{},\"consumed_records\":{},\"requests_started\":{},",
            "\"requests_failed\":{},\"retries\":{},\"retry_ratio\":{:.6},",
            "\"latency_p50_p95_p99\":{{\"p50_ms\":{},\"p95_ms\":{},\"p99_ms\":{}}},",
            "\"rss_baseline_terminal_slope\":{{\"baseline_bytes\":{},\"terminal_bytes\":{},",
            "\"growth_bytes\":{},\"slope_bytes_per_second\":{},\"sample_count\":{}}},",
            "\"loss_count\":0,\"duplicate_count\":0,\"rss_bytes\":{},",
            "\"in_flight_requests\":{},\"buffered_records\":{}}}"
        ),
        profile,
        settings.warmup_seconds,
        settings.measured_seconds,
        settings.sample_seconds,
        settings.workers,
        settings.batch_size,
        settings.payload_bytes,
        compression_name(settings.compression),
        delta_u64(final_snapshot.produced_records, baseline.produced_records),
        delta_u64(final_snapshot.consumed_records, baseline.consumed_records),
        delta_u64(final_snapshot.requests_started, baseline.requests_started),
        delta_u64(final_snapshot.requests_failed, baseline.requests_failed),
        delta_u64(final_snapshot.retries, baseline.retries),
        retry_ratio(&baseline, &final_snapshot),
        optional_latency_json(&measured_metrics, 50),
        optional_latency_json(&measured_metrics, 95),
        optional_latency_json(&measured_metrics, 99),
        optional_u64_json(rss_baseline),
        optional_u64_json(rss_terminal),
        optional_i128_json(rss_growth),
        optional_f64_json(rss_slope),
        rss_samples.len(),
        optional_u64_json(resident_bytes()),
        final_snapshot.in_flight_requests,
        final_snapshot.buffered_records,
    );
    Ok(())
}

async fn run_campaign_worker(
    mut producer: Producer,
    mut consumer: Consumer,
    topic: String,
    partition: i32,
    mut next_offset: i64,
    settings: CampaignSettings,
    barriers: CampaignBarriers,
) -> kafrust::Result<()> {
    let payload = vec![b'x'; settings.payload_bytes];
    wait_for_barrier(barriers.start.clone(), CONSUME_TIMEOUT).await?;
    let warmup_deadline = Instant::now() + Duration::from_secs(settings.warmup_seconds);
    while Instant::now() < warmup_deadline {
        next_offset = send_and_consume_batch(
            &mut producer,
            &mut consumer,
            BatchWork {
                topic: &topic,
                partition,
                payload: &payload,
                batch_size: settings.batch_size,
            },
            next_offset,
            warmup_deadline + CONSUME_TIMEOUT,
        )
        .await?;
    }
    let measurement_wait =
        Duration::from_secs(settings.warmup_seconds).saturating_add(CONSUME_TIMEOUT);
    wait_for_barrier(barriers.measurement.clone(), measurement_wait).await?;
    let measured_deadline = Instant::now() + Duration::from_secs(settings.measured_seconds);
    while Instant::now() < measured_deadline {
        next_offset = send_and_consume_batch(
            &mut producer,
            &mut consumer,
            BatchWork {
                topic: &topic,
                partition,
                payload: &payload,
                batch_size: settings.batch_size,
            },
            next_offset,
            measured_deadline + CONSUME_TIMEOUT,
        )
        .await?;
    }
    Ok(())
}

async fn send_and_consume_batch(
    producer: &mut Producer,
    consumer: &mut Consumer,
    work: BatchWork<'_>,
    next_offset: i64,
    deadline: Instant,
) -> kafrust::Result<i64> {
    let metadata = producer
        .send_batch(records_on_partition(
            work.topic,
            work.partition,
            work.payload,
            work.batch_size,
        ))
        .await?;
    let Some(first) = metadata.first() else {
        return Err(kafrust::Error::Unsupported(
            "benchmark campaign producer returned no record metadata",
        ));
    };
    if first.offset() < 0 {
        return Err(kafrust::Error::Unsupported(
            "benchmark campaign requires concrete broker offsets",
        ));
    }
    if first.offset() != next_offset {
        return Err(kafrust::Error::Unsupported(
            "benchmark campaign observed a non-contiguous broker offset",
        ));
    }
    let mut next_offset = next_offset;
    let mut remaining = metadata.len();
    while remaining > 0 {
        if Instant::now() >= deadline {
            return Err(kafrust::Error::Unsupported(
                "benchmark campaign consumer did not drain the current batch",
            ));
        }
        let fetched = consumer
            .fetch(work.topic.to_owned(), work.partition, next_offset)
            .await?;
        if fetched.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        if fetched.len() > remaining {
            return Err(kafrust::Error::Unsupported(
                "benchmark campaign fetched records beyond the current batch",
            ));
        }
        for record in fetched {
            if record.offset() != next_offset {
                return Err(kafrust::Error::Unsupported(
                    "benchmark campaign observed a non-contiguous fetched offset",
                ));
            }
            next_offset = record.offset().saturating_add(1);
            remaining -= 1;
        }
    }
    Ok(next_offset)
}

async fn wait_for_barrier(barrier: Arc<Barrier>, timeout: Duration) -> kafrust::Result<()> {
    tokio::time::timeout(timeout, barrier.wait())
        .await
        .map(|_| ())
        .map_err(|_| kafrust::Error::Unsupported("benchmark campaign barrier timed out"))
}

fn campaign_config() -> kafrust::Result<Option<(u64, u64, u64)>> {
    let warmup = optional_u64_from_env("KAFRUST_BENCH_WARMUP_SECONDS")?;
    let measured = optional_u64_from_env("KAFRUST_BENCH_MEASURED_SECONDS")?;
    let sample = optional_u64_from_env("KAFRUST_BENCH_SAMPLE_SECONDS")?;
    if warmup.is_none() && measured.is_none() && sample.is_none() {
        return Ok(None);
    }
    let (Some(warmup), Some(measured), Some(sample)) = (warmup, measured, sample) else {
        return Err(kafrust::Error::Unsupported(
            "campaign mode requires warmup, measured, and sample seconds",
        ));
    };
    if measured == 0 || sample == 0 {
        return Err(kafrust::Error::Unsupported(
            "campaign measured and sample seconds must be greater than zero",
        ));
    }
    Ok(Some((warmup, measured, sample)))
}

fn records(topic: &str, payload: &[u8], count: usize) -> Vec<ProducerRecord> {
    records_on_partition(topic, 0, payload, count)
}

fn records_on_partition(
    topic: &str,
    partition: i32,
    payload: &[u8],
    count: usize,
) -> Vec<ProducerRecord> {
    (0..count)
        .map(|_| {
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
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

fn optional_u64_from_env(name: &'static str) -> kafrust::Result<Option<u64>> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(None);
    };
    value
        .parse()
        .map(Some)
        .map_err(|_| kafrust::Error::Unsupported("benchmark duration variables must be integers"))
}

fn campaign_workers() -> kafrust::Result<usize> {
    let workers = usize_from_env("KAFRUST_BENCH_WORKERS", 1)?;
    if workers == 0 {
        return Err(kafrust::Error::Unsupported(
            "KAFRUST_BENCH_WORKERS must be greater than zero",
        ));
    }
    Ok(workers)
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

fn print_campaign_sample(
    profile: &str,
    sample_index: u64,
    elapsed_seconds: f64,
    window_start_seconds: f64,
    window_end_seconds: f64,
    before: &kafrust::ClientMetricsSnapshot,
    after: &kafrust::ClientMetricsSnapshot,
) {
    let delta = delta_snapshot(before, after);
    let produced = delta.produced_records;
    let consumed = delta.consumed_records;
    let window_seconds = (window_end_seconds - window_start_seconds).max(f64::EPSILON);
    println!(
        concat!(
            "{{\"mode\":\"campaign-sample\",\"profile\":\"{}\",",
            "\"sample_index\":{},\"elapsed_seconds\":{:.3},",
            "\"sample_start_seconds\":{:.3},\"sample_end_seconds\":{:.3},",
            "\"produced_records\":{},\"consumed_records\":{},",
            "\"produce_records_per_second\":{:.3},\"consume_records_per_second\":{:.3},",
            "\"requests_started\":{},\"requests_failed\":{},\"retries\":{},",
            "\"retry_ratio\":{:.6},\"request_p50_ms\":{:.3},",
            "\"request_p95_ms\":{:.3},\"request_p99_ms\":{:.3},",
            "\"rss_bytes\":{},\"in_flight_requests\":{},\"buffered_records\":{}}}"
        ),
        profile,
        sample_index,
        elapsed_seconds,
        window_start_seconds,
        window_end_seconds,
        produced,
        consumed,
        produced as f64 / window_seconds,
        consumed as f64 / window_seconds,
        delta.requests_started,
        delta.requests_failed,
        delta.retries,
        retry_ratio(before, after),
        request_percentile_ms(&delta, 50),
        request_percentile_ms(&delta, 95),
        request_percentile_ms(&delta, 99),
        optional_u64_json(resident_bytes()),
        after.in_flight_requests,
        after.buffered_records,
    );
}

fn delta_snapshot(
    before: &kafrust::ClientMetricsSnapshot,
    after: &kafrust::ClientMetricsSnapshot,
) -> kafrust::ClientMetricsSnapshot {
    kafrust::ClientMetricsSnapshot {
        requests_started: delta_u64(after.requests_started, before.requests_started),
        requests_succeeded: delta_u64(after.requests_succeeded, before.requests_succeeded),
        requests_failed: delta_u64(after.requests_failed, before.requests_failed),
        requests_timed_out: delta_u64(after.requests_timed_out, before.requests_timed_out),
        requests_cancelled: delta_u64(after.requests_cancelled, before.requests_cancelled),
        broker_errors: delta_u64(after.broker_errors, before.broker_errors),
        retries: delta_u64(after.retries, before.retries),
        buffered_records: after.buffered_records,
        max_buffered_records: after.max_buffered_records,
        produced_records: delta_u64(after.produced_records, before.produced_records),
        produce_batches: delta_u64(after.produce_batches, before.produce_batches),
        consumed_records: delta_u64(after.consumed_records, before.consumed_records),
        request_bytes: delta_u64(after.request_bytes, before.request_bytes),
        response_bytes: delta_u64(after.response_bytes, before.response_bytes),
        in_flight_requests: after.in_flight_requests,
        max_in_flight_requests: after.max_in_flight_requests,
        total_latency: after.total_latency.saturating_sub(before.total_latency),
        max_latency: after.max_latency,
        request_latency_buckets: std::array::from_fn(|index| {
            delta_u64(
                after.request_latency_buckets[index],
                before.request_latency_buckets[index],
            )
        }),
    }
}

fn retry_ratio(
    before: &kafrust::ClientMetricsSnapshot,
    after: &kafrust::ClientMetricsSnapshot,
) -> f64 {
    let attempts = delta_u64(after.requests_started, before.requests_started);
    if attempts == 0 {
        return 0.0;
    }
    delta_u64(after.retries, before.retries) as f64 / attempts as f64
}

fn delta_u64(after: u64, before: u64) -> u64 {
    after.saturating_sub(before)
}

fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kilobytes = line.split_whitespace().nth(1)?.parse::<u64>().ok()?;
    Some(kilobytes.saturating_mul(1024))
}

fn optional_u64_json(value: Option<u64>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn optional_i128_json(value: Option<i128>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| value.to_string())
}

fn optional_f64_json(value: Option<f64>) -> String {
    value.map_or_else(|| "null".to_owned(), |value| format!("{value:.6}"))
}

fn optional_latency_json(snapshot: &ClientMetricsSnapshot, percentile: u8) -> String {
    snapshot.latency_percentile(percentile).map_or_else(
        || "null".to_owned(),
        |latency| format!("{:.3}", latency.as_secs_f64() * 1_000.0),
    )
}

fn rss_summary(samples: &[(f64, u64)]) -> (Option<u64>, Option<u64>, Option<i128>, Option<f64>) {
    let Some(&(first_time, _)) = samples.first() else {
        return (None, None, None, None);
    };
    let Some(&(last_time, _)) = samples.last() else {
        return (None, None, None, None);
    };
    let baseline_values: Vec<u64> = samples
        .iter()
        .filter(|(time, _)| *time <= first_time + 1_800.0)
        .map(|(_, value)| *value)
        .collect();
    let terminal_values: Vec<u64> = samples
        .iter()
        .filter(|(time, _)| *time + 1_800.0 >= last_time)
        .map(|(_, value)| *value)
        .collect();
    let baseline = median_u64(&baseline_values);
    let terminal = median_u64(&terminal_values);
    let growth = baseline
        .zip(terminal)
        .map(|(baseline, terminal)| i128::from(terminal) - i128::from(baseline));
    let slope = least_squares_slope(samples);
    (baseline, terminal, growth, slope)
}

fn median_u64(values: &[u64]) -> Option<u64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let middle = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        let lower = u128::from(sorted[middle - 1]);
        let upper = u128::from(sorted[middle]);
        Some(((lower + upper) / 2) as u64)
    } else {
        Some(sorted[middle])
    }
}

fn least_squares_slope(samples: &[(f64, u64)]) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let count = samples.len() as f64;
    let mean_x = samples.iter().map(|(x, _)| *x).sum::<f64>() / count;
    let mean_y = samples.iter().map(|(_, y)| *y as f64).sum::<f64>() / count;
    let numerator = samples
        .iter()
        .map(|(x, y)| (*x - mean_x) * (*y as f64 - mean_y))
        .sum::<f64>();
    let denominator = samples
        .iter()
        .map(|(x, _)| (*x - mean_x).powi(2))
        .sum::<f64>();
    (denominator > f64::EPSILON).then_some(numerator / denominator)
}

#[cfg(test)]
mod tests {
    use super::{
        delta_snapshot, least_squares_slope, median_u64, percentile_ms, rate,
        request_percentile_ms, retry_ratio, rss_summary,
    };
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

    #[test]
    fn campaign_delta_keeps_window_histogram_and_retry_ratio() {
        let before = ClientMetricsSnapshot {
            requests_started: 10,
            retries: 2,
            request_latency_buckets: [1; 13],
            ..ClientMetricsSnapshot::default()
        };
        let after = ClientMetricsSnapshot {
            requests_started: 30,
            retries: 5,
            request_latency_buckets: [3; 13],
            ..ClientMetricsSnapshot::default()
        };

        let delta = delta_snapshot(&before, &after);
        assert_eq!(delta.requests_started, 20);
        assert_eq!(delta.retries, 3);
        assert_eq!(delta.request_latency_buckets, [2; 13]);
        assert_eq!(retry_ratio(&before, &after), 0.15);
    }

    #[test]
    fn summarizes_rss_windows_and_slope() {
        let samples = [(0.0, 100), (10.0, 110), (20.0, 120), (30.0, 130)];
        assert_eq!(median_u64(&[100, 120, 110, 130]), Some(115));
        assert_eq!(
            rss_summary(&samples),
            (Some(115), Some(115), Some(0), Some(1.0))
        );
        assert_eq!(least_squares_slope(&samples), Some(1.0));
    }
}
