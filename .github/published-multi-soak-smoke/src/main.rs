use std::time::{Duration, Instant};

use kafrust::{Acks, ClientMetrics, ConsumerConfig, ProducerConfig, ProducerRecord};

const PARTITIONS: usize = 3;
const DEFAULT_DURATION_SECONDS: u64 = 120;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const RECOVERY_BACKOFF: Duration = Duration::from_millis(100);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let duration = Duration::from_secs(u64_from_env(
        "KAFRUST_SOAK_SECONDS",
        DEFAULT_DURATION_SECONDS,
    )?);
    let batch_size = usize_from_env("KAFRUST_SOAK_BATCH_SIZE", DEFAULT_BATCH_SIZE)?.max(1);
    let payload_bytes = usize_from_env("KAFRUST_SOAK_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES)?;
    let metrics = ClientMetrics::new();
    let payload = vec![b'x'; payload_bytes];

    let mut producer = ProducerConfig::new(vec![bootstrap_servers.clone()])
        .client_id("kafrust-published-multi-soak-producer")
        .metrics(metrics.clone())
        .acks(Acks::Leader)
        .max_retries(3)
        .max_records_per_batch(batch_size)
        .max_batch_bytes(900 * 1024)
        .build()
        .await?;
    let mut consumer = ConsumerConfig::new(vec![bootstrap_servers])
        .client_id("kafrust-published-multi-soak-consumer")
        .metrics(metrics.clone())
        .max_wait_ms(100)
        .max_retries(3)
        .max_partition_bytes(16 * 1024 * 1024)
        .build()
        .await?;

    let started = Instant::now();
    let deadline = started + duration;
    let hard_deadline = deadline + DRAIN_TIMEOUT;
    let mut next_offsets = [None; PARTITIONS];
    let mut produced = 0usize;
    let mut consumed = 0usize;
    let mut operation_errors = 0usize;
    let mut saw_error = false;
    let mut recovered_after_error = false;

    while Instant::now() < deadline {
        for (partition, next_offset) in next_offsets.iter_mut().enumerate() {
            let metadata = match producer
                .send_batch(records(&topic, &payload, batch_size, partition as i32))
                .await
            {
                Ok(metadata) => {
                    if saw_error {
                        recovered_after_error = true;
                    }
                    metadata
                }
                Err(error) => {
                    eprintln!("published multi-soak produce operation failed: {error}");
                    operation_errors += 1;
                    saw_error = true;
                    tokio::time::sleep(RECOVERY_BACKOFF).await;
                    continue;
                }
            };
            let Some(first) = metadata.first() else {
                return Err(kafrust::Error::Unsupported(
                    "published multi-soak producer returned no metadata",
                ));
            };
            let mut fetch_offset = next_offset.unwrap_or(first.offset());
            if fetch_offset < 0 {
                return Err(kafrust::Error::Unsupported(
                    "published multi-soak requires concrete broker offsets",
                ));
            }
            *next_offset = Some(fetch_offset);
            let mut remaining = metadata.len();
            produced += remaining;

            while remaining > 0 {
                if Instant::now() >= hard_deadline {
                    return Err(kafrust::Error::Unsupported(
                        "published multi-soak consumer did not drain before the deadline",
                    ));
                }
                match consumer
                    .fetch(topic.clone(), partition as i32, fetch_offset)
                    .await
                {
                    Ok(records) if records.is_empty() => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(records) => {
                        if saw_error {
                            recovered_after_error = true;
                        }
                        let accepted = records.len().min(remaining);
                        if let Some(last) = records.get(accepted.saturating_sub(1)) {
                            fetch_offset = last.offset().saturating_add(1);
                            *next_offset = Some(fetch_offset);
                        }
                        consumed += accepted;
                        remaining -= accepted;
                    }
                    Err(error) => {
                        eprintln!("published multi-soak fetch operation failed: {error}");
                        operation_errors += 1;
                        saw_error = true;
                        tokio::time::sleep(RECOVERY_BACKOFF).await;
                    }
                }
            }
        }
    }

    let snapshot = metrics.snapshot();
    if produced != consumed || snapshot.produced_records != snapshot.consumed_records {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak finished with unmatched produced and consumed records",
        ));
    }
    if snapshot.in_flight_requests != 0 || snapshot.buffered_records != 0 {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak finished with non-zero client gauges",
        ));
    }
    if !saw_error || !recovered_after_error {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak did not observe both an error and recovery",
        ));
    }

    println!(
        concat!(
            "{{\"topic\":\"{}\",\"duration_seconds\":{:.3},\"partitions\":{},",
            "\"records\":{},\"payload_bytes\":{},\"operation_errors\":{},",
            "\"recovered\":{},\"requests_started\":{},\"requests_failed\":{},",
            "\"retries\":{},\"in_flight_requests\":{},\"buffered_records\":{}}}"
        ),
        topic,
        started.elapsed().as_secs_f64(),
        PARTITIONS,
        produced,
        payload_bytes,
        operation_errors,
        recovered_after_error,
        snapshot.requests_started,
        snapshot.requests_failed,
        snapshot.retries,
        snapshot.in_flight_requests,
        snapshot.buffered_records,
    );
    Ok(())
}

fn records(topic: &str, payload: &[u8], count: usize, partition: i32) -> Vec<ProducerRecord> {
    (0..count)
        .map(|_| {
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(payload.to_vec())
        })
        .collect()
}

fn required(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| kafrust::Error::Unsupported("soak variable is missing"))
}

fn usize_from_env(name: &'static str, default: usize) -> kafrust::Result<usize> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| kafrust::Error::Unsupported("soak size variable is invalid"))
}

fn u64_from_env(name: &'static str, default: u64) -> kafrust::Result<u64> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| kafrust::Error::Unsupported("soak duration variable is invalid"))
}
