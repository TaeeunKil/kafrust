mod common;

use std::time::{Duration, Instant};

use kafrust::{Acks, ClientMetrics, ConsumerConfig, ProducerConfig, ProducerRecord};

const DEFAULT_DURATION_SECONDS: u64 = 300;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const RECOVERY_BACKOFF: Duration = Duration::from_millis(100);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(90);

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-soak".to_owned());
    let duration = Duration::from_secs(u64_from_env(
        "KAFRUST_SOAK_SECONDS",
        DEFAULT_DURATION_SECONDS,
    )?);
    let batch_size = usize_from_env("KAFRUST_SOAK_BATCH_SIZE", DEFAULT_BATCH_SIZE)?.max(1);
    let payload_bytes = usize_from_env("KAFRUST_SOAK_PAYLOAD_BYTES", DEFAULT_PAYLOAD_BYTES)?;
    let require_failure = bool_from_env("KAFRUST_SOAK_REQUIRE_FAILURE", false)?;
    let metrics = ClientMetrics::new();

    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-soak-producer")
            .metrics(metrics.clone()),
    )?
    .acks(Acks::Leader)
    .max_retries(3)
    .max_records_per_batch(batch_size)
    .max_batch_bytes(900 * 1024)
    .build()
    .await?;
    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers)
            .client_id("kafrust-soak-consumer")
            .metrics(metrics.clone()),
    )?
    .max_wait_ms(100)
    .max_retries(3)
    .max_partition_bytes(16 * 1024 * 1024)
    .build()
    .await?;

    let started = Instant::now();
    let deadline = started + duration;
    let hard_deadline = deadline + DRAIN_TIMEOUT;
    let payload = vec![b'x'; payload_bytes];
    let mut produced = 0usize;
    let mut consumed = 0usize;
    let mut operation_errors = 0usize;
    let mut saw_error = false;
    let mut recovered_after_error = false;

    while Instant::now() < deadline {
        let records = records(&topic, &payload, batch_size);
        let metadata = match producer.send_batch(records).await {
            Ok(metadata) => {
                if saw_error {
                    recovered_after_error = true;
                }
                metadata
            }
            Err(error) => {
                eprintln!("soak produce operation failed: {error}");
                operation_errors += 1;
                saw_error = true;
                tokio::time::sleep(RECOVERY_BACKOFF).await;
                continue;
            }
        };
        let Some(first) = metadata.first() else {
            return Err(kafrust::Error::Unsupported(
                "soak producer returned no record metadata",
            ));
        };
        let mut next_offset = first.offset();
        if next_offset < 0 {
            return Err(kafrust::Error::Unsupported(
                "soak profile requires concrete broker offsets",
            ));
        }
        let mut remaining = metadata.len();
        produced += remaining;

        while remaining > 0 {
            if Instant::now() >= hard_deadline {
                return Err(kafrust::Error::Unsupported(
                    "soak consumer did not drain produced records after the deadline",
                ));
            }
            match consumer.fetch(topic.clone(), 0, next_offset).await {
                Ok(records) if records.is_empty() => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(records) => {
                    if saw_error {
                        recovered_after_error = true;
                    }
                    let accepted = records.len().min(remaining);
                    if let Some(last) = records.get(accepted.saturating_sub(1)) {
                        next_offset = last.offset().saturating_add(1);
                    }
                    consumed += accepted;
                    remaining -= accepted;
                }
                Err(error) => {
                    eprintln!("soak fetch operation failed: {error}");
                    operation_errors += 1;
                    saw_error = true;
                    tokio::time::sleep(RECOVERY_BACKOFF).await;
                }
            }
        }
    }

    let snapshot = metrics.snapshot();
    if produced != consumed || snapshot.produced_records != snapshot.consumed_records {
        return Err(kafrust::Error::Unsupported(
            "soak profile finished with unmatched produced and consumed records",
        ));
    }
    if snapshot.in_flight_requests != 0 || snapshot.buffered_records != 0 {
        return Err(kafrust::Error::Unsupported(
            "soak profile detected a non-zero final client gauge",
        ));
    }
    if require_failure && (!saw_error || !recovered_after_error) {
        return Err(kafrust::Error::Unsupported(
            "soak failure injection did not observe both an error and recovery",
        ));
    }

    println!(
        concat!(
            "{{\"topic\":\"{}\",\"duration_seconds\":{:.3},\"records\":{},",
            "\"payload_bytes\":{},\"operation_errors\":{},\"recovered\":{},",
            "\"requests_started\":{},\"requests_failed\":{},\"retries\":{},",
            "\"in_flight_requests\":{},\"buffered_records\":{}}}"
        ),
        topic,
        started.elapsed().as_secs_f64(),
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
        .map_err(|_| kafrust::Error::Unsupported("soak size variables must be integers"))
}

fn u64_from_env(name: &'static str, default: u64) -> kafrust::Result<u64> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| kafrust::Error::Unsupported("soak duration must be an integer"))
}

fn bool_from_env(name: &'static str, default: bool) -> kafrust::Result<bool> {
    let Some(value) = std::env::var(name).ok() else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(kafrust::Error::Unsupported(
            "soak boolean variables must be true or false",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::bool_from_env;

    #[test]
    fn rejects_invalid_soak_boolean() {
        std::env::set_var("KAFRUST_SOAK_TEST_BOOL", "invalid");
        let result = bool_from_env("KAFRUST_SOAK_TEST_BOOL", false);
        std::env::remove_var("KAFRUST_SOAK_TEST_BOOL");

        assert!(result.is_err());
    }
}
