use std::{
    cmp::Ordering,
    time::{Duration, Instant},
};

use kafrust::{Acks, ClientMetrics, ConsumerConfig, ProducerConfig, ProducerRecord};
use sha2::{Digest, Sha256};

const PARTITIONS: usize = 3;
const DEFAULT_DURATION_SECONDS: u64 = 120;
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_PAYLOAD_BYTES: usize = 1_024;
const RECOVERY_BACKOFF: Duration = Duration::from_millis(100);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);

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

    let mut producer = ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-multi-soak-producer")
        .metrics(metrics.clone())
        .request_timeout_ms(5_000)
        .acks(Acks::All)
        .enable_idempotence(true)
        .max_retries(10)
        .max_records_per_batch(batch_size)
        .max_batch_bytes(900 * 1024)
        .build()
        .await?;
    let mut consumer = ConsumerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-multi-soak-consumer")
        .metrics(metrics.clone())
        .request_timeout_ms(5_000)
        .max_wait_ms(100)
        .max_retries(10)
        .max_partition_bytes(16 * 1024 * 1024)
        .build()
        .await?;

    let started = Instant::now();
    let deadline = started + duration;
    let hard_deadline = deadline + DRAIN_TIMEOUT;
    let mut next_offsets = [None; PARTITIONS];
    let mut next_produce_sequences = [0_u64; PARTITIONS];
    let mut next_consume_sequences = [0_u64; PARTITIONS];
    let mut pending_batches: [Option<Vec<ProducerRecord>>; PARTITIONS] = [None, None, None];
    let mut produced = 0usize;
    let mut consumed = 0usize;
    let mut attempted_records = 0_u64;
    let mut acknowledged_records = 0_u64;
    let mut duplicate_count = 0_u64;
    let mut loss_count = 0_u64;
    let mut unknown_outcomes = 0_u64;
    let mut operation_errors = 0usize;
    let mut saw_error = false;
    let mut recovered_after_error = false;
    let mut last_progress = Instant::now();

    while Instant::now() < deadline {
        for (partition, next_offset) in next_offsets.iter_mut().enumerate() {
            let batch_records = if let Some(batch_records) = pending_batches[partition].take() {
                batch_records
            } else {
                let sequence_start = next_produce_sequences[partition];
                let batch_records = records(
                    &topic,
                    &payload,
                    batch_size,
                    partition as i32,
                    sequence_start,
                );
                let batch_len = u64::try_from(batch_records.len())
                    .map_err(|_| kafrust::Error::Unsupported("soak batch length is too large"))?;
                attempted_records = attempted_records.saturating_add(batch_len);
                batch_records
            };
            let batch_len = u64::try_from(batch_records.len())
                .map_err(|_| kafrust::Error::Unsupported("soak batch length is too large"))?;
            let metadata = match producer.send_batch(batch_records.iter().cloned()).await {
                Ok(metadata) => {
                    next_produce_sequences[partition] =
                        next_produce_sequences[partition].saturating_add(batch_len);
                    acknowledged_records = acknowledged_records.saturating_add(
                        u64::try_from(metadata.len()).map_err(|_| {
                            kafrust::Error::Unsupported("soak metadata length is too large")
                        })?,
                    );
                    if saw_error {
                        recovered_after_error = true;
                    }
                    metadata
                }
                Err(error) => {
                    eprintln!("published multi-soak produce operation failed: {error}");
                    pending_batches[partition] = Some(batch_records);
                    operation_errors += 1;
                    unknown_outcomes = unknown_outcomes.saturating_add(batch_len);
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
                    eprintln!(
                        "published multi-soak drain timeout: partition={partition} next_offset={fetch_offset} remaining={remaining} produced={produced} consumed={consumed}"
                    );
                    return Err(kafrust::Error::Unsupported(
                        "published multi-soak consumer did not drain before the deadline",
                    ));
                }
                if last_progress.elapsed() >= Duration::from_secs(10) {
                    eprintln!(
                        "published multi-soak progress: partition={partition} next_offset={fetch_offset} remaining={remaining} produced={produced} consumed={consumed}"
                    );
                    last_progress = Instant::now();
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
                        for record in &records {
                            let (record_partition, sequence) = record_identity(
                                record.value().ok_or(kafrust::Error::Unsupported(
                                    "published multi-soak record value was null",
                                ))?,
                            )?;
                            if record_partition != partition {
                                return Err(kafrust::Error::Unsupported(
                                    "published multi-soak record partition identity diverged",
                                ));
                            }
                            observe_identity(
                                &mut next_consume_sequences[partition],
                                sequence,
                                &mut duplicate_count,
                                &mut loss_count,
                            );
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

    if pending_batches.iter().any(Option::is_some) {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak ended with unresolved produce outcomes",
        ));
    }

    let snapshot = metrics.snapshot();
    let consumed_records = u64::try_from(consumed)
        .map_err(|_| kafrust::Error::Unsupported("soak consumed record count is too large"))?;
    if produced != consumed
        || acknowledged_records != consumed_records
        || snapshot.produced_records != snapshot.consumed_records
        || snapshot.produced_records
            != u64::try_from(produced).map_err(|_| {
                kafrust::Error::Unsupported("soak produced record count is too large")
            })?
    {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak finished with unmatched produced and consumed records",
        ));
    }
    if snapshot.in_flight_requests != 0 || snapshot.buffered_records != 0 {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak finished with non-zero client gauges",
        ));
    }
    let recovered_after_error = recovered_after_error || snapshot.retries > 0;
    if snapshot.retries == 0 || !recovered_after_error {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak did not observe retry and recovery",
        ));
    }
    for (produced_sequence, consumed_sequence) in next_produce_sequences
        .iter()
        .zip(next_consume_sequences.iter())
    {
        match consumed_sequence.cmp(produced_sequence) {
            Ordering::Less => {
                loss_count = loss_count.saturating_add(produced_sequence - consumed_sequence);
            }
            Ordering::Greater => {
                return Err(kafrust::Error::Unsupported(
                    "published multi-soak observed an out-of-range record identity",
                ));
            }
            Ordering::Equal => {}
        }
    }
    if loss_count != 0 || duplicate_count != 0 {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak record identity reconciliation failed",
        ));
    }
    let expected_digest = identity_digest(&next_produce_sequences);
    let observed_digest = identity_digest(&next_consume_sequences);
    if expected_digest != observed_digest || attempted_records != consumed_records {
        return Err(kafrust::Error::Unsupported(
            "published multi-soak record identity digest did not reconcile",
        ));
    }

    println!(
        concat!(
            "{{\"topic\":\"{}\",\"duration_seconds\":{:.3},\"partitions\":{},",
            "\"records\":{},\"attempted_records\":{},\"acknowledged_records\":{},",
            "\"consumed_unique_records\":{},\"payload_bytes\":{},\"operation_errors\":{},",
            "\"recovered\":{},\"requests_started\":{},\"requests_failed\":{},",
            "\"retries\":{},\"unknown_outcomes\":{},\"in_flight_requests\":{},",
            "\"max_in_flight_requests\":{},\"buffered_records\":{},",
            "\"max_buffered_records\":{},\"record_id_reconciliation\":{{",
            "\"qualified\":true,\"unique_records\":{},\"loss_count\":{},",
            "\"duplicate_count\":{},\"digest\":\"{}\"}}}}"
        ),
        topic,
        started.elapsed().as_secs_f64(),
        PARTITIONS,
        produced,
        attempted_records,
        acknowledged_records,
        consumed_records,
        payload_bytes,
        operation_errors,
        recovered_after_error,
        snapshot.requests_started,
        snapshot.requests_failed,
        snapshot.retries,
        unknown_outcomes,
        snapshot.in_flight_requests,
        snapshot.max_in_flight_requests,
        snapshot.buffered_records,
        snapshot.max_buffered_records,
        consumed_records,
        loss_count,
        duplicate_count,
        observed_digest,
    );
    Ok(())
}

fn records(
    topic: &str,
    payload: &[u8],
    count: usize,
    partition: i32,
    sequence_start: u64,
) -> Vec<ProducerRecord> {
    (0..count)
        .map(|index| {
            let sequence = sequence_start.saturating_add(index as u64);
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(record_value(payload, partition, sequence))
        })
        .collect()
}

fn record_value(payload: &[u8], partition: i32, sequence: u64) -> Vec<u8> {
    let mut value = vec![b'x'; payload.len().max(12)];
    value[..4].copy_from_slice(&partition.to_be_bytes());
    value[4..12].copy_from_slice(&sequence.to_be_bytes());
    value
}

fn record_identity(value: &[u8]) -> kafrust::Result<(usize, u64)> {
    let partition = value.get(..4).ok_or(kafrust::Error::Unsupported(
        "soak record identity is truncated",
    ))?;
    let sequence = value.get(4..12).ok_or(kafrust::Error::Unsupported(
        "soak record identity is truncated",
    ))?;
    let partition = i32::from_be_bytes(
        partition
            .try_into()
            .map_err(|_| kafrust::Error::Unsupported("soak partition identity is invalid"))?,
    );
    let sequence = u64::from_be_bytes(
        sequence
            .try_into()
            .map_err(|_| kafrust::Error::Unsupported("soak sequence identity is invalid"))?,
    );
    let partition = usize::try_from(partition)
        .map_err(|_| kafrust::Error::Unsupported("soak partition identity is negative"))?;
    Ok((partition, sequence))
}

fn observe_identity(next: &mut u64, sequence: u64, duplicates: &mut u64, losses: &mut u64) {
    match sequence.cmp(next) {
        Ordering::Less => *duplicates = duplicates.saturating_add(1),
        Ordering::Greater => {
            *losses = losses.saturating_add(sequence - *next);
            *next = sequence.saturating_add(1);
        }
        Ordering::Equal => *next = next.saturating_add(1),
    }
}

fn identity_digest(sequences: &[u64; PARTITIONS]) -> String {
    let mut digest = Sha256::new();
    for (partition, sequence) in sequences.iter().enumerate() {
        digest.update((partition as u32).to_be_bytes());
        digest.update(sequence.to_be_bytes());
    }
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest.finalize() {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
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

#[cfg(test)]
mod tests {
    use super::{identity_digest, observe_identity, record_identity, record_value, PARTITIONS};

    #[test]
    fn record_identity_roundtrips_with_short_payload() {
        let value = record_value(b"x", 2, 41);
        assert_eq!(
            record_identity(&value).expect("identity must decode"),
            (2, 41)
        );
        assert_eq!(value.len(), 12);
    }

    #[test]
    fn identity_observer_counts_gaps_and_duplicates() {
        let mut next = 0;
        let mut duplicates = 0;
        let mut losses = 0;
        observe_identity(&mut next, 0, &mut duplicates, &mut losses);
        observe_identity(&mut next, 2, &mut duplicates, &mut losses);
        observe_identity(&mut next, 1, &mut duplicates, &mut losses);
        assert_eq!(next, 3);
        assert_eq!(losses, 1);
        assert_eq!(duplicates, 1);
    }

    #[test]
    fn identity_digest_is_stable_for_partition_counters() {
        let first = [3_u64; PARTITIONS];
        let second = [3_u64; PARTITIONS];
        assert_eq!(identity_digest(&first), identity_digest(&second));
        assert_ne!(
            identity_digest(&first),
            identity_digest(&[4_u64; PARTITIONS])
        );
    }
}
