use std::{
    cmp::Ordering,
    env, fs,
    time::{Duration, Instant},
};

use kafrust::{
    Acks, ClientMetrics, ConsumerConfig, Error, ProducerConfig, ProducerRecord, SecurityProtocol,
};
use sha2::{Digest, Sha256};

const PARTITIONS: usize = 3;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const RECOVERY_BACKOFF: Duration = Duration::from_millis(100);
const ERROR_REPORT_INTERVAL: Duration = Duration::from_secs(10);

struct SecuritySettings {
    server_name: String,
    root_der: Vec<u8>,
    username: String,
    password: String,
}

impl SecuritySettings {
    fn from_env() -> kafrust::Result<Self> {
        if env::var("KAFRUST_SECURITY_PROTOCOL").as_deref() != Ok("sasl_tls")
            || env::var("KAFRUST_SASL_MECHANISM").as_deref() != Ok("scram-sha-256")
        {
            return Err(Error::Unsupported(
                "published secure soak requires sasl_tls and scram-sha-256",
            ));
        }
        let root_path = required("KAFRUST_TLS_ROOT_CERT_DER_PATH")?;
        Ok(Self {
            server_name: required("KAFRUST_TLS_SERVER_NAME")?,
            root_der: fs::read(root_path)
                .map_err(|_| Error::Unsupported("TLS root certificate could not be read"))?,
            username: required("KAFRUST_SASL_USERNAME")?,
            password: required("KAFRUST_SASL_PASSWORD")?,
        })
    }

    fn producer(&self, config: ProducerConfig) -> ProducerConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.server_name.clone())
            .tls_root_certificate_der(self.root_der.clone())
            .sasl_scram_sha_256(self.username.clone(), self.password.clone())
    }

    fn consumer(&self, config: ConsumerConfig) -> ConsumerConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.server_name.clone())
            .tls_root_certificate_der(self.root_der.clone())
            .sasl_scram_sha_256(self.username.clone(), self.password.clone())
    }
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let security = SecuritySettings::from_env()?;
    let duration = Duration::from_secs(u64_from_env("KAFRUST_SOAK_SECONDS", 120)?);
    let batch_size = usize_from_env("KAFRUST_SOAK_BATCH_SIZE", 100)?.max(1);
    let payload = vec![b'x'; usize_from_env("KAFRUST_SOAK_PAYLOAD_BYTES", 1024)?];
    let metrics = ClientMetrics::new();
    let server_list = servers.split(',').map(str::to_owned).collect::<Vec<_>>();

    let mut producer = security
        .producer(
            ProducerConfig::new(server_list.clone())
                .client_id("kafrust-published-secure-multi-soak-producer")
                .metrics(metrics.clone())
                .request_timeout_ms(5_000)
                // The failover gate must only count records that survived
                // replication; leader-only acknowledgements can be lost when
                // the leader and a follower stop together.
                .acks(Acks::All)
                // Preserve producer identity across retryable failover errors
                // instead of turning an ambiguous batch into an untracked gap.
                .enable_idempotence(true)
                .max_retries(5)
                .max_records_per_batch(batch_size)
                .max_batch_bytes(900 * 1024),
        )
        .build()
        .await?;
    let mut consumer = security
        .consumer(
            ConsumerConfig::new(server_list)
                .client_id("kafrust-published-secure-multi-soak-consumer")
                .metrics(metrics.clone())
                .request_timeout_ms(5_000)
                .max_wait_ms(100)
                .max_retries(5)
                .max_partition_bytes(16 * 1024 * 1024),
        )
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
    let mut recovered = false;
    let mut last_progress = Instant::now();
    let mut last_error_report: Option<Instant> = None;

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
                    .map_err(|_| Error::Unsupported("secure soak batch length is too large"))?;
                attempted_records = attempted_records.saturating_add(batch_len);
                batch_records
            };
            let batch_len = u64::try_from(batch_records.len())
                .map_err(|_| Error::Unsupported("secure soak batch length is too large"))?;
            let metadata = match producer.send_batch(batch_records.iter().cloned()).await {
                Ok(metadata) => {
                    next_produce_sequences[partition] =
                        next_produce_sequences[partition].saturating_add(batch_len);
                    acknowledged_records = acknowledged_records.saturating_add(
                        u64::try_from(metadata.len()).map_err(|_| {
                            Error::Unsupported("secure soak metadata length is too large")
                        })?,
                    );
                    if saw_error {
                        recovered = true;
                    }
                    metadata
                }
                Err(error) => {
                    let should_report = last_error_report
                        .map(|last| last.elapsed() >= ERROR_REPORT_INTERVAL)
                        .unwrap_or(true);
                    if should_report {
                        eprintln!("secure multi-soak produce failed: {error}");
                        last_error_report = Some(Instant::now());
                    }
                    pending_batches[partition] = Some(batch_records);
                    operation_errors += 1;
                    unknown_outcomes = unknown_outcomes.saturating_add(batch_len);
                    saw_error = true;
                    tokio::time::sleep(RECOVERY_BACKOFF).await;
                    continue;
                }
            };
            let Some(first) = metadata.first() else {
                return Err(Error::Unsupported("secure soak returned no metadata"));
            };
            let batch_start = first.offset();
            if batch_start < 0 {
                return Err(Error::Unsupported("secure soak requires concrete offsets"));
            }
            let batch_len = i64::try_from(metadata.len())
                .map_err(|_| Error::Unsupported("secure soak batch length is too large"))?;
            let batch_end = batch_start
                .checked_add(batch_len)
                .ok_or(Error::Unsupported("secure soak batch offset overflow"))?;
            let mut fetch_offset = next_offset.unwrap_or(batch_start);
            *next_offset = Some(fetch_offset);
            let mut remaining = metadata.len();
            produced += remaining;
            while remaining > 0 {
                if Instant::now() >= hard_deadline {
                    return Err(Error::Unsupported(
                        "secure multi-soak consumer did not drain before the deadline",
                    ));
                }
                if last_progress.elapsed() >= Duration::from_secs(10) {
                    eprintln!("secure multi-soak progress: partition={partition} offset={fetch_offset} remaining={remaining} produced={produced} consumed={consumed}");
                    last_progress = Instant::now();
                }
                match consumer
                    .fetch(topic.clone(), partition as i32, fetch_offset)
                    .await
                {
                    Ok(records) if records.is_empty() => {
                        tokio::time::sleep(Duration::from_millis(10)).await
                    }
                    Ok(records) => {
                        if saw_error {
                            recovered = true;
                        }
                        for record in &records {
                            let (record_partition, sequence) =
                                record_identity(record.value().ok_or(Error::Unsupported(
                                    "secure soak record value was null",
                                ))?)?;
                            if record_partition != partition {
                                return Err(Error::Unsupported(
                                    "secure soak record partition identity diverged",
                                ));
                            }
                            observe_identity(
                                &mut next_consume_sequences[partition],
                                sequence,
                                &mut duplicate_count,
                                &mut loss_count,
                            );
                        }
                        if let Some(last) = records.last() {
                            fetch_offset = last.offset().saturating_add(1);
                            *next_offset = Some(fetch_offset);
                        }
                        // A failed Produce can be present on the surviving
                        // broker even though Kafka did not acknowledge it.
                        // Count only records in the acknowledged batch range.
                        let accepted = records
                            .iter()
                            .filter(|record| (batch_start..batch_end).contains(&record.offset()))
                            .count()
                            .min(remaining);
                        consumed += accepted;
                        remaining -= accepted;
                    }
                    Err(error) => {
                        let should_report = last_error_report
                            .map(|last| last.elapsed() >= ERROR_REPORT_INTERVAL)
                            .unwrap_or(true);
                        if should_report {
                            eprintln!("secure multi-soak fetch failed: {error}");
                            last_error_report = Some(Instant::now());
                        }
                        operation_errors += 1;
                        saw_error = true;
                        tokio::time::sleep(RECOVERY_BACKOFF).await;
                    }
                }
            }
        }
    }

    if pending_batches.iter().any(Option::is_some) {
        return Err(Error::Unsupported(
            "secure soak ended with unresolved produce outcomes",
        ));
    }

    let snapshot = metrics.snapshot();
    let produced_records = u64::try_from(produced)
        .map_err(|_| Error::Unsupported("secure soak record count is too large"))?;
    let consumed_records = u64::try_from(consumed)
        .map_err(|_| Error::Unsupported("secure soak consumed count is too large"))?;
    if produced != consumed
        || acknowledged_records != consumed_records
        || snapshot.produced_records != produced_records
        || snapshot.consumed_records != consumed_records
    {
        eprintln!(
            "secure multi-soak count mismatch: local produced={produced} consumed={consumed}; metrics produced={} consumed={}",
            snapshot.produced_records, snapshot.consumed_records
        );
        return Err(Error::Unsupported(
            "secure multi-soak record counts did not reconcile",
        ));
    }
    if snapshot.in_flight_requests != 0 || snapshot.buffered_records != 0 {
        return Err(Error::Unsupported(
            "secure multi-soak finished with non-zero gauges",
        ));
    }
    let recovered = recovered || snapshot.retries > 0;
    if snapshot.retries == 0 || !recovered {
        return Err(Error::Unsupported(
            "secure multi-soak did not observe retry and recovery",
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
                return Err(Error::Unsupported(
                    "secure soak observed an out-of-range record identity",
                ));
            }
            Ordering::Equal => {}
        }
    }
    if loss_count != 0 || duplicate_count != 0 {
        return Err(Error::Unsupported(
            "secure soak record identity reconciliation failed",
        ));
    }
    let expected_digest = identity_digest(&next_produce_sequences);
    let observed_digest = identity_digest(&next_consume_sequences);
    if expected_digest != observed_digest || attempted_records != consumed_records {
        return Err(Error::Unsupported(
            "secure soak record identity digest did not reconcile",
        ));
    }
    let duration_seconds = started.elapsed().as_secs_f64();
    let records_per_second = produced as f64 / duration_seconds.max(f64::EPSILON);
    let operation_error_rate_percent = operation_errors as f64 / (produced.max(1) as f64) * 100.0;
    let retry_ratio_percent = snapshot.retries as f64 / (produced.max(1) as f64) * 100.0;
    println!(
        concat!(
            "{{\"topic\":\"{}\",\"duration_seconds\":{:.3},",
            "\"partitions\":{},\"records\":{},\"attempted_records\":{},",
            "\"acknowledged_records\":{},\"consumed_unique_records\":{},",
            "\"records_per_second\":{:.3},\"payload_bytes\":{},",
            "\"operation_errors\":{},\"operation_error_rate_percent\":{:.6},",
            "\"recovered\":{},\"requests_started\":{},\"requests_failed\":{},",
            "\"retries\":{},\"retry_ratio_percent\":{:.6},",
            "\"unknown_outcomes\":{},\"in_flight_requests\":{},",
            "\"max_in_flight_requests\":{},\"buffered_records\":{},",
            "\"max_buffered_records\":{},\"record_id_reconciliation\":{{",
            "\"qualified\":true,\"unique_records\":{},\"loss_count\":{},",
            "\"duplicate_count\":{},\"digest\":\"{}\"}}}}"
        ),
        topic,
        duration_seconds,
        PARTITIONS,
        produced,
        attempted_records,
        acknowledged_records,
        consumed_records,
        records_per_second,
        payload.len(),
        operation_errors,
        operation_error_rate_percent,
        recovered,
        snapshot.requests_started,
        snapshot.requests_failed,
        snapshot.retries,
        retry_ratio_percent,
        unknown_outcomes,
        snapshot.in_flight_requests,
        snapshot.max_in_flight_requests,
        snapshot.buffered_records,
        snapshot.max_buffered_records,
        consumed_records,
        loss_count,
        duplicate_count,
        observed_digest
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
    let partition = value
        .get(..4)
        .ok_or(Error::Unsupported("secure soak identity is truncated"))?;
    let sequence = value
        .get(4..12)
        .ok_or(Error::Unsupported("secure soak identity is truncated"))?;
    let partition = i32::from_be_bytes(
        partition
            .try_into()
            .map_err(|_| Error::Unsupported("secure soak partition identity is invalid"))?,
    );
    let sequence = u64::from_be_bytes(
        sequence
            .try_into()
            .map_err(|_| Error::Unsupported("secure soak sequence identity is invalid"))?,
    );
    let partition = usize::try_from(partition)
        .map_err(|_| Error::Unsupported("secure soak partition identity is negative"))?;
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
    env::var(name).map_err(|_| Error::Unsupported("secure soak variable is missing"))
}

fn usize_from_env(name: &'static str, default: usize) -> kafrust::Result<usize> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| Error::Unsupported("secure soak size variable is invalid"))
    })
}

fn u64_from_env(name: &'static str, default: u64) -> kafrust::Result<u64> {
    env::var(name).ok().map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| Error::Unsupported("secure soak duration variable is invalid"))
    })
}

#[cfg(test)]
mod tests {
    use super::{identity_digest, observe_identity, record_identity, record_value, PARTITIONS};

    #[test]
    fn secure_record_identity_roundtrips() {
        let value = record_value(b"x", 1, 7);
        assert_eq!(
            record_identity(&value).expect("identity must decode"),
            (1, 7)
        );
        assert_eq!(value.len(), 12);
    }

    #[test]
    fn secure_identity_observer_counts_anomaly() {
        let mut next = 0;
        let mut duplicates = 0;
        let mut losses = 0;
        observe_identity(&mut next, 0, &mut duplicates, &mut losses);
        observe_identity(&mut next, 2, &mut duplicates, &mut losses);
        observe_identity(&mut next, 1, &mut duplicates, &mut losses);
        assert_eq!((next, losses, duplicates), (3, 1, 1));
    }

    #[test]
    fn secure_identity_digest_is_deterministic() {
        assert_eq!(
            identity_digest(&[2_u64; PARTITIONS]),
            identity_digest(&[2_u64; PARTITIONS])
        );
        assert_ne!(
            identity_digest(&[2_u64; PARTITIONS]),
            identity_digest(&[3_u64; PARTITIONS])
        );
    }
}
