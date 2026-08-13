use std::{
    env, fs,
    time::{Duration, Instant},
};

use kafrust::{
    Acks, ClientMetrics, ConsumerConfig, Error, ProducerConfig, ProducerRecord, SecurityProtocol,
};

const PARTITIONS: usize = 3;
const DRAIN_TIMEOUT: Duration = Duration::from_secs(300);
const RECOVERY_BACKOFF: Duration = Duration::from_millis(100);

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
                .acks(Acks::Leader)
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
    let mut produced = 0usize;
    let mut consumed = 0usize;
    let mut operation_errors = 0usize;
    let mut saw_error = false;
    let mut recovered = false;
    let mut last_progress = Instant::now();

    while Instant::now() < deadline {
        for (partition, next_offset) in next_offsets.iter_mut().enumerate() {
            let metadata = match producer
                .send_batch(records(&topic, &payload, batch_size, partition as i32))
                .await
            {
                Ok(metadata) => {
                    if saw_error {
                        recovered = true;
                    }
                    metadata
                }
                Err(error) => {
                    eprintln!("secure multi-soak produce failed: {error}");
                    operation_errors += 1;
                    saw_error = true;
                    tokio::time::sleep(RECOVERY_BACKOFF).await;
                    continue;
                }
            };
            let Some(first) = metadata.first() else {
                return Err(Error::Unsupported("secure soak returned no metadata"));
            };
            let mut fetch_offset = next_offset.unwrap_or(first.offset());
            if fetch_offset < 0 {
                return Err(Error::Unsupported("secure soak requires concrete offsets"));
            }
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
                        let accepted = records.len().min(remaining);
                        if let Some(last) = records.get(accepted.saturating_sub(1)) {
                            fetch_offset = last.offset().saturating_add(1);
                            *next_offset = Some(fetch_offset);
                        }
                        consumed += accepted;
                        remaining -= accepted;
                    }
                    Err(error) => {
                        eprintln!("secure multi-soak fetch failed: {error}");
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
    println!("{{\"topic\":\"{}\",\"duration_seconds\":{:.3},\"partitions\":{},\"records\":{},\"payload_bytes\":{},\"operation_errors\":{},\"recovered\":{},\"requests_started\":{},\"requests_failed\":{},\"retries\":{},\"in_flight_requests\":{},\"buffered_records\":{}}}", topic, started.elapsed().as_secs_f64(), PARTITIONS, produced, payload.len(), operation_errors, recovered, snapshot.requests_started, snapshot.requests_failed, snapshot.retries, snapshot.in_flight_requests, snapshot.buffered_records);
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
