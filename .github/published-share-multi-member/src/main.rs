use kafrust::{
    ClientMetrics, ProducerConfig, ProducerRecord, SecurityProtocol, ShareAcknowledgementType,
    ShareAcquireMode, ShareConsumerConfig,
};
use std::collections::BTreeSet;
use std::io::Write;
use std::time::{Duration, Instant};

const PARTITION_COUNT: i32 = 6;

fn required_env(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| kafrust::Error::InvalidConfiguration {
        field: name,
        reason: "published Share multi-member environment variable is required",
    })
}

fn bootstrap_servers() -> kafrust::Result<Vec<String>> {
    let value = required_env("KAFRUST_BOOTSTRAP_SERVERS")?;
    let servers = value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_BOOTSTRAP_SERVERS",
            reason: "at least one bootstrap server is required",
        });
    }
    Ok(servers)
}

fn tls_root_certificate_der() -> kafrust::Result<Option<Vec<u8>>> {
    let Some(path) = std::env::var_os("KAFRUST_TLS_ROOT_CERT_DER_PATH") else {
        return Ok(None);
    };
    let certificate = std::fs::read(path).map_err(kafrust::Error::Io)?;
    if certificate.is_empty() {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_TLS_ROOT_CERT_DER_PATH",
            reason: "TLS CA certificate must not be empty",
        });
    }
    Ok(Some(certificate))
}

fn apply_security(mut config: ProducerConfig, ca: Option<&[u8]>) -> ProducerConfig {
    if let Some(ca) = ca {
        config = config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name("localhost")
            .tls_root_certificate_der(ca.to_vec())
            .sasl_scram_sha_256("kafrust", "kafrust-secret");
    }
    config
}

fn apply_share_security(mut config: ShareConsumerConfig, ca: Option<&[u8]>) -> ShareConsumerConfig {
    if let Some(ca) = ca {
        config = config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name("localhost")
            .tls_root_certificate_der(ca.to_vec())
            .sasl_scram_sha_256("kafrust", "kafrust-secret");
    }
    config
}

fn records_per_partition() -> kafrust::Result<i32> {
    let value =
        std::env::var("KAFRUST_SHARE_RECORDS_PER_PARTITION").unwrap_or_else(|_| "1".to_owned());
    let records = value
        .parse::<i32>()
        .map_err(|_| kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_RECORDS_PER_PARTITION",
            reason: "published Share record count must be a positive integer",
        })?;
    if records <= 0 {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_RECORDS_PER_PARTITION",
            reason: "published Share record count must be a positive integer",
        });
    }
    Ok(records)
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    match required_env("KAFRUST_OPERATION")?.as_str() {
        "seed" => seed().await,
        "member" => member().await,
        _ => Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_OPERATION",
            reason: "published Share multi-member operation must be seed or member",
        }),
    }
}

async fn seed() -> kafrust::Result<()> {
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let prefix = required_env("KAFRUST_SHARE_VALUE_PREFIX")?;
    let records_per_partition = records_per_partition()?;
    let ca = tls_root_certificate_der()?;
    let mut producer = apply_security(
        ProducerConfig::new(bootstrap_servers()?)
            .client_id("kafrust-published-share-multi-member-seeder"),
        ca.as_deref(),
    )
    .build()
    .await?;
    for partition in 0..PARTITION_COUNT {
        for _ in 0..records_per_partition {
            let value = format!("{prefix}{partition}");
            let metadata = producer
                .send(
                    ProducerRecord::to(topic.clone())
                        .partition(partition)
                        .value(value.into_bytes()),
                )
                .await?;
            if metadata.partition() != partition {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_VALUE_PREFIX",
                    reason: "published Share seeder returned a different partition",
                });
            }
        }
    }
    println!(
        "published Share multi-member seeded partitions={PARTITION_COUNT} records_per_partition={records_per_partition}"
    );
    Ok(())
}

fn run_seconds() -> kafrust::Result<u64> {
    let value = required_env("KAFRUST_SHARE_MEMBER_RUN_SECONDS")?;
    let seconds = value
        .parse::<u64>()
        .map_err(|_| kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_MEMBER_RUN_SECONDS",
            reason: "published Share member run seconds must be a positive integer",
        })?;
    if seconds == 0 {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_MEMBER_RUN_SECONDS",
            reason: "published Share member run seconds must be a positive integer",
        });
    }
    Ok(seconds)
}

async fn member() -> kafrust::Result<()> {
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let group_id = required_env("KAFRUST_SHARE_GROUP_ID")?;
    let member_id = required_env("KAFRUST_SHARE_MEMBER_ID")?;
    let value_prefix = required_env("KAFRUST_SHARE_VALUE_PREFIX")?;
    let ready_path = required_env("KAFRUST_SHARE_MEMBER_READY_FILE")?;
    let start_path = required_env("KAFRUST_SHARE_MEMBER_START_FILE")?;
    let output_path = required_env("KAFRUST_SHARE_MEMBER_OUTPUT_FILE")?;
    let heartbeat_ready_path = std::env::var("KAFRUST_SHARE_MEMBER_HEARTBEAT_READY_FILE").ok();
    let run_seconds = run_seconds()?;
    let metrics = ClientMetrics::new();
    let ca = tls_root_certificate_der()?;

    let mut consumer = apply_share_security(
        ShareConsumerConfig::new(bootstrap_servers()?, group_id)
            .client_id(member_id.clone())
            .subscribe(topic.clone())
            .max_wait_ms(100)
            .max_records(1)
            .batch_size(1)
            .max_retries(10)
            .acquire_mode(ShareAcquireMode::RecordLimit)
            .metrics(metrics.clone()),
        ca.as_deref(),
    )
    .build()
    .await?;
    std::fs::write(&ready_path, b"joined\n").map_err(kafrust::Error::Io)?;

    wait_for_start(&start_path).await?;
    consumer
        .spawn_heartbeat_task(Duration::from_millis(500))
        .await?;
    if let Some(path) = heartbeat_ready_path {
        std::fs::write(path, b"heartbeat-started\n").map_err(kafrust::Error::Io)?;
    }
    let mut output = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&output_path)
        .map_err(kafrust::Error::Io)?;
    let deadline = Instant::now() + Duration::from_secs(run_seconds);
    let mut accepted = BTreeSet::new();

    while Instant::now() < deadline {
        let records = consumer.poll().await?;
        let received_records = !records.is_empty();
        for record in records {
            let Some(value) = record.value() else {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_VALUE_PREFIX",
                    reason: "published Share member received a null value",
                });
            };
            let expected = format!("{value_prefix}{}", record.partition());
            if value != expected.as_bytes() {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_VALUE_PREFIX",
                    reason: "published Share member received an unexpected value",
                });
            }
            let key = (record.partition(), record.offset());
            if !accepted.insert(key) {
                return Err(kafrust::Error::InvalidConfiguration {
                    field: "KAFRUST_SHARE_MEMBER_OUTPUT_FILE",
                    reason: "published Share member received a duplicate record",
                });
            }
            consumer.acknowledge(&record, ShareAcknowledgementType::Accept)?;
            consumer.commit().await?;
            writeln!(
                output,
                "{member_id},{},{}",
                record.partition(),
                record.offset()
            )
            .map_err(kafrust::Error::Io)?;
        }
        if !received_records {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    if accepted.is_empty() {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_MEMBER_ID",
            reason: "published Share member received no records",
        });
    }
    let assignment_count = consumer.assignment_count();
    consumer.stop_heartbeat_task().await?;
    consumer.close().await?;
    let snapshot = metrics.snapshot();
    if snapshot.consumed_records != accepted.len() as u64 {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_MEMBER_OUTPUT_FILE",
            reason: "published Share metrics consumed count differs from accepted records",
        });
    }
    if snapshot.in_flight_requests != 0 || snapshot.buffered_records != 0 {
        return Err(kafrust::Error::InvalidConfiguration {
            field: "KAFRUST_SHARE_MEMBER_OUTPUT_FILE",
            reason: "published Share member closed with outstanding client resources",
        });
    }
    println!(
        "published Share multi-member member={} assignment={} accepted={} consumed={} in_flight={} buffered={} failed={} retries={} max_in_flight={} max_buffered={}",
        member_id,
        assignment_count,
        accepted.len(),
        snapshot.consumed_records,
        snapshot.in_flight_requests,
        snapshot.buffered_records,
        snapshot.requests_failed,
        snapshot.retries,
        snapshot.max_in_flight_requests,
        snapshot.max_buffered_records,
    );
    Ok(())
}

async fn wait_for_start(path: &str) -> kafrust::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(90);
    while !std::path::Path::new(path).exists() {
        if Instant::now() >= deadline {
            return Err(kafrust::Error::RequestTimedOut { timeout_ms: 90_000 });
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}
