use std::collections::BTreeSet;
use std::time::Duration;
use std::{env, fs};

use kafrust::{
    Acks, ConsumerGroup, ConsumerGroupConfig, ConsumerGroupProtocol, Error, OffsetResetPolicy,
    ProducerConfig, ProducerRecord, SecurityProtocol,
};

const PARTITION_COUNT: i32 = 6;
const POLL_ATTEMPTS: usize = 80;

struct SecuritySettings {
    tls_server_name: String,
    tls_root_certificate_der: Vec<u8>,
    sasl_username: String,
    sasl_password: String,
}

impl SecuritySettings {
    fn from_env() -> kafrust::Result<Self> {
        if env::var("KAFRUST_SECURITY_PROTOCOL").as_deref() != Ok("sasl_tls") {
            return Err(Error::Unsupported(
                "published secure group smoke requires sasl_tls",
            ));
        }
        if env::var("KAFRUST_SASL_MECHANISM").as_deref() != Ok("scram-sha-256") {
            return Err(Error::Unsupported(
                "published secure group smoke requires scram-sha-256",
            ));
        }
        let certificate_path = env::var("KAFRUST_TLS_ROOT_CERT_DER_PATH")
            .map_err(|_| Error::Unsupported("KAFRUST_TLS_ROOT_CERT_DER_PATH is required"))?;
        Ok(Self {
            tls_server_name: env::var("KAFRUST_TLS_SERVER_NAME")
                .map_err(|_| Error::Unsupported("KAFRUST_TLS_SERVER_NAME is required"))?,
            tls_root_certificate_der: fs::read(certificate_path).map_err(|_| {
                Error::Unsupported("KAFRUST_TLS_ROOT_CERT_DER_PATH could not be read")
            })?,
            sasl_username: env::var("KAFRUST_SASL_USERNAME")
                .map_err(|_| Error::Unsupported("KAFRUST_SASL_USERNAME is required"))?,
            sasl_password: env::var("KAFRUST_SASL_PASSWORD")
                .map_err(|_| Error::Unsupported("KAFRUST_SASL_PASSWORD is required"))?,
        })
    }

    fn configure_producer(&self, config: ProducerConfig) -> ProducerConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.tls_server_name.clone())
            .tls_root_certificate_der(self.tls_root_certificate_der.clone())
            .sasl_scram_sha_256(self.sasl_username.clone(), self.sasl_password.clone())
    }

    fn configure_group(&self, config: ConsumerGroupConfig) -> ConsumerGroupConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.tls_server_name.clone())
            .tls_root_certificate_der(self.tls_root_certificate_der.clone())
            .sasl_scram_sha_256(self.sasl_username.clone(), self.sasl_password.clone())
    }
}

fn required(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| Error::Unsupported("published secure group smoke variable missing"))
}

fn group_protocol() -> Result<ConsumerGroupProtocol, Error> {
    match env::var("KAFRUST_GROUP_PROTOCOL")
        .unwrap_or_else(|_| "classic".to_owned())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "classic" => Ok(ConsumerGroupProtocol::Classic),
        "consumer" | "kip-848" => Ok(ConsumerGroupProtocol::Consumer),
        _ => Err(Error::Unsupported(
            "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
        )),
    }
}

fn group_config(
    bootstrap_servers: &str,
    group_id: &str,
    protocol: ConsumerGroupProtocol,
    security: &SecuritySettings,
) -> ConsumerGroupConfig {
    security.configure_group(
        ConsumerGroupConfig::new(
            bootstrap_servers.split(',').map(str::to_owned),
            group_id.to_owned(),
        )
        .group_protocol(protocol)
        .session_timeout_ms(6_000)
        .rebalance_timeout_ms(10_000)
        .max_wait_ms(100)
        .max_retries(20)
        .max_poll_records(20)
        .offset_reset_policy(OffsetResetPolicy::Earliest),
    )
}

async fn run() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let group_id = required("KAFRUST_GROUP_ID")?;
    let protocol = group_protocol()?;
    let security = SecuritySettings::from_env()?;

    let mut producer = security
        .configure_producer(
            ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-group-rebalance-producer")
                .acks(Acks::Leader)
                .enable_idempotence(true),
        )
        .build()
        .await?;
    for partition in 0..PARTITION_COUNT {
        let value = format!("published-secure-group-rebalance-{partition}");
        producer
            .send(
                ProducerRecord::to(topic.clone())
                    .partition(partition)
                    .value(value.into_bytes()),
            )
            .await?;
    }

    let config =
        group_config(&bootstrap_servers, &group_id, protocol, &security).subscribe(topic.clone());
    let mut first = config
        .clone()
        .client_id("kafrust-published-secure-group-rebalance-first")
        .join()
        .await?;
    if first.assignments().is_empty() {
        return Err(Error::Unsupported(
            "published secure group smoke first member received no partitions",
        ));
    }

    let mut seen_records = BTreeSet::new();
    let second_join = tokio::spawn(
        config
            .client_id("kafrust-published-secure-group-rebalance-second")
            .join(),
    );
    while !second_join.is_finished() {
        record_expected_records(&mut seen_records, &topic, first.poll().await?);
    }
    let mut second = second_join.await.map_err(|_| {
        Error::Unsupported("published secure group smoke second member task failed")
    })??;

    wait_for_two_member_coverage(&mut first, &mut second, &topic, seen_records).await?;
    println!(
        "published secure group rebalance passed protocol={protocol:?} first={} second={} partitions={PARTITION_COUNT}",
        first.member_id(),
        second.member_id(),
    );
    first.leave().await?;
    second.leave().await?;
    Ok(())
}

async fn wait_for_two_member_coverage(
    first: &mut ConsumerGroup,
    second: &mut ConsumerGroup,
    topic: &str,
    mut seen_records: BTreeSet<(String, i32)>,
) -> kafrust::Result<()> {
    let expected: BTreeSet<_> = (0..PARTITION_COUNT)
        .map(|partition| (topic.to_owned(), partition))
        .collect();
    for _ in 0..POLL_ATTEMPTS {
        let (first_records, second_records) = poll_pair(first, second).await?;
        record_expected_records(&mut seen_records, topic, first_records);
        record_expected_records(&mut seen_records, topic, second_records);

        let first_partitions = assignment_keys(first);
        let second_partitions = assignment_keys(second);
        if !first_partitions.is_empty()
            && !second_partitions.is_empty()
            && first_partitions.is_disjoint(&second_partitions)
            && first_partitions
                .union(&second_partitions)
                .cloned()
                .collect::<BTreeSet<_>>()
                == expected
            && seen_records == expected
        {
            return Ok(());
        }
    }

    eprintln!(
        "published secure group smoke final state: first_assignments={:?} second_assignments={:?} seen_records={seen_records:?}",
        assignment_keys(first),
        assignment_keys(second),
    );
    Err(Error::Unsupported(
        "published secure group smoke members did not converge on disjoint ownership and records",
    ))
}

fn record_expected_records(
    seen_records: &mut BTreeSet<(String, i32)>,
    topic: &str,
    records: impl IntoIterator<Item = kafrust::ConsumerRecord>,
) {
    for record in records {
        if record.topic() == topic
            && record.value().is_some_and(|value| {
                value
                    == format!("published-secure-group-rebalance-{}", record.partition()).as_bytes()
            })
        {
            seen_records.insert((record.topic().to_owned(), record.partition()));
        }
    }
}

async fn poll_pair(
    first: &mut ConsumerGroup,
    second: &mut ConsumerGroup,
) -> kafrust::Result<(Vec<kafrust::ConsumerRecord>, Vec<kafrust::ConsumerRecord>)> {
    let (first_result, second_result) = tokio::join!(first.poll(), second.poll());
    Ok((first_result?, second_result?))
}

fn assignment_keys(group: &ConsumerGroup) -> BTreeSet<(String, i32)> {
    group
        .assignments()
        .iter()
        .map(|assignment| (assignment.topic().to_owned(), assignment.partition()))
        .collect()
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .map_err(|_| Error::Unsupported("published secure group smoke timed out"))?
}
