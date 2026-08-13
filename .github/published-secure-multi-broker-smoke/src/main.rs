use std::{env, fs};

use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerGroupConfig, ConsumerGroupProtocol, Error,
    OffsetResetPolicy, ProducerConfig, ProducerRecord, SecurityProtocol,
};

struct SecuritySettings {
    tls_server_name: String,
    tls_root_certificate_der: Vec<u8>,
    sasl_username: String,
    sasl_password: String,
}

impl SecuritySettings {
    fn from_env() -> kafrust::Result<Self> {
        let protocol = env::var("KAFRUST_SECURITY_PROTOCOL")
            .map_err(|_| Error::Unsupported("KAFRUST_SECURITY_PROTOCOL is required"))?;
        if protocol != "sasl_tls" {
            return Err(Error::Unsupported(
                "published secure multi-broker smoke requires sasl_tls",
            ));
        }

        let mechanism = env::var("KAFRUST_SASL_MECHANISM")
            .map_err(|_| Error::Unsupported("KAFRUST_SASL_MECHANISM is required"))?;
        if mechanism != "scram-sha-256" {
            return Err(Error::Unsupported(
                "published secure multi-broker smoke requires scram-sha-256",
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

    fn configure_client(&self, config: ClientConfig) -> ClientConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.tls_server_name.clone())
            .tls_root_certificate_der(self.tls_root_certificate_der.clone())
            .sasl_scram_sha_256(self.sasl_username.clone(), self.sasl_password.clone())
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
    env::var(name)
        .map_err(|_| Error::Unsupported("published secure multi-broker smoke variable missing"))
}

fn required_i32(name: &str) -> Result<i32, Error> {
    required(name)?.parse().map_err(|_| {
        Error::Unsupported("published secure multi-broker smoke variable was not an integer")
    })
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

fn producer_config(bootstrap_servers: &str, security: &SecuritySettings) -> ProducerConfig {
    security.configure_producer(
        ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
            .client_id("kafrust-published-secure-multi-producer")
            .acks(Acks::Leader)
            .enable_idempotence(true),
    )
}

fn group_config(
    bootstrap_servers: &str,
    group_id: &str,
    security: &SecuritySettings,
) -> Result<ConsumerGroupConfig, Error> {
    Ok(security.configure_group(
        ConsumerGroupConfig::new(
            bootstrap_servers.split(',').map(str::to_owned),
            group_id.to_owned(),
        )
        .client_id("kafrust-published-secure-multi-group")
        .group_protocol(group_protocol()?)
        .max_retries(20)
        .max_poll_records(20)
        .offset_reset_policy(OffsetResetPolicy::Earliest),
    ))
}

async fn run_pre(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    partition: i32,
    value: &str,
    security: &SecuritySettings,
) -> kafrust::Result<()> {
    let mut producer = producer_config(bootstrap_servers, security).build().await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(value.as_bytes()),
        )
        .await?;

    let mut group = group_config(bootstrap_servers, group_id, security)?
        .subscribe(topic.to_owned())
        .join()
        .await?;
    let mut found = false;
    for _ in 0..30 {
        let records = group.poll().await?;
        if let Some(record) = records.iter().find(|record| {
            record.topic() == topic
                && record.partition() == metadata.partition()
                && record.offset() == metadata.offset()
                && record.value() == Some(value.as_bytes())
        }) {
            group.commit_record(record)?;
            found = true;
            break;
        }
    }
    if !found {
        return Err(Error::Unsupported(
            "published secure multi-broker group did not read the pre-failover record",
        ));
    }
    group.commit_queued_offsets().await?;
    let coordinator = AdminClient::new(
        security.configure_client(
            ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-multi-coordinator-admin"),
        ),
    )
    .list_groups()
    .await?
    .into_iter()
    .find(|listing| listing.group_id() == group_id)
    .ok_or(Error::Unsupported(
        "published secure multi-broker group coordinator was not listed",
    ))?;
    println!(
        "published secure multi-broker group coordinator node {}",
        coordinator.coordinator_id()
    );
    group.leave().await?;
    println!(
        "published secure multi-broker pre-failover committed {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}

async fn run_post(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    partition: i32,
    value: &str,
    security: &SecuritySettings,
) -> kafrust::Result<()> {
    let mut producer = producer_config(bootstrap_servers, security).build().await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(value.as_bytes()),
        )
        .await?;

    let mut group = group_config(bootstrap_servers, group_id, security)?
        .subscribe(topic.to_owned())
        .join()
        .await?;
    let mut found = false;
    for _ in 0..30 {
        let records = group.poll().await?;
        if records.iter().any(|record| {
            record.topic() == topic
                && record.partition() == metadata.partition()
                && record.offset() == metadata.offset()
                && record.value() == Some(value.as_bytes())
        }) {
            found = true;
            break;
        }
    }
    group.leave().await?;
    if !found {
        return Err(Error::Unsupported(
            "published secure multi-broker group did not resume on the replacement leader",
        ));
    }
    println!(
        "published secure multi-broker post-failover resumed {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let group_id = required("KAFRUST_GROUP_ID")?;
    let partition = required_i32("KAFRUST_PARTITION")?;
    let phase = required("KAFRUST_PHASE")?;
    let value = required("KAFRUST_VALUE")?;
    let security = SecuritySettings::from_env()?;

    let cluster = AdminClient::new(
        security.configure_client(
            ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-multi-admin"),
        ),
    );
    let brokers = cluster.describe_cluster().await?;
    let expected_broker_count = if phase == "pre" { 3 } else { 2 };
    if brokers.brokers().len() < expected_broker_count {
        return Err(Error::Unsupported(
            "published secure multi-broker smoke did not observe the expected live brokers",
        ));
    }

    match phase.as_str() {
        "pre" => {
            run_pre(
                &bootstrap_servers,
                &topic,
                &group_id,
                partition,
                &value,
                &security,
            )
            .await
        }
        "post" => {
            run_post(
                &bootstrap_servers,
                &topic,
                &group_id,
                partition,
                &value,
                &security,
            )
            .await
        }
        _ => Err(Error::Unsupported(
            "published secure multi-broker smoke phase was invalid",
        )),
    }
}
