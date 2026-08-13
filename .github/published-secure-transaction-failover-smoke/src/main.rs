use std::env;
use std::fs;
use std::io::{self, Write};
use std::time::Duration;

use kafrust::{
    ClientConfig, ConsumerConfig, Error, IsolationLevel, ProducerConfig, ProducerRecord,
    SecurityProtocol,
};

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
                "published secure transaction smoke requires sasl_tls",
            ));
        }
        if env::var("KAFRUST_SASL_MECHANISM").as_deref() != Ok("scram-sha-256") {
            return Err(Error::Unsupported(
                "published secure transaction smoke requires scram-sha-256",
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

    fn configure_consumer(&self, config: ConsumerConfig) -> ConsumerConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.tls_server_name.clone())
            .tls_root_certificate_der(self.tls_root_certificate_der.clone())
            .sasl_scram_sha_256(self.sasl_username.clone(), self.sasl_password.clone())
    }
}

fn required(name: &str) -> Result<String, Error> {
    env::var(name)
        .map_err(|_| Error::Unsupported("published secure transaction smoke variable missing"))
}

fn max_retries() -> Result<u32, Error> {
    env::var("KAFRUST_MAX_RETRIES")
        .unwrap_or_else(|_| "300".to_owned())
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_MAX_RETRIES must be an integer"))
}

fn pause() -> Result<Duration, Error> {
    env::var("KAFRUST_FAILOVER_PAUSE_MS")
        .unwrap_or_else(|_| "0".to_owned())
        .parse::<u64>()
        .map(Duration::from_millis)
        .map_err(|_| Error::Unsupported("KAFRUST_FAILOVER_PAUSE_MS must be milliseconds"))
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let transactional_id = required("KAFRUST_TRANSACTIONAL_ID")?;
    let max_retries = max_retries()?;
    let security = SecuritySettings::from_env()?;

    let mut producer = security
        .configure_producer(
            ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-transaction-failover-producer")
                .transactional_id(transactional_id.clone())
                .max_retries(max_retries),
        )
        .build()
        .await?;
    let coordinator_client = security.configure_client(
        ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
            .client_id("kafrust-published-secure-transaction-failover-coordinator"),
    );
    let coordinator = coordinator_client
        .connect()
        .await?
        .find_transaction_coordinator(transactional_id)
        .await?;
    if coordinator.error_code != 0 {
        return Err(Error::Broker {
            code: coordinator.error_code,
            context: "find published secure transaction coordinator".to_owned(),
        });
    }

    producer.begin_transaction()?;
    let produced = producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(b"published secure transaction coordinator failover"),
        )
        .await?;
    println!("transaction coordinator node {}", coordinator.node_id);
    io::stdout().flush().map_err(Error::Io)?;

    tokio::time::sleep(pause()?).await;
    producer.commit_transaction().await?;

    let mut consumer = security
        .configure_consumer(
            ConsumerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-transaction-failover-consumer")
                .isolation_level(IsolationLevel::ReadCommitted),
        )
        .build()
        .await?;
    let records = consumer
        .fetch(&topic, produced.partition(), produced.offset())
        .await?;
    if !records.iter().any(|record| {
        record.topic() == topic
            && record.partition() == produced.partition()
            && record.offset() == produced.offset()
            && record.value() == Some(b"published secure transaction coordinator failover")
    }) {
        return Err(Error::Unsupported(
            "published secure read_committed did not return the failover transaction",
        ));
    }

    println!(
        "published secure transaction failover committed and read_committed verified at {}-{}@{}",
        produced.topic(),
        produced.partition(),
        produced.offset()
    );
    Ok(())
}
