use std::{env, fs};

use kafrust::{
    Acks, AdminClient, ClientConfig, Compression, ConsumerConfig, ConsumerGroupConfig,
    ConsumerGroupProtocol, CreateTopicsOptions, DeleteTopicsOptions, DescribeConfigsOptions, Error,
    IsolationLevel, NewTopic, OffsetResetPolicy, ProducerConfig, ProducerRecord, SecurityProtocol,
    TopicConfigResource,
};

struct SecuritySettings {
    protocol: SecurityProtocol,
    tls_server_name: Option<String>,
    tls_root_certificate_der: Option<Vec<u8>>,
    sasl_mechanism: Option<String>,
    sasl_username: Option<String>,
    sasl_password: Option<String>,
}

impl SecuritySettings {
    fn from_env() -> kafrust::Result<Self> {
        let protocol_name =
            env::var("KAFRUST_SECURITY_PROTOCOL").unwrap_or_else(|_| "plaintext".to_owned());
        let protocol = match protocol_name.as_str() {
            "plaintext" => SecurityProtocol::Plaintext,
            "tls" => SecurityProtocol::Tls,
            "sasl_plaintext" => SecurityProtocol::SaslPlaintext,
            "sasl_tls" => SecurityProtocol::SaslTls,
            _ => {
                return Err(Error::Unsupported(
                    "KAFRUST_SECURITY_PROTOCOL must be plaintext, tls, sasl_plaintext, or sasl_tls",
                ));
            }
        };

        let tls_server_name = match protocol {
            SecurityProtocol::Tls | SecurityProtocol::SaslTls => Some(
                env::var("KAFRUST_TLS_SERVER_NAME")
                    .map_err(|_| Error::Unsupported("KAFRUST_TLS_SERVER_NAME is required"))?,
            ),
            _ => None,
        };
        let tls_root_certificate_der = match protocol {
            SecurityProtocol::Tls | SecurityProtocol::SaslTls => {
                let path = env::var("KAFRUST_TLS_ROOT_CERT_DER_PATH").map_err(|_| {
                    Error::Unsupported("KAFRUST_TLS_ROOT_CERT_DER_PATH is required")
                })?;
                Some(fs::read(path).map_err(|_| {
                    Error::Unsupported("KAFRUST_TLS_ROOT_CERT_DER_PATH could not be read")
                })?)
            }
            _ => None,
        };

        let sasl_mechanism = match protocol {
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslTls => Some(
                env::var("KAFRUST_SASL_MECHANISM")
                    .map_err(|_| Error::Unsupported("KAFRUST_SASL_MECHANISM is required"))?,
            ),
            _ => None,
        };
        if let Some(mechanism) = sasl_mechanism.as_deref() {
            if !matches!(mechanism, "plain" | "scram-sha-256" | "scram-sha-512") {
                return Err(Error::Unsupported(
                    "KAFRUST_SASL_MECHANISM must be plain, scram-sha-256, or scram-sha-512",
                ));
            }
        }
        let (sasl_username, sasl_password) = match protocol {
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslTls => (
                Some(
                    env::var("KAFRUST_SASL_USERNAME")
                        .map_err(|_| Error::Unsupported("KAFRUST_SASL_USERNAME is required"))?,
                ),
                Some(
                    env::var("KAFRUST_SASL_PASSWORD")
                        .map_err(|_| Error::Unsupported("KAFRUST_SASL_PASSWORD is required"))?,
                ),
            ),
            _ => (None, None),
        };

        Ok(Self {
            protocol,
            tls_server_name,
            tls_root_certificate_der,
            sasl_mechanism,
            sasl_username,
            sasl_password,
        })
    }
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let topic =
        env::var("KAFRUST_TOPIC").map_err(|_| Error::Unsupported("KAFRUST_TOPIC is required"))?;
    let admin_topic = env::var("KAFRUST_ADMIN_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_TOPIC is required"))?;
    let transaction_topic = env::var("KAFRUST_TRANSACTION_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_TRANSACTION_TOPIC is required"))?;
    let group_id = env::var("KAFRUST_GROUP_ID")
        .map_err(|_| Error::Unsupported("KAFRUST_GROUP_ID is required"))?;
    let transactional_id = env::var("KAFRUST_TRANSACTIONAL_ID")
        .map_err(|_| Error::Unsupported("KAFRUST_TRANSACTIONAL_ID is required"))?;
    let value =
        env::var("KAFRUST_VALUE").map_err(|_| Error::Unsupported("KAFRUST_VALUE is required"))?;
    let compression = match env::var("KAFRUST_COMPRESSION").as_deref() {
        Ok("none") | Err(_) => Compression::None,
        Ok("gzip") => Compression::Gzip,
        Ok("snappy") => Compression::Snappy,
        Ok("lz4") => Compression::Lz4,
        Ok("zstd") => Compression::Zstd,
        Ok(_) => {
            return Err(Error::Unsupported(
                "KAFRUST_COMPRESSION must be none, gzip, snappy, lz4, or zstd",
            ));
        }
    };
    let security = SecuritySettings::from_env()?;
    let group_protocol = match env::var("KAFRUST_GROUP_PROTOCOL").as_deref() {
        Ok("classic") | Err(_) => ConsumerGroupProtocol::Classic,
        Ok("consumer") => ConsumerGroupProtocol::Consumer,
        Ok(_) => {
            return Err(Error::Unsupported(
                "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
            ));
        }
    };

    macro_rules! configure_security {
        ($config:expr) => {{
            let config = $config.security_protocol(security.protocol);
            let config = match security.tls_server_name.as_deref() {
                Some(server_name) => config.tls_server_name(server_name),
                None => config,
            };
            let config = match security.tls_root_certificate_der.as_deref() {
                Some(certificate) => config.tls_root_certificate_der(certificate.to_vec()),
                None => config,
            };
            match security.sasl_mechanism.as_deref() {
                Some("plain") | Some("scram-sha-256") | Some("scram-sha-512") => {
                    let username = security
                        .sasl_username
                        .as_deref()
                        .ok_or(Error::Unsupported("SASL username is missing"))?;
                    let password = security
                        .sasl_password
                        .as_deref()
                        .ok_or(Error::Unsupported("SASL password is missing"))?;
                    match security.sasl_mechanism.as_deref() {
                        Some("plain") => config.sasl_plain(username, password),
                        Some("scram-sha-256") => config.sasl_scram_sha_256(username, password),
                        Some("scram-sha-512") => config.sasl_scram_sha_512(username, password),
                        _ => config,
                    }
                }
                _ => config,
            }
        }};
    }

    let admin = AdminClient::new(configure_security!(ClientConfig::new([
        bootstrap_servers.clone()
    ])
    .client_id("kafrust-published-smoke-admin")));
    let cluster = admin.describe_cluster().await?;
    if cluster.brokers().is_empty() {
        return Err(Error::Unsupported(
            "published crate admin client returned no brokers",
        ));
    }

    let created_topics = admin
        .create_topics(
            &[NewTopic::new(&admin_topic, 1, 1).config("cleanup.policy", "delete")],
            CreateTopicsOptions::new(),
        )
        .await?;
    let created_topic = created_topics.topics().first().ok_or(Error::Unsupported(
        "published admin create returned no topic",
    ))?;
    if !created_topic.is_success() {
        return Err(Error::Broker {
            code: created_topic.error_code(),
            context: "published admin create topic failed".to_owned(),
        });
    }

    let listed_topic = admin
        .list_topics()
        .await?
        .into_iter()
        .find(|listed| listed.name() == admin_topic)
        .ok_or(Error::Unsupported(
            "published admin list did not return the created topic",
        ))?;
    if listed_topic.partition_count() != 1 || !listed_topic.is_success() {
        return Err(Error::Unsupported(
            "published admin list returned an invalid created topic",
        ));
    }

    let described_configs = admin
        .describe_topic_configs(
            &[TopicConfigResource::with_keys(
                &admin_topic,
                ["cleanup.policy"],
            )],
            DescribeConfigsOptions::new(),
        )
        .await?;
    let described_resource = described_configs
        .resources()
        .first()
        .ok_or(Error::Unsupported(
            "published admin config returned no resource",
        ))?;
    if !described_resource.is_success()
        || described_resource
            .entries()
            .iter()
            .find(|entry| entry.name() == "cleanup.policy")
            .and_then(|entry| entry.value())
            != Some("delete")
    {
        return Err(Error::Unsupported(
            "published admin config did not preserve cleanup.policy",
        ));
    }

    let deleted_topics = admin
        .delete_topics(&[admin_topic.clone()], DeleteTopicsOptions::new())
        .await?;
    let deleted_topic = deleted_topics.topics().first().ok_or(Error::Unsupported(
        "published admin delete returned no topic",
    ))?;
    if !deleted_topic.is_success() {
        return Err(Error::Broker {
            code: deleted_topic.error_code(),
            context: "published admin delete topic failed".to_owned(),
        });
    }

    let mut producer = configure_security!(ProducerConfig::new([bootstrap_servers.clone()])
        .client_id("kafrust-published-smoke-producer")
        .acks(Acks::Leader)
        .enable_idempotence(true)
        .compression(compression))
    .build()
    .await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(value.as_bytes()),
        )
        .await?;

    let mut consumer = configure_security!(ConsumerConfig::new([bootstrap_servers.clone()])
        .client_id("kafrust-published-smoke-consumer")
        .max_poll_records(10))
    .build()
    .await?;
    consumer.assign(&topic, metadata.partition(), metadata.offset());
    let records = consumer.poll().await?;
    if !records.iter().any(|record| {
        record.topic() == topic
            && record.partition() == metadata.partition()
            && record.offset() == metadata.offset()
            && record.value() == Some(value.as_bytes())
    }) {
        return Err(Error::Unsupported(
            "published crate consumer did not read the produced record",
        ));
    }

    let mut transactional_producer =
        configure_security!(ProducerConfig::new([bootstrap_servers.clone()])
            .client_id("kafrust-published-smoke-transaction-producer")
            .transactional_id(transactional_id)
            .compression(compression))
        .build()
        .await?;
    transactional_producer.begin_transaction()?;
    let aborted_value = format!("{value}-aborted");
    let aborted_metadata = transactional_producer
        .send(
            ProducerRecord::to(transaction_topic.clone())
                .partition(0)
                .value(aborted_value.as_bytes()),
        )
        .await?;
    transactional_producer.abort_transaction().await?;

    transactional_producer.begin_transaction()?;
    let committed_value = format!("{value}-committed");
    let committed_metadata = transactional_producer
        .send(
            ProducerRecord::to(transaction_topic.clone())
                .partition(0)
                .value(committed_value.as_bytes()),
        )
        .await?;
    transactional_producer.commit_transaction().await?;

    let mut committed_consumer =
        configure_security!(ConsumerConfig::new([bootstrap_servers.clone()])
            .client_id("kafrust-published-smoke-read-committed")
            .max_poll_records(10)
            .isolation_level(IsolationLevel::ReadCommitted))
        .build()
        .await?;
    committed_consumer.assign(
        &transaction_topic,
        aborted_metadata.partition(),
        aborted_metadata.offset(),
    );
    let committed_records = committed_consumer.poll().await?;
    if committed_records.iter().any(|record| {
        record.topic() == transaction_topic && record.value() == Some(aborted_value.as_bytes())
    }) || !committed_records.iter().any(|record| {
        record.topic() == transaction_topic
            && record.partition() == committed_metadata.partition()
            && record.offset() == committed_metadata.offset()
            && record.value() == Some(committed_value.as_bytes())
    }) {
        return Err(Error::Unsupported(
            "published crate read_committed consumer returned the wrong transaction records",
        ));
    }

    let mut group = configure_security!(ConsumerGroupConfig::new([bootstrap_servers], group_id)
        .client_id("kafrust-published-smoke-group"))
    .group_protocol(group_protocol)
    .max_retries(5)
    .max_poll_records(10)
    .offset_reset_policy(OffsetResetPolicy::Earliest)
    .subscribe(topic.clone())
    .join()
    .await?;
    let group_records = group.poll().await?;
    if !group_records
        .iter()
        .any(|record| record.topic() == topic && record.value() == Some(value.as_bytes()))
    {
        return Err(Error::Unsupported(
            "published crate consumer group did not read the produced record",
        ));
    }
    group.leave().await?;

    println!(
        "published kafrust verified admin cluster/topic lifecycle, idempotent producer, transaction commit/abort, read_committed, direct consumer, and group {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}
