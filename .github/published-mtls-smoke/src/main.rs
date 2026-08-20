use std::{env, fs};

use kafrust::{
    AdminClient, ClientConfig, ConsumerConfig, ConsumerGroupConfig, CreateTopicsOptions,
    DeleteTopicsOptions, Error, IsolationLevel, NewTopic, OffsetResetPolicy, ProducerConfig,
    ProducerRecord, SecurityProtocol,
};

fn required_env(name: &str) -> kafrust::Result<String> {
    env::var(name).map_err(|_| Error::Unsupported("published mTLS environment is incomplete"))
}

fn read_der(name: &str) -> kafrust::Result<Vec<u8>> {
    let path = required_env(name)?;
    fs::read(path)
        .map_err(|_| Error::Unsupported("published mTLS certificate file could not be read"))
}

fn tls_config(
    bootstrap_servers: &[String],
    client_id: &str,
    server_name: &str,
    root_certificate: &[u8],
    client_certificate: &[u8],
    client_key: &[u8],
) -> ClientConfig {
    ClientConfig::new(bootstrap_servers.iter().cloned())
        .client_id(client_id)
        .security_protocol(SecurityProtocol::Tls)
        .tls_server_name(server_name)
        .tls_root_certificate_der(root_certificate.to_vec())
        .tls_client_certificate_der(client_certificate.to_vec())
        .tls_client_private_key_der(client_key.to_vec())
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = vec![required_env("KAFRUST_BOOTSTRAP_SERVERS")?];
    let topic = required_env("KAFRUST_TOPIC")?;
    let transaction_topic = required_env("KAFRUST_TRANSACTION_TOPIC")?;
    let admin_topic = required_env("KAFRUST_ADMIN_TOPIC")?;
    let group_id = required_env("KAFRUST_GROUP_ID")?;
    let transactional_id = required_env("KAFRUST_TRANSACTIONAL_ID")?;
    let value = required_env("KAFRUST_VALUE")?;
    let server_name = required_env("KAFRUST_TLS_SERVER_NAME")?;
    let root_certificate = read_der("KAFRUST_TLS_ROOT_CERT_DER_PATH")?;
    let client_certificate = read_der("KAFRUST_TLS_CLIENT_CERT_DER_PATH")?;
    let client_key = read_der("KAFRUST_TLS_CLIENT_KEY_DER_PATH")?;

    let admin = AdminClient::new(tls_config(
        &bootstrap_servers,
        "published-mtls-admin",
        &server_name,
        &root_certificate,
        &client_certificate,
        &client_key,
    ));
    if admin.describe_cluster().await?.brokers().is_empty() {
        return Err(Error::Unsupported(
            "published mTLS Admin returned no brokers",
        ));
    }

    let created = admin
        .create_topics(
            &[NewTopic::new(&admin_topic, 1, 1)],
            CreateTopicsOptions::new(),
        )
        .await?;
    let created_topic = created.topics().first().ok_or(Error::Unsupported(
        "published mTLS Admin create returned no topic",
    ))?;
    if !created_topic.is_success() {
        return Err(Error::Broker {
            code: created_topic.error_code(),
            context: "published mTLS Admin create failed".to_owned(),
        });
    }
    let deleted = admin
        .delete_topics(&[admin_topic], DeleteTopicsOptions::new())
        .await?;
    let deleted_topic = deleted.topics().first().ok_or(Error::Unsupported(
        "published mTLS Admin delete returned no topic",
    ))?;
    if !deleted_topic.is_success() {
        return Err(Error::Broker {
            code: deleted_topic.error_code(),
            context: "published mTLS Admin delete failed".to_owned(),
        });
    }

    let mut producer = ProducerConfig::new(bootstrap_servers.clone())
        .with_client_config(tls_config(
            &bootstrap_servers,
            "published-mtls-producer",
            &server_name,
            &root_certificate,
            &client_certificate,
            &client_key,
        ))
        .build()
        .await?;
    let produced = producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(value.as_bytes()),
        )
        .await?;

    let mut consumer = ConsumerConfig::new(bootstrap_servers.clone())
        .with_client_config(tls_config(
            &bootstrap_servers,
            "published-mtls-consumer",
            &server_name,
            &root_certificate,
            &client_certificate,
            &client_key,
        ))
        .build()
        .await?;
    let records = consumer
        .fetch(&topic, produced.partition(), produced.offset())
        .await?;
    if !records.iter().any(|record| {
        record.topic() == topic
            && record.offset() == produced.offset()
            && record.value() == Some(value.as_bytes())
    }) {
        return Err(Error::Unsupported(
            "published mTLS consumer did not read the record",
        ));
    }

    let mut transactional_producer = ProducerConfig::new(bootstrap_servers.clone())
        .with_client_config(tls_config(
            &bootstrap_servers,
            "published-mtls-transaction-producer",
            &server_name,
            &root_certificate,
            &client_certificate,
            &client_key,
        ))
        .transactional_id(transactional_id)
        .build()
        .await?;
    transactional_producer.begin_transaction()?;
    let aborted = transactional_producer
        .send(
            ProducerRecord::to(transaction_topic.clone())
                .partition(0)
                .value(b"aborted"),
        )
        .await?;
    transactional_producer.abort_transaction().await?;
    transactional_producer.begin_transaction()?;
    let committed = transactional_producer
        .send(
            ProducerRecord::to(transaction_topic.clone())
                .partition(0)
                .value(b"committed"),
        )
        .await?;
    transactional_producer.commit_transaction().await?;

    let mut committed_consumer = ConsumerConfig::new(bootstrap_servers.clone())
        .with_client_config(tls_config(
            &bootstrap_servers,
            "published-mtls-read-committed",
            &server_name,
            &root_certificate,
            &client_certificate,
            &client_key,
        ))
        .isolation_level(IsolationLevel::ReadCommitted)
        .build()
        .await?;
    let committed_records = committed_consumer
        .fetch(&transaction_topic, aborted.partition(), aborted.offset())
        .await?;
    if committed_records
        .iter()
        .any(|record| record.value() == Some(b"aborted"))
        || !committed_records.iter().any(|record| {
            record.offset() == committed.offset() && record.value() == Some(b"committed")
        })
    {
        return Err(Error::Unsupported(
            "published mTLS read_committed returned the wrong records",
        ));
    }

    let mut group = ConsumerGroupConfig::new(bootstrap_servers.clone(), group_id)
        .with_client_config(tls_config(
            &bootstrap_servers,
            "published-mtls-group",
            &server_name,
            &root_certificate,
            &client_certificate,
            &client_key,
        ))
        .offset_reset_policy(OffsetResetPolicy::Earliest)
        .subscribe(topic)
        .join()
        .await?;
    let group_records = group.poll().await?;
    let group_record = group_records.first().ok_or(Error::Unsupported(
        "published mTLS group did not poll a record",
    ))?;
    group.commit_record(group_record)?;
    group.commit_queued_offsets().await?;
    group.leave().await?;

    println!(
        "published mTLS passed: admin, producer, direct consumer, transaction/read_committed, and group (offset={})",
        produced.offset()
    );
    Ok(())
}
