use std::{env, fs};

use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerConfig, ConsumerGroupConfig, ConsumerGroupProtocol,
    CreateTopicsOptions, DeleteTopicsOptions, DescribeConfigsOptions, Error, NewTopic,
    OffsetResetPolicy, ProducerConfig, ProducerRecord, SecurityProtocol, TopicConfigResource,
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
                "published secure Admin auth smoke requires sasl_tls",
            ));
        }
        if env::var("KAFRUST_SASL_MECHANISM").as_deref() != Ok("scram-sha-256") {
            return Err(Error::Unsupported(
                "published secure Admin auth smoke requires scram-sha-256",
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

    fn configure_group(&self, config: ConsumerGroupConfig) -> ConsumerGroupConfig {
        config
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name(self.tls_server_name.clone())
            .tls_root_certificate_der(self.tls_root_certificate_der.clone())
            .sasl_scram_sha_256(self.sasl_username.clone(), self.sasl_password.clone())
    }
}

fn required(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| Error::Unsupported("published secure Admin auth variable missing"))
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let allowed_topic = required("KAFRUST_ALLOWED_TOPIC")?;
    let denied_topic = required("KAFRUST_DENIED_TOPIC")?;
    let group_id = required("KAFRUST_GROUP_ID")?;
    let security = SecuritySettings::from_env()?;

    let admin = AdminClient::new(
        security.configure_client(
            ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-admin-auth"),
        ),
    );
    let cluster = admin.describe_cluster().await?;
    if cluster.brokers().is_empty() {
        return Err(Error::Unsupported(
            "published secure Admin auth returned no brokers",
        ));
    }

    let allowed_configs = admin
        .describe_topic_configs(
            &[TopicConfigResource::with_keys(
                &allowed_topic,
                ["cleanup.policy"],
            )],
            DescribeConfigsOptions::new(),
        )
        .await?;
    let allowed_resource = allowed_configs
        .resources()
        .iter()
        .find(|resource| resource.name() == allowed_topic)
        .ok_or(Error::Unsupported(
            "published secure Admin auth omitted allowed topic config",
        ))?;
    if !allowed_resource.is_success() {
        return Err(Error::Broker {
            code: allowed_resource.error_code(),
            context: "published secure Admin auth allowed topic config failed".to_owned(),
        });
    }

    let denied_configs = admin
        .describe_topic_configs(
            &[TopicConfigResource::with_keys(
                &denied_topic,
                ["cleanup.policy"],
            )],
            DescribeConfigsOptions::new(),
        )
        .await?;
    let denied_resource = denied_configs
        .resources()
        .iter()
        .find(|resource| resource.name() == denied_topic)
        .ok_or(Error::Unsupported(
            "published secure Admin auth omitted denied topic config",
        ))?;
    if denied_resource.is_success() {
        return Err(Error::Unsupported(
            "published secure Admin auth exposed denied topic config",
        ));
    }

    let create_denied = admin
        .create_topics(
            &[NewTopic::new(
                format!("{denied_topic}-create-attempt"),
                1,
                1,
            )],
            CreateTopicsOptions::new(),
        )
        .await?;
    let create_result = create_denied.topics().first().ok_or(Error::Unsupported(
        "published secure Admin auth omitted denied create result",
    ))?;
    if create_result.is_success() {
        return Err(Error::Unsupported(
            "published secure Admin auth allowed an unauthorized topic create",
        ));
    }

    let delete_denied = admin
        .delete_topics(&[allowed_topic.clone()], DeleteTopicsOptions::new())
        .await?;
    let delete_result = delete_denied.topics().first().ok_or(Error::Unsupported(
        "published secure Admin auth omitted denied delete result",
    ))?;
    if delete_result.is_success() {
        return Err(Error::Unsupported(
            "published secure Admin auth allowed an unauthorized topic delete",
        ));
    }

    let mut producer = security
        .configure_producer(
            ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-admin-auth-producer")
                .acks(Acks::Leader)
                .enable_idempotence(true),
        )
        .build()
        .await?;
    let produced = producer
        .send(
            ProducerRecord::to(allowed_topic.clone())
                .partition(0)
                .value(b"published secure authorization record"),
        )
        .await?;

    let mut consumer = security
        .configure_consumer(
            ConsumerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-admin-auth-consumer"),
        )
        .build()
        .await?;
    consumer.assign(&allowed_topic, produced.partition(), produced.offset());
    let records = consumer.poll().await?;
    if !records.iter().any(|record| {
        record.topic() == allowed_topic
            && record.partition() == produced.partition()
            && record.offset() == produced.offset()
            && record.value() == Some(b"published secure authorization record")
    }) {
        return Err(Error::Unsupported(
            "published secure authorization consumer did not read allowed topic",
        ));
    }

    let mut group = security
        .configure_group(
            ConsumerGroupConfig::new(
                bootstrap_servers.split(',').map(str::to_owned),
                group_id.clone(),
            )
            .client_id("kafrust-published-secure-admin-auth-group")
            .group_protocol(ConsumerGroupProtocol::Classic)
            .max_retries(10)
            .offset_reset_policy(OffsetResetPolicy::Earliest)
            .subscribe(allowed_topic.clone()),
        )
        .join()
        .await?;
    let group_records = group.poll().await?;
    if !group_records
        .iter()
        .any(|record| record.topic() == allowed_topic)
    {
        return Err(Error::Unsupported(
            "published secure authorization group did not read allowed topic",
        ));
    }
    group.leave().await?;

    println!(
        "published secure Admin authorization passed cluster/config allow, mutation deny, producer, consumer, and group paths for {}",
        allowed_topic
    );
    Ok(())
}
