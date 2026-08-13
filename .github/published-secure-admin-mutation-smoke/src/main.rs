use std::{env, fs};

use kafrust::{
    Acks, AdminClient, AlterConfigsOptions, ClientConfig, ConsumerConfig, ConsumerGroupConfig,
    ConsumerGroupOffset, ConsumerGroupOffsetQuery, ConsumerGroupProtocol, Error, OffsetResetPolicy,
    ProducerConfig, ProducerRecord, SecurityProtocol, TopicConfigAlteration,
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
                "published secure Admin mutation smoke requires sasl_tls",
            ));
        }
        if env::var("KAFRUST_SASL_MECHANISM").as_deref() != Ok("scram-sha-256") {
            return Err(Error::Unsupported(
                "published secure Admin mutation smoke requires scram-sha-256",
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
    env::var(name)
        .map_err(|_| Error::Unsupported("published secure Admin mutation variable missing"))
}

async fn read_offset(admin: &AdminClient, group_id: &str, topic: &str) -> kafrust::Result<i64> {
    let query = [ConsumerGroupOffsetQuery::new(topic.to_owned(), [0])];
    let result = admin
        .list_consumer_group_offsets(group_id, Some(&query))
        .await?;
    if result.error_code() != 0 {
        return Err(Error::Broker {
            code: result.error_code(),
            context: "published secure Admin mutation offset lookup failed".to_owned(),
        });
    }
    result
        .topics()
        .iter()
        .find(|result_topic| result_topic.topic() == topic)
        .and_then(|result_topic| result_topic.partitions().first())
        .filter(|partition| partition.is_success())
        .map(|partition| partition.committed_offset())
        .ok_or(Error::Unsupported(
            "published secure Admin mutation offset result was incomplete",
        ))
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
                .client_id("kafrust-published-secure-admin-mutation"),
        ),
    );
    if admin.describe_cluster().await?.brokers().is_empty() {
        return Err(Error::Unsupported(
            "published secure Admin mutation returned no brokers",
        ));
    }

    let allowed_alter = admin
        .incremental_alter_topic_configs(
            &[TopicConfigAlteration::new(&allowed_topic).set("retention.ms", "60000")],
            AlterConfigsOptions::new(),
        )
        .await?;
    let allowed_resource = allowed_alter
        .resources()
        .iter()
        .find(|resource| resource.name() == allowed_topic)
        .ok_or(Error::Unsupported(
            "published secure Admin mutation omitted allowed config result",
        ))?;
    if !allowed_resource.is_success() {
        return Err(Error::Broker {
            code: allowed_resource.error_code(),
            context: "published secure Admin mutation allowed config failed".to_owned(),
        });
    }

    let denied_alter = admin
        .incremental_alter_topic_configs(
            &[TopicConfigAlteration::new(&denied_topic).set("retention.ms", "60000")],
            AlterConfigsOptions::new(),
        )
        .await?;
    let denied_resource = denied_alter
        .resources()
        .iter()
        .find(|resource| resource.name() == denied_topic)
        .ok_or(Error::Unsupported(
            "published secure Admin mutation omitted denied config result",
        ))?;
    if denied_resource.is_success() {
        return Err(Error::Unsupported(
            "published secure Admin mutation allowed unauthorized config alteration",
        ));
    }

    let mut producer = security
        .configure_producer(
            ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-admin-mutation-producer")
                .acks(Acks::Leader)
                .enable_idempotence(true),
        )
        .build()
        .await?;
    let produced = producer
        .send(
            ProducerRecord::to(allowed_topic.clone())
                .partition(0)
                .value(b"published secure admin mutation record"),
        )
        .await?;

    let mut direct_consumer = security
        .configure_consumer(
            ConsumerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
                .client_id("kafrust-published-secure-admin-mutation-consumer"),
        )
        .build()
        .await?;
    direct_consumer.assign(&allowed_topic, produced.partition(), produced.offset());
    if !direct_consumer
        .poll()
        .await?
        .iter()
        .any(|record| record.value() == Some(b"published secure admin mutation record"))
    {
        return Err(Error::Unsupported(
            "published secure Admin mutation direct consumer missed record",
        ));
    }

    let mut group = security
        .configure_group(
            ConsumerGroupConfig::new(
                bootstrap_servers.split(',').map(str::to_owned),
                group_id.clone(),
            )
            .client_id("kafrust-published-secure-admin-mutation-group")
            .group_protocol(ConsumerGroupProtocol::Classic)
            .max_retries(10)
            .offset_reset_policy(OffsetResetPolicy::Earliest)
            .subscribe(allowed_topic.clone()),
        )
        .join()
        .await?;
    let group_record = group
        .poll()
        .await?
        .into_iter()
        .find(|record| record.value() == Some(b"published secure admin mutation record"))
        .ok_or(Error::Unsupported(
            "published secure Admin mutation group missed record",
        ))?;
    group.commit_record(&group_record)?;
    group.commit_queued_offsets().await?;
    let committed_offset = group_record.offset() + 1;
    if read_offset(&admin, &group_id, &allowed_topic).await? != committed_offset {
        return Err(Error::Unsupported(
            "published secure Admin mutation group commit was not visible to Admin",
        ));
    }
    group.leave().await?;

    let reset = admin
        .alter_consumer_group_offsets(
            &group_id,
            &[ConsumerGroupOffset::new(&allowed_topic, 0, 0).metadata("admin-reset")],
        )
        .await?;
    if !reset.is_success() {
        return Err(Error::Unsupported(
            "published secure Admin mutation offset reset returned a partition error",
        ));
    }
    if read_offset(&admin, &group_id, &allowed_topic).await? != 0 {
        return Err(Error::Unsupported(
            "published secure Admin mutation offset reset was not visible to Admin",
        ));
    }

    let mut restored_group = security
        .configure_group(
            ConsumerGroupConfig::new(bootstrap_servers.split(',').map(str::to_owned), group_id)
                .client_id("kafrust-published-secure-admin-mutation-restore")
                .group_protocol(ConsumerGroupProtocol::Classic)
                .max_retries(10)
                .offset_reset_policy(OffsetResetPolicy::Earliest)
                .subscribe(allowed_topic),
        )
        .join()
        .await?;
    let restored = restored_group.poll().await?;
    if !restored
        .iter()
        .any(|record| record.offset() == group_record.offset())
    {
        return Err(Error::Unsupported(
            "published secure Admin mutation group did not restore the reset offset",
        ));
    }
    restored_group.leave().await?;

    println!(
        "published secure Admin mutation passed allowed/denied topic config, group commit, Admin offset reset, and restored consumption"
    );
    Ok(())
}
