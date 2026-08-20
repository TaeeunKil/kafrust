use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerConfig, Error, ProducerConfig, ProducerRecord,
    SecurityProtocol,
};
use std::{env, fs, sync::Arc};

fn required_env(name: &'static str) -> kafrust::Result<String> {
    env::var(name).map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "published OAUTHBEARER environment variable is required",
    })
}

fn oauth_client_config(
    bootstrap: &str,
    client_id: &'static str,
    ca_der: Vec<u8>,
    token: Arc<String>,
) -> ClientConfig {
    ClientConfig::new([bootstrap])
        .client_id(client_id)
        .security_protocol(SecurityProtocol::SaslTls)
        .tls_server_name("localhost")
        .tls_root_certificate_der(ca_der)
        .sasl_oauthbearer_provider(move || {
            let token = Arc::clone(&token);
            async move { Ok((*token).clone()) }
        })
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap = required_env("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required_env("KAFRUST_TOPIC")?;
    let ca_path = required_env("KAFRUST_TLS_ROOT_CERT_DER_PATH")?;
    let token = Arc::new(required_env("KAFRUST_OAUTH_TOKEN")?);
    let ca_der = fs::read(ca_path).map_err(|_| Error::Unsupported(
        "KAFRUST_TLS_ROOT_CERT_DER_PATH could not be read",
    ))?;

    let admin = AdminClient::new(oauth_client_config(
        &bootstrap,
        "kafrust-published-oauth-admin",
        ca_der.clone(),
        Arc::clone(&token),
    ));
    let cluster = admin.describe_cluster().await?;
    if cluster.brokers().is_empty() {
        return Err(Error::Unsupported(
            "published OAUTHBEARER Admin returned no brokers",
        ));
    }

    let mut producer = ProducerConfig::new([bootstrap.as_str()])
        .with_client_config(oauth_client_config(
            &bootstrap,
            "kafrust-published-oauth-producer",
            ca_der.clone(),
            Arc::clone(&token),
        ))
        .acks(Acks::All)
        .build()
        .await?;
    let metadata = producer
        .send(ProducerRecord::to(topic.clone()).value("published-oauth-record"))
        .await?;

    let mut consumer = ConsumerConfig::new([bootstrap.as_str()])
        .with_client_config(oauth_client_config(
            &bootstrap,
            "kafrust-published-oauth-consumer",
            ca_der,
            Arc::clone(&token),
        ))
        .max_poll_records(10)
        .build()
        .await?;
    consumer.assign(&topic, metadata.partition(), metadata.offset());
    let records = consumer.poll().await?;
    let consumed = records.iter().any(|record| {
        record.value() == Some(b"published-oauth-record".as_slice())
    });
    if !consumed {
        return Err(Error::Unsupported(
            "published OAUTHBEARER consumer did not read its produced record",
        ));
    }

    println!(
        "published oauthbearer ok brokers={} produced_partition={} produced_offset={} consumed=true token_provider=true",
        cluster.brokers().len(),
        metadata.partition(),
        metadata.offset(),
    );
    Ok(())
}
