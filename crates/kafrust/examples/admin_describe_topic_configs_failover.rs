mod common;

use std::io::{self, Write};

use kafrust::{
    AdminClient, ClientConfig, ClientMetrics, DescribeConfigsOptions, Error, TopicConfigResource,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_request_gate(32)?;

    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let metrics = ClientMetrics::new();
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers)
            .client_id("kafrust-admin-describe-topic-configs-failover")
            .metrics(metrics.clone()),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .describe_topic_configs(
            &[TopicConfigResource::new(topic.clone())],
            DescribeConfigsOptions::new(),
        )
        .await?;
    let resource = result
        .resources()
        .iter()
        .find(|resource| resource.name() == topic)
        .ok_or_else(|| {
            Error::Unsupported("DescribeConfigs response omitted the requested topic")
        })?;
    if !resource.is_success() {
        return Err(Error::Broker {
            code: resource.error_code(),
            context: format!("describe config for topic {topic}"),
        });
    }

    println!(
        "admin describe topic configs failover completed {topic} entries={} retries={}",
        resource.entries().len(),
        metrics.snapshot().retries,
    );
    io::stdout().flush().map_err(Error::Io)?;
    Ok(())
}
