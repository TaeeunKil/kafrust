mod common;

use kafrust::{AdminClient, ClientConfig, ConsumerGroupOffsetDelete, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = std::env::var("KAFRUST_PARTITION")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-offset-delete-example"),
    )?;
    let admin = AdminClient::new(config);
    let result = admin
        .delete_consumer_group_offsets(
            &group_id,
            &[ConsumerGroupOffsetDelete::new(topic.clone(), [partition])],
        )
        .await?;

    if !result.is_success() {
        let partition_error = result
            .topics()
            .iter()
            .flat_map(|topic| topic.partitions())
            .find(|partition| !partition.is_success())
            .map(|partition| partition.error_code());
        return Err(Error::Broker {
            code: partition_error.unwrap_or(result.error_code()),
            context: format!("delete committed offset for {group_id}/{topic}-{partition}"),
        });
    }

    println!(
        "deleted committed offset for {group_id}/{topic}-{partition} throttle={:?}",
        result.throttle_time()
    );
    Ok(())
}
