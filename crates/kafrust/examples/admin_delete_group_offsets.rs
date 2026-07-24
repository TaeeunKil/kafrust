mod common;

use kafrust::{AdminClient, BrokerErrorKind, ClientConfig, ConsumerGroupOffsetDelete, Error};
use std::time::Duration;

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
    let request = [ConsumerGroupOffsetDelete::new(topic.clone(), [partition])];
    let mut attempt = 0;
    let result = loop {
        let result = admin
            .delete_consumer_group_offsets(&group_id, &request)
            .await?;
        if result.is_success() {
            break result;
        }

        let partition_errors: Vec<_> = result
            .topics()
            .iter()
            .flat_map(|topic| topic.partitions())
            .filter(|partition| !partition.is_success())
            .collect();
        let group_still_active = result.broker_error_kind()
            == Some(BrokerErrorKind::GroupSubscribedToTopic)
            || (result.error_code() == 0
                && !partition_errors.is_empty()
                && partition_errors.iter().all(|partition| {
                    partition.broker_error_kind() == Some(BrokerErrorKind::GroupSubscribedToTopic)
                }));
        attempt += 1;
        if group_still_active && attempt < 30 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        return Err(Error::Broker {
            code: partition_errors
                .first()
                .map(|partition| partition.error_code())
                .unwrap_or(result.error_code()),
            context: format!("delete committed offset for {group_id}/{topic}-{partition}"),
        });
    };

    println!(
        "deleted committed offset for {group_id}/{topic}-{partition} throttle={:?}",
        result.throttle_time()
    );

    let deleted = admin
        .delete_consumer_groups(std::slice::from_ref(&group_id))
        .await?
        .into_iter()
        .next()
        .ok_or_else(|| Error::MissingDeleteGroupResult {
            group_id: group_id.clone(),
        })?;
    let already_removed = deleted.broker_error_kind() == Some(BrokerErrorKind::GroupIdNotFound);
    if !deleted.is_success() && !already_removed {
        return Err(Error::Broker {
            code: deleted.error_code(),
            context: format!("delete consumer group {group_id}"),
        });
    }
    println!(
        "consumer group {group_id} cleanup complete already_removed={already_removed} throttle={:?}",
        deleted.throttle_time(),
    );
    Ok(())
}
