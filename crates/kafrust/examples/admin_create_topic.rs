mod common;

use kafrust::{
    AdminClient, ClientConfig, CreateTopicsOptions, DeleteTopicsOptions, Error, NewTopic,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic =
        std::env::var("KAFRUST_ADMIN_TOPIC").unwrap_or_else(|_| "kafrust-admin-smoke".to_owned());
    let partitions = parse_env("KAFRUST_ADMIN_PARTITIONS", 1_i32)?;
    let replication_factor = parse_env("KAFRUST_ADMIN_REPLICATION_FACTOR", 1_i16)?;
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-example"),
    )?;
    let admin = AdminClient::new(config.clone());
    let result = admin
        .create_topics(
            &[NewTopic::new(&topic, partitions, replication_factor)
                .config("cleanup.policy", "delete")],
            CreateTopicsOptions::new(),
        )
        .await?;

    for topic_result in result.topics() {
        if !topic_result.is_success() {
            return Err(Error::Broker {
                code: topic_result.error_code(),
                context: format!(
                    "create topic {}{}",
                    topic_result.name(),
                    topic_result
                        .error_message()
                        .map(|message| format!(": {message}"))
                        .unwrap_or_default()
                ),
            });
        }
        println!(
            "created topic {} (controller throttle {:?})",
            topic_result.name(),
            result.throttle_time()
        );
    }

    let mut client = config.connect().await?;
    let metadata = client.metadata(Some(vec![topic.clone()])).await?;
    let created = metadata
        .topics
        .iter()
        .find(|metadata| metadata.name == topic)
        .ok_or(Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition: -1,
        })?;
    if created.error_code != 0 {
        return Err(Error::Broker {
            code: created.error_code,
            context: format!("describe newly created topic {topic}"),
        });
    }
    println!(
        "described topic {} with {} partitions",
        created.name,
        created.partitions.len()
    );

    let delete_result = admin
        .delete_topics(&[topic.clone()], DeleteTopicsOptions::new())
        .await?;
    for topic_result in delete_result.topics() {
        if !topic_result.is_success() {
            return Err(Error::Broker {
                code: topic_result.error_code(),
                context: format!("delete topic {}", topic_result.name()),
            });
        }
        println!(
            "deleted topic {} (controller throttle {:?})",
            topic_result.name(),
            delete_result.throttle_time()
        );
    }

    Ok(())
}

fn parse_env<T>(name: &'static str, default: T) -> kafrust::Result<T>
where
    T: std::str::FromStr,
{
    let Ok(value) = std::env::var(name) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| Error::Unsupported("admin topic numeric environment value is invalid"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::parse_env;

    #[test]
    fn uses_default_when_admin_numeric_environment_value_is_absent() {
        assert_eq!(
            parse_env("KAFRUST_TEST_MISSING_ADMIN_VALUE", 3_i32).unwrap(),
            3
        );
    }
}
