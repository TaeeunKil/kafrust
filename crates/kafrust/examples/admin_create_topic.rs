mod common;

use kafrust::{
    AdminClient, ClientConfig, CreateTopicsOptions, DeleteTopicsOptions, DescribeConfigsOptions,
    Error, NewTopic, TopicConfigResource,
};
use std::time::{Duration, Instant};

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
    let cluster = admin.describe_cluster().await?;
    println!(
        "cluster has {} broker(s); controller {}",
        cluster.brokers().len(),
        cluster.controller_id()
    );
    if cluster.controller().is_none() {
        return Err(Error::MissingBroker {
            node_id: cluster.controller_id(),
        });
    }

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

    let partition_count = wait_for_topic_metadata(&config, &topic).await?;
    println!(
        "described topic {} with {} partitions",
        topic, partition_count
    );

    let listed_topics = admin.list_topics().await?;
    let listed = listed_topics
        .iter()
        .find(|listed| listed.name() == topic)
        .ok_or_else(|| Error::Broker {
            code: -1,
            context: format!("created topic {topic} absent from topic listing"),
        })?;
    println!(
        "listed topic {} with {} partitions",
        listed.name(),
        listed.partition_count()
    );

    let config_result = admin
        .describe_topic_configs(
            &[TopicConfigResource::with_keys(&topic, ["cleanup.policy"])],
            DescribeConfigsOptions::new().include_synonyms(true),
        )
        .await?;
    let topic_config = config_result
        .resources()
        .first()
        .ok_or_else(|| Error::Broker {
            code: -1,
            context: format!("missing config response for topic {topic}"),
        })?;
    if !topic_config.is_success() {
        return Err(Error::Broker {
            code: topic_config.error_code(),
            context: format!("describe configs for topic {topic}"),
        });
    }
    let cleanup_policy = topic_config
        .entries()
        .iter()
        .find(|entry| entry.name() == "cleanup.policy")
        .and_then(|entry| entry.value())
        .ok_or_else(|| Error::Broker {
            code: -1,
            context: format!("cleanup.policy absent for topic {topic}"),
        })?;
    if cleanup_policy != "delete" {
        return Err(Error::Broker {
            code: -1,
            context: format!("unexpected cleanup.policy {cleanup_policy} for topic {topic}"),
        });
    }
    println!("described cleanup.policy={cleanup_policy} for topic {topic}");

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

async fn wait_for_topic_metadata(config: &ClientConfig, topic: &str) -> kafrust::Result<usize> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let result = async {
            let mut client = config.clone().connect().await?;
            let metadata = client.metadata(Some(vec![topic.to_owned()])).await?;
            let created = metadata
                .topics
                .iter()
                .find(|metadata| metadata.name == topic)
                .ok_or_else(|| Error::UnknownTopicOrPartition {
                    topic: topic.to_owned(),
                    partition: -1,
                })?;
            if created.error_code != 0 {
                return Err(Error::Broker {
                    code: created.error_code,
                    context: format!("describe newly created topic {topic}"),
                });
            }
            Ok(created.partitions.len())
        }
        .await;

        match result {
            Ok(partition_count) => return Ok(partition_count),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
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
