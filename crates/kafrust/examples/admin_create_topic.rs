mod common;

use kafrust::{
    AdminClient, AlterConfigsOptions, ClientConfig, CreatePartitionsOptions, CreateTopicsOptions,
    DeleteTopicsOptions, DescribeConfigsOptions, Error, NewPartitions, NewTopic,
    TopicConfigAlteration, TopicConfigResource,
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

    let partition_count = wait_for_topic_metadata(&config, &topic, partitions).await?;
    println!(
        "described topic {} with {} partitions",
        topic, partition_count
    );

    let expanded_partition_count = partitions
        .checked_add(2)
        .ok_or(Error::Unsupported("admin partition count overflow"))?;
    let expansion = admin
        .create_partitions(
            &[NewPartitions::new(&topic, expanded_partition_count)],
            CreatePartitionsOptions::new(),
        )
        .await?;
    for topic_result in expansion.topics() {
        if !topic_result.is_success() {
            return Err(Error::Broker {
                code: topic_result.error_code(),
                context: format!(
                    "create partitions for {}{}",
                    topic_result.name(),
                    topic_result
                        .error_message()
                        .map(|message| format!(": {message}"))
                        .unwrap_or_default()
                ),
            });
        }
    }
    wait_for_topic_metadata(&config, &topic, expanded_partition_count).await?;
    println!(
        "expanded topic {} from {} to {} partitions",
        topic, partitions, expanded_partition_count
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

    let alter_result = admin
        .incremental_alter_topic_configs(
            &[TopicConfigAlteration::new(&topic).set("retention.ms", "60000")],
            AlterConfigsOptions::new(),
        )
        .await?;
    for resource in alter_result.resources() {
        if !resource.is_success() {
            return Err(Error::Broker {
                code: resource.error_code(),
                context: format!("alter configs for topic {}", resource.name()),
            });
        }
    }
    wait_for_topic_config_value(&admin, &topic, "retention.ms", "60000").await?;
    println!("altered retention.ms=60000 for topic {topic}");

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

async fn wait_for_topic_config_value(
    admin: &AdminClient,
    topic: &str,
    key: &str,
    expected: &str,
) -> kafrust::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let result = async {
            let response = admin
                .describe_topic_configs(
                    &[TopicConfigResource::with_keys(topic, [key])],
                    DescribeConfigsOptions::new(),
                )
                .await?;
            let resource = response.resources().first().ok_or_else(|| Error::Broker {
                code: -1,
                context: format!("missing config response for topic {topic}"),
            })?;
            if !resource.is_success() {
                return Err(Error::Broker {
                    code: resource.error_code(),
                    context: format!("describe configs for topic {topic}"),
                });
            }
            let value = resource
                .entries()
                .iter()
                .find(|entry| entry.name() == key)
                .and_then(|entry| entry.value());
            if value != Some(expected) {
                return Err(Error::Broker {
                    code: -1,
                    context: format!(
                        "expected {key}={expected} for topic {topic}, received {value:?}"
                    ),
                });
            }
            Ok(())
        }
        .await;

        match result {
            Ok(()) => return Ok(()),
            Err(_) if Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_topic_metadata(
    config: &ClientConfig,
    topic: &str,
    expected_partition_count: i32,
) -> kafrust::Result<usize> {
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
            let partition_count = created.partitions.len();
            if partition_count != usize::try_from(expected_partition_count).unwrap_or(usize::MAX) {
                return Err(Error::Broker {
                    code: -1,
                    context: format!(
                        "expected {expected_partition_count} partitions for topic {topic}, received {partition_count}"
                    ),
                });
            }
            Ok(partition_count)
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
