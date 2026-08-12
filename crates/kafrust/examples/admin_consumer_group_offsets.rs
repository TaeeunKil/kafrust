mod common;

use kafrust::{
    AdminClient, ClientConfig, ConsumerGroupOffset, ConsumerGroupOffsetQuery, Error,
    ListConsumerGroupOffsetsResult,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = parse_partition()?;
    let target_offset = parse_target_offset()?;
    let config = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-admin-group-offsets-example"),
    )?;
    let admin = AdminClient::new(config);
    let query = [ConsumerGroupOffsetQuery::new(topic.clone(), [partition])];

    let before = admin
        .list_consumer_group_offsets(&group_id, Some(&query))
        .await?;
    let before_partition = checked_partition(&before, &group_id, &topic, partition)?;
    println!(
        "before {group_id}/{topic}-{partition} offset={} metadata={:?}",
        before_partition.committed_offset(),
        before_partition.metadata()
    );

    let altered = admin
        .alter_consumer_group_offsets(
            &group_id,
            &[
                ConsumerGroupOffset::new(topic.clone(), partition, target_offset)
                    .metadata("kafrust-admin-smoke"),
            ],
        )
        .await?;
    if !altered.is_success() {
        let error_code = altered
            .topics()
            .iter()
            .flat_map(|topic| topic.partitions())
            .find(|partition| !partition.is_success())
            .map(|partition| partition.error_code())
            .unwrap_or(-1);
        return Err(Error::Broker {
            code: error_code,
            context: format!("alter committed offset for {group_id}/{topic}-{partition}"),
        });
    }

    let after = admin
        .list_consumer_group_offsets(&group_id, Some(&query))
        .await?;
    let after_partition = checked_partition(&after, &group_id, &topic, partition)?;
    if after_partition.committed_offset() != target_offset {
        return Err(Error::Broker {
            code: -1,
            context: format!(
                "committed offset verification failed for {group_id}/{topic}-{partition}: expected {target_offset}, received {}",
                after_partition.committed_offset()
            ),
        });
    }
    println!(
        "after {group_id}/{topic}-{partition} offset={} metadata={:?} verified=true",
        after_partition.committed_offset(),
        after_partition.metadata()
    );
    Ok(())
}

fn checked_partition<'a>(
    result: &'a ListConsumerGroupOffsetsResult,
    group_id: &str,
    topic: &str,
    partition: i32,
) -> kafrust::Result<&'a kafrust::ConsumerGroupOffsetPartitionResult> {
    if result.error_code() != 0 {
        return Err(Error::Broker {
            code: result.error_code(),
            context: format!("list committed offsets for consumer group {group_id}"),
        });
    }
    let topic_result = result
        .topics()
        .iter()
        .find(|result_topic| result_topic.topic() == topic)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.to_owned(),
            partition,
        })?;
    let partition_result = topic_result
        .partitions()
        .iter()
        .find(|result_partition| result_partition.partition_index() == partition)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.to_owned(),
            partition,
        })?;
    if !partition_result.is_success() {
        return Err(Error::Broker {
            code: partition_result.error_code(),
            context: format!("list committed offset for {group_id}/{topic}-{partition}"),
        });
    }
    Ok(partition_result)
}

fn parse_partition() -> kafrust::Result<i32> {
    std::env::var("KAFRUST_PARTITION")
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be a partition index"))
        })
        .transpose()
        .map(|partition| partition.unwrap_or(0))
}

fn parse_target_offset() -> kafrust::Result<i64> {
    std::env::var("KAFRUST_ADMIN_OFFSET")
        .ok()
        .map(|value| {
            value
                .parse()
                .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_OFFSET must be an offset"))
        })
        .transpose()
        .map(|offset| offset.unwrap_or(0))
}
