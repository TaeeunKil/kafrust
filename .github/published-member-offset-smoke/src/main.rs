mod common;

use kafrust::{
    AdminClient, ClientConfig, ConsumerGroupConfig, ConsumerGroupOffset, ConsumerGroupOffsetQuery,
    ConsumerGroupProtocol, Error,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_tracing()?;
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .map_err(|_| Error::Unsupported("KAFRUST_GROUP_ID must be set"))?;
    let topic = std::env::var("KAFRUST_TOPIC")
        .map_err(|_| Error::Unsupported("KAFRUST_TOPIC must be set"))?;
    let partition = parse_partition()?;
    let target_offset = parse_target_offset()?;

    let admin_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-published-member-offset"),
    )?;
    let admin = AdminClient::new(admin_config);
    let group_config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-published-member-offset-member")
            .group_protocol(ConsumerGroupProtocol::Consumer),
    )?
    .subscribe(topic.clone());
    let group = group_config.join().await?;
    let member_id = group.member_id().to_owned();
    let member_epoch = group.generation_id();
    let topic_id = group
        .topic_id(&topic)
        .ok_or(Error::Unsupported("KIP-848 assignment has no topic UUID"))?;
    let query = [ConsumerGroupOffsetQuery::new(topic.clone(), [partition]).topic_id(topic_id)];

    let before = admin
        .list_consumer_group_offsets_with_member(
            &group_id,
            Some(&member_id),
            member_epoch,
            Some(&query),
            true,
        )
        .await?;
    ensure_success(before.error_code(), "list member-aware offsets before commit")?;
    let altered = admin
        .alter_consumer_group_offsets_with_member(
            &group_id,
            &member_id,
            member_epoch,
            None,
            &[ConsumerGroupOffset::new(topic.clone(), partition, target_offset)
                .topic_id(topic_id)
                .metadata("kafrust-published-member-offset")],
        )
        .await?;
    if !altered.is_success() {
        return Err(Error::Broker {
            code: first_partition_error(&altered).unwrap_or(-1),
            context: "member-aware OffsetCommit v10".to_owned(),
        });
    }

    let after = admin
        .list_consumer_group_offsets_with_member(
            &group_id,
            Some(&member_id),
            member_epoch,
            Some(&query),
            true,
        )
        .await?;
    ensure_success(after.error_code(), "list member-aware offsets after commit")?;
    let committed = checked_partition(&after, &topic, partition)?.committed_offset();
    if committed != target_offset {
        return Err(Error::Broker {
            code: -1,
            context: format!(
                "member-aware OffsetCommit v10 expected {target_offset}, received {committed}"
            ),
        });
    }
    println!("member-aware after {group_id}/{topic}-{partition} offset={committed}");
    group.leave().await?;
    Ok(())
}

fn checked_partition<'a>(
    result: &'a kafrust::ListConsumerGroupOffsetsResult,
    topic: &str,
    partition: i32,
) -> kafrust::Result<&'a kafrust::ConsumerGroupOffsetPartitionResult> {
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
            context: format!("member-aware offset for {topic}-{partition}"),
        });
    }
    Ok(partition_result)
}

fn first_partition_error(result: &kafrust::AlterConsumerGroupOffsetsResult) -> Option<i16> {
    result
        .topics()
        .iter()
        .flat_map(|topic| topic.partitions())
        .find(|partition| !partition.is_success())
        .map(|partition| partition.error_code())
}

fn ensure_success(error_code: i16, context: &str) -> kafrust::Result<()> {
    if error_code == 0 {
        Ok(())
    } else {
        Err(Error::Broker {
            code: error_code,
            context: context.to_owned(),
        })
    }
}

fn parse_partition() -> kafrust::Result<i32> {
    std::env::var("KAFRUST_PARTITION")
        .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be set"))?
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_PARTITION must be an integer"))
}

fn parse_target_offset() -> kafrust::Result<i64> {
    std::env::var("KAFRUST_ADMIN_OFFSET")
        .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_OFFSET must be set"))?
        .parse()
        .map_err(|_| Error::Unsupported("KAFRUST_ADMIN_OFFSET must be an integer"))
}
