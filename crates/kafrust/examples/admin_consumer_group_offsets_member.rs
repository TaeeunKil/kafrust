mod common;

use kafrust::{
    AdminClient, ClientConfig, ConsumerGroupConfig, ConsumerGroupOffset, ConsumerGroupOffsetQuery,
    ConsumerGroupProtocol, Error,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-kip848-admin-offsets".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let partition = parse_partition()?;
    let target_offset = parse_target_offset()?;

    let admin_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-kip848-admin-offsets"),
    )?;
    let admin = AdminClient::new(admin_config);

    let group_config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-kip848-admin-offsets-member")
            .group_protocol(ConsumerGroupProtocol::Consumer),
    )?
    .subscribe(topic.clone());
    let group = group_config.join().await?;
    let member_id = group.member_id().to_owned();
    let member_epoch = group.generation_id();
    let query = [ConsumerGroupOffsetQuery::new(topic.clone(), [partition])];

    let descriptions = admin
        .describe_consumer_groups_modern(&[group_id.clone()], true)
        .await?;
    let description = descriptions.first().ok_or(Error::MissingGroupDescription {
        group_id: group_id.clone(),
    })?;
    if !description.is_success() {
        return Err(Error::Broker {
            code: description.error_code(),
            context: format!(
                "modern consumer group description for {}: {:?}",
                group_id,
                description.error_message()
            ),
        });
    }
    if !description
        .members()
        .iter()
        .any(|member| member.member_id() == member_id)
    {
        return Err(Error::Unsupported(
            "modern consumer group description did not include the joined member",
        ));
    }
    println!(
        "modern description {group_id} state={} group_epoch={} assignment_epoch={} members={}",
        description.state(),
        description.group_epoch(),
        description.assignment_epoch(),
        description.members().len()
    );

    let before = admin
        .list_consumer_group_offsets_with_member(
            &group_id,
            Some(&member_id),
            member_epoch,
            Some(&query),
            true,
        )
        .await?;
    ensure_success(before.error_code(), &format!("list offsets for {group_id}"))?;
    println!(
        "member-aware before {group_id}/{topic}-{partition} offset={}",
        checked_partition(&before, &topic, partition)?.committed_offset()
    );

    let altered = admin
        .alter_consumer_group_offsets_with_member(
            &group_id,
            &member_id,
            member_epoch,
            None,
            &[
                ConsumerGroupOffset::new(topic.clone(), partition, target_offset)
                    .metadata("kafrust-kip848-admin-smoke"),
            ],
        )
        .await?;
    if !altered.is_success() {
        return Err(Error::Broker {
            code: first_partition_error(&altered).unwrap_or(-1),
            context: format!("member-aware offset commit for {group_id}/{topic}-{partition}"),
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
    ensure_success(
        after.error_code(),
        &format!("verify offsets for {group_id}"),
    )?;
    let committed = checked_partition(&after, &topic, partition)?.committed_offset();
    if committed != target_offset {
        return Err(Error::Broker {
            code: -1,
            context: format!(
                "member-aware offset verification expected {target_offset}, received {committed}"
            ),
        });
    }
    println!("member-aware after {group_id}/{topic}-{partition} offset={committed} verified=true");
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
            context: format!("offset for {topic}-{partition}"),
        });
    }
    Ok(partition_result)
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

fn first_partition_error(result: &kafrust::AlterConsumerGroupOffsetsResult) -> Option<i16> {
    result
        .topics()
        .iter()
        .flat_map(|topic| topic.partitions())
        .find(|partition| !partition.is_success())
        .map(|partition| partition.error_code())
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
