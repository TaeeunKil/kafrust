mod common;

use kafrust::{AdminClient, ClientConfig, ConsumerGroupConfig, ConsumerGroupProtocol, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_tracing()?;
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = required_env("KAFRUST_GROUP_ID")?;
    let topic = required_env("KAFRUST_TOPIC")?;

    let admin_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-published-group-describe"),
    )?;
    let admin = AdminClient::new(admin_config);
    let group_config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-published-group-describe-member")
            .group_protocol(ConsumerGroupProtocol::Consumer),
    )?
    .subscribe(topic.clone());
    let group = group_config.join().await?;
    let member_id = group.member_id().to_owned();
    let member_epoch = group.generation_id();

    let descriptions = admin
        .describe_consumer_groups_modern(std::slice::from_ref(&group_id), true)
        .await?;
    let description = descriptions
        .into_iter()
        .find(|description| description.group_id() == group_id)
        .ok_or_else(|| Error::MissingGroupDescription {
            group_id: group_id.clone(),
        })?;
    if !description.is_success() {
        return Err(Error::Broker {
            code: description.error_code(),
            context: format!("ConsumerGroupDescribe for {group_id}"),
        });
    }
    if description.group_epoch() < 0 || description.assignment_epoch() < 0 {
        return Err(Error::Unsupported(
            "ConsumerGroupDescribe returned invalid group or assignment epoch",
        ));
    }
    if description.members().len() != 1 {
        return Err(Error::Unsupported(
            "published ConsumerGroupDescribe expected exactly one member",
        ));
    }

    let member = description
        .members()
        .iter()
        .find(|member| member.member_id() == member_id)
        .ok_or(Error::Unsupported(
            "ConsumerGroupDescribe did not return the joined member",
        ))?;
    if member.member_type() != 1 {
        return Err(Error::Unsupported(
            "ConsumerGroupDescribe did not identify a consumer-protocol member",
        ));
    }
    if member.member_epoch() != member_epoch {
        return Err(Error::Unsupported(
            "ConsumerGroupDescribe member epoch did not match the joined member",
        ));
    }
    if !assignment_contains_partition(member.assignment(), &topic, 0)
        || !assignment_contains_partition(member.target_assignment(), &topic, 0)
    {
        return Err(Error::UnknownTopicOrPartition {
            topic,
            partition: 0,
        });
    }

    println!(
        "modern group group_id={} state={} group_epoch={} assignment_epoch={} assignor={} member_id={} member_type={} member_epoch={} assignment_topic={} assignment_partition=0 target_topic={} authorized_operations={}",
        description.group_id(),
        description.state(),
        description.group_epoch(),
        description.assignment_epoch(),
        description.assignor_name(),
        member.member_id(),
        member.member_type(),
        member.member_epoch(),
        topic,
        topic,
        description.authorized_operations(),
    );

    group.leave().await?;
    Ok(())
}

fn assignment_contains_partition(
    assignment: &kafrust::ModernConsumerGroupAssignment,
    topic: &str,
    partition: i32,
) -> bool {
    assignment
        .topic_partitions()
        .iter()
        .any(|entry| entry.topic_name() == topic && entry.partitions().contains(&partition))
}

fn required_env(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| {
        Error::Unsupported(match name {
            "KAFRUST_GROUP_ID" => "KAFRUST_GROUP_ID must be set",
            "KAFRUST_TOPIC" => "KAFRUST_TOPIC must be set",
            _ => "required environment variable must be set",
        })
    })
}
