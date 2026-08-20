use kafrust::{AdminClient, ClientConfig, Error, ShareAcquireMode, ShareConsumerConfig};
use std::time::{Duration, Instant};

fn required_env(name: &'static str) -> kafrust::Result<String> {
    std::env::var(name).map_err(|_| Error::InvalidConfiguration {
        field: name,
        reason: "published ShareGroupDescribe environment variable is required",
    })
}

fn bootstrap_servers() -> kafrust::Result<Vec<String>> {
    let value = required_env("KAFRUST_BOOTSTRAP_SERVERS")?;
    let servers = value
        .split(',')
        .map(str::trim)
        .filter(|server| !server.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if servers.is_empty() {
        return Err(Error::InvalidConfiguration {
            field: "KAFRUST_BOOTSTRAP_SERVERS",
            reason: "at least one bootstrap server is required",
        });
    }
    Ok(servers)
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = bootstrap_servers()?;
    let topic = required_env("KAFRUST_SHARE_TOPIC")?;
    let group_id = required_env("KAFRUST_SHARE_GROUP_ID")?;

    let mut consumer = ShareConsumerConfig::new(bootstrap_servers.clone(), group_id.clone())
        .client_id("kafrust-published-share-group-describe-member")
        .subscribe(topic.clone())
        .max_wait_ms(100)
        .max_records(1)
        .batch_size(1)
        .max_retries(10)
        .acquire_mode(ShareAcquireMode::RecordLimit)
        .build()
        .await?;
    let member_id = consumer.member_id().to_owned();

    let admin = AdminClient::new(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-published-share-group-describe"),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let description = loop {
        consumer.heartbeat().await?;
        let member_epoch = consumer.member_epoch();
        let descriptions = admin
            .describe_share_groups(std::slice::from_ref(&group_id), true)
            .await?;
        let description = descriptions
            .into_iter()
            .find(|description| description.group_id() == group_id)
            .ok_or_else(|| Error::MissingGroupDescription {
                group_id: group_id.clone(),
            })?;

        if description.is_success()
            && !description.state().is_empty()
            && description.group_epoch() >= 0
            && description.assignment_epoch() >= 0
            && description.members().len() == 1
            && description.members()[0].member_id() == member_id
            && description.members()[0].member_epoch() == member_epoch
            && assignment_contains_partition(
                description.members()[0].assignment(),
                &topic,
                0,
            )
        {
            break description;
        }

        if Instant::now() >= deadline {
            return Err(Error::RequestTimedOut { timeout_ms: 30_000 });
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    };

    let member = &description.members()[0];
    println!(
        "share group group_id={} state={} group_epoch={} assignment_epoch={} assignor={} member_id={} member_epoch={} assignment_topic={} assignment_partition=0 subscribed_topic={} authorized_operations={}",
        description.group_id(),
        description.state(),
        description.group_epoch(),
        description.assignment_epoch(),
        description.assignor_name(),
        member.member_id(),
        member.member_epoch(),
        topic,
        member.subscribed_topic_names().iter().any(|name| name == &topic),
        description.authorized_operations(),
    );

    consumer.stop_heartbeat_task().await?;
    consumer.close().await?;
    Ok(())
}

fn assignment_contains_partition(
    assignment: &kafrust::ShareGroupAssignment,
    topic: &str,
    partition: i32,
) -> bool {
    assignment
        .topic_partitions()
        .iter()
        .any(|entry| entry.topic_name() == topic && entry.partitions().contains(&partition))
}
