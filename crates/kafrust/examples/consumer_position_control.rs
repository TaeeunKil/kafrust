mod common;

use kafrust::{ConsumerConfig, ConsumerGroupConfig, Error, OffsetResetPolicy};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id =
        std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-position-control".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers.clone()).client_id("kafrust-position-control-direct"),
    )?
    .build()
    .await?;
    consumer.assign(&topic, 0, 0);
    consumer.pause(&topic, 0)?;
    if !consumer.poll().await?.is_empty() {
        return Err(Error::Unsupported(
            "paused direct consumer returned records",
        ));
    }
    consumer.seek(&topic, 0, 0)?;
    consumer.resume(&topic, 0)?;
    let direct_records = consumer.poll().await?;
    if direct_records.is_empty()
        || !consumer
            .position(&topic, 0)
            .is_some_and(|offset| offset > 0)
    {
        return Err(Error::Unsupported(
            "resumed direct consumer did not advance position",
        ));
    }

    let mut group = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .client_id("kafrust-position-control-group"),
    )?
    .offset_reset_policy(OffsetResetPolicy::Earliest)
    .subscribe(&topic)
    .join()
    .await?;
    let assigned = group
        .assignments()
        .iter()
        .map(|assignment| (assignment.topic().to_owned(), assignment.partition()))
        .collect::<Vec<_>>();
    for (assigned_topic, partition) in &assigned {
        group.pause(assigned_topic, *partition)?;
    }
    if !group.poll().await?.is_empty() {
        return Err(Error::Unsupported("paused consumer group returned records"));
    }
    let (assigned_topic, partition) = assigned
        .first()
        .ok_or(Error::Unsupported("consumer group has no assignment"))?;
    group.seek(assigned_topic, *partition, 0)?;
    for (assigned_topic, partition) in &assigned {
        group.resume(assigned_topic, *partition)?;
    }
    let group_records = group.poll().await?;
    if group_records.is_empty()
        || !group
            .position(assigned_topic, *partition)
            .is_some_and(|offset| offset > 0)
    {
        return Err(Error::Unsupported(
            "resumed consumer group did not advance position",
        ));
    }
    group.leave().await?;

    println!("verified direct and group consumer seek, pause, resume, and position");
    Ok(())
}
