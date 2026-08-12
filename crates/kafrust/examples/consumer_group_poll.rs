mod common;

use kafrust::{ConsumerGroupAssignmentStrategy, ConsumerGroupConfig, ConsumerGroupProtocol, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let mut config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id).client_id("kafrust-consumer-group"),
    )?;
    if let Ok(group_instance_id) = std::env::var("KAFRUST_GROUP_INSTANCE_ID") {
        config = config.group_instance_id(group_instance_id);
    }
    if let Ok(strategy) = std::env::var("KAFRUST_ASSIGNMENT_STRATEGY") {
        config =
            config.assignment_strategy(match strategy.to_ascii_lowercase().as_str() {
                "range" => ConsumerGroupAssignmentStrategy::Range,
                "roundrobin" | "round-robin" => ConsumerGroupAssignmentStrategy::RoundRobin,
                "cooperative-sticky" | "cooperative_sticky" => {
                    ConsumerGroupAssignmentStrategy::CooperativeSticky
                }
                _ => return Err(Error::Unsupported(
                    "KAFRUST_ASSIGNMENT_STRATEGY must be range, roundrobin, or cooperative-sticky",
                )),
            });
    }
    if let Ok(protocol) = std::env::var("KAFRUST_GROUP_PROTOCOL") {
        config = config.group_protocol(match protocol.to_ascii_lowercase().as_str() {
            "classic" => ConsumerGroupProtocol::Classic,
            "consumer" | "kip-848" => ConsumerGroupProtocol::Consumer,
            _ => {
                return Err(Error::Unsupported(
                    "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
                ))
            }
        });
    }
    let mut group = config.subscribe(topic).join().await?;

    let use_partition_queue = std::env::var("KAFRUST_PARTITION_QUEUE")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let require_partition_queue_record = std::env::var("KAFRUST_PARTITION_QUEUE_REQUIRE_RECORD")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));
    let mut partition_queue = if use_partition_queue {
        let assignment = group
            .assignments()
            .first()
            .ok_or(Error::Unsupported("consumer group has no assignment"))?;
        let topic = assignment.topic().to_owned();
        let partition = assignment.partition();
        Some(group.split_partition_queue(topic, partition)?)
    } else {
        None
    };

    println!(
        "joined group {} as member {} generation {} instance {:?} with {} assignments",
        group.group_id(),
        group.member_id(),
        group.generation_id(),
        group.metadata().group_instance_id(),
        group.assignments().len()
    );

    let records = group.poll().await?;
    let mut queued_records = Vec::new();
    if let Some(queue) = partition_queue.as_mut() {
        while let Some(record) = queue.try_recv() {
            queued_records.push(record);
        }
    }
    for record in &records {
        println!(
            "{}-{}@{}",
            record.topic(),
            record.partition(),
            record.offset()
        );
    }
    if require_partition_queue_record && queued_records.is_empty() {
        return Err(Error::Unsupported(
            "consumer group partition queue smoke expected at least one record",
        ));
    }
    for record in &queued_records {
        println!(
            "queued {}-{}@{}",
            record.topic(),
            record.partition(),
            record.offset()
        );
    }
    group.commit_offsets().await?;
    println!(
        "committed offsets for {} polled and {} queued records",
        records.len(),
        queued_records.len()
    );
    group.leave().await?;
    println!("left consumer group");

    Ok(())
}
