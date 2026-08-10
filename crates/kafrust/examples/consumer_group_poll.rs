mod common;

use kafrust::{ConsumerGroupAssignmentStrategy, ConsumerGroupConfig, Error};

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
    let mut group = config.subscribe(topic).join().await?;

    println!(
        "joined group {} as member {} generation {} instance {:?} with {} assignments",
        group.group_id(),
        group.member_id(),
        group.generation_id(),
        group.metadata().group_instance_id(),
        group.assignments().len()
    );

    let records = group.poll().await?;
    for record in &records {
        println!(
            "{}-{}@{}",
            record.topic(),
            record.partition(),
            record.offset()
        );
    }
    group.commit_offsets().await?;
    println!("committed offsets for {} records", records.len());
    group.leave().await?;
    println!("left consumer group");

    Ok(())
}
