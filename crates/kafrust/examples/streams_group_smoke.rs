mod common;

use kafrust::streams::{
    StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTask, StreamsGroupHeartbeatTopology,
};
use kafrust::{ClientConfig, StreamsGroupConfig, StreamsGroupSession};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_STREAMS_TOPIC")
        .unwrap_or_else(|_| "kafrust-streams-smoke".to_owned());
    let group_id = std::env::var("KAFRUST_STREAMS_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-streams-smoke".to_owned());
    let process_id = std::env::var("KAFRUST_STREAMS_PROCESS_ID")
        .unwrap_or_else(|_| "kafrust-streams-smoke-process".to_owned());

    let client = common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-streams-group-smoke"),
    )?;
    let topology = StreamsGroupHeartbeatTopology {
        epoch: 1,
        subtopologies: vec![StreamsGroupHeartbeatSubtopology {
            subtopology_id: "subtopology-0".to_owned(),
            source_topics: vec![topic],
            source_topic_regex: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_sink_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    };
    let config = StreamsGroupConfig::new(client.bootstrap_servers().to_vec(), group_id, topology)
        .client_config(client)
        .process_id(process_id)
        .rebalance_timeout_ms(30_000)
        .max_retries(3);

    let mut session = StreamsGroupSession::join(config).await?;
    println!(
        "joined streams group member_id={} member_epoch={} heartbeat_interval_ms={}",
        session.member_id(),
        session.member_epoch(),
        session.heartbeat_interval().as_millis()
    );

    session.set_task_state_with_optional_offsets(
        vec![StreamsGroupHeartbeatTask {
            subtopology_id: "subtopology-0".to_owned(),
            partitions: Vec::new(),
        }],
        Vec::new(),
        Vec::new(),
        None,
        None,
    );
    session.heartbeat().await?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    session.heartbeat().await?;
    session.close().await?;
    println!("left streams group cleanly");
    Ok(())
}
