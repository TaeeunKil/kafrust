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

    let session = StreamsGroupSession::join(config).await?;
    println!(
        "joined streams group member_id={} member_epoch={} heartbeat_interval_ms={}",
        session.member_id(),
        session.member_epoch(),
        session.heartbeat_interval().as_millis()
    );

    let handle = session.spawn_heartbeat_task();
    handle
        .set_task_state(
            vec![StreamsGroupHeartbeatTask {
                subtopology_id: "subtopology-0".to_owned(),
                partitions: Vec::new(),
            }],
            Vec::new(),
            Vec::new(),
            None,
            None,
        )
        .await?;

    let mut assignments = handle.subscribe_assignment();
    tokio::time::timeout(Duration::from_secs(10), assignments.changed())
        .await
        .map_err(|_| kafrust::Error::RequestTimedOut { timeout_ms: 10_000 })?
        .map_err(|_| kafrust::Error::StreamsGroupBackgroundTaskClosed)?;
    let assignment = handle.assignment();
    println!(
        "background heartbeat received assignment active_tasks={} task_offset_interval_ms={}",
        assignment.active_tasks.as_ref().map_or(0, Vec::len),
        assignment.task_offset_interval_ms
    );
    handle.close().await?;
    println!("left streams group cleanly");
    Ok(())
}
