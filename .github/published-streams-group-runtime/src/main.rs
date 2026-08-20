use kafrust::streams::{
    StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTopology,
};
use kafrust::{
    StreamsGroupConfig, StreamsGroupSession, StreamsTaskRuntime,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = std::env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .unwrap_or_else(|_| "localhost:9092".to_owned());
    let topic = std::env::var("KAFRUST_STREAMS_TOPIC")
        .unwrap_or_else(|_| "kafrust-published-streams-runtime".to_owned());
    let group_id = std::env::var("KAFRUST_STREAMS_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-published-streams-runtime".to_owned());

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

    let session = StreamsGroupSession::join(StreamsGroupConfig::new(
        [bootstrap_servers],
        group_id,
        topology,
    ))
    .await?;
    println!(
        "published streams runtime joined member_id={} member_epoch={}",
        session.member_id(),
        session.member_epoch()
    );

    let handle = session.spawn_heartbeat_task();
    let mut task_runtime = StreamsTaskRuntime::new();
    handle
        .set_task_state(Vec::new(), Vec::new(), Vec::new(), None, None)
        .await?;
    let mut assignments = handle.subscribe_assignment();
    tokio::time::timeout(Duration::from_secs(10), assignments.changed())
        .await
        .map_err(|_| kafrust::Error::RequestTimedOut { timeout_ms: 10_000 })?
        .map_err(|_| kafrust::Error::StreamsGroupBackgroundTaskClosed)?;
    let transitions = handle.reconcile_task_runtime(&mut task_runtime)?;
    println!(
        "published streams runtime heartbeat assignment transitions={}",
        transitions.len()
    );
    handle.close().await?;
    println!("published streams runtime left group cleanly");
    Ok(())
}
