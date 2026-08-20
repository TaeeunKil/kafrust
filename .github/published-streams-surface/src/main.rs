use kafrust::streams::{StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTopology};
use kafrust::{StreamsGroupConfig, StreamsGroupSession};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let topology = StreamsGroupHeartbeatTopology {
        epoch: 1,
        subtopologies: vec![StreamsGroupHeartbeatSubtopology {
            subtopology_id: "subtopology-0".to_owned(),
            source_topics: vec!["orders".to_owned()],
            source_topic_regex: Vec::new(),
            state_changelog_topics: Vec::new(),
            repartition_sink_topics: Vec::new(),
            repartition_source_topics: Vec::new(),
            copartition_groups: Vec::new(),
        }],
    };

    let session = StreamsGroupSession::join(StreamsGroupConfig::new(
        ["localhost:9092"],
        "orders-streams",
        topology,
    ))
    .await?;
    let handle = session.spawn_heartbeat_task();
    let _assignment = handle.assignment();
    let _updates = handle.subscribe_assignment();
    handle.close().await?;
    Ok(())
}
