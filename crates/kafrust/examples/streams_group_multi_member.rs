mod common;

use kafrust::streams::{StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTopology};
use kafrust::{AdminClient, ClientConfig, Error, StreamsGroupConfig, StreamsGroupSession};
use std::time::Duration;

const MEMBER_WAIT_ATTEMPTS: usize = 80;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(60), run_scenario())
        .await
        .map_err(|_| Error::RequestTimedOut { timeout_ms: 60_000 })?
}

async fn run_scenario() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_STREAMS_TOPIC")
        .unwrap_or_else(|_| "kafrust-streams-multi-member".to_owned());
    let group_id = std::env::var("KAFRUST_STREAMS_MULTI_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-streams-multi-member".to_owned());
    let base_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-streams-multi-member"),
    )?;
    let admin = AdminClient::new(
        base_config
            .clone()
            .client_id("kafrust-streams-multi-member-admin"),
    );
    let topology = topology(topic);

    let first = StreamsGroupSession::join(streams_config(
        &base_config,
        &bootstrap_servers,
        &group_id,
        &topology,
        "kafrust-streams-multi-first",
        "kafrust-streams-multi-first-process",
    ))
    .await?;
    let first_member_id = first.member_id().to_owned();
    let first_handle = first.spawn_heartbeat_task();
    println!("first streams member joined member_id={first_member_id}");

    let second = StreamsGroupSession::join(streams_config(
        &base_config,
        &bootstrap_servers,
        &group_id,
        &topology,
        "kafrust-streams-multi-second",
        "kafrust-streams-multi-second-process",
    ))
    .await?;
    let second_member_id = second.member_id().to_owned();
    let second_handle = second.spawn_heartbeat_task();
    wait_for_at_least_members(&admin, &group_id, 2).await?;
    println!(
        "streams multi-member established members=2 first={first_member_id} second={second_member_id}"
    );

    second_handle.close().await?;
    wait_for_at_most_members(&admin, &group_id, 1).await?;
    println!("streams member departure converged remaining_members=1");

    first_handle.close().await?;
    println!("streams multi-member lifecycle left cleanly");
    Ok(())
}

fn streams_config(
    base_config: &ClientConfig,
    bootstrap_servers: &[String],
    group_id: &str,
    topology: &StreamsGroupHeartbeatTopology,
    client_id: &str,
    process_id: &str,
) -> StreamsGroupConfig {
    StreamsGroupConfig::new(
        bootstrap_servers.to_owned(),
        group_id.to_owned(),
        topology.clone(),
    )
    .client_config(base_config.clone())
    .client_id(client_id)
    .process_id(process_id)
    .rebalance_timeout_ms(30_000)
    .max_retries(5)
}

fn topology(topic: String) -> StreamsGroupHeartbeatTopology {
    StreamsGroupHeartbeatTopology {
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
    }
}

async fn wait_for_at_least_members(
    admin: &AdminClient,
    group_id: &str,
    expected: usize,
) -> kafrust::Result<()> {
    for _ in 0..MEMBER_WAIT_ATTEMPTS {
        if describe_member_count(admin, group_id).await? >= expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(Error::Unsupported(
        "Streams group did not converge on the expected multi-member count",
    ))
}

async fn wait_for_at_most_members(
    admin: &AdminClient,
    group_id: &str,
    expected: usize,
) -> kafrust::Result<()> {
    for _ in 0..MEMBER_WAIT_ATTEMPTS {
        if describe_member_count(admin, group_id).await? <= expected {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(Error::Unsupported(
        "Streams group did not converge after member departure",
    ))
}

async fn describe_member_count(admin: &AdminClient, group_id: &str) -> kafrust::Result<usize> {
    let descriptions = admin
        .describe_streams_groups(&[group_id.to_owned()], false)
        .await?;
    let description = descriptions
        .first()
        .ok_or_else(|| Error::MissingGroupDescription {
            group_id: group_id.to_owned(),
        })?;
    if !description.is_success() {
        return Err(Error::Broker {
            code: description.error_code(),
            context: format!("describe Streams group {group_id}"),
        });
    }
    Ok(description.members().len())
}
