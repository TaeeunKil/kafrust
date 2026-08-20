mod common;

use kafrust::streams::{StreamsGroupHeartbeatSubtopology, StreamsGroupHeartbeatTopology};
use kafrust::{
    BrokerErrorKind, Client, ClientConfig, Error, StreamsGroupConfig, StreamsGroupSession,
};
use std::time::Duration;

const COORDINATOR_LOOKUP_ATTEMPTS: usize = 120;

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(90), run_scenario())
        .await
        .map_err(|_| Error::RequestTimedOut { timeout_ms: 90_000 })?
}

async fn run_scenario() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_STREAMS_TOPIC")
        .unwrap_or_else(|_| "kafrust-streams-failover".to_owned());
    let group_id = std::env::var("KAFRUST_STREAMS_FAILOVER_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-streams-failover".to_owned());
    let pause = pause_from_env()?;
    let base_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-streams-failover"),
    )?;
    let mut bootstrap = base_config.clone().connect().await?;
    let coordinator = find_coordinator_with_retry(&mut bootstrap, &group_id).await?;
    let config = StreamsGroupConfig::new(bootstrap_servers, group_id, topology(topic))
        .client_config(base_config)
        .client_id("kafrust-streams-failover-member")
        .process_id("kafrust-streams-failover-process")
        .rebalance_timeout_ms(30_000)
        .max_retries(100);

    let session = StreamsGroupSession::join(config).await?;
    println!(
        "streams group failover joined member_id={} member_epoch={} coordinator node {}",
        session.member_id(),
        session.member_epoch(),
        coordinator.node_id
    );
    let handle = session.spawn_heartbeat_task();

    let before = handle.heartbeat_now().await?;
    ensure_success(&before.error_code, "before coordinator failover")?;
    println!(
        "streams group failover before heartbeat member_epoch={}",
        before.member_epoch
    );
    if !pause.is_zero() {
        println!("streams group failover pause {}ms", pause.as_millis());
        tokio::time::sleep(pause).await;
    }

    let after = handle.heartbeat_now().await?;
    ensure_success(&after.error_code, "after coordinator failover")?;
    if after.member_epoch <= 0 {
        return Err(Error::Unsupported(
            "Streams coordinator failover returned a non-member epoch",
        ));
    }
    println!(
        "streams group failover after heartbeat member_epoch={}",
        after.member_epoch
    );
    handle.close().await?;
    println!("streams group failover left group cleanly");
    Ok(())
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

async fn find_coordinator_with_retry(
    bootstrap: &mut Client,
    group_id: &str,
) -> kafrust::Result<kafrust::protocol::api::find_coordinator::FindCoordinatorResponseV1> {
    for attempt in 0..COORDINATOR_LOOKUP_ATTEMPTS {
        let response = bootstrap.find_group_coordinator(group_id).await?;
        if response.error_code == 0 {
            return Ok(response);
        }
        let retryable = matches!(
            BrokerErrorKind::from_code(response.error_code),
            BrokerErrorKind::CoordinatorLoadInProgress
                | BrokerErrorKind::CoordinatorNotAvailable
                | BrokerErrorKind::NotCoordinator
        );
        if !retryable || attempt + 1 == COORDINATOR_LOOKUP_ATTEMPTS {
            return Err(Error::Broker {
                code: response.error_code,
                context: "find Streams group failover coordinator".to_owned(),
            });
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    Err(Error::RequestTimedOut { timeout_ms: 30_000 })
}

fn pause_from_env() -> kafrust::Result<Duration> {
    std::env::var("KAFRUST_FAILOVER_PAUSE_MS")
        .map(|value| {
            value
                .trim()
                .parse::<u64>()
                .map(Duration::from_millis)
                .map_err(|_| Error::Unsupported("KAFRUST_FAILOVER_PAUSE_MS must be milliseconds"))
        })
        .unwrap_or(Ok(Duration::ZERO))
}

fn ensure_success(error_code: &i16, phase: &str) -> kafrust::Result<()> {
    if *error_code == 0 {
        return Ok(());
    }
    Err(Error::Broker {
        code: *error_code,
        context: format!("Streams group heartbeat {phase}"),
    })
}
