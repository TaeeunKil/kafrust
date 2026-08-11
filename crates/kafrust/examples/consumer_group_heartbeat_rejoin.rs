mod common;

use std::time::Duration;

use kafrust::{ConsumerGroupConfig, ConsumerGroupProtocol, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(25), run_rejoin_scenario())
        .await
        .map_err(|_| Error::Unsupported("background heartbeat rejoin scenario timed out"))?
}

async fn run_rejoin_scenario() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id =
        std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-heartbeat-rejoin".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let mut config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .session_timeout_ms(6_000)
            .rebalance_timeout_ms(10_000)
            .max_wait_ms(100)
            .subscribe(topic),
    )?;
    let protocol = std::env::var("KAFRUST_GROUP_PROTOCOL").unwrap_or_else(|_| "classic".to_owned());
    config = match protocol.to_ascii_lowercase().as_str() {
        "classic" => config,
        "consumer" | "kip-848" => config.group_protocol(ConsumerGroupProtocol::Consumer),
        _ => {
            return Err(Error::Unsupported(
                "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
            ))
        }
    };

    let mut first = config
        .clone()
        .client_id("kafrust-heartbeat-rejoin-first")
        .join()
        .await?;
    let initial_generation = first.generation_id();
    let mut heartbeat = first
        .spawn_heartbeat_task(Duration::from_millis(100))
        .await?;

    let second_join = tokio::spawn(config.client_id("kafrust-heartbeat-rejoin-second").join());
    while !second_join.is_finished() {
        first.poll_with_heartbeat(&mut heartbeat).await?;
    }
    let second = second_join.await??;

    first.poll_with_heartbeat(&mut heartbeat).await?;
    if first.generation_id() == initial_generation {
        return Err(Error::Unsupported(
            "consumer group generation did not change during rebalance",
        ));
    }
    if heartbeat.group_id() != first.group_id()
        || heartbeat.member_id() != first.member_id()
        || heartbeat.generation_id() != first.generation_id()
    {
        return Err(Error::Unsupported(
            "background heartbeat handle was not replaced after rejoin",
        ));
    }

    println!(
        "rejoined group {} generation {} -> {} as {} with restarted {}ms heartbeat",
        first.group_id(),
        initial_generation,
        first.generation_id(),
        first.member_id(),
        heartbeat.interval().as_millis()
    );

    heartbeat.stop().await?;
    first.leave().await?;
    second.leave().await?;
    Ok(())
}
