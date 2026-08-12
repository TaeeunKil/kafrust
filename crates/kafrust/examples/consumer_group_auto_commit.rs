mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use kafrust::{ConsumerGroupConfig, ConsumerGroupProtocol, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-auto-commit-smoke".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let interval_ms = std::env::var("KAFRUST_AUTO_COMMIT_INTERVAL_MS")
        .unwrap_or_else(|_| "100".to_owned())
        .parse::<u64>()
        .map_err(|_| Error::Unsupported("KAFRUST_AUTO_COMMIT_INTERVAL_MS must be an integer"))?;
    let require_record = std::env::var("KAFRUST_AUTO_COMMIT_REQUIRE_RECORD")
        .is_ok_and(|value| value == "1" || value.eq_ignore_ascii_case("true"));

    let mut config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-consumer-group-auto-commit")
            .enable_auto_commit(true)
            .auto_commit_interval_ms(interval_ms),
    )?;
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

    let mut first_group = config.clone().subscribe(topic.clone()).join().await?;
    let records = first_group.poll().await?;
    if require_record && records.is_empty() {
        return Err(Error::Unsupported(
            "automatic commit smoke expected at least one record",
        ));
    }

    let expected_positions = first_group
        .assignments()
        .iter()
        .map(|assignment| {
            (
                (assignment.topic().to_owned(), assignment.partition()),
                assignment.next_offset(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let wait_ms = interval_ms.saturating_mul(3).max(100);
    tokio::time::sleep(Duration::from_millis(wait_ms)).await;
    if first_group.pending_commit_count() != 0 {
        return Err(Error::Unsupported(
            "automatic consumer group commit did not flush",
        ));
    }
    first_group.leave().await?;

    let mut second_group = config
        .enable_auto_commit(false)
        .subscribe(topic)
        .join()
        .await?;
    if second_group.group_protocol() == ConsumerGroupProtocol::Consumer {
        for _ in 0..10 {
            if !second_group.assignments().is_empty() {
                break;
            }
            second_group.heartbeat().await?;
            if second_group.assignments().is_empty() {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    let observed_positions = second_group
        .assignments()
        .iter()
        .map(|assignment| {
            (
                (assignment.topic().to_owned(), assignment.partition()),
                assignment.next_offset(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if expected_positions != observed_positions {
        eprintln!(
            "automatic commit positions differ: expected={expected_positions:?} observed={observed_positions:?}"
        );
        return Err(Error::Unsupported(
            "automatic consumer group commit position was not restored",
        ));
    }

    println!(
        "automatic commit restored {} partition positions for group {}",
        observed_positions.len(),
        second_group.group_id()
    );
    second_group.leave().await?;
    Ok(())
}
