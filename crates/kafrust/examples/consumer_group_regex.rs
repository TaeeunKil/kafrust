mod common;

use std::collections::BTreeSet;

use kafrust::{ConsumerGroupConfig, Error};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-regex-consumer-group".to_owned());
    let pattern = std::env::var("KAFRUST_TOPIC_PATTERN")
        .unwrap_or_else(|_| r"^kafrust-regex-(orders|payments)$".to_owned());
    let expected_topics = expected_topics_from_env();

    let config = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .client_id("kafrust-consumer-group-regex")
            .subscribe_pattern(pattern.clone()),
    )?;
    let mut group = config.join().await?;
    let assigned_topics = group
        .assignments()
        .iter()
        .map(|assignment| assignment.topic().to_owned())
        .collect::<BTreeSet<_>>();

    if !expected_topics.is_empty() && !expected_topics.is_subset(&assigned_topics) {
        return Err(Error::Unsupported(
            "regex subscription did not assign every expected topic",
        ));
    }
    if assigned_topics.is_empty() {
        return Err(Error::Unsupported(
            "regex subscription produced no assignments",
        ));
    }

    let records = group.poll().await?;
    if records.is_empty() {
        return Err(Error::Unsupported(
            "regex subscription did not fetch the smoke record",
        ));
    }
    for record in &records {
        group.commit_record(record)?;
    }
    group.commit_queued_offsets().await?;
    if group.pending_commit_count() != 0 {
        return Err(Error::Unsupported(
            "regex subscription left queued offsets after commit",
        ));
    }

    group.rejoin().await?;
    let rejoined_topics = group
        .assignments()
        .iter()
        .map(|assignment| assignment.topic().to_owned())
        .collect::<BTreeSet<_>>();
    if !expected_topics.is_empty() && !expected_topics.is_subset(&rejoined_topics) {
        return Err(Error::Unsupported(
            "regex subscription rejoin did not assign every expected topic",
        ));
    }
    if rejoined_topics.is_empty() {
        return Err(Error::Unsupported(
            "regex subscription rejoin produced no assignments",
        ));
    }

    println!(
        "regex subscription {pattern:?} assigned topics: {} then {}",
        assigned_topics.into_iter().collect::<Vec<_>>().join(","),
        rejoined_topics.into_iter().collect::<Vec<_>>().join(",")
    );
    println!("polled {} records", records.len());
    group.leave().await?;
    Ok(())
}

fn expected_topics_from_env() -> BTreeSet<String> {
    std::env::var("KAFRUST_REGEX_EXPECTED_TOPICS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|topic| !topic.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
