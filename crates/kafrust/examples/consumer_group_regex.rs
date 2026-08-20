mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use kafrust::{
    AdminClient, ClientConfig, ConsumerGroupConfig, CreateTopicsOptions, Error, NewTopic,
    ProducerConfig, ProducerRecord,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id = std::env::var("KAFRUST_GROUP_ID")
        .unwrap_or_else(|_| "kafrust-regex-consumer-group".to_owned());
    let pattern = std::env::var("KAFRUST_TOPIC_PATTERN")
        .unwrap_or_else(|_| r"^kafrust-regex-(orders|payments)$".to_owned());
    let expected_topics = expected_topics_from_env();

    let client_config = common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-consumer-group-regex"),
    )?;
    let config = ConsumerGroupConfig::new(bootstrap_servers.clone(), group_id)
        .with_client_config(client_config.clone())
        .subscribe_pattern(pattern.clone());
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

    let mut commit_worker = if std::env::var_os("KAFRUST_COMMIT_WORKER").is_some() {
        let interval_ms = std::env::var("KAFRUST_COMMIT_WORKER_INTERVAL_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(100);
        Some(
            group
                .spawn_commit_worker(Duration::from_millis(interval_ms))
                .await?,
        )
    } else {
        None
    };

    let records = group.poll().await?;
    if records.is_empty() {
        return Err(Error::Unsupported(
            "regex subscription did not fetch the smoke record",
        ));
    }
    for record in &records {
        group.commit_record(record)?;
    }
    if let Some(worker) = &mut commit_worker {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if group.pending_commit_count() == 0 {
                break;
            }
            if worker.try_wait().await?.is_some() {
                return Err(Error::Unsupported(
                    "background commit worker stopped before flushing offsets",
                ));
            }
            if Instant::now() >= deadline {
                return Err(Error::Unsupported(
                    "background commit worker did not flush offsets",
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        group.commit_queued_offsets().await?;
    } else {
        group.commit_queued_offsets().await?;
    }
    if group.pending_commit_count() != 0 {
        return Err(Error::Unsupported(
            "regex subscription left queued offsets after commit",
        ));
    }

    if let Some(dynamic_topic) = std::env::var_os("KAFRUST_REGEX_DYNAMIC_TOPIC") {
        let dynamic_topic = dynamic_topic
            .into_string()
            .map_err(|_| Error::Unsupported("KAFRUST_REGEX_DYNAMIC_TOPIC is not valid UTF-8"))?;
        let admin = AdminClient::new(client_config.clone());
        let result = admin
            .create_topics(
                &[NewTopic::new(&dynamic_topic, 1, 1)],
                CreateTopicsOptions::new(),
            )
            .await?;
        for topic in result.topics() {
            if !topic.is_success() && topic.error_code() != 36 {
                return Err(Error::Broker {
                    code: topic.error_code(),
                    context: format!("create dynamic regex topic {}", topic.name()),
                });
            }
        }

        let mut producer = ProducerConfig::new(bootstrap_servers.clone())
            .with_client_config(client_config.clone())
            .build()
            .await?;
        producer
            .send(
                ProducerRecord::to(&dynamic_topic)
                    .key("kafrust-regex-dynamic-key")
                    .value("kafrust-regex-dynamic-value"),
            )
            .await?;

        let deadline = Instant::now() + Duration::from_secs(30);
        let dynamic_records = loop {
            let records = group.poll().await?;
            if records.iter().any(|record| record.topic() == dynamic_topic) {
                break records;
            }
            if Instant::now() >= deadline {
                return Err(Error::Unsupported(
                    "regex subscription did not receive the dynamically created topic",
                ));
            }
        };
        for record in &dynamic_records {
            group.commit_record(record)?;
        }
        group.commit_queued_offsets().await?;
        println!(
            "regex subscription received dynamic topic {dynamic_topic:?} in {} record(s)",
            dynamic_records.len()
        );
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

    if let Some(worker) = commit_worker {
        worker.stop().await?;
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
