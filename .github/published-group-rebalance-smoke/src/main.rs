use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::time::Duration;

use kafrust::{
    Acks, ConsumerGroup, ConsumerGroupConfig, ConsumerGroupProtocol, Error, OffsetResetPolicy,
    ProducerConfig, ProducerRecord,
};

const PARTITION_COUNT: i32 = 6;
const POLL_ATTEMPTS: usize = 80;
const MAX_CHURN_CYCLES: usize = 100;

fn required(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| Error::Unsupported("published group smoke variable missing"))
}

fn group_protocol() -> Result<ConsumerGroupProtocol, Error> {
    match env::var("KAFRUST_GROUP_PROTOCOL")
        .unwrap_or_else(|_| "classic".to_owned())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "classic" => Ok(ConsumerGroupProtocol::Classic),
        "consumer" | "kip-848" => Ok(ConsumerGroupProtocol::Consumer),
        _ => Err(Error::Unsupported(
            "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
        )),
    }
}

fn member_exit_is_abrupt() -> Result<bool, Error> {
    match env::var("KAFRUST_MEMBER_EXIT")
        .unwrap_or_else(|_| "leave".to_owned())
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "leave" => Ok(false),
        "drop" => Ok(true),
        _ => Err(Error::Unsupported(
            "KAFRUST_MEMBER_EXIT must be leave or drop",
        )),
    }
}

fn churn_cycles() -> Result<usize, Error> {
    let cycles = env::var("KAFRUST_GROUP_CHURN_CYCLES")
        .unwrap_or_else(|_| "1".to_owned())
        .parse::<usize>()
        .map_err(|_| Error::Unsupported("KAFRUST_GROUP_CHURN_CYCLES must be an integer"))?;
    if !(1..=MAX_CHURN_CYCLES).contains(&cycles) {
        return Err(Error::Unsupported(
            "KAFRUST_GROUP_CHURN_CYCLES must be between 1 and 100",
        ));
    }
    Ok(cycles)
}

fn group_config(
    bootstrap_servers: &str,
    group_id: &str,
    protocol: ConsumerGroupProtocol,
) -> ConsumerGroupConfig {
    ConsumerGroupConfig::new(
        bootstrap_servers.split(',').map(str::to_owned),
        group_id.to_owned(),
    )
    .group_protocol(protocol)
    .session_timeout_ms(6_000)
    .rebalance_timeout_ms(10_000)
    .max_wait_ms(100)
    .max_retries(20)
    .max_poll_records(20)
    .offset_reset_policy(OffsetResetPolicy::Earliest)
}

async fn run() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let group_id = required("KAFRUST_GROUP_ID")?;
    let protocol = group_protocol()?;
    let abrupt_member_exit = member_exit_is_abrupt()?;
    let cycles = churn_cycles()?;

    let mut producer = ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-group-rebalance-producer")
        .acks(Acks::Leader)
        .enable_idempotence(true)
        .build()
        .await?;
    for partition in 0..PARTITION_COUNT {
        let value = format!("published-group-rebalance-{partition}");
        producer
            .send(
                ProducerRecord::to(topic.clone())
                    .partition(partition)
                    .value(value.into_bytes()),
            )
            .await?;
    }

    for cycle in 0..cycles {
        let cycle_group_id = if cycles == 1 {
            group_id.clone()
        } else {
            format!("{group_id}-{cycle}")
        };
        let config =
            group_config(&bootstrap_servers, &cycle_group_id, protocol).subscribe(topic.clone());
        let mut first = config
            .clone()
            .client_id("kafrust-published-group-rebalance-first")
            .join()
            .await?;
        if first.assignments().is_empty() {
            return Err(Error::Unsupported(
                "published group smoke first member received no partitions",
            ));
        }

        let mut seen_records = BTreeMap::new();
        let second_join = tokio::spawn(
            config
                .clone()
                .client_id("kafrust-published-group-rebalance-second")
                .join(),
        );
        while !second_join.is_finished() {
            record_expected_records(&mut seen_records, &topic, first.poll().await?)?;
        }
        let mut second = second_join
            .await
            .map_err(|_| Error::Unsupported("published group smoke second member task failed"))??;

        wait_for_two_member_coverage(&mut first, &mut second, &topic, seen_records).await?;
        verify_position_survives_rejoin(&mut first, &topic).await?;
        if abrupt_member_exit {
            drop(second);
        } else {
            leave_after_member_rejoin(second).await?;
        }
        verify_member_departure_rejoin(&mut first, &topic).await?;
        verify_committed_offset_restore(&config, first, &topic).await?;
        println!(
            "published group churn cycle {}/{} passed protocol={protocol:?} exit={}",
            cycle + 1,
            cycles,
            if abrupt_member_exit { "drop" } else { "leave" },
        );
    }
    println!(
        "published group churn passed protocol={protocol:?} exit={} cycles={cycles} partitions={PARTITION_COUNT}",
        if abrupt_member_exit { "drop" } else { "leave" },
    );
    Ok(())
}

async fn leave_after_member_rejoin(group: ConsumerGroup) -> kafrust::Result<()> {
    match group.leave().await {
        Ok(()) => Ok(()),
        Err(Error::Broker { code: 25, .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

async fn wait_for_two_member_coverage(
    first: &mut ConsumerGroup,
    second: &mut ConsumerGroup,
    topic: &str,
    mut seen_records: BTreeMap<(String, i32), usize>,
) -> kafrust::Result<()> {
    let expected_assignments: BTreeSet<_> = (0..PARTITION_COUNT)
        .map(|partition| (topic.to_owned(), partition))
        .collect();
    let expected_records: BTreeMap<_, _> = (0..PARTITION_COUNT)
        .map(|partition| ((topic.to_owned(), partition), 1_usize))
        .collect();
    for _ in 0..POLL_ATTEMPTS {
        let (first_records, second_records) = poll_pair(first, second).await?;
        record_expected_records(&mut seen_records, topic, first_records)?;
        record_expected_records(&mut seen_records, topic, second_records)?;

        let first_partitions = assignment_keys(first);
        let second_partitions = assignment_keys(second);
        if !first_partitions.is_empty()
            && !second_partitions.is_empty()
            && first_partitions.is_disjoint(&second_partitions)
            && first_partitions
                .union(&second_partitions)
                .cloned()
                .collect::<BTreeSet<_>>()
                == expected_assignments
            && seen_records == expected_records
        {
            return Ok(());
        }
    }

    eprintln!(
        "published group smoke final state: first_assignments={:?} second_assignments={:?} seen_records={seen_records:?}",
        assignment_keys(first),
        assignment_keys(second),
    );

    Err(Error::Unsupported(
        "published group smoke members did not converge on disjoint ownership and records",
    ))
}

async fn verify_position_survives_rejoin(
    group: &mut ConsumerGroup,
    topic: &str,
) -> kafrust::Result<()> {
    let assignment = group
        .assignments()
        .iter()
        .find(|assignment| assignment.topic() == topic)
        .ok_or(Error::Unsupported(
            "published group smoke has no partition for position rejoin check",
        ))?;
    let partition = assignment.partition();
    let previous_position = assignment.next_offset();
    if previous_position <= 0 {
        return Err(Error::Unsupported(
            "published group smoke did not advance a position before rejoin",
        ));
    }

    group.seek(topic, partition, 0)?;
    group.rejoin().await?;
    if group.position(topic, partition) != Some(0) {
        return Err(Error::Unsupported(
            "published group smoke lost an explicit seek across rejoin",
        ));
    }
    Ok(())
}

async fn verify_member_departure_rejoin(
    group: &mut ConsumerGroup,
    topic: &str,
) -> kafrust::Result<()> {
    let expected: BTreeSet<_> = (0..PARTITION_COUNT)
        .map(|partition| (topic.to_owned(), partition))
        .collect();

    for _ in 0..POLL_ATTEMPTS {
        let _ = group.poll().await?;
        if assignment_keys(group) == expected {
            return Ok(());
        }
    }

    Err(Error::Unsupported(
        "published group smoke did not recover all partitions after member departure",
    ))
}

async fn verify_committed_offset_restore(
    config: &ConsumerGroupConfig,
    mut group: ConsumerGroup,
    topic: &str,
) -> kafrust::Result<()> {
    let expected: BTreeSet<_> = (0..PARTITION_COUNT)
        .map(|partition| (topic.to_owned(), partition))
        .collect();
    for _ in 0..POLL_ATTEMPTS {
        if assignment_keys(&group) == expected
            && group
                .assignments()
                .iter()
                .all(|assignment| assignment.next_offset() >= 1)
        {
            break;
        }
        let _ = group.poll().await?;
    }
    if assignment_keys(&group) != expected
        || !group
            .assignments()
            .iter()
            .all(|assignment| assignment.next_offset() >= 1)
    {
        return Err(Error::Unsupported(
            "published group smoke did not advance every assigned position before offset commit",
        ));
    }
    group.commit_offsets().await?;
    group.leave().await?;

    let mut replacement = config
        .clone()
        .client_id("kafrust-published-group-rebalance-restored")
        .join()
        .await?;
    for _ in 0..POLL_ATTEMPTS {
        if !replacement.poll().await?.is_empty() {
            return Err(Error::Unsupported(
                "published group smoke replayed a record after committed offset restore",
            ));
        }
        if replacement.assignments().len() == PARTITION_COUNT as usize
            && replacement
                .assignments()
                .iter()
                .all(|assignment| assignment.next_offset() >= 1)
        {
            replacement.leave().await?;
            return Ok(());
        }
        if replacement.assignments().is_empty() {
            // KIP-848 computes a new target assignment asynchronously after a
            // member joins, so give the coordinator time to push it.
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    let positions = replacement
        .assignments()
        .iter()
        .map(|assignment| (assignment.partition(), assignment.next_offset()))
        .collect::<Vec<_>>();
    eprintln!(
        "published group smoke committed-offset state protocol={:?} member={} generation={} assignments={positions:?}",
        replacement.group_protocol(),
        replacement.member_id(),
        replacement.generation_id(),
    );
    Err(Error::Unsupported(
        "published group smoke did not restore committed offsets for every partition",
    ))
}

fn record_expected_records(
    seen_records: &mut BTreeMap<(String, i32), usize>,
    topic: &str,
    records: impl IntoIterator<Item = kafrust::ConsumerRecord>,
) -> kafrust::Result<()> {
    for record in records {
        if record.topic() != topic {
            continue;
        }
        observe_expected_record(
            seen_records,
            record.topic(),
            record.partition(),
            record.value(),
        )?;
    }
    Ok(())
}

fn observe_expected_record(
    seen_records: &mut BTreeMap<(String, i32), usize>,
    topic: &str,
    partition: i32,
    value: Option<&[u8]>,
) -> kafrust::Result<()> {
    let expected_value = format!("published-group-rebalance-{partition}");
    if value != Some(expected_value.as_bytes()) {
        return Err(Error::Unsupported(
            "published group smoke observed an unexpected record value",
        ));
    }
    let count = seen_records
        .entry((topic.to_owned(), partition))
        .or_default();
    *count = count.saturating_add(1);
    if *count > 1 {
        return Err(Error::Unsupported(
            "published group smoke observed a duplicate record",
        ));
    }
    Ok(())
}

async fn poll_pair(
    first: &mut ConsumerGroup,
    second: &mut ConsumerGroup,
) -> kafrust::Result<(Vec<kafrust::ConsumerRecord>, Vec<kafrust::ConsumerRecord>)> {
    let (first_result, second_result) = tokio::join!(first.poll(), second.poll());
    Ok((first_result?, second_result?))
}

fn assignment_keys(group: &ConsumerGroup) -> BTreeSet<(String, i32)> {
    group
        .assignments()
        .iter()
        .map(|assignment| (assignment.topic().to_owned(), assignment.partition()))
        .collect()
}

fn timeout_seconds() -> u64 {
    let cycles = env::var("KAFRUST_GROUP_CHURN_CYCLES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(1);
    300_u64.saturating_add(cycles.saturating_mul(10))
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(timeout_seconds()), run())
        .await
        .map_err(|_| Error::Unsupported("published group smoke timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expected_record_counts_reject_duplicates() {
        let mut seen = BTreeMap::new();
        let value = b"published-group-rebalance-2";
        assert!(observe_expected_record(&mut seen, "topic", 2, Some(value)).is_ok());
        assert!(observe_expected_record(&mut seen, "topic", 2, Some(value)).is_err());
    }

    #[test]
    fn expected_record_values_reject_unexpected_payloads() {
        let mut seen = BTreeMap::new();
        assert!(observe_expected_record(&mut seen, "topic", 2, Some(b"wrong")).is_err());
    }
}
