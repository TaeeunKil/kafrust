use std::collections::BTreeSet;
use std::env;
use std::time::Duration;

use kafrust::{
    Acks, ConsumerGroup, ConsumerGroupConfig, ConsumerGroupProtocol, Error, OffsetResetPolicy,
    ProducerConfig, ProducerRecord,
};

const PARTITION_COUNT: i32 = 6;
const POLL_ATTEMPTS: usize = 80;

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

    let config = group_config(&bootstrap_servers, &group_id, protocol).subscribe(topic.clone());
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

    let second_join = tokio::spawn(
        config
            .client_id("kafrust-published-group-rebalance-second")
            .join(),
    );
    while !second_join.is_finished() {
        first.poll().await?;
    }
    let mut second = second_join
        .await
        .map_err(|_| Error::Unsupported("published group smoke second member task failed"))??;

    wait_for_two_member_coverage(&mut first, &mut second, &topic).await?;
    println!(
        "published group rebalance passed protocol={protocol:?} first={} second={} partitions={PARTITION_COUNT}",
        first.member_id(),
        second.member_id(),
    );
    first.leave().await?;
    second.leave().await?;
    Ok(())
}

async fn wait_for_two_member_coverage(
    first: &mut ConsumerGroup,
    second: &mut ConsumerGroup,
    topic: &str,
) -> kafrust::Result<()> {
    let expected: BTreeSet<_> = (0..PARTITION_COUNT)
        .map(|partition| (topic.to_owned(), partition))
        .collect();
    let mut seen_records = BTreeSet::new();

    for _ in 0..POLL_ATTEMPTS {
        let (first_records, second_records) = poll_pair(first, second).await?;
        for record in first_records.into_iter().chain(second_records) {
            if record.topic() == topic
                && record.value().is_some_and(|value| {
                    value == format!("published-group-rebalance-{}", record.partition()).as_bytes()
                })
            {
                seen_records.insert((record.topic().to_owned(), record.partition()));
            }
        }

        let first_partitions = assignment_keys(first);
        let second_partitions = assignment_keys(second);
        if !first_partitions.is_empty()
            && !second_partitions.is_empty()
            && first_partitions.is_disjoint(&second_partitions)
            && first_partitions
                .union(&second_partitions)
                .cloned()
                .collect::<BTreeSet<_>>()
                == expected
            && seen_records == expected
        {
            return Ok(());
        }
    }

    Err(Error::Unsupported(
        "published group smoke members did not converge on disjoint ownership and records",
    ))
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

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    tokio::time::timeout(Duration::from_secs(60), run())
        .await
        .map_err(|_| Error::Unsupported("published group smoke timed out"))?
}
