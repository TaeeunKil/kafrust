mod common;

use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerGroupConfig, DeleteRecordsOptions, DeleteRecordsTopic,
    Error, OffsetResetPolicy, ProducerConfig, ProducerRecord,
};

const BEFORE_VALUE: &[u8] = b"kafrust-offset-reset-before";
const FILLER_VALUE: &[u8] = b"kafrust-offset-reset-filler";
const RECOVERED_VALUE: &[u8] = b"kafrust-offset-reset-recovered";
const AFTER_VALUE: &[u8] = b"kafrust-offset-reset-after";

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let group_id =
        std::env::var("KAFRUST_GROUP_ID").unwrap_or_else(|_| "kafrust-offset-reset".to_owned());
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());

    let producer_config = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone()).client_id("kafrust-offset-reset-producer"),
    )?
    .acks(Acks::Leader);
    let mut producer = producer_config.build().await?;
    producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(BEFORE_VALUE),
        )
        .await?;
    producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(FILLER_VALUE),
        )
        .await?;

    let mut earliest = group(
        bootstrap_servers.clone(),
        format!("{group_id}-earliest"),
        &topic,
        OffsetResetPolicy::Earliest,
    )
    .await?;
    poll_until_value(&mut earliest, BEFORE_VALUE, "earliest offset reset").await?;
    earliest.leave().await?;

    let committed_group_id = format!("{group_id}-committed-out-of-range");
    let mut committed = group(
        bootstrap_servers.clone(),
        committed_group_id.clone(),
        &topic,
        OffsetResetPolicy::Earliest,
    )
    .await?;
    poll_until_value(&mut committed, BEFORE_VALUE, "committed offset setup").await?;
    let committed_offset = committed.position(&topic, 0).ok_or(Error::Unsupported(
        "earliest group has no partition position",
    ))?;
    committed.commit_offsets().await?;
    committed.leave().await?;

    let admin = AdminClient::new(common::apply_security(
        ClientConfig::new(bootstrap_servers.clone()).client_id("kafrust-offset-recovery-admin"),
    )?);
    let delete_result = admin
        .delete_records(
            &[DeleteRecordsTopic::new(topic.clone()).partition(0, committed_offset + 1)],
            DeleteRecordsOptions::new(),
        )
        .await?;
    let deleted_topic = delete_result
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .ok_or(Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition: 0,
        })?;
    let deleted_partition = deleted_topic
        .partitions()
        .iter()
        .find(|candidate| candidate.partition_index() == 0)
        .ok_or(Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition: 0,
        })?;
    if !deleted_partition.is_success() || deleted_partition.low_watermark() <= committed_offset {
        return Err(Error::Unsupported(
            "delete records did not move the low watermark past the committed offset",
        ));
    }
    producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(RECOVERED_VALUE),
        )
        .await?;

    let mut recovered = group(
        bootstrap_servers.clone(),
        committed_group_id,
        &topic,
        OffsetResetPolicy::Earliest,
    )
    .await?;
    let recovered_records = poll_until_value(
        &mut recovered,
        RECOVERED_VALUE,
        "committed out-of-range offset reset",
    )
    .await?;
    if !contains_value(&recovered_records, RECOVERED_VALUE)
        || contains_value(&recovered_records, BEFORE_VALUE)
        || contains_value(&recovered_records, FILLER_VALUE)
    {
        return Err(Error::Unsupported(
            "committed out-of-range group offset did not recover from the earliest watermark",
        ));
    }
    recovered.leave().await?;

    let mut latest = group(
        bootstrap_servers,
        format!("{group_id}-latest"),
        &topic,
        OffsetResetPolicy::Latest,
    )
    .await?;
    producer
        .send(ProducerRecord::to(topic).partition(0).value(AFTER_VALUE))
        .await?;
    let latest_records = poll_until_value(&mut latest, AFTER_VALUE, "latest offset reset").await?;
    if contains_value(&latest_records, BEFORE_VALUE)
        || !contains_value(&latest_records, AFTER_VALUE)
    {
        return Err(Error::Unsupported(
            "latest offset reset returned records outside the post-join range",
        ));
    }
    latest.leave().await?;

    println!("verified earliest and latest consumer group offset reset policies");
    Ok(())
}

async fn group(
    bootstrap_servers: Vec<String>,
    group_id: String,
    topic: &str,
    policy: OffsetResetPolicy,
) -> kafrust::Result<kafrust::ConsumerGroup> {
    common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id)
            .client_id("kafrust-offset-reset-consumer"),
    )?
    .offset_reset_policy(policy)
    .subscribe(topic)
    .max_poll_records(1)
    .join()
    .await
}

fn contains_value(records: &[kafrust::ConsumerRecord], expected: &[u8]) -> bool {
    records
        .iter()
        .any(|record| record.value() == Some(expected))
}

async fn poll_until_value(
    group: &mut kafrust::ConsumerGroup,
    expected: &[u8],
    description: &str,
) -> kafrust::Result<Vec<kafrust::ConsumerRecord>> {
    for _ in 0..20 {
        let records = group.poll().await?;
        if contains_value(&records, expected) {
            return Ok(records);
        }
    }
    eprintln!("{description} did not return the expected record");
    Err(Error::Unsupported(
        "consumer group offset reset did not return the expected record",
    ))
}
