mod common;

use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerConfig, DeleteRecordsOptions, DeleteRecordsTopic,
    Error, OffsetResetPolicy, ProducerConfig, ProducerRecord,
};

const BEFORE_VALUE: &[u8] = b"kafrust-retention-before";
const FILLER_VALUE: &[u8] = b"kafrust-retention-filler";
const TAIL_VALUE: &[u8] = b"kafrust-retention-tail";
const RECOVERED_VALUE: &[u8] = b"kafrust-retention-recovered";

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    common::init_tracing()?;
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC")
        .unwrap_or_else(|_| "kafrust-consumer-retention-recovery".to_owned());

    let producer_config = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-retention-recovery-producer")
            .acks(Acks::Leader),
    )?;
    let mut producer = producer_config.build().await?;
    for value in [BEFORE_VALUE, FILLER_VALUE, TAIL_VALUE] {
        producer
            .send(ProducerRecord::to(topic.clone()).partition(0).value(value))
            .await?;
    }

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-retention-recovery-consumer")
            .max_retries(3)
            .max_poll_records(1)
            .offset_reset_policy(OffsetResetPolicy::Earliest),
    )?
    .build()
    .await?;
    consumer.assign(&topic, 0, 0);

    let before = consumer.poll().await?;
    if !contains_value(&before, BEFORE_VALUE) {
        return Err(Error::Unsupported(
            "retention recovery did not fetch the pre-delete record",
        ));
    }
    let position_before = consumer.position(&topic, 0).ok_or(Error::Unsupported(
        "retention recovery has no consumer position",
    ))?;
    println!("retention recovery fetched before-delete record at position={position_before}");

    let admin = AdminClient::new(common::apply_security(
        ClientConfig::new(bootstrap_servers).client_id("kafrust-retention-recovery-admin"),
    )?);
    let delete_offset = position_before + 1;
    let delete_result = admin
        .delete_records(
            &[DeleteRecordsTopic::new(topic.clone()).partition(0, delete_offset)],
            DeleteRecordsOptions::new(),
        )
        .await?;
    let partition_result = delete_result
        .topics()
        .iter()
        .find(|candidate| candidate.name() == topic)
        .and_then(|candidate| {
            candidate
                .partitions()
                .iter()
                .find(|partition| partition.partition_index() == 0)
        })
        .ok_or(Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition: 0,
        })?;
    if !partition_result.is_success() || partition_result.low_watermark() <= position_before {
        return Err(Error::Unsupported(
            "DeleteRecords did not move the low watermark past the consumer position",
        ));
    }
    println!(
        "retention recovery deleted through offset={delete_offset} low_watermark={}",
        partition_result.low_watermark()
    );

    producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(RECOVERED_VALUE),
        )
        .await?;

    let mut recovered_records = Vec::new();
    for _ in 0..20 {
        let records = consumer.poll().await?;
        let found_recovered = contains_value(&records, RECOVERED_VALUE);
        recovered_records.extend(records);
        if found_recovered {
            break;
        }
    }
    if !contains_value(&recovered_records, RECOVERED_VALUE)
        || contains_value(&recovered_records, BEFORE_VALUE)
        || contains_value(&recovered_records, FILLER_VALUE)
    {
        return Err(Error::Unsupported(
            "direct consumer did not recover from the retained-log boundary",
        ));
    }
    for record in &recovered_records {
        println!(
            "retention recovery fetched {}-{}@{} value={:?}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.value().map(String::from_utf8_lossy)
        );
    }
    println!("verified direct consumer recovery after DeleteRecords");
    Ok(())
}

fn contains_value(records: &[kafrust::ConsumerRecord], expected: &[u8]) -> bool {
    records
        .iter()
        .any(|record| record.value() == Some(expected))
}
