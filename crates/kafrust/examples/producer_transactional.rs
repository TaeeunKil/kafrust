mod common;

use kafrust::{
    ConsumerConfig, ConsumerGroupConfig, Error, IsolationLevel, ProducerConfig, ProducerRecord,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transactional-smoke".to_owned());
    let group_id = format!("{transactional_id}-offsets");
    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-transactional-producer-example")
            .transactional_id(transactional_id),
    )?
    .build()
    .await?;

    producer.begin_transaction()?;
    let committed = producer
        .send(
            ProducerRecord::to(topic.clone())
                .key("kafrust-transaction-commit")
                .value("committed by kafrust"),
        )
        .await?;
    producer.commit_transaction().await?;
    println!(
        "committed {}-{}@{}",
        committed.topic(),
        committed.partition(),
        committed.offset()
    );

    producer.begin_transaction()?;
    let aborted = producer
        .send(
            ProducerRecord::to(topic.clone())
                .key("kafrust-transaction-abort")
                .value("aborted by kafrust"),
        )
        .await?;
    producer.abort_transaction().await?;
    println!(
        "aborted {}-{}@{}",
        aborted.topic(),
        aborted.partition(),
        aborted.offset()
    );

    let mut uncommitted_consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-read-uncommitted-example")
            .isolation_level(IsolationLevel::ReadUncommitted),
    )?
    .build()
    .await?;
    let uncommitted_records = uncommitted_consumer
        .fetch(&topic, committed.partition(), committed.offset())
        .await?;
    if !contains_value(&uncommitted_records, b"committed by kafrust")
        || !contains_value(&uncommitted_records, b"aborted by kafrust")
    {
        return Err(Error::Unsupported(
            "read_uncommitted did not return committed and aborted records",
        ));
    }

    let mut committed_consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-read-committed-example")
            .isolation_level(IsolationLevel::ReadCommitted),
    )?
    .build()
    .await?;
    let committed_records = committed_consumer
        .fetch(&topic, committed.partition(), committed.offset())
        .await?;
    if !contains_value(&committed_records, b"committed by kafrust")
        || contains_value(&committed_records, b"aborted by kafrust")
    {
        return Err(Error::Unsupported(
            "read_committed did not isolate aborted records",
        ));
    }
    println!(
        "verified read_uncommitted={} read_committed={}",
        uncommitted_records.len(),
        committed_records.len()
    );

    let mut group = common::apply_security(
        ConsumerGroupConfig::new(bootstrap_servers, group_id.clone())
            .client_id("kafrust-transactional-offset-group-example")
            .subscribe(topic.clone())
            .start_offset(committed.offset())
            .isolation_level(IsolationLevel::ReadCommitted),
    )?
    .join()
    .await?;
    let consumed = group.poll().await?;
    if !contains_value(&consumed, b"committed by kafrust")
        || contains_value(&consumed, b"aborted by kafrust")
    {
        return Err(Error::Unsupported(
            "transaction offset group did not read the committed input",
        ));
    }
    let group_metadata = group.metadata();
    let assignments = group.assignments().to_vec();

    producer.begin_transaction()?;
    producer
        .send(
            ProducerRecord::to(topic)
                .key("kafrust-transaction-offset-output")
                .value("offsets committed by kafrust"),
        )
        .await?;
    producer
        .send_group_offsets_to_transaction(&group_metadata, &assignments)
        .await?;
    producer.commit_transaction().await?;
    println!(
        "committed {} consumed offsets in transaction",
        assignments.len()
    );

    Ok(())
}

fn contains_value(records: &[kafrust::ConsumerRecord], expected: &[u8]) -> bool {
    records
        .iter()
        .any(|record| record.value() == Some(expected))
}
