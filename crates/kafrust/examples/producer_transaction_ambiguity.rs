mod common;

use kafrust::{ConsumerConfig, Error, IsolationLevel, ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC")
        .unwrap_or_else(|_| "kafrust-transaction-ambiguity".to_owned());
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transaction-ambiguity".to_owned());

    let mut ambiguous_producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-transaction-ambiguity-producer")
            .transactional_id(transactional_id.clone())
            .max_retries(30),
    )?
    .build()
    .await?;
    ambiguous_producer.begin_transaction()?;
    let ambiguous = ambiguous_producer
        .send(
            ProducerRecord::to(topic.clone())
                .key("ambiguous")
                .value("ambiguous transaction committed by broker"),
        )
        .await?;
    let outcome = ambiguous_producer.commit_transaction().await;
    if !matches!(
        outcome,
        Err(Error::TransactionOutcomeUnknown {
            operation: "commit"
        })
    ) {
        return Err(Error::Unsupported(
            "EndTxn response drop did not produce an unknown commit outcome",
        ));
    }
    if ambiguous_producer.transaction_status() != Some(kafrust::TransactionStatus::Defunct) {
        return Err(Error::Unsupported(
            "unknown transaction outcome did not defunct the producer",
        ));
    }
    println!(
        "observed unknown commit outcome for {}-{}@{}",
        ambiguous.topic(),
        ambiguous.partition(),
        ambiguous.offset()
    );

    let mut recovery_producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers.clone())
            .client_id("kafrust-transaction-ambiguity-recovery")
            .transactional_id(transactional_id)
            .max_retries(10),
    )?
    .build()
    .await?;
    recovery_producer.begin_transaction()?;
    let recovery = recovery_producer
        .send(
            ProducerRecord::to(topic.clone())
                .key("recovery")
                .value("recovery transaction committed after ambiguity"),
        )
        .await?;
    recovery_producer.commit_transaction().await?;

    let mut consumer = common::apply_security(
        ConsumerConfig::new(bootstrap_servers)
            .client_id("kafrust-transaction-ambiguity-read-committed")
            .isolation_level(IsolationLevel::ReadCommitted),
    )?
    .build()
    .await?;
    let records = consumer.fetch(&topic, ambiguous.partition(), 0).await?;
    let ambiguous_count = records
        .iter()
        .filter(|record| record.value() == Some(b"ambiguous transaction committed by broker"))
        .count();
    let recovery_count = records
        .iter()
        .filter(|record| record.value() == Some(b"recovery transaction committed after ambiguity"))
        .count();
    if ambiguous_count != 1 || recovery_count != 1 {
        return Err(Error::Unsupported(
            "read_committed did not preserve exactly one result for each transaction",
        ));
    }
    println!(
        "read_committed reconciled ambiguous transaction at {}-{}@{} with recovery at {}-{}@{}",
        ambiguous.topic(),
        ambiguous.partition(),
        ambiguous.offset(),
        recovery.topic(),
        recovery.partition(),
        recovery.offset()
    );
    Ok(())
}
