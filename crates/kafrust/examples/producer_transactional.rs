mod common;

use kafrust::{ProducerConfig, ProducerRecord};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = common::bootstrap_servers_from_env();
    let topic = std::env::var("KAFRUST_TOPIC").unwrap_or_else(|_| "kafrust-smoke".to_owned());
    let transactional_id = std::env::var("KAFRUST_TRANSACTIONAL_ID")
        .unwrap_or_else(|_| "kafrust-transactional-smoke".to_owned());
    let mut producer = common::apply_security(
        ProducerConfig::new(bootstrap_servers)
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
            ProducerRecord::to(topic)
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

    Ok(())
}
