mod common;

use kafrust::{
    Acks, ConsumerGroupConfig, Error, OffsetResetPolicy, ProducerConfig, ProducerRecord,
};

const BEFORE_VALUE: &[u8] = b"kafrust-offset-reset-before";
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

    let mut earliest = group(
        bootstrap_servers.clone(),
        format!("{group_id}-earliest"),
        &topic,
        OffsetResetPolicy::Earliest,
    )
    .await?;
    let earliest_records = earliest.poll().await?;
    if !contains_value(&earliest_records, BEFORE_VALUE) {
        return Err(Error::Unsupported(
            "earliest offset reset did not return an existing record",
        ));
    }
    earliest.leave().await?;

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
    let latest_records = latest.poll().await?;
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
    .join()
    .await
}

fn contains_value(records: &[kafrust::ConsumerRecord], expected: &[u8]) -> bool {
    records
        .iter()
        .any(|record| record.value() == Some(expected))
}
