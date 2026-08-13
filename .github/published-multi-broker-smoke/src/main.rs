use std::env;

use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerGroupConfig, Error, OffsetResetPolicy, ProducerConfig,
    ProducerRecord,
};

fn required(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| Error::Unsupported("published multi-broker smoke variable missing"))
}

fn required_i32(name: &str) -> Result<i32, Error> {
    required(name)?
        .parse()
        .map_err(|_| Error::Unsupported("published multi-broker smoke variable was not an integer"))
}

fn producer_config(bootstrap_servers: &str) -> ProducerConfig {
    ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-multi-broker-producer")
        .acks(Acks::Leader)
        .enable_idempotence(true)
}

fn group_config(bootstrap_servers: &str, group_id: &str) -> ConsumerGroupConfig {
    ConsumerGroupConfig::new(
        bootstrap_servers.split(',').map(str::to_owned),
        group_id.to_owned(),
    )
    .client_id("kafrust-published-multi-broker-group")
    .max_retries(10)
    .max_poll_records(20)
    .offset_reset_policy(OffsetResetPolicy::Earliest)
}

async fn run_pre(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    partition: i32,
    value: &str,
) -> kafrust::Result<()> {
    let mut producer = producer_config(bootstrap_servers).build().await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(value.as_bytes()),
        )
        .await?;

    let mut group = group_config(bootstrap_servers, group_id)
        .subscribe(topic.to_owned())
        .join()
        .await?;
    let mut found = false;
    for _ in 0..10 {
        let records = group.poll().await?;
        if let Some(record) = records.iter().find(|record| {
            record.topic() == topic
                && record.partition() == metadata.partition()
                && record.offset() == metadata.offset()
                && record.value() == Some(value.as_bytes())
        }) {
            group.commit_record(record)?;
            found = true;
            break;
        }
    }
    if !found {
        return Err(Error::Unsupported(
            "published multi-broker group did not read the pre-failover record",
        ));
    }
    group.commit_queued_offsets().await?;
    group.leave().await?;
    println!(
        "published multi-broker pre-failover committed {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}

async fn run_post(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    partition: i32,
    value: &str,
) -> kafrust::Result<()> {
    let mut producer = producer_config(bootstrap_servers).build().await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(value.as_bytes()),
        )
        .await?;

    let mut group = group_config(bootstrap_servers, group_id)
        .subscribe(topic.to_owned())
        .join()
        .await?;
    let mut found = false;
    for _ in 0..10 {
        let records = group.poll().await?;
        if records.iter().any(|record| {
            record.topic() == topic
                && record.partition() == metadata.partition()
                && record.offset() == metadata.offset()
                && record.value() == Some(value.as_bytes())
        }) {
            found = true;
            break;
        }
    }
    group.leave().await?;
    if !found {
        return Err(Error::Unsupported(
            "published multi-broker group did not resume on the replacement leader",
        ));
    }
    println!(
        "published multi-broker post-failover resumed {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = required("KAFRUST_BOOTSTRAP_SERVERS")?;
    let topic = required("KAFRUST_TOPIC")?;
    let group_id = required("KAFRUST_GROUP_ID")?;
    let partition = required_i32("KAFRUST_PARTITION")?;
    let phase = required("KAFRUST_PHASE")?;
    let value = required("KAFRUST_VALUE")?;

    let cluster = AdminClient::new(ClientConfig::new(
        bootstrap_servers.split(',').map(str::to_owned),
    ));
    let brokers = cluster.describe_cluster().await?;
    let expected_broker_count = if phase == "pre" { 3 } else { 2 };
    if brokers.brokers().len() < expected_broker_count {
        return Err(Error::Unsupported(
            "published multi-broker smoke did not observe the expected live brokers",
        ));
    }

    match phase.as_str() {
        "pre" => run_pre(&bootstrap_servers, &topic, &group_id, partition, &value).await,
        "post" => run_post(&bootstrap_servers, &topic, &group_id, partition, &value).await,
        _ => Err(Error::Unsupported(
            "published multi-broker smoke phase was invalid",
        )),
    }
}
