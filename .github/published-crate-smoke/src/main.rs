use std::env;

use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerConfig, ConsumerGroupConfig, ConsumerGroupProtocol,
    Error, OffsetResetPolicy, ProducerConfig, ProducerRecord,
};

#[tokio::main]
async fn main() -> kafrust::Result<()> {
    let bootstrap_servers = env::var("KAFRUST_BOOTSTRAP_SERVERS")
        .map_err(|_| Error::Unsupported("KAFRUST_BOOTSTRAP_SERVERS is required"))?;
    let topic =
        env::var("KAFRUST_TOPIC").map_err(|_| Error::Unsupported("KAFRUST_TOPIC is required"))?;
    let group_id = env::var("KAFRUST_GROUP_ID")
        .map_err(|_| Error::Unsupported("KAFRUST_GROUP_ID is required"))?;
    let value =
        env::var("KAFRUST_VALUE").map_err(|_| Error::Unsupported("KAFRUST_VALUE is required"))?;
    let group_protocol = match env::var("KAFRUST_GROUP_PROTOCOL").as_deref() {
        Ok("classic") | Err(_) => ConsumerGroupProtocol::Classic,
        Ok("consumer") => ConsumerGroupProtocol::Consumer,
        Ok(_) => {
            return Err(Error::Unsupported(
                "KAFRUST_GROUP_PROTOCOL must be classic or consumer",
            ));
        }
    };

    let admin = AdminClient::new(
        ClientConfig::new([bootstrap_servers.clone()]).client_id("kafrust-published-smoke-admin"),
    );
    let cluster = admin.describe_cluster().await?;
    if cluster.brokers().is_empty() {
        return Err(Error::Unsupported(
            "published crate admin client returned no brokers",
        ));
    }

    let mut producer = ProducerConfig::new([bootstrap_servers.clone()])
        .client_id("kafrust-published-smoke-producer")
        .acks(Acks::Leader)
        .enable_idempotence(true)
        .build()
        .await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.clone())
                .partition(0)
                .value(value.as_bytes()),
        )
        .await?;

    let mut consumer = ConsumerConfig::new([bootstrap_servers.clone()])
        .client_id("kafrust-published-smoke-consumer")
        .max_poll_records(10)
        .build()
        .await?;
    consumer.assign(&topic, metadata.partition(), metadata.offset());
    let records = consumer.poll().await?;
    if !records.iter().any(|record| {
        record.topic() == topic
            && record.partition() == metadata.partition()
            && record.offset() == metadata.offset()
            && record.value() == Some(value.as_bytes())
    }) {
        return Err(Error::Unsupported(
            "published crate consumer did not read the produced record",
        ));
    }

    let mut group = ConsumerGroupConfig::new([bootstrap_servers], group_id)
        .client_id("kafrust-published-smoke-group")
        .group_protocol(group_protocol)
        .max_retries(5)
        .max_poll_records(10)
        .offset_reset_policy(OffsetResetPolicy::Earliest)
        .subscribe(topic.clone())
        .join()
        .await?;
    let group_records = group.poll().await?;
    if !group_records
        .iter()
        .any(|record| record.topic() == topic && record.value() == Some(value.as_bytes()))
    {
        return Err(Error::Unsupported(
            "published crate consumer group did not read the produced record",
        ));
    }
    group.leave().await?;

    println!(
        "published kafrust verified admin, idempotent producer, direct consumer, and group {}-{}@{}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset()
    );
    Ok(())
}
