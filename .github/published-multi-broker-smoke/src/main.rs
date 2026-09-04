use std::env;

use kafrust::protocol::api::metadata::MetadataRequestTopicV12;
use kafrust::{
    Acks, AdminClient, ClientConfig, ConsumerGroupConfig, ConsumerGroupProtocol, Error,
    OffsetResetPolicy, ProducerConfig, ProducerRecord,
};

fn required(name: &str) -> Result<String, Error> {
    env::var(name).map_err(|_| Error::Unsupported("published multi-broker smoke variable missing"))
}

fn required_i32(name: &str) -> Result<i32, Error> {
    required(name)?
        .parse()
        .map_err(|_| Error::Unsupported("published multi-broker smoke variable was not an integer"))
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

fn producer_config(bootstrap_servers: &str) -> ProducerConfig {
    ProducerConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-multi-broker-producer")
        .acks(Acks::Leader)
        .enable_idempotence(true)
}

fn group_config(bootstrap_servers: &str, group_id: &str) -> Result<ConsumerGroupConfig, Error> {
    Ok(ConsumerGroupConfig::new(
        bootstrap_servers.split(',').map(str::to_owned),
        group_id.to_owned(),
    )
    .client_id("kafrust-published-multi-broker-group")
    .group_protocol(group_protocol()?)
    .max_retries(10)
    .max_poll_records(20)
    .offset_reset_policy(OffsetResetPolicy::Earliest))
}

fn topic_id_hex(topic_id: [u8; 16]) -> String {
    let mut encoded = String::with_capacity(32);
    for byte in topic_id {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}

fn topic_id_from_hex(value: &str) -> Result<[u8; 16], Error> {
    if value.len() != 32 {
        return Err(Error::Unsupported(
            "published multi-broker topic ID must contain 32 hexadecimal characters",
        ));
    }
    let mut topic_id = [0_u8; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(Error::Unsupported(
            "published multi-broker topic ID contained a non-hexadecimal character",
        ))?;
        let low = hex_nibble(pair[1]).ok_or(Error::Unsupported(
            "published multi-broker topic ID contained a non-hexadecimal character",
        ))?;
        topic_id[index] = (high << 4) | low;
    }
    Ok(topic_id)
}

fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

async fn topic_id_for_topic(bootstrap_servers: &str, topic: &str) -> Result<[u8; 16], Error> {
    let mut client = ClientConfig::new(bootstrap_servers.split(',').map(str::to_owned))
        .client_id("kafrust-published-multi-broker-metadata")
        .connect()
        .await?;
    let metadata = client
        .metadata_v12(Some(vec![MetadataRequestTopicV12 {
            topic_id: [0; 16],
            name: Some(topic.to_owned()),
        }]))
        .await?;
    let topic_metadata = metadata
        .topics
        .iter()
        .find(|entry| entry.name.as_deref() == Some(topic))
        .ok_or(Error::Unsupported(
            "published multi-broker metadata omitted the requested topic",
        ))?;
    if topic_metadata.error_code != 0 || topic_metadata.topic_id == [0; 16] {
        return Err(Error::Unsupported(
            "published multi-broker metadata did not return a topic UUID",
        ));
    }
    Ok(topic_metadata.topic_id)
}

async fn run_pre(
    bootstrap_servers: &str,
    topic: &str,
    group_id: &str,
    partition: i32,
    value: &str,
) -> kafrust::Result<()> {
    let topic_id = topic_id_for_topic(bootstrap_servers, topic).await?;
    let mut producer = producer_config(bootstrap_servers).build().await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(value.as_bytes()),
        )
        .await?;

    let mut group = group_config(bootstrap_servers, group_id)?
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
        "published multi-broker pre-failover committed {}-{}@{} topic_id={}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset(),
        topic_id_hex(topic_id),
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
    let topic_id = topic_id_for_topic(bootstrap_servers, topic).await?;
    if let Ok(expected_hex) = env::var("KAFRUST_EXPECTED_TOPIC_ID") {
        let expected = topic_id_from_hex(&expected_hex)?;
        if topic_id != expected {
            return Err(Error::Unsupported(
                "published multi-broker topic UUID changed across leader movement",
            ));
        }
    }
    let mut producer = producer_config(bootstrap_servers).build().await?;
    let metadata = producer
        .send(
            ProducerRecord::to(topic.to_owned())
                .partition(partition)
                .value(value.as_bytes()),
        )
        .await?;

    let mut group = group_config(bootstrap_servers, group_id)?
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
        "published multi-broker post-failover resumed {}-{}@{} topic_id={}",
        metadata.topic(),
        metadata.partition(),
        metadata.offset(),
        topic_id_hex(topic_id),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{topic_id_from_hex, topic_id_hex};

    #[test]
    fn topic_id_hex_roundtrips() {
        let topic_id = [
            0x00, 0x01, 0x0a, 0x10, 0x2b, 0x3c, 0x4d, 0x5e, 0x6f, 0x70, 0x81, 0x92, 0xa3, 0xb4,
            0xc5, 0xd6,
        ];
        let encoded = topic_id_hex(topic_id);
        assert_eq!(encoded, "00010a102b3c4d5e6f708192a3b4c5d6");
        assert_eq!(topic_id_from_hex(&encoded).unwrap(), topic_id);
    }

    #[test]
    fn topic_id_hex_rejects_wrong_shape() {
        assert!(topic_id_from_hex("00").is_err());
        assert!(topic_id_from_hex("0000000000000000000000000000000g").is_err());
    }
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
