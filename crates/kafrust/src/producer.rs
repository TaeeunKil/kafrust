use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};
use kafrust_protocol::api::produce::{
    MessageSetMessage, ProducePartitionResponseV2, ProduceResponseV2,
};

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{BrokerErrorKind, Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Acks {
    None,
    Leader,
    All,
}

impl Acks {
    pub fn as_i16(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    key: String,
    value: Vec<u8>,
}

impl Header {
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRecord {
    topic: String,
    partition: Option<i32>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<Header>,
    timestamp: Option<SystemTime>,
}

impl ProducerRecord {
    pub fn to(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: None,
            headers: Vec::new(),
            timestamp: None,
        }
    }

    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    pub fn key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    pub fn value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = Some(value.into());
        self
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(Header::new(key, value));
        self
    }

    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition_ref(&self) -> Option<i32> {
        self.partition
    }

    pub fn key_ref(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value_ref(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    pub fn timestamp_ref(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: Option<SystemTime>,
}

impl RecordMetadata {
    pub fn new(
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        timestamp: Option<SystemTime>,
    ) -> Self {
        Self {
            topic: topic.into(),
            partition,
            offset,
            timestamp,
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    pub fn timestamp(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

#[derive(Debug)]
pub struct Producer {
    client: Client,
    config: ProducerConfig,
}

impl Producer {
    pub async fn send(&mut self, record: ProducerRecord) -> Result<RecordMetadata> {
        if self.config.acks == Acks::None {
            return Err(Error::Unsupported("producer acks=0 send without response"));
        }
        if !record.headers().is_empty() {
            return Err(Error::Unsupported(
                "record headers require Kafka record batch encoding",
            ));
        }

        let timestamp = record.timestamp_ref().unwrap_or_else(SystemTime::now);
        let timestamp_ms = timestamp_millis(timestamp);
        let mut attempt = 0;

        loop {
            let metadata = self
                .client
                .metadata(Some(vec![record.topic().to_owned()]))
                .await?;
            let result = self
                .send_with_metadata(&record, &metadata, timestamp, timestamp_ms)
                .await;

            match result {
                Err(error) if attempt < self.config.max_retries && can_retry_send(&error) => {
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    async fn send_with_metadata(
        &self,
        record: &ProducerRecord,
        metadata: &MetadataResponseV1,
        timestamp: SystemTime,
        timestamp_ms: i64,
    ) -> Result<RecordMetadata> {
        let partition = choose_partition(record, metadata)?;
        let leader = leader_for(metadata, record.topic(), partition)?;
        let broker_addr = broker_addr_for(metadata, leader)?;

        let mut leader_client = Client::connect_with_request_timeout(
            broker_addr,
            self.config.client.client_id_ref().map(str::to_owned),
            self.config.client.request_timeout(),
        )
        .await?;
        let response = leader_client
            .produce_one_v2(
                self.config.acks.as_i16(),
                30_000,
                record.topic().to_owned(),
                partition,
                vec![MessageSetMessage::new(
                    record.key_ref().map(|key| key.to_vec()),
                    record.value_ref().map(|value| value.to_vec()),
                    timestamp_ms,
                )],
            )
            .await?;
        let partition_response = produce_partition_response(&response, record.topic(), partition)?;
        if partition_response.error_code != 0 {
            return Err(Error::Broker {
                code: partition_response.error_code,
                context: format!("produce {}-{}", record.topic(), partition),
            });
        }

        Ok(RecordMetadata::new(
            record.topic(),
            partition,
            partition_response.base_offset,
            Some(timestamp),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerConfig {
    client: ClientConfig,
    acks: Acks,
    max_retries: u32,
}

impl ProducerConfig {
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            acks: Acks::Leader,
            max_retries: 1,
        }
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(request_timeout_ms);
        self
    }

    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn acks_ref(&self) -> Acks {
        self.acks
    }

    pub fn max_retries_ref(&self) -> u32 {
        self.max_retries
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    pub async fn build(self) -> Result<Producer> {
        let client = self.client.clone().connect().await?;
        Ok(Producer {
            client,
            config: self,
        })
    }
}

fn choose_partition(record: &ProducerRecord, metadata: &MetadataResponseV1) -> Result<i32> {
    if let Some(partition) = record.partition_ref() {
        return Ok(partition);
    }

    metadata
        .topics
        .iter()
        .find(|topic| topic.name == record.topic())
        .and_then(|topic| topic.partitions.first())
        .map(|partition| partition.partition_index)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: record.topic().to_owned(),
            partition: -1,
        })
}

fn leader_for(
    metadata: &MetadataResponseV1,
    topic_name: &str,
    partition_index: i32,
) -> Result<i32> {
    metadata
        .topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == partition_index)
        })
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic_name.to_owned(),
            partition: partition_index,
        })
        .and_then(|partition| {
            (partition.leader_id >= 0)
                .then_some(partition.leader_id)
                .ok_or_else(|| Error::MissingLeader {
                    topic: topic_name.to_owned(),
                    partition: partition_index,
                })
        })
}

fn broker_addr_for(metadata: &MetadataResponseV1, node_id: i32) -> Result<String> {
    metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == node_id)
        .map(broker_addr)
        .ok_or(Error::MissingBroker { node_id })
}

fn broker_addr(broker: &BrokerMetadata) -> String {
    format!("{}:{}", broker.host, broker.port)
}

fn timestamp_millis(timestamp: SystemTime) -> i64 {
    let duration = timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_millis(0));
    match i64::try_from(duration.as_millis()) {
        Ok(value) => value,
        Err(_) => i64::MAX,
    }
}

fn produce_partition_response<'a>(
    response: &'a ProduceResponseV2,
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a ProducePartitionResponseV2> {
    response
        .responses
        .iter()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == partition_index)
        })
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic_name.to_owned(),
            partition: partition_index,
        })
}

fn can_retry_send(error: &Error) -> bool {
    match error {
        Error::Broker { code, .. } => BrokerErrorKind::from_code(*code).is_produce_retryable(),
        Error::Io(_) | Error::RequestTimedOut { .. } => true,
        Error::MissingBootstrapServer
        | Error::UnknownTopicOrPartition { .. }
        | Error::MissingLeader { .. }
        | Error::MissingBroker { .. }
        | Error::Unsupported(_)
        | Error::Protocol(_) => false,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        can_retry_send, choose_partition, leader_for, Acks, ProducerConfig, ProducerRecord,
        RecordMetadata,
    };
    use crate::{BrokerErrorKind, Error};
    use kafrust_protocol::api::metadata::{
        BrokerMetadata, MetadataResponseV1, PartitionMetadata, TopicMetadata,
    };

    #[test]
    fn maps_acks_to_kafka_values() {
        assert_eq!(Acks::None.as_i16(), 0);
        assert_eq!(Acks::Leader.as_i16(), 1);
        assert_eq!(Acks::All.as_i16(), -1);
    }

    #[test]
    fn builds_producer_record_with_kafka_concepts() {
        let record = ProducerRecord::to("orders")
            .partition(2)
            .key("order-123")
            .value("created")
            .header("source", "checkout");

        assert_eq!(record.topic(), "orders");
        assert_eq!(record.partition_ref(), Some(2));
        assert_eq!(record.key_ref().unwrap(), b"order-123");
        assert_eq!(record.value_ref().unwrap(), b"created");
        assert_eq!(record.headers()[0].key(), "source");
        assert_eq!(record.headers()[0].value(), b"checkout");
    }

    #[test]
    fn builds_producer_config() {
        let config = ProducerConfig::new(["localhost:9092"])
            .client_id("orders-api")
            .request_timeout_ms(5_000)
            .max_retries(3)
            .acks(Acks::All);

        assert_eq!(config.acks_ref(), Acks::All);
        assert_eq!(config.max_retries_ref(), 3);
        assert_eq!(config.client_config().client_id_ref(), Some("orders-api"));
    }

    #[test]
    fn exposes_record_metadata() {
        let metadata = RecordMetadata::new("orders", 1, 42, None);

        assert_eq!(metadata.topic(), "orders");
        assert_eq!(metadata.partition(), 1);
        assert_eq!(metadata.offset(), 42);
        assert_eq!(metadata.timestamp(), None);
    }

    #[test]
    fn chooses_explicit_partition() {
        let metadata = metadata_fixture();
        let record = ProducerRecord::to("orders").partition(1);

        assert_eq!(choose_partition(&record, &metadata).unwrap(), 1);
    }

    #[test]
    fn chooses_first_partition_when_record_has_no_partition() {
        let metadata = metadata_fixture();
        let record = ProducerRecord::to("orders");

        assert_eq!(choose_partition(&record, &metadata).unwrap(), 0);
    }

    #[test]
    fn resolves_partition_leader() {
        let metadata = metadata_fixture();

        assert_eq!(leader_for(&metadata, "orders", 0).unwrap(), 1);
    }

    #[test]
    fn classifies_retriable_send_errors() {
        assert!(can_retry_send(&Error::Broker {
            code: 5,
            context: "produce orders-0".to_owned(),
        }));
        assert!(can_retry_send(&Error::RequestTimedOut { timeout_ms: 5 }));
        assert!(can_retry_send(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))));
        assert!(!can_retry_send(&Error::Unsupported("record headers")));
        assert_eq!(
            Error::Broker {
                code: 5,
                context: "produce orders-0".to_owned(),
            }
            .broker_error_kind(),
            Some(BrokerErrorKind::LeaderNotAvailable)
        );
    }

    fn metadata_fixture() -> MetadataResponseV1 {
        MetadataResponseV1 {
            brokers: vec![BrokerMetadata {
                node_id: 1,
                host: "localhost".to_owned(),
                port: 9092,
                rack: None,
            }],
            controller_id: 1,
            topics: vec![TopicMetadata {
                error_code: 0,
                name: "orders".to_owned(),
                is_internal: false,
                partitions: vec![
                    PartitionMetadata {
                        error_code: 0,
                        partition_index: 0,
                        leader_id: 1,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                    },
                    PartitionMetadata {
                        error_code: 0,
                        partition_index: 1,
                        leader_id: 1,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                    },
                ],
            }],
        }
    }
}
