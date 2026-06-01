use kafrust_protocol::api::fetch::{FetchPartitionResponseV2, FetchResponseV2, MessageSetRecord};
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};

use crate::client::{Client, FetchOneRequestV2};
use crate::config::ClientConfig;
use crate::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerRecord {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp_ms: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
}

impl ConsumerRecord {
    fn from_message_set(topic: &str, partition: i32, record: MessageSetRecord) -> Self {
        Self {
            topic: topic.to_owned(),
            partition,
            offset: record.offset,
            timestamp_ms: record.timestamp_ms,
            key: record.key,
            value: record.value,
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

    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

#[derive(Debug)]
pub struct Consumer {
    client: Client,
    config: ConsumerConfig,
}

impl Consumer {
    pub async fn fetch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<ConsumerRecord>> {
        let topic = topic.into();
        let metadata = self.client.metadata(Some(vec![topic.clone()])).await?;
        let leader = leader_for(&metadata, &topic, partition)?;
        let broker_addr = broker_addr_for(&metadata, leader)?;
        let mut leader_client = Client::connect(
            broker_addr,
            self.config.client.client_id_ref().map(str::to_owned),
        )
        .await?;
        let response = leader_client
            .fetch_one_v2(FetchOneRequestV2 {
                replica_id: -1,
                max_wait_ms: self.config.max_wait_ms,
                min_bytes: self.config.min_bytes,
                topic: topic.clone(),
                partition_index: partition,
                fetch_offset: offset,
                max_bytes: self.config.max_partition_bytes,
            })
            .await?;
        let partition_response = fetch_partition_response(&response, &topic, partition)?;
        if partition_response.error_code != 0 {
            return Err(Error::Broker {
                code: partition_response.error_code,
                context: format!("fetch {topic}-{partition}@{offset}"),
            });
        }

        Ok(partition_response
            .records
            .iter()
            .cloned()
            .map(|record| ConsumerRecord::from_message_set(&topic, partition, record))
            .collect())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
    client: ClientConfig,
    max_wait_ms: i32,
    min_bytes: i32,
    max_partition_bytes: i32,
}

impl ConsumerConfig {
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            max_wait_ms: 500,
            min_bytes: 1,
            max_partition_bytes: 1_048_576,
        }
    }

    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.max_wait_ms = max_wait_ms;
        self
    }

    pub fn min_bytes(mut self, min_bytes: i32) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    pub fn max_partition_bytes(mut self, max_partition_bytes: i32) -> Self {
        self.max_partition_bytes = max_partition_bytes;
        self
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    pub async fn build(self) -> Result<Consumer> {
        let client = self.client.clone().connect().await?;
        Ok(Consumer {
            client,
            config: self,
        })
    }
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

fn fetch_partition_response<'a>(
    response: &'a FetchResponseV2,
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a FetchPartitionResponseV2> {
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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{leader_for, ConsumerConfig, ConsumerRecord};
    use kafrust_protocol::api::fetch::MessageSetRecord;
    use kafrust_protocol::api::metadata::{
        BrokerMetadata, MetadataResponseV1, PartitionMetadata, TopicMetadata,
    };

    #[test]
    fn builds_consumer_config() {
        let config = ConsumerConfig::new(["localhost:9092"])
            .client_id("orders-reader")
            .max_wait_ms(250)
            .min_bytes(10)
            .max_partition_bytes(1024);

        assert_eq!(
            config.client_config().client_id_ref(),
            Some("orders-reader")
        );
    }

    #[test]
    fn maps_message_set_record() {
        let record = ConsumerRecord::from_message_set(
            "orders",
            1,
            MessageSetRecord {
                offset: 42,
                timestamp_ms: 123,
                key: Some(b"order-1".to_vec()),
                value: Some(b"created".to_vec()),
            },
        );

        assert_eq!(record.topic(), "orders");
        assert_eq!(record.partition(), 1);
        assert_eq!(record.offset(), 42);
        assert_eq!(record.timestamp_ms(), 123);
        assert_eq!(record.key().unwrap(), b"order-1");
        assert_eq!(record.value().unwrap(), b"created");
    }

    #[test]
    fn resolves_partition_leader() {
        assert_eq!(leader_for(&metadata_fixture(), "orders", 0).unwrap(), 1);
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
                partitions: vec![PartitionMetadata {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 1,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                }],
            }],
        }
    }
}
