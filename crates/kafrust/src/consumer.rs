use std::collections::BTreeMap;

use kafrust_protocol::api::fetch::{FetchPartitionResponseV2, FetchResponseV2, MessageSetRecord};
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};

use crate::client::{Client, FetchOneRequestV2};
use crate::config::{ClientConfig, SecurityProtocol};
use crate::error::{BrokerErrorKind, Error, Result};
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
/// Record fetched from a Kafka topic partition.
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

    /// Returns the Kafka topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the Kafka partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the Kafka record offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the Kafka record timestamp in milliseconds since the Unix epoch.
    pub fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    /// Returns the record key bytes.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the record value bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

#[derive(Debug)]
/// Direct Kafka consumer for manually assigned topic partitions.
pub struct Consumer {
    client: Client,
    config: ConsumerConfig,
    assignments: Vec<ConsumerAssignment>,
    metadata_cache: BTreeMap<String, MetadataResponseV1>,
}

impl Consumer {
    pub(crate) fn from_assignments(
        client: Client,
        config: ConsumerConfig,
        assignments: Vec<ConsumerAssignment>,
    ) -> Self {
        Self {
            client,
            config,
            assignments,
            metadata_cache: BTreeMap::new(),
        }
    }

    /// Assigns a topic partition and next offset to fetch.
    pub fn assign(&mut self, topic: impl Into<String>, partition: i32, offset: i64) {
        assign_partition(&mut self.assignments, topic.into(), partition, offset);
    }

    /// Returns the current topic partition assignments.
    pub fn assignments(&self) -> &[ConsumerAssignment] {
        &self.assignments
    }

    /// Polls assigned partitions and advances in-memory offsets for fetched records.
    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        let assignments = self.assignments.clone();
        let mut records = Vec::new();
        debug!(
            assignment_count = assignments.len(),
            max_poll_records = self.config.max_poll_records,
            "polling kafka consumer assignments"
        );

        for assignment in assignments {
            if records.len() >= self.config.max_poll_records {
                break;
            }

            let mut fetched = self
                .fetch(
                    &assignment.topic,
                    assignment.partition,
                    assignment.next_offset,
                )
                .await?;
            limit_fetched_records(&mut fetched, records.len(), self.config.max_poll_records);
            if let Some(last) = fetched.last() {
                self.update_assignment_offset(
                    &assignment.topic,
                    assignment.partition,
                    last.offset().saturating_add(1),
                );
            }
            records.extend(fetched);
        }

        debug!(
            record_count = records.len(),
            "polled kafka consumer records"
        );
        Ok(records)
    }

    /// Fetches records for one topic partition without changing assignment state.
    pub async fn fetch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<ConsumerRecord>> {
        let topic = topic.into();
        let mut attempt = 0;
        debug!(
            topic = topic.as_str(),
            partition, offset, "fetching kafka records"
        );

        loop {
            let result = self.fetch_once(&topic, partition, offset).await;
            match result {
                Err(error) if attempt < self.config.max_retries && can_retry_fetch(&error) => {
                    invalidate_metadata_cache(&mut self.metadata_cache, &topic);
                    attempt += 1;
                }
                Ok(records) => {
                    debug!(
                        topic = topic.as_str(),
                        partition,
                        offset,
                        record_count = records.len(),
                        "fetched kafka records"
                    );
                    return Ok(records);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn fetch_once(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<ConsumerRecord>> {
        let metadata = self.metadata_for_topic(topic).await?;
        let leader = leader_for(&metadata, topic, partition)?;
        let broker_addr = broker_addr_for(&metadata, leader)?;
        debug!(
            topic = topic,
            partition,
            leader,
            broker_addr = broker_addr.as_str(),
            "resolved fetch leader"
        );
        let mut leader_client = self.config.client.connect_broker(broker_addr).await?;
        let response = leader_client
            .fetch_one_v2(FetchOneRequestV2 {
                replica_id: -1,
                max_wait_ms: self.config.max_wait_ms,
                min_bytes: self.config.min_bytes,
                topic: topic.to_owned(),
                partition_index: partition,
                fetch_offset: offset,
                max_bytes: self.config.max_partition_bytes,
            })
            .await?;
        let partition_response = fetch_partition_response(&response, topic, partition)?;
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
            .map(|record| ConsumerRecord::from_message_set(topic, partition, record))
            .collect())
    }

    fn update_assignment_offset(&mut self, topic: &str, partition: i32, next_offset: i64) {
        if let Some(assignment) = self
            .assignments
            .iter_mut()
            .find(|assignment| assignment.topic == topic && assignment.partition == partition)
        {
            assignment.next_offset = next_offset;
        }
    }

    async fn metadata_for_topic(&mut self, topic: &str) -> Result<MetadataResponseV1> {
        if let Some(metadata) = self.metadata_cache.get(topic) {
            return Ok(metadata.clone());
        }

        let metadata = self.client.metadata(Some(vec![topic.to_owned()])).await?;
        self.metadata_cache
            .insert(topic.to_owned(), metadata.clone());
        Ok(metadata)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Direct consumer topic partition assignment.
pub struct ConsumerAssignment {
    topic: String,
    partition: i32,
    next_offset: i64,
}

impl ConsumerAssignment {
    pub(crate) fn new(topic: String, partition: i32, next_offset: i64) -> Self {
        Self {
            topic,
            partition,
            next_offset,
        }
    }

    /// Returns the Kafka topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the Kafka partition index.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the next offset that will be fetched or committed.
    pub fn next_offset(&self) -> i64 {
        self.next_offset
    }
}

fn assign_partition(
    assignments: &mut Vec<ConsumerAssignment>,
    topic: String,
    partition: i32,
    offset: i64,
) {
    if let Some(assignment) = assignments
        .iter_mut()
        .find(|assignment| assignment.topic == topic && assignment.partition == partition)
    {
        assignment.next_offset = offset;
        return;
    }

    assignments.push(ConsumerAssignment {
        topic,
        partition,
        next_offset: offset,
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Configuration builder for [`Consumer`].
pub struct ConsumerConfig {
    client: ClientConfig,
    max_wait_ms: i32,
    min_bytes: i32,
    max_partition_bytes: i32,
    max_retries: u32,
    max_poll_records: usize,
}

impl ConsumerConfig {
    /// Creates a consumer configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            max_wait_ms: 500,
            min_bytes: 1,
            max_partition_bytes: 1_048_576,
            max_retries: 1,
            max_poll_records: 500,
        }
    }

    /// Sets the Kafka client ID used by consumer requests.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    /// Sets the request timeout in milliseconds.
    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(request_timeout_ms);
        self
    }

    /// Sets the Kafka security protocol used for consumer broker connections.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.client = self.client.security_protocol(security_protocol);
        self
    }

    /// Sets SASL/PLAIN credentials for consumer broker connections.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.client = self.client.sasl_plain(username, password);
        self
    }

    /// Sets the Kafka fetch max wait time in milliseconds.
    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.max_wait_ms = max_wait_ms;
        self
    }

    /// Sets the Kafka fetch minimum response bytes.
    pub fn min_bytes(mut self, min_bytes: i32) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    /// Sets the maximum bytes to fetch from each partition.
    pub fn max_partition_bytes(mut self, max_partition_bytes: i32) -> Self {
        self.max_partition_bytes = max_partition_bytes;
        self
    }

    /// Sets the maximum number of retry attempts for transient fetch failures.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Returns the configured maximum retry count.
    pub fn max_retries_ref(&self) -> u32 {
        self.max_retries
    }

    /// Sets the maximum number of records returned by one poll.
    pub fn max_poll_records(mut self, max_poll_records: usize) -> Self {
        self.max_poll_records = max_poll_records;
        self
    }

    /// Returns the configured maximum records per poll.
    pub fn max_poll_records_ref(&self) -> usize {
        self.max_poll_records
    }

    /// Returns the shared client configuration.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Connects to Kafka and builds a direct consumer.
    pub async fn build(self) -> Result<Consumer> {
        let client = self.client.clone().connect().await?;
        Ok(Consumer {
            client,
            config: self,
            assignments: Vec::new(),
            metadata_cache: BTreeMap::new(),
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

fn can_retry_fetch(error: &Error) -> bool {
    match error {
        Error::Broker { code, .. } => matches!(
            BrokerErrorKind::from_code(*code),
            BrokerErrorKind::UnknownTopicOrPartition
                | BrokerErrorKind::LeaderNotAvailable
                | BrokerErrorKind::NotLeaderOrFollower
                | BrokerErrorKind::RequestTimedOut
                | BrokerErrorKind::ReplicaNotAvailable
        ),
        Error::Io(_) | Error::RequestTimedOut { .. } => true,
        Error::MissingBootstrapServer
        | Error::UnknownTopicOrPartition { .. }
        | Error::MissingLeader { .. }
        | Error::MissingBroker { .. }
        | Error::TlsConfig { .. }
        | Error::InvalidTlsServerName { .. }
        | Error::Unsupported(_)
        | Error::TaskJoin(_)
        | Error::Protocol(_) => false,
    }
}

fn limit_fetched_records(
    fetched: &mut Vec<ConsumerRecord>,
    current_record_count: usize,
    max_poll_records: usize,
) {
    let remaining = max_poll_records.saturating_sub(current_record_count);
    fetched.truncate(remaining);
}

fn invalidate_metadata_cache(
    metadata_cache: &mut BTreeMap<String, MetadataResponseV1>,
    topic: &str,
) {
    metadata_cache.remove(topic);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        assign_partition, can_retry_fetch, invalidate_metadata_cache, leader_for,
        limit_fetched_records, ConsumerAssignment, ConsumerConfig, ConsumerRecord,
        SecurityProtocol,
    };
    use crate::Error;
    use kafrust_protocol::api::fetch::MessageSetRecord;
    use kafrust_protocol::api::metadata::{
        BrokerMetadata, MetadataResponseV1, PartitionMetadata, TopicMetadata,
    };
    use std::collections::BTreeMap;

    #[test]
    fn builds_consumer_config() {
        let config = ConsumerConfig::new(["localhost:9092"])
            .client_id("orders-reader")
            .request_timeout_ms(5_000)
            .security_protocol(SecurityProtocol::Tls)
            .sasl_plain("alice", "secret-password")
            .max_wait_ms(250)
            .min_bytes(10)
            .max_partition_bytes(1024)
            .max_retries(3)
            .max_poll_records(10);

        assert_eq!(
            config.client_config().client_id_ref(),
            Some("orders-reader")
        );
        assert_eq!(
            config.client_config().security_protocol_ref(),
            SecurityProtocol::Tls
        );
        assert_eq!(
            config
                .client_config()
                .sasl_credentials_ref()
                .unwrap()
                .username(),
            "alice"
        );
        assert_eq!(config.max_retries_ref(), 3);
        assert_eq!(config.max_poll_records_ref(), 10);
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
    fn tracks_assignments() {
        let mut assignments = Vec::<ConsumerAssignment>::new();

        assign_partition(&mut assignments, "orders".to_owned(), 0, 10);
        assign_partition(&mut assignments, "orders".to_owned(), 0, 20);

        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].topic(), "orders");
        assert_eq!(assignments[0].partition(), 0);
        assert_eq!(assignments[0].next_offset(), 20);
    }

    #[test]
    fn resolves_partition_leader() {
        assert_eq!(leader_for(&metadata_fixture(), "orders", 0).unwrap(), 1);
    }

    #[test]
    fn classifies_retriable_fetch_errors() {
        assert!(can_retry_fetch(&Error::Broker {
            code: 6,
            context: "fetch orders-0@0".to_owned(),
        }));
        assert!(can_retry_fetch(&Error::RequestTimedOut { timeout_ms: 5 }));
        assert!(can_retry_fetch(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))));
        assert!(!can_retry_fetch(&Error::Unsupported("fetch v99")));
    }

    #[test]
    fn limits_fetched_records_to_remaining_poll_budget() {
        let mut records = vec![
            ConsumerRecord::from_message_set("orders", 0, message(10)),
            ConsumerRecord::from_message_set("orders", 0, message(11)),
            ConsumerRecord::from_message_set("orders", 0, message(12)),
        ];

        limit_fetched_records(&mut records, 1, 3);

        assert_eq!(records.len(), 2);
        assert_eq!(records[1].offset(), 11);
    }

    #[test]
    fn clears_fetched_records_when_poll_budget_is_exhausted() {
        let mut records = vec![ConsumerRecord::from_message_set("orders", 0, message(10))];

        limit_fetched_records(&mut records, 3, 3);

        assert!(records.is_empty());
    }

    #[test]
    fn invalidates_topic_metadata_cache() {
        let mut cache = BTreeMap::new();
        cache.insert("orders".to_owned(), metadata_fixture());
        cache.insert("payments".to_owned(), metadata_fixture());

        invalidate_metadata_cache(&mut cache, "orders");

        assert!(!cache.contains_key("orders"));
        assert!(cache.contains_key("payments"));
    }

    fn message(offset: i64) -> MessageSetRecord {
        MessageSetRecord {
            offset,
            timestamp_ms: 123,
            key: None,
            value: None,
        }
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
