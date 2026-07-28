use std::collections::BTreeMap;

use kafrust_protocol::api::fetch::{FetchPartitionResponseV4, FetchResponseV4, MessageSetRecord};
use kafrust_protocol::api::list_offsets::{
    ListOffsetsPartitionResponseV1, ListOffsetsPartitionV1, ListOffsetsTopicResponseV1,
    ListOffsetsTopicV1, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP,
};
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};

use crate::client::{Client, FetchOneRequestV4};
use crate::config::{ClientConfig, SecurityProtocol};
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;
use tracing::debug;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Controls whether a consumer can return records from aborted transactions.
pub enum IsolationLevel {
    /// Return all records, including records from aborted transactions.
    #[default]
    ReadUncommitted,
    /// Return only committed records and hide Kafka transaction control records.
    ReadCommitted,
}

impl IsolationLevel {
    fn as_i8(self) -> i8 {
        match self {
            Self::ReadUncommitted => 0,
            Self::ReadCommitted => 1,
        }
    }
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Earliest and latest available offsets for one Kafka topic partition.
pub struct PartitionWatermarks {
    low: i64,
    high: i64,
}

impl PartitionWatermarks {
    /// Returns the earliest available offset.
    pub fn low(&self) -> i64 {
        self.low
    }

    /// Returns the latest offset, which is the next offset after the log end.
    pub fn high(&self) -> i64 {
        self.high
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

    /// Returns the next offset for an assigned topic partition.
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assignment(topic, partition)
            .map(ConsumerAssignment::next_offset)
    }

    /// Changes the next offset for an assigned topic partition.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.assignment_mut(topic, partition)?.next_offset = offset;
        Ok(())
    }

    /// Pauses fetching from an assigned topic partition.
    pub fn pause(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.assignment_mut(topic, partition)?.paused = true;
        Ok(())
    }

    /// Resumes fetching from an assigned topic partition.
    pub fn resume(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.assignment_mut(topic, partition)?.paused = false;
        Ok(())
    }

    /// Fetches the earliest and latest available offsets for a topic partition.
    ///
    /// The partition does not need to be assigned to this consumer. Kafka's
    /// latest offset is the next offset after the current log end.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer.fetch_watermarks",
        skip_all,
        fields(topic = tracing::field::Empty, partition),
        err
    )]
    pub async fn fetch_watermarks(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<PartitionWatermarks> {
        let topic = topic.into();
        tracing::Span::current().record("topic", topic.as_str());
        let mut attempt = 0;

        loop {
            match self.fetch_watermarks_once(&topic, partition).await {
                Err(error) if attempt < self.config.max_retries && can_retry_fetch(&error) => {
                    invalidate_metadata_cache(&mut self.metadata_cache, &topic);
                    self.config.client.record_retry();
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    async fn fetch_watermarks_once(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionWatermarks> {
        let metadata = self.metadata_for_topic(topic).await?;
        let leader = leader_for(&metadata, topic, partition)?;
        let broker_addr = broker_addr_for(&metadata, leader)?;
        let mut leader_client = self.config.client.connect_broker(broker_addr).await?;
        let low = self
            .request_partition_offset(&mut leader_client, topic, partition, EARLIEST_TIMESTAMP)
            .await?;
        let high = self
            .request_partition_offset(&mut leader_client, topic, partition, LATEST_TIMESTAMP)
            .await?;
        Ok(PartitionWatermarks { low, high })
    }

    async fn request_partition_offset(
        &self,
        client: &mut Client,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Result<i64> {
        let response = client
            .list_offsets_v1(vec![ListOffsetsTopicV1 {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartitionV1 {
                    partition_index: partition,
                    timestamp,
                }],
            }])
            .await?;
        let partition_response =
            list_offset_partition_response(&response.topics, topic, partition)?;
        if partition_response.error_code != 0 {
            return Err(self.config.client.broker_error(
                partition_response.error_code,
                format!("list offsets {topic}-{partition}"),
            ));
        }
        Ok(partition_response.offset)
    }

    /// Polls assigned partitions and advances in-memory offsets for fetched records.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer.poll",
        skip_all,
        fields(assignment_count = self.assignments.len(), max_poll_records = self.config.max_poll_records),
        err
    )]
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
            if assignment.paused {
                continue;
            }

            let mut fetched = self
                .fetch_with_progress(
                    &assignment.topic,
                    assignment.partition,
                    assignment.next_offset,
                )
                .await?;
            let fetched_record_count = fetched.records.len();
            limit_fetched_records(
                &mut fetched.records,
                records.len(),
                self.config.max_poll_records,
            );
            let next_offset = if fetched.records.len() < fetched_record_count {
                fetched
                    .records
                    .last()
                    .map(|record| record.offset().saturating_add(1))
            } else {
                Some(fetched.next_offset)
            };
            if let Some(next_offset) = next_offset {
                self.update_assignment_offset(&assignment.topic, assignment.partition, next_offset);
            }
            records.extend(fetched.records);
        }

        debug!(
            record_count = records.len(),
            "polled kafka consumer records"
        );
        self.config.client.record_consumed(records.len());
        Ok(records)
    }

    /// Fetches records for one topic partition without changing assignment state.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer.fetch",
        skip_all,
        fields(topic = tracing::field::Empty, partition, offset),
        err
    )]
    pub async fn fetch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<ConsumerRecord>> {
        let topic = topic.into();
        tracing::Span::current().record("topic", topic.as_str());
        let records = self
            .fetch_with_progress(&topic, partition, offset)
            .await?
            .records;
        self.config.client.record_consumed(records.len());
        Ok(records)
    }

    async fn fetch_with_progress(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<FetchedPartition> {
        let mut attempt = 0;
        debug!(topic, partition, offset, "fetching kafka records");

        loop {
            let result = self.fetch_once(topic, partition, offset).await;
            match result {
                Err(error) if attempt < self.config.max_retries && can_retry_fetch(&error) => {
                    invalidate_metadata_cache(&mut self.metadata_cache, topic);
                    self.config.client.record_retry();
                    attempt += 1;
                }
                Ok(records) => {
                    debug!(
                        topic,
                        partition,
                        offset,
                        record_count = records.records.len(),
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
    ) -> Result<FetchedPartition> {
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
            .fetch_one_v4(FetchOneRequestV4 {
                replica_id: -1,
                max_wait_ms: self.config.max_wait_ms,
                min_bytes: self.config.min_bytes,
                max_bytes: self.config.max_partition_bytes,
                isolation_level: self.config.isolation_level.as_i8(),
                topic: topic.to_owned(),
                partition_index: partition,
                fetch_offset: offset,
                max_partition_bytes: self.config.max_partition_bytes,
            })
            .await?;
        let partition_response = fetch_partition_response(&response, topic, partition)?;
        if partition_response.error_code != 0 {
            return Err(self.config.client.broker_error(
                partition_response.error_code,
                format!("fetch {topic}-{partition}@{offset}"),
            ));
        }

        let next_offset = partition_response
            .records
            .last()
            .map(|record| record.offset.saturating_add(1))
            .unwrap_or(offset);
        let records = visible_partition_records(partition_response, self.config.isolation_level)
            .into_iter()
            .map(|record| ConsumerRecord::from_message_set(topic, partition, record))
            .collect();
        Ok(FetchedPartition {
            records,
            next_offset,
        })
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

    fn assignment(&self, topic: &str, partition: i32) -> Option<&ConsumerAssignment> {
        self.assignments
            .iter()
            .find(|assignment| assignment.topic == topic && assignment.partition == partition)
    }

    fn assignment_mut(&mut self, topic: &str, partition: i32) -> Result<&mut ConsumerAssignment> {
        self.assignments
            .iter_mut()
            .find(|assignment| assignment.topic == topic && assignment.partition == partition)
            .ok_or_else(|| Error::UnassignedTopicPartition {
                topic: topic.to_owned(),
                partition,
            })
    }

    async fn metadata_for_topic(&mut self, topic: &str) -> Result<MetadataResponseV1> {
        if let Some(metadata) = self.metadata_cache.get(topic) {
            return Ok(metadata.clone());
        }

        let metadata = self.request_metadata_for_topic(topic).await?;
        self.metadata_cache
            .insert(topic.to_owned(), metadata.clone());
        Ok(metadata)
    }

    async fn request_metadata_for_topic(&mut self, topic: &str) -> Result<MetadataResponseV1> {
        let topics = Some(vec![topic.to_owned()]);
        match self.client.metadata(topics.clone()).await {
            Ok(metadata) => Ok(metadata),
            Err(error) if can_retry_fetch(&error) => {
                self.config.client.record_retry();
                debug!(
                    topic,
                    error = %error,
                    "reconnecting metadata client after metadata request failure"
                );
                self.client = self.config.client.clone().connect().await?;
                self.client.metadata(topics).await
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Debug)]
struct FetchedPartition {
    records: Vec<ConsumerRecord>,
    next_offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Direct consumer topic partition assignment.
pub struct ConsumerAssignment {
    topic: String,
    partition: i32,
    next_offset: i64,
    paused: bool,
}

impl ConsumerAssignment {
    pub(crate) fn new(topic: String, partition: i32, next_offset: i64) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            paused: false,
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

    /// Returns whether fetching is paused for this assignment.
    pub fn is_paused(&self) -> bool {
        self.paused
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
        paused: false,
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
    isolation_level: IsolationLevel,
}

impl ConsumerConfig {
    /// Creates a consumer configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::from_client_config(ClientConfig::new(bootstrap_servers))
    }

    pub(crate) fn from_client_config(client: ClientConfig) -> Self {
        Self {
            client,
            max_wait_ms: 500,
            min_bytes: 1,
            max_partition_bytes: 1_048_576,
            max_retries: 1,
            max_poll_records: 500,
            isolation_level: IsolationLevel::ReadUncommitted,
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

    /// Sets the maximum broker response payload allocated for one consumer request.
    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.client = self.client.max_response_bytes(max_response_bytes);
        self
    }

    /// Sets the maximum number of elements allocated for one Kafka response array.
    pub fn max_decode_array_elements(mut self, max: usize) -> Self {
        self.client = self.client.max_decode_array_elements(max);
        self
    }

    /// Sets the maximum uncompressed size of one fetched record batch.
    pub fn max_decompressed_record_bytes(mut self, max: usize) -> Self {
        self.client = self.client.max_decompressed_record_bytes(max);
        self
    }

    /// Sets the shared metrics handle used by consumer broker connections.
    pub fn metrics(mut self, metrics: ClientMetrics) -> Self {
        self.client = self.client.metrics(metrics);
        self
    }

    /// Sets the Kafka security protocol used for consumer broker connections.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.client = self.client.security_protocol(security_protocol);
        self
    }

    /// Sets the TLS server name used for consumer broker certificate validation.
    pub fn tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.client = self.client.tls_server_name(server_name);
        self
    }

    /// Adds a DER-encoded TLS root certificate for consumer broker validation.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_root_certificate_der(certificate);
        self
    }

    /// Sets SASL/PLAIN credentials for consumer broker connections.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.client = self.client.sasl_plain(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-256 credentials for consumer broker connections.
    pub fn sasl_scram_sha_256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_256(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-512 credentials for consumer broker connections.
    pub fn sasl_scram_sha_512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_512(username, password);
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

    /// Sets whether fetches expose records from aborted transactions.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Returns the configured transaction isolation level.
    pub fn isolation_level_ref(&self) -> IsolationLevel {
        self.isolation_level
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
    response: &'a FetchResponseV4,
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a FetchPartitionResponseV4> {
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

fn list_offset_partition_response<'a>(
    topics: &'a [ListOffsetsTopicResponseV1],
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a ListOffsetsPartitionResponseV1> {
    topics
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

fn visible_partition_records(
    partition: &FetchPartitionResponseV4,
    isolation_level: IsolationLevel,
) -> Vec<MessageSetRecord> {
    let mut aborted_transactions = partition.aborted_transactions.clone();
    let mut records = Vec::new();

    for record in &partition.records {
        if record.control {
            if let Some(producer_id) = record.producer_id {
                if let Some(index) = aborted_transactions.iter().position(|transaction| {
                    transaction.producer_id == producer_id
                        && transaction.first_offset <= record.offset
                }) {
                    aborted_transactions.remove(index);
                }
            }
            continue;
        }

        let aborted = isolation_level == IsolationLevel::ReadCommitted
            && record.transactional
            && record.producer_id.is_some_and(|producer_id| {
                aborted_transactions.iter().any(|transaction| {
                    transaction.producer_id == producer_id
                        && transaction.first_offset <= record.offset
                })
            });
        if !aborted {
            records.push(record.clone());
        }
    }

    records
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
        Error::Io(_)
        | Error::RequestTimedOut { .. }
        | Error::UnknownTopicOrPartition { .. }
        | Error::MissingLeader { .. }
        | Error::MissingBroker { .. } => true,
        Error::MissingBootstrapServer
        | Error::UnassignedTopicPartition { .. }
        | Error::MissingGroupDescription { .. }
        | Error::MissingDeleteGroupResult { .. }
        | Error::MissingSaslCredentials
        | Error::InvalidSaslResponse { .. }
        | Error::ResponseTooLarge { .. }
        | Error::TlsConfig { .. }
        | Error::InvalidTlsServerName { .. }
        | Error::InvalidGroupInstanceId
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
        limit_fetched_records, visible_partition_records, Consumer, ConsumerAssignment,
        ConsumerConfig, ConsumerRecord, IsolationLevel, PartitionWatermarks, SecurityProtocol,
    };
    use crate::{Client, ClientMetrics, Error};
    use kafrust_protocol::api::fetch::{
        AbortedTransactionV4, FetchPartitionResponseV4, MessageSetRecord,
    };
    use kafrust_protocol::api::metadata::{
        BrokerMetadata, MetadataResponseV1, PartitionMetadata, TopicMetadata,
    };
    use kafrust_protocol::codec::Encoder;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn builds_consumer_config() {
        let config = ConsumerConfig::new(["localhost:9092"])
            .client_id("orders-reader")
            .request_timeout_ms(5_000)
            .security_protocol(SecurityProtocol::Tls)
            .tls_server_name("broker.example.com")
            .tls_root_certificate_der([1, 2, 3])
            .sasl_plain("alice", "secret-password")
            .max_wait_ms(250)
            .min_bytes(10)
            .max_partition_bytes(1024)
            .max_retries(3)
            .max_poll_records(10)
            .isolation_level(IsolationLevel::ReadCommitted);

        assert_eq!(
            config.client_config().client_id_ref(),
            Some("orders-reader")
        );
        assert_eq!(
            config.client_config().security_protocol_ref(),
            SecurityProtocol::Tls
        );
        assert_eq!(
            config.client_config().tls_server_name_ref(),
            Some("broker.example.com")
        );
        assert_eq!(
            config.client_config().tls_root_certificates_der(),
            &[vec![1, 2, 3]]
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
        assert_eq!(config.isolation_level_ref(), IsolationLevel::ReadCommitted);
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
                producer_id: None,
                transactional: false,
                control: false,
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
        assert!(!assignments[0].is_paused());
    }

    #[tokio::test]
    async fn controls_assignment_position_and_pause_state() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let _connection = listener.accept().await.unwrap();
        });
        let mut consumer = ConsumerConfig::new([addr.to_string()])
            .build()
            .await
            .unwrap();
        consumer.assign("orders", 0, 10);

        assert_eq!(consumer.position("orders", 0), Some(10));
        consumer.seek("orders", 0, 20).unwrap();
        consumer.pause("orders", 0).unwrap();
        assert_eq!(consumer.position("orders", 0), Some(20));
        assert!(consumer.assignments()[0].is_paused());
        assert!(consumer.poll().await.unwrap().is_empty());

        consumer.resume("orders", 0).unwrap();
        assert!(!consumer.assignments()[0].is_paused());
        assert!(matches!(
            consumer.seek("orders", 1, 0).unwrap_err(),
            Error::UnassignedTopicPartition {
                topic,
                partition: 1
            } if topic == "orders"
        ));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetches_partition_watermarks_from_partition_leader() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for (correlation_id, timestamp, offset) in [(1, -2_i64, 4_i64), (2, -1_i64, 9_i64)] {
                let request = read_frame(&mut socket).await;
                assert_eq!(&request[0..4], &[0, 2, 0, 1]);
                assert_eq!(
                    i64::from_be_bytes(request[request.len() - 8..].try_into().unwrap()),
                    timestamp
                );
                write_frame(
                    &mut socket,
                    &list_offsets_response_frame(correlation_id, offset),
                )
                .await;
            }
        });
        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-watermarks-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()]).request_timeout_ms(500);
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);

        let watermarks = consumer.fetch_watermarks("orders", 0).await.unwrap();

        assert_eq!(watermarks, PartitionWatermarks { low: 4, high: 9 });
        assert_eq!(watermarks.low(), 4);
        assert_eq!(watermarks.high(), 9);
        server.await.unwrap();
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
        assert!(can_retry_fetch(&Error::UnknownTopicOrPartition {
            topic: "orders".to_owned(),
            partition: 3,
        }));
        assert!(can_retry_fetch(&Error::MissingLeader {
            topic: "orders".to_owned(),
            partition: 0,
        }));
        assert!(can_retry_fetch(&Error::MissingBroker { node_id: 2 }));
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

    #[test]
    fn read_committed_hides_aborted_records_and_control_markers() {
        let partition = FetchPartitionResponseV4 {
            partition_index: 0,
            error_code: 0,
            high_watermark: 14,
            last_stable_offset: 14,
            aborted_transactions: vec![AbortedTransactionV4 {
                producer_id: 7,
                first_offset: 10,
            }],
            records: vec![
                transactional_message(10, 7, false),
                transactional_message(11, 8, false),
                transactional_message(12, 7, true),
                transactional_message(13, 7, false),
            ],
        };

        let committed = visible_partition_records(&partition, IsolationLevel::ReadCommitted);
        let uncommitted = visible_partition_records(&partition, IsolationLevel::ReadUncommitted);

        assert_eq!(
            committed
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![11, 13]
        );
        assert_eq!(
            uncommitted
                .iter()
                .map(|record| record.offset)
                .collect::<Vec<_>>(),
            vec![10, 11, 13]
        );
    }

    #[tokio::test]
    async fn reconnects_metadata_client_after_request_io_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut socket).await;
            assert_eq!(&request[0..2], &[0, 3]);
            write_frame(&mut socket, &metadata_response_frame()).await;
        });

        let (client_stream, broker_stream) = tokio::io::duplex(64);
        drop(broker_stream);
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-consumer-test".to_owned()),
            Some(std::time::Duration::from_millis(50)),
        );
        let metrics = ClientMetrics::new();
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .metrics(metrics.clone());
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());

        let metadata = consumer.metadata_for_topic("orders").await.unwrap();

        assert_eq!(metadata.brokers[0].node_id, 1);
        assert_eq!(metadata.topics[0].name, "orders");
        assert!(consumer.metadata_cache.contains_key("orders"));
        assert_eq!(metrics.snapshot().retries, 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn records_consumed_metrics_for_fetch_results() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut socket).await;
            assert_eq!(&request[0..4], &[0, 1, 0, 4]);
            write_frame(&mut socket, &fetch_v4_response_frame()).await;
        });
        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-consumer-metrics-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let metrics = ClientMetrics::new();
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .metrics(metrics.clone());
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);

        let records = consumer.fetch("orders", 0, 42).await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset(), 42);
        assert_eq!(metrics.snapshot().consumed_records, 1);
        server.await.unwrap();
    }

    fn message(offset: i64) -> MessageSetRecord {
        MessageSetRecord {
            offset,
            timestamp_ms: 123,
            key: None,
            value: None,
            producer_id: None,
            transactional: false,
            control: false,
        }
    }

    fn transactional_message(offset: i64, producer_id: i64, control: bool) -> MessageSetRecord {
        MessageSetRecord {
            offset,
            timestamp_ms: 123,
            key: None,
            value: None,
            producer_id: Some(producer_id),
            transactional: true,
            control,
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

    async fn read_frame<T>(stream: &mut T) -> Vec<u8>
    where
        T: AsyncRead + Unpin,
    {
        let mut size = [0u8; 4];
        stream.read_exact(&mut size).await.unwrap();
        let size = usize::try_from(i32::from_be_bytes(size)).unwrap();
        let mut request = vec![0u8; size];
        stream.read_exact(&mut request).await.unwrap();
        request
    }

    async fn write_frame<T>(stream: &mut T, frame: &[u8])
    where
        T: AsyncWrite + Unpin,
    {
        stream
            .write_all(&(frame.len() as i32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(frame).await.unwrap();
        stream.flush().await.unwrap();
    }

    fn metadata_response_frame() -> Vec<u8> {
        vec![
            0, 0, 0, 1, // correlation id
            0, 0, 0, 1, // brokers count
            0, 0, 0, 1, // node id
            0, 9, b'l', b'o', b'c', b'a', b'l', b'h', b'o', b's', b't', // host
            0, 0, 35, 132, // port 9092
            0xff, 0xff, // null rack
            0, 0, 0, 1, // controller id
            0, 0, 0, 1, // topics count
            0, 0, // topic error code
            0, 6, b'o', b'r', b'd', b'e', b'r', b's', // topic name
            0,    // is internal false
            0, 0, 0, 1, // partition count
            0, 0, // partition error code
            0, 0, 0, 0, // partition index
            0, 0, 0, 1, // leader id
            0, 0, 0, 1, // replica count
            0, 0, 0, 1, // replica node
            0, 0, 0, 1, // isr count
            0, 0, 0, 1, // isr node
        ]
    }

    fn fetch_v4_response_frame() -> Vec<u8> {
        let mut message = Encoder::new();
        message.write_i32(0);
        message.write_i8(1);
        message.write_i8(0);
        message.write_i64(123);
        message.write_nullable_bytes(Some(b"order-1")).unwrap();
        message.write_nullable_bytes(Some(b"created")).unwrap();
        let message = message.into_bytes();

        let mut records = Encoder::new();
        records.write_i64(42);
        records.write_i32(i32::try_from(message.len()).unwrap());
        records.write_raw(&message);
        let records = records.into_bytes();

        let mut response = Encoder::new();
        response.write_i32(1);
        response.write_i32(0);
        response.write_i32(1);
        response.write_string("orders").unwrap();
        response.write_i32(1);
        response.write_i32(0);
        response.write_i16(0);
        response.write_i64(43);
        response.write_i64(43);
        response.write_i32(0);
        response.write_bytes(&records).unwrap();
        response.into_bytes()
    }

    fn list_offsets_response_frame(correlation_id: i32, offset: i64) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_i32(1);
        response.write_string("orders").unwrap();
        response.write_i32(1);
        response.write_i32(0);
        response.write_i16(0);
        response.write_i64(-1);
        response.write_i64(offset);
        response.into_bytes()
    }
}
