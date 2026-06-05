use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kafrust_protocol::api::api_versions::ApiVersionsResponseV0;
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};
use kafrust_protocol::api::produce::{
    MessageSetMessage, ProducePartitionResponseV2, ProduceResponseV2, RecordBatchMessage,
    API_KEY as PRODUCE_API_KEY,
};

use crate::client::Client;
use crate::config::ClientConfig;
use crate::error::{BrokerErrorKind, Error, Result};
use tracing::debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka produce acknowledgement policy.
pub enum Acks {
    /// Do not wait for a broker response.
    None,
    /// Wait for the partition leader to acknowledge the write.
    Leader,
    /// Wait for all in-sync replicas required by the topic configuration.
    All,
}

impl Acks {
    /// Returns the Kafka protocol value for this acknowledgement policy.
    pub fn as_i16(self) -> i16 {
        match self {
            Self::None => 0,
            Self::Leader => 1,
            Self::All => -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Kafka record header.
pub struct Header {
    key: String,
    value: Vec<u8>,
}

impl Header {
    /// Creates a record header from a key and raw value bytes.
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Returns the header key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the header value bytes.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Record to produce to a Kafka topic.
pub struct ProducerRecord {
    topic: String,
    partition: Option<i32>,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<Header>,
    timestamp: Option<SystemTime>,
}

impl ProducerRecord {
    /// Creates a record targeting a Kafka topic.
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

    /// Sets an explicit Kafka partition for this record.
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Sets the record key bytes.
    pub fn key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Sets the record value bytes.
    pub fn value(mut self, value: impl Into<Vec<u8>>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Adds a Kafka record header.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(Header::new(key, value));
        self
    }

    /// Sets the record timestamp.
    pub fn timestamp(mut self, timestamp: SystemTime) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Returns the target Kafka topic.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the explicit partition, when one was set.
    pub fn partition_ref(&self) -> Option<i32> {
        self.partition
    }

    /// Returns the record key bytes.
    pub fn key_ref(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// Returns the record value bytes.
    pub fn value_ref(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }

    /// Returns the configured record headers.
    pub fn headers(&self) -> &[Header] {
        &self.headers
    }

    /// Returns the configured timestamp.
    pub fn timestamp_ref(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Metadata returned after a successful produce request.
pub struct RecordMetadata {
    topic: String,
    partition: i32,
    offset: i64,
    timestamp: Option<SystemTime>,
}

impl RecordMetadata {
    /// Creates metadata for a produced record.
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

    /// Returns the Kafka topic that accepted the record.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the Kafka partition that accepted the record.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the base offset reported by Kafka.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Returns the timestamp associated with the produced record.
    pub fn timestamp(&self) -> Option<SystemTime> {
        self.timestamp
    }
}

#[derive(Debug)]
/// Kafka producer using metadata-based leader routing.
pub struct Producer {
    client: Client,
    config: ProducerConfig,
    metadata_cache: BTreeMap<String, MetadataResponseV1>,
}

impl Producer {
    /// Sends one record and returns Kafka metadata for the accepted write.
    pub async fn send(&mut self, record: ProducerRecord) -> Result<RecordMetadata> {
        if self.config.acks == Acks::None {
            return Err(Error::Unsupported("producer acks=0 send without response"));
        }
        debug!(
            topic = record.topic(),
            partition = ?record.partition_ref(),
            key_bytes = record.key_ref().map(|key| key.len()),
            value_bytes = record.value_ref().map(|value| value.len()),
            header_count = record.headers().len(),
            "sending kafka record"
        );

        let timestamp = record.timestamp_ref().unwrap_or_else(SystemTime::now);
        let timestamp_ms = timestamp_millis(timestamp);
        let mut attempt = 0;
        let topic = record.topic().to_owned();

        loop {
            let metadata = self.metadata_for_topic(&topic).await?;
            let result = self
                .send_with_metadata(&record, &metadata, timestamp, timestamp_ms)
                .await;

            match result {
                Err(error) if attempt < self.config.max_retries && can_retry_send(&error) => {
                    invalidate_metadata_cache(&mut self.metadata_cache, &topic);
                    attempt += 1;
                }
                Ok(metadata) => {
                    debug!(
                        topic = metadata.topic(),
                        partition = metadata.partition(),
                        offset = metadata.offset(),
                        "sent kafka record"
                    );
                    return Ok(metadata);
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Sends multiple records and returns one metadata entry per input record.
    ///
    /// Records are grouped by topic, partition, and partition leader. Records in
    /// the same group are sent in one Produce request. Returned metadata keeps
    /// the same order as the input records.
    pub async fn send_batch(
        &mut self,
        records: impl IntoIterator<Item = ProducerRecord>,
    ) -> Result<Vec<RecordMetadata>> {
        if self.config.acks == Acks::None {
            return Err(Error::Unsupported("producer acks=0 send without response"));
        }

        let records = records
            .into_iter()
            .map(BatchRecord::new)
            .collect::<Vec<_>>();
        if records.is_empty() {
            return Ok(Vec::new());
        }

        debug!(record_count = records.len(), "sending kafka record batch");

        let mut attempt = 0;
        loop {
            let result = self.send_batch_once(&records).await;
            match result {
                Err(error) if attempt < self.config.max_retries && can_retry_send(&error) => {
                    invalidate_metadata_cache_for_records(&mut self.metadata_cache, &records);
                    attempt += 1;
                }
                Ok(metadata) => {
                    debug!(record_count = metadata.len(), "sent kafka record batch");
                    return Ok(metadata);
                }
                Err(error) => return Err(error),
            }
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

    async fn send_batch_once(&mut self, records: &[BatchRecord]) -> Result<Vec<RecordMetadata>> {
        let mut groups = BTreeMap::<ProduceBatchKey, Vec<PreparedBatchRecord<'_>>>::new();
        for (index, record) in records.iter().enumerate() {
            let metadata = self.metadata_for_topic(record.record.topic()).await?;
            let partition = choose_partition(&record.record, &metadata)?;
            let leader = leader_for(&metadata, record.record.topic(), partition)?;
            let broker_addr = broker_addr_for(&metadata, leader)?;
            groups
                .entry(ProduceBatchKey {
                    broker_addr,
                    topic: record.record.topic().to_owned(),
                    partition,
                })
                .or_default()
                .push(PreparedBatchRecord { index, record });
        }

        let mut output = vec![None; records.len()];
        for (key, records) in groups {
            for (index, metadata) in self.send_batch_group(&key, &records).await? {
                output[index] = Some(metadata);
            }
        }

        output
            .into_iter()
            .map(|metadata| metadata.ok_or(Error::Unsupported("missing batch record metadata")))
            .collect()
    }

    async fn send_batch_group(
        &self,
        key: &ProduceBatchKey,
        records: &[PreparedBatchRecord<'_>],
    ) -> Result<Vec<(usize, RecordMetadata)>> {
        debug!(
            topic = key.topic.as_str(),
            partition = key.partition,
            broker_addr = key.broker_addr.as_str(),
            record_count = records.len(),
            "resolved produce batch leader"
        );

        let mut leader_client = Client::connect_with_request_timeout(
            key.broker_addr.clone(),
            self.config.client.client_id_ref().map(str::to_owned),
            self.config.client.request_timeout(),
        )
        .await?;
        let api_versions = leader_client.api_versions().await?;
        if api_versions.error_code != 0 {
            return Err(Error::Broker {
                code: api_versions.error_code,
                context: format!("api versions for produce {}-{}", key.topic, key.partition),
            });
        }

        let produce_version = select_produce_batch_version(&api_versions, records)?;
        debug!(
            topic = key.topic.as_str(),
            partition = key.partition,
            produce_version = ?produce_version,
            record_count = records.len(),
            "selected produce batch api version"
        );

        let response = match produce_version {
            ProduceVersion::V3 => {
                leader_client
                    .produce_one_v3(
                        None,
                        self.config.acks.as_i16(),
                        30_000,
                        key.topic.clone(),
                        key.partition,
                        records
                            .iter()
                            .map(|record| {
                                record_batch_message(
                                    &record.record.record,
                                    record.record.timestamp_ms,
                                )
                            })
                            .collect(),
                    )
                    .await?
            }
            ProduceVersion::V2 => {
                leader_client
                    .produce_one_v2(
                        self.config.acks.as_i16(),
                        30_000,
                        key.topic.clone(),
                        key.partition,
                        records
                            .iter()
                            .map(|record| {
                                message_set_message(
                                    &record.record.record,
                                    record.record.timestamp_ms,
                                )
                            })
                            .collect(),
                    )
                    .await?
            }
        };
        let partition_response = produce_partition_response(&response, &key.topic, key.partition)?;
        if partition_response.error_code != 0 {
            return Err(Error::Broker {
                code: partition_response.error_code,
                context: format!("produce {}-{}", key.topic, key.partition),
            });
        }

        records
            .iter()
            .enumerate()
            .map(|(relative_offset, record)| {
                Ok((
                    record.index,
                    RecordMetadata::new(
                        key.topic.clone(),
                        key.partition,
                        partition_response.base_offset
                            + i64::try_from(relative_offset).unwrap_or(0),
                        Some(record.record.timestamp),
                    ),
                ))
            })
            .collect()
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
        debug!(
            topic = record.topic(),
            partition,
            leader,
            broker_addr = broker_addr.as_str(),
            "resolved produce leader"
        );

        let mut leader_client = Client::connect_with_request_timeout(
            broker_addr,
            self.config.client.client_id_ref().map(str::to_owned),
            self.config.client.request_timeout(),
        )
        .await?;
        let api_versions = leader_client.api_versions().await?;
        if api_versions.error_code != 0 {
            return Err(Error::Broker {
                code: api_versions.error_code,
                context: format!("api versions for produce {}-{}", record.topic(), partition),
            });
        }

        let produce_version = select_produce_version(&api_versions, record)?;
        debug!(
            topic = record.topic(),
            partition,
            produce_version = ?produce_version,
            "selected produce api version"
        );

        let response = match produce_version {
            ProduceVersion::V3 => {
                leader_client
                    .produce_one_v3(
                        None,
                        self.config.acks.as_i16(),
                        30_000,
                        record.topic().to_owned(),
                        partition,
                        vec![record_batch_message(record, timestamp_ms)],
                    )
                    .await?
            }
            ProduceVersion::V2 => {
                leader_client
                    .produce_one_v2(
                        self.config.acks.as_i16(),
                        30_000,
                        record.topic().to_owned(),
                        partition,
                        vec![message_set_message(record, timestamp_ms)],
                    )
                    .await?
            }
        };
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

#[derive(Debug)]
struct BatchRecord {
    record: ProducerRecord,
    timestamp: SystemTime,
    timestamp_ms: i64,
}

impl BatchRecord {
    fn new(record: ProducerRecord) -> Self {
        let timestamp = record.timestamp_ref().unwrap_or_else(SystemTime::now);
        Self {
            record,
            timestamp,
            timestamp_ms: timestamp_millis(timestamp),
        }
    }
}

#[derive(Debug)]
struct PreparedBatchRecord<'a> {
    index: usize,
    record: &'a BatchRecord,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ProduceBatchKey {
    broker_addr: String,
    topic: String,
    partition: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Configuration builder for [`Producer`].
pub struct ProducerConfig {
    client: ClientConfig,
    acks: Acks,
    max_retries: u32,
}

impl ProducerConfig {
    /// Creates a producer configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            acks: Acks::Leader,
            max_retries: 1,
        }
    }

    /// Sets the Kafka client ID used by producer requests.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    /// Sets the request timeout in milliseconds.
    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(request_timeout_ms);
        self
    }

    /// Sets the Kafka produce acknowledgement policy.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.acks = acks;
        self
    }

    /// Sets the maximum number of retry attempts for retriable send failures.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Returns the configured acknowledgement policy.
    pub fn acks_ref(&self) -> Acks {
        self.acks
    }

    /// Returns the configured maximum retry count.
    pub fn max_retries_ref(&self) -> u32 {
        self.max_retries
    }

    /// Returns the shared client configuration.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Connects to Kafka and builds a producer.
    pub async fn build(self) -> Result<Producer> {
        let client = self.client.clone().connect().await?;
        Ok(Producer {
            client,
            config: self,
            metadata_cache: BTreeMap::new(),
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

fn record_batch_message(record: &ProducerRecord, timestamp_ms: i64) -> RecordBatchMessage {
    let mut message = RecordBatchMessage::new(
        record.key_ref().map(|key| key.to_vec()),
        record.value_ref().map(|value| value.to_vec()),
        timestamp_ms,
    );
    for header in record.headers() {
        message = message.header(header.key(), Some(header.value().to_vec()));
    }
    message
}

fn message_set_message(record: &ProducerRecord, timestamp_ms: i64) -> MessageSetMessage {
    MessageSetMessage::new(
        record.key_ref().map(|key| key.to_vec()),
        record.value_ref().map(|value| value.to_vec()),
        timestamp_ms,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProduceVersion {
    V2,
    V3,
}

fn select_produce_version(
    api_versions: &ApiVersionsResponseV0,
    record: &ProducerRecord,
) -> Result<ProduceVersion> {
    if api_versions
        .highest_supported_version(PRODUCE_API_KEY, 3)
        .is_some_and(|version| version >= 3)
    {
        return Ok(ProduceVersion::V3);
    }

    if api_versions
        .highest_supported_version(PRODUCE_API_KEY, 2)
        .is_some_and(|version| version >= 2)
    {
        if record.headers().is_empty() {
            return Ok(ProduceVersion::V2);
        }
        return Err(Error::Unsupported("record headers require Produce API v3"));
    }

    Err(Error::Unsupported("Produce API v2 or newer"))
}

fn select_produce_batch_version(
    api_versions: &ApiVersionsResponseV0,
    records: &[PreparedBatchRecord<'_>],
) -> Result<ProduceVersion> {
    if api_versions
        .highest_supported_version(PRODUCE_API_KEY, 3)
        .is_some_and(|version| version >= 3)
    {
        return Ok(ProduceVersion::V3);
    }

    if api_versions
        .highest_supported_version(PRODUCE_API_KEY, 2)
        .is_some_and(|version| version >= 2)
    {
        if records
            .iter()
            .all(|record| record.record.record.headers().is_empty())
        {
            return Ok(ProduceVersion::V2);
        }
        return Err(Error::Unsupported("record headers require Produce API v3"));
    }

    Err(Error::Unsupported("Produce API v2 or newer"))
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
        | Error::TaskJoin(_)
        | Error::Protocol(_) => false,
    }
}

fn invalidate_metadata_cache(
    metadata_cache: &mut BTreeMap<String, MetadataResponseV1>,
    topic: &str,
) {
    metadata_cache.remove(topic);
}

fn invalidate_metadata_cache_for_records(
    metadata_cache: &mut BTreeMap<String, MetadataResponseV1>,
    records: &[BatchRecord],
) {
    for record in records {
        metadata_cache.remove(record.record.topic());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        can_retry_send, choose_partition, invalidate_metadata_cache,
        invalidate_metadata_cache_for_records, leader_for, message_set_message,
        record_batch_message, select_produce_batch_version, select_produce_version, Acks,
        BatchRecord, PreparedBatchRecord, ProduceVersion, ProducerConfig, ProducerRecord,
        RecordMetadata,
    };
    use crate::{BrokerErrorKind, Error};
    use kafrust_protocol::api::api_versions::{ApiKeyVersion, ApiVersionsResponseV0};
    use kafrust_protocol::api::metadata::{
        BrokerMetadata, MetadataResponseV1, PartitionMetadata, TopicMetadata,
    };
    use kafrust_protocol::api::produce::API_KEY as PRODUCE_API_KEY;
    use std::collections::BTreeMap;

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
    fn maps_producer_record_headers_to_record_batch_message() {
        let record = ProducerRecord::to("orders")
            .key("order-123")
            .value("created")
            .header("source", "checkout");

        let message = record_batch_message(&record, 1_000);

        assert_eq!(message.key.as_deref(), Some(&b"order-123"[..]));
        assert_eq!(message.value.as_deref(), Some(&b"created"[..]));
        assert_eq!(message.timestamp_ms, 1_000);
        assert_eq!(message.headers[0].key, "source");
        assert_eq!(message.headers[0].value.as_deref(), Some(&b"checkout"[..]));
    }

    #[test]
    fn maps_producer_record_to_message_set_message() {
        let record = ProducerRecord::to("orders")
            .key("order-123")
            .value("created");

        let message = message_set_message(&record, 1_000);

        assert_eq!(message.key.as_deref(), Some(&b"order-123"[..]));
        assert_eq!(message.value.as_deref(), Some(&b"created"[..]));
        assert_eq!(message.timestamp_ms, 1_000);
    }

    #[test]
    fn selects_record_batch_when_produce_v3_is_available() {
        let versions = api_versions(3);
        let record = ProducerRecord::to("orders").header("source", "checkout");

        assert_eq!(
            select_produce_version(&versions, &record).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn falls_back_to_message_set_without_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let record = ProducerRecord::to("orders");

        assert_eq!(
            select_produce_version(&versions, &record).unwrap(),
            ProduceVersion::V2
        );
    }

    #[test]
    fn rejects_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let record = ProducerRecord::to("orders").header("source", "checkout");

        assert!(matches!(
            select_produce_version(&versions, &record).unwrap_err(),
            Error::Unsupported("record headers require Produce API v3")
        ));
    }

    #[test]
    fn selects_record_batch_for_batch_when_produce_v3_is_available() {
        let versions = api_versions(3);
        let first = BatchRecord::new(ProducerRecord::to("orders").header("source", "checkout"));
        let second = BatchRecord::new(ProducerRecord::to("orders"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert_eq!(
            select_produce_batch_version(&versions, &records).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn falls_back_to_message_set_for_batch_without_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").key("order-2"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert_eq!(
            select_produce_batch_version(&versions, &records).unwrap(),
            ProduceVersion::V2
        );
    }

    #[test]
    fn rejects_batch_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").header("source", "checkout"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert!(matches!(
            select_produce_batch_version(&versions, &records).unwrap_err(),
            Error::Unsupported("record headers require Produce API v3")
        ));
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
    fn invalidates_batch_record_topics() {
        let mut cache = BTreeMap::new();
        cache.insert("orders".to_owned(), metadata_fixture());
        cache.insert("payments".to_owned(), metadata_fixture());
        cache.insert("shipments".to_owned(), metadata_fixture());
        let records = vec![
            BatchRecord::new(ProducerRecord::to("orders")),
            BatchRecord::new(ProducerRecord::to("payments")),
        ];

        invalidate_metadata_cache_for_records(&mut cache, &records);

        assert!(!cache.contains_key("orders"));
        assert!(!cache.contains_key("payments"));
        assert!(cache.contains_key("shipments"));
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

    fn api_versions(max_produce_version: i16) -> ApiVersionsResponseV0 {
        ApiVersionsResponseV0 {
            error_code: 0,
            api_keys: vec![ApiKeyVersion {
                api_key: PRODUCE_API_KEY,
                min_version: 0,
                max_version: max_produce_version,
            }],
        }
    }

    fn prepared_records(records: &[BatchRecord]) -> Vec<PreparedBatchRecord<'_>> {
        records
            .iter()
            .enumerate()
            .map(|(index, record)| PreparedBatchRecord { index, record })
            .collect()
    }
}
