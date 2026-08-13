use std::collections::{BTreeMap, BTreeSet};

use kafrust_protocol::api::fetch::{
    AbortedTransactionV4, FetchPartitionResponseV11, FetchPartitionResponseV12,
    FetchPartitionResponseV4, FetchResponseV11, FetchResponseV12, FetchResponseV4,
    MessageSetRecord,
};
use kafrust_protocol::api::list_offsets::{
    ListOffsetsPartitionResponseV1, ListOffsetsPartitionV1, ListOffsetsTopicResponseV1,
    ListOffsetsTopicV1, EARLIEST_TIMESTAMP, LATEST_TIMESTAMP,
};
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};
use kafrust_protocol::api::offset_for_leader_epoch::{
    OffsetForLeaderEpochPartitionResponseV3, OffsetForLeaderEpochPartitionV3,
    OffsetForLeaderEpochTopicResponseV3, OffsetForLeaderEpochTopicV3,
};

use crate::client::{Client, FetchOneRequestV11, FetchOneRequestV12, FetchOneRequestV4};
use crate::config::{ClientConfig, OAuthBearerTokenProvider, SecurityProtocol};
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Starting position used when a consumer has no usable offset.
pub enum OffsetResetPolicy {
    /// Start at the partition's earliest retained offset.
    Earliest,
    /// Start after the partition's current log end.
    Latest,
    /// Start at an explicit absolute offset.
    Offset(i64),
}

impl Default for OffsetResetPolicy {
    fn default() -> Self {
        Self::Offset(0)
    }
}

impl OffsetResetPolicy {
    pub(crate) fn timestamp(self) -> Option<i64> {
        match self {
            Self::Earliest => Some(EARLIEST_TIMESTAMP),
            Self::Latest => Some(LATEST_TIMESTAMP),
            Self::Offset(_) => None,
        }
    }

    fn is_recovery(self) -> bool {
        matches!(self, Self::Earliest | Self::Latest)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Record fetched from a Kafka topic partition.
pub struct ConsumerRecord {
    topic: String,
    partition: i32,
    offset: i64,
    leader_epoch: i32,
    timestamp_ms: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<ConsumerRecordHeader>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Header attached to a fetched Kafka record.
///
/// Kafka permits a header value to be null. Use [`Self::value`] to preserve
/// that distinction instead of treating a null value as an empty byte slice.
pub struct ConsumerRecordHeader {
    key: String,
    value: Option<Vec<u8>>,
}

impl ConsumerRecordHeader {
    fn from_protocol(header: kafrust_protocol::api::produce::RecordBatchHeader) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }

    /// Returns the header key.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the nullable header value bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.value.as_deref()
    }
}

/// A bounded queue containing records fetched for one topic partition.
///
/// A queue is created with [`Consumer::split_partition_queue`]. The owning
/// consumer continues to perform network polling, while this handle provides
/// an independent receive path for the selected partition. Dropping the
/// handle closes the split and causes subsequent records to return through
/// [`Consumer::poll`]. Records already buffered in the queue remain readable
/// until it is drained.
#[derive(Debug)]
pub struct ConsumerPartitionQueue {
    topic: String,
    partition: i32,
    receiver: mpsc::Receiver<ConsumerRecord>,
}

impl ConsumerPartitionQueue {
    /// Returns the topic served by this queue.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the partition served by this queue.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Waits for the next record, or returns `None` after the queue is closed
    /// and drained.
    pub async fn recv(&mut self) -> Option<ConsumerRecord> {
        self.receiver.recv().await
    }

    /// Returns one immediately available record.
    ///
    /// `None` means that the queue is currently empty or has been closed and
    /// drained. Use [`Self::recv`] when the distinction matters.
    pub fn try_recv(&mut self) -> Option<ConsumerRecord> {
        self.receiver.try_recv().ok()
    }

    /// Receives up to `max_records`, waiting for the first record when the
    /// queue is not already closed. A zero limit returns an empty vector.
    pub async fn recv_batch(&mut self, max_records: usize) -> Vec<ConsumerRecord> {
        if max_records == 0 {
            return Vec::new();
        }

        let Some(first) = self.recv().await else {
            return Vec::new();
        };
        let mut records = Vec::with_capacity(max_records.min(16));
        records.push(first);
        while records.len() < max_records {
            let Some(record) = self.try_recv() else {
                break;
            };
            records.push(record);
        }
        records
    }
}

impl ConsumerRecord {
    fn from_message_set(topic: &str, partition: i32, record: MessageSetRecord) -> Self {
        Self {
            topic: topic.to_owned(),
            partition,
            offset: record.offset,
            leader_epoch: record.leader_epoch,
            timestamp_ms: record.timestamp_ms,
            key: record.key,
            value: record.value,
            headers: record
                .headers
                .into_iter()
                .map(ConsumerRecordHeader::from_protocol)
                .collect(),
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

    /// Returns the Kafka partition leader epoch attached to this record.
    ///
    /// Legacy MessageSet records return `-1` because that wire format has no
    /// leader epoch field.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
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

    /// Returns the headers attached to this record in wire order.
    pub fn headers(&self) -> &[ConsumerRecordHeader] {
        &self.headers
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Earliest and latest available offsets for one Kafka topic partition.
pub struct PartitionWatermarks {
    low: i64,
    high: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The end offset recorded for a requested Kafka partition leader epoch.
pub struct LeaderEpochOffset {
    leader_epoch: i32,
    end_offset: i64,
}

impl LeaderEpochOffset {
    /// Returns the leader epoch reported by Kafka.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    /// Returns the first offset after the requested epoch's log range.
    pub fn end_offset(&self) -> i64 {
        self.end_offset
    }
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
    partition_queues: BTreeMap<(String, i32), mpsc::Sender<ConsumerRecord>>,
    metadata_cache: BTreeMap<String, MetadataResponseV1>,
    broker_clients: BTreeMap<String, Client>,
    fetch_sessions: BTreeMap<String, FetchSessionState>,
    preferred_read_replicas: BTreeMap<(String, i32), i32>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct FetchSessionState {
    session_id: i32,
    next_epoch: i32,
}

impl FetchSessionState {
    fn next_request(self) -> (i32, i32) {
        (self.session_id, self.next_epoch)
    }

    fn advance_with_response(self, session_id: i32) -> Self {
        Self {
            session_id,
            next_epoch: self.next_epoch.saturating_add(1),
        }
    }
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
            partition_queues: BTreeMap::new(),
            metadata_cache: BTreeMap::new(),
            broker_clients: BTreeMap::new(),
            fetch_sessions: BTreeMap::new(),
            preferred_read_replicas: BTreeMap::new(),
        }
    }

    pub(crate) fn replace_assignments(&mut self, mut assignments: Vec<ConsumerAssignment>) {
        let previous_positions = self
            .assignments
            .iter()
            .map(|assignment| {
                (
                    (assignment.topic.clone(), assignment.partition),
                    assignment.next_offset,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let previous_leader_epochs = self
            .assignments
            .iter()
            .map(|assignment| {
                (
                    (assignment.topic.clone(), assignment.partition),
                    assignment.leader_epoch,
                )
            })
            .collect::<BTreeMap<_, _>>();
        let paused = self
            .assignments
            .iter()
            .filter(|assignment| assignment.paused)
            .map(|assignment| (assignment.topic.clone(), assignment.partition))
            .collect::<BTreeSet<_>>();
        for assignment in &mut assignments {
            assignment.paused = paused.contains(&(assignment.topic.clone(), assignment.partition));
            if let Some(previous_epoch) =
                previous_leader_epochs.get(&(assignment.topic.clone(), assignment.partition))
            {
                assignment.leader_epoch = *previous_epoch;
            }
        }
        assignments.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then_with(|| left.partition.cmp(&right.partition))
        });
        self.partition_queues.retain(|(topic, partition), _| {
            assignments.iter().any(|assignment| {
                assignment.topic == *topic
                    && assignment.partition == *partition
                    && previous_positions.get(&(topic.clone(), *partition))
                        == Some(&assignment.next_offset)
            })
        });
        self.assignments = assignments;
        self.metadata_cache.clear();
        self.fetch_sessions.clear();
    }

    /// Assigns a topic partition and next offset to fetch.
    pub fn assign(&mut self, topic: impl Into<String>, partition: i32, offset: i64) {
        let topic = topic.into();
        self.partition_queues.remove(&(topic.clone(), partition));
        assign_partition(&mut self.assignments, topic, partition, offset);
        self.fetch_sessions.clear();
    }

    /// Returns the current topic partition assignments.
    pub fn assignments(&self) -> &[ConsumerAssignment] {
        &self.assignments
    }

    /// Splits one assigned topic partition into a bounded receive queue.
    ///
    /// Records fetched for the partition are delivered to the returned queue
    /// instead of the vector returned by [`Self::poll`]. The queue capacity is
    /// configured with [`ConsumerConfig::partition_queue_capacity`]. When the
    /// queue is full, `poll` returns an error and does not advance beyond the
    /// last record accepted by the queue.
    pub fn split_partition_queue(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<ConsumerPartitionQueue> {
        let topic = topic.into();
        if self.assignment(&topic, partition).is_none() {
            return Err(Error::UnassignedTopicPartition { topic, partition });
        }

        let key = (topic.clone(), partition);
        if let Some(sender) = self.partition_queues.get(&key) {
            if !sender.is_closed() {
                return Err(Error::Unsupported("partition queue is already split"));
            }
        }
        self.partition_queues.remove(&key);

        let (sender, receiver) = mpsc::channel(self.config.partition_queue_capacity);
        self.partition_queues.insert(key, sender);
        Ok(ConsumerPartitionQueue {
            topic,
            partition,
            receiver,
        })
    }

    /// Returns the next offset for an assigned topic partition.
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.assignment(topic, partition)
            .map(ConsumerAssignment::next_offset)
    }

    /// Changes the next offset for an assigned topic partition.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        if self
            .partition_queues
            .get(&(topic.to_owned(), partition))
            .is_some_and(|sender| !sender.is_closed())
        {
            return Err(Error::Unsupported(
                "seek requires dropping the active partition queue",
            ));
        }
        self.assignment_mut(topic, partition)?.next_offset = offset;
        self.fetch_sessions.clear();
        Ok(())
    }

    /// Pauses fetching from an assigned topic partition.
    pub fn pause(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.assignment_mut(topic, partition)?.paused = true;
        self.fetch_sessions.clear();
        Ok(())
    }

    /// Resumes fetching from an assigned topic partition.
    pub fn resume(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.assignment_mut(topic, partition)?.paused = false;
        self.fetch_sessions.clear();
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

    /// Resolves the end offset for a partition leader epoch.
    ///
    /// `current_leader_epoch` is the epoch from the consumer's current
    /// metadata, or `-1` when it is unknown. `leader_epoch` is the epoch whose
    /// end offset should be returned. The partition does not need to be
    /// assigned to this consumer.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer.offset_for_leader_epoch",
        skip_all,
        fields(topic = tracing::field::Empty, partition, current_leader_epoch, leader_epoch),
        err
    )]
    pub async fn offset_for_leader_epoch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<LeaderEpochOffset> {
        let topic = topic.into();
        tracing::Span::current().record("topic", topic.as_str());
        let mut attempt = 0;

        loop {
            match self
                .offset_for_leader_epoch_once(&topic, partition, current_leader_epoch, leader_epoch)
                .await
            {
                Err(error) if attempt < self.config.max_retries && can_retry_fetch(&error) => {
                    invalidate_metadata_cache(&mut self.metadata_cache, &topic);
                    self.config.client.record_retry();
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    async fn offset_for_leader_epoch_once(
        &mut self,
        topic: &str,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<LeaderEpochOffset> {
        let metadata = self.metadata_for_topic(topic).await?;
        let leader = leader_for(&metadata, topic, partition)?;
        let broker_addr = broker_addr_for(&metadata, leader)?;
        let mut leader_client = self.connect_or_reuse_broker(&broker_addr).await?;
        let response = leader_client
            .offset_for_leader_epoch_v3(vec![OffsetForLeaderEpochTopicV3 {
                name: topic.to_owned(),
                partitions: vec![OffsetForLeaderEpochPartitionV3 {
                    partition_index: partition,
                    current_leader_epoch,
                    leader_epoch,
                }],
            }])
            .await?;
        let partition_response =
            offset_for_leader_epoch_partition_response(&response.topics, topic, partition)?;
        if partition_response.error_code != 0 {
            return Err(self.config.client.broker_error(
                partition_response.error_code,
                format!("offset for leader epoch {topic}-{partition}"),
            ));
        }
        self.broker_clients.insert(broker_addr, leader_client);
        Ok(LeaderEpochOffset {
            leader_epoch: partition_response.leader_epoch,
            end_offset: partition_response.end_offset,
        })
    }

    async fn fetch_watermarks_once(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<PartitionWatermarks> {
        let metadata = self.metadata_for_topic(topic).await?;
        let leader = leader_for(&metadata, topic, partition)?;
        let broker_addr = broker_addr_for(&metadata, leader)?;
        let mut leader_client = self.connect_or_reuse_broker(&broker_addr).await?;
        let low = self
            .request_partition_offset(&mut leader_client, topic, partition, EARLIEST_TIMESTAMP)
            .await?;
        let high = self
            .request_partition_offset(&mut leader_client, topic, partition, LATEST_TIMESTAMP)
            .await?;
        self.broker_clients.insert(broker_addr, leader_client);
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
        let mut delivered_record_count = 0;
        debug!(
            assignment_count = assignments.len(),
            max_poll_records = self.config.max_poll_records,
            "polling kafka consumer assignments"
        );

        for assignment in assignments {
            if delivered_record_count >= self.config.max_poll_records {
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
                    assignment.leader_epoch,
                    Some(self.config.offset_reset_policy),
                )
                .await?;
            if let Some(leader_epoch) = fetched.leader_epoch {
                self.update_assignment_leader_epoch(
                    &assignment.topic,
                    assignment.partition,
                    leader_epoch,
                );
            }
            let fetched_record_count = fetched.records.len();
            limit_fetched_records(
                &mut fetched.records,
                delivered_record_count,
                self.config.max_poll_records,
            );
            let was_truncated = fetched.records.len() < fetched_record_count;

            let key = (assignment.topic.clone(), assignment.partition);
            if self.partition_queues.contains_key(&key) {
                let route = self.enqueue_partition_records(&key, fetched.records)?;
                fetched.records = route.records;
                delivered_record_count += route.queued_count;
                if fetched.records.is_empty() {
                    let next_offset = if was_truncated {
                        route.next_offset
                    } else {
                        (fetched_record_count > 0).then_some(fetched.next_offset)
                    };
                    if let Some(next_offset) = next_offset {
                        self.update_assignment_offset(
                            &assignment.topic,
                            assignment.partition,
                            next_offset,
                        );
                    }
                    continue;
                }
            }
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
            if was_truncated && fetched.records.is_empty() {
                break;
            }
            delivered_record_count += fetched.records.len();
            records.extend(fetched.records);
        }

        debug!(
            record_count = records.len(),
            "polled kafka consumer records"
        );
        self.config.client.record_consumed(delivered_record_count);
        Ok(records)
    }

    fn enqueue_partition_records(
        &mut self,
        key: &(String, i32),
        records: Vec<ConsumerRecord>,
    ) -> Result<PartitionRoute> {
        let Some(sender) = self.partition_queues.get(key).cloned() else {
            return Ok(PartitionRoute {
                records,
                next_offset: None,
                queued_count: 0,
            });
        };
        if sender.is_closed() {
            self.partition_queues.remove(key);
            return Ok(PartitionRoute {
                records,
                next_offset: None,
                queued_count: 0,
            });
        }

        let mut iterator = records.into_iter();
        let mut queued_count = 0;
        let mut next_offset = None;
        while let Some(record) = iterator.next() {
            let record_next_offset = record.offset().saturating_add(1);
            match sender.try_send(record) {
                Ok(()) => {
                    queued_count += 1;
                    next_offset = Some(record_next_offset);
                }
                Err(TrySendError::Closed(record)) => {
                    self.partition_queues.remove(key);
                    let mut main_records = Vec::with_capacity(iterator.len() + 1);
                    main_records.push(record);
                    main_records.extend(iterator);
                    return Ok(PartitionRoute {
                        records: main_records,
                        next_offset,
                        queued_count,
                    });
                }
                Err(TrySendError::Full(_record)) => {
                    if let Some(next_offset) = next_offset {
                        self.update_assignment_offset(key.0.as_str(), key.1, next_offset);
                    }
                    return Err(Error::PartitionQueueFull {
                        topic: key.0.clone(),
                        partition: key.1,
                        capacity: self.config.partition_queue_capacity,
                    });
                }
            }
        }

        Ok(PartitionRoute {
            records: Vec::new(),
            next_offset,
            queued_count,
        })
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
            .fetch_with_progress(&topic, partition, offset, -1, None)
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
        current_leader_epoch: i32,
        offset_reset_policy: Option<OffsetResetPolicy>,
    ) -> Result<FetchedPartition> {
        let mut attempt = 0;
        let mut fetch_offset = offset;
        let mut request_leader_epoch = current_leader_epoch;
        let mut reset_applied = false;
        debug!(topic, partition, offset, "fetching kafka records");

        loop {
            let result = self
                .fetch_once(topic, partition, fetch_offset, request_leader_epoch)
                .await;
            if result.is_err() {
                self.preferred_read_replicas
                    .remove(&(topic.to_owned(), partition));
            }
            match result {
                Err(error)
                    if !reset_applied
                        && offset_reset_policy.is_some_and(OffsetResetPolicy::is_recovery)
                        && is_offset_out_of_range(&error) =>
                {
                    let Some(policy) = offset_reset_policy else {
                        return Err(error);
                    };
                    fetch_offset = self.reset_fetch_offset(topic, partition, policy).await?;
                    request_leader_epoch = -1;
                    reset_applied = true;
                    self.config.client.record_retry();
                    attempt += 1;
                }
                Err(error)
                    if attempt < self.config.max_retries
                        && request_leader_epoch >= 0
                        && is_leader_epoch_transition_error(&error) =>
                {
                    request_leader_epoch = -1;
                    invalidate_metadata_cache(&mut self.metadata_cache, topic);
                    self.config.client.record_retry();
                    attempt += 1;
                }
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

    async fn reset_fetch_offset(
        &mut self,
        topic: &str,
        partition: i32,
        policy: OffsetResetPolicy,
    ) -> Result<i64> {
        match policy {
            OffsetResetPolicy::Earliest => Ok(self.fetch_watermarks(topic, partition).await?.low),
            OffsetResetPolicy::Latest => Ok(self.fetch_watermarks(topic, partition).await?.high),
            OffsetResetPolicy::Offset(_) => Err(Error::Unsupported(
                "explicit offset reset policy cannot recover an out-of-range fetch",
            )),
        }
    }

    async fn fetch_once(
        &mut self,
        topic: &str,
        partition: i32,
        offset: i64,
        current_leader_epoch: i32,
    ) -> Result<FetchedPartition> {
        let metadata = self.metadata_for_topic(topic).await?;
        let leader = leader_for(&metadata, topic, partition)?;
        let selected_broker = self
            .preferred_read_replicas
            .get(&(topic.to_owned(), partition))
            .copied()
            .filter(|broker| broker_addr_for(&metadata, *broker).is_ok())
            .unwrap_or(leader);
        let broker_addr = broker_addr_for(&metadata, selected_broker)?;
        debug!(
            topic = topic,
            partition,
            leader,
            selected_broker,
            broker_addr = broker_addr.as_str(),
            "resolved fetch broker"
        );
        let mut leader_client = self.connect_or_reuse_broker(&broker_addr).await?;
        let rack_id = self
            .config
            .client_config()
            .client_rack_ref()
            .map(str::to_owned);
        let session = self
            .fetch_sessions
            .get(&broker_addr)
            .copied()
            .unwrap_or_default();
        let (session_id, session_epoch) = session.next_request();
        let (
            error_code,
            preferred_read_replica,
            aborted_transactions,
            records,
            response_session_id,
        ) = if let Some(rack_id) = rack_id {
            if leader_client.supports_fetch_v12().await? {
                let response = leader_client
                    .fetch_one_v12(FetchOneRequestV12 {
                        replica_id: -1,
                        max_wait_ms: self.config.max_wait_ms,
                        min_bytes: self.config.min_bytes,
                        max_bytes: self.config.max_partition_bytes,
                        isolation_level: self.config.isolation_level.as_i8(),
                        topic: topic.to_owned(),
                        partition_index: partition,
                        current_leader_epoch,
                        fetch_offset: offset,
                        last_fetched_epoch: current_leader_epoch,
                        max_partition_bytes: self.config.max_partition_bytes,
                        session_id,
                        session_epoch,
                        rack_id,
                    })
                    .await?;
                if response.error_code != 0 {
                    self.fetch_sessions.remove(&broker_addr);
                    return Err(self.config.client.broker_error(
                        response.error_code,
                        format!("fetch {topic}-{partition}@{offset}"),
                    ));
                }
                let partition_response = fetch_partition_response_v12(&response, topic, partition)?;
                (
                    partition_response.error_code,
                    Some(partition_response.preferred_read_replica),
                    partition_response
                        .aborted_transactions
                        .iter()
                        .map(|transaction| AbortedTransactionV4 {
                            producer_id: transaction.producer_id,
                            first_offset: transaction.first_offset,
                        })
                        .collect(),
                    partition_response.records.clone(),
                    response.session_id,
                )
            } else if leader_client.supports_fetch_v11().await? {
                let response = leader_client
                    .fetch_one_v11(FetchOneRequestV11 {
                        replica_id: -1,
                        max_wait_ms: self.config.max_wait_ms,
                        min_bytes: self.config.min_bytes,
                        max_bytes: self.config.max_partition_bytes,
                        isolation_level: self.config.isolation_level.as_i8(),
                        topic: topic.to_owned(),
                        partition_index: partition,
                        current_leader_epoch,
                        fetch_offset: offset,
                        max_partition_bytes: self.config.max_partition_bytes,
                        session_id,
                        session_epoch,
                        rack_id,
                    })
                    .await?;
                if response.error_code != 0 {
                    self.fetch_sessions.remove(&broker_addr);
                    return Err(self.config.client.broker_error(
                        response.error_code,
                        format!("fetch {topic}-{partition}@{offset}"),
                    ));
                }
                let partition_response = fetch_partition_response_v11(&response, topic, partition)?;
                (
                    partition_response.error_code,
                    Some(partition_response.preferred_read_replica),
                    partition_response.aborted_transactions.clone(),
                    partition_response.records.clone(),
                    response.session_id,
                )
            } else {
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
                (
                    partition_response.error_code,
                    Some(-1),
                    partition_response.aborted_transactions.clone(),
                    partition_response.records.clone(),
                    0,
                )
            }
        } else {
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
            (
                partition_response.error_code,
                None,
                partition_response.aborted_transactions.clone(),
                partition_response.records.clone(),
                0,
            )
        };
        if error_code != 0 {
            self.fetch_sessions.remove(&broker_addr);
            return Err(self
                .config
                .client
                .broker_error(error_code, format!("fetch {topic}-{partition}@{offset}")));
        }

        if response_session_id > 0 {
            self.fetch_sessions.insert(
                broker_addr.clone(),
                session.advance_with_response(response_session_id),
            );
        } else {
            self.fetch_sessions.remove(&broker_addr);
        }

        if let Some(preferred_read_replica) = preferred_read_replica {
            if preferred_read_replica >= 0
                && broker_addr_for(&metadata, preferred_read_replica).is_ok()
            {
                self.preferred_read_replicas
                    .insert((topic.to_owned(), partition), preferred_read_replica);
            } else {
                self.preferred_read_replicas
                    .remove(&(topic.to_owned(), partition));
            }
        }

        let next_offset = records
            .last()
            .map(|record| record.offset.saturating_add(1))
            .unwrap_or(offset);
        let leader_epoch = records
            .last()
            .map(|record| record.leader_epoch)
            .filter(|leader_epoch| *leader_epoch >= 0);
        let records = visible_records(&aborted_transactions, &records, self.config.isolation_level)
            .into_iter()
            .map(|record| ConsumerRecord::from_message_set(topic, partition, record))
            .collect();
        self.broker_clients.insert(broker_addr, leader_client);
        Ok(FetchedPartition {
            records,
            next_offset,
            leader_epoch,
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

    fn update_assignment_leader_epoch(&mut self, topic: &str, partition: i32, leader_epoch: i32) {
        if let Some(assignment) = self
            .assignments
            .iter_mut()
            .find(|assignment| assignment.topic == topic && assignment.partition == partition)
        {
            assignment.set_leader_epoch(assignment.leader_epoch.max(leader_epoch));
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

    async fn connect_or_reuse_broker(&mut self, broker_addr: &str) -> Result<Client> {
        if let Some(client) = self.broker_clients.remove(broker_addr) {
            return Ok(client);
        }
        self.fetch_sessions.remove(broker_addr);
        self.config
            .client
            .connect_broker(broker_addr.to_owned())
            .await
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
    leader_epoch: Option<i32>,
}

struct PartitionRoute {
    records: Vec<ConsumerRecord>,
    next_offset: Option<i64>,
    queued_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Direct consumer topic partition assignment.
pub struct ConsumerAssignment {
    topic: String,
    partition: i32,
    next_offset: i64,
    leader_epoch: i32,
    paused: bool,
}

impl ConsumerAssignment {
    pub(crate) fn new(topic: String, partition: i32, next_offset: i64) -> Self {
        Self {
            topic,
            partition,
            next_offset,
            leader_epoch: -1,
            paused: false,
        }
    }

    pub(crate) fn set_leader_epoch(&mut self, leader_epoch: i32) {
        self.leader_epoch = leader_epoch;
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

    /// Returns the latest partition leader epoch observed by this assignment.
    ///
    /// The initial value is `-1` until a RecordBatch response provides an
    /// epoch. Legacy MessageSet responses do not update this value.
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
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
        assignment.leader_epoch = -1;
        return;
    }

    assignments.push(ConsumerAssignment {
        topic,
        partition,
        next_offset: offset,
        leader_epoch: -1,
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
    partition_queue_capacity: usize,
    isolation_level: IsolationLevel,
    offset_reset_policy: OffsetResetPolicy,
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
            partition_queue_capacity: 1024,
            isolation_level: IsolationLevel::ReadUncommitted,
            offset_reset_policy: OffsetResetPolicy::Offset(0),
        }
    }

    /// Sets the Kafka client ID used by consumer requests.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    /// Sets the rack ID used by rack-aware consumer Fetch requests.
    pub fn client_rack(mut self, client_rack: impl Into<String>) -> Self {
        self.client = self.client.client_rack(client_rack);
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

    /// Sets SASL/OAUTHBEARER credentials for consumer broker connections.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.client = self.client.sasl_oauthbearer(token);
        self
    }

    /// Sets SASL/OAUTHBEARER credentials with an authorization identity.
    pub fn sasl_oauthbearer_with_username(
        mut self,
        username: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_oauthbearer_with_username(username, token);
        self
    }

    /// Sets SASL/OAUTHBEARER credentials from an async token provider.
    pub fn sasl_oauthbearer_provider<P>(mut self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        self.client = self.client.sasl_oauthbearer_provider(provider);
        self
    }

    /// Sets SASL/OAUTHBEARER credentials with an authorization identity and
    /// an async token provider.
    pub fn sasl_oauthbearer_with_username_and_provider<P>(
        mut self,
        username: impl Into<String>,
        provider: P,
    ) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        self.client = self
            .client
            .sasl_oauthbearer_with_username_and_provider(username, provider);
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

    /// Sets the bounded capacity of queues returned by
    /// [`Consumer::split_partition_queue`]. A value of zero is normalized to
    /// one because Tokio channels cannot be created without capacity.
    pub fn partition_queue_capacity(mut self, partition_queue_capacity: usize) -> Self {
        self.partition_queue_capacity = partition_queue_capacity.max(1);
        self
    }

    /// Returns the configured partition queue capacity.
    pub fn partition_queue_capacity_ref(&self) -> usize {
        self.partition_queue_capacity
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

    /// Sets the fallback used when an assigned fetch offset is out of range.
    ///
    /// `Earliest` and `Latest` perform one bounded reset through the partition
    /// leader. `Offset(n)` preserves the explicit-offset behavior and returns
    /// the broker error instead of silently changing the requested position.
    pub fn offset_reset_policy(mut self, offset_reset_policy: OffsetResetPolicy) -> Self {
        self.offset_reset_policy = offset_reset_policy;
        self
    }

    /// Returns the configured out-of-range offset policy.
    pub fn offset_reset_policy_ref(&self) -> OffsetResetPolicy {
        self.offset_reset_policy
    }

    /// Returns the shared client configuration.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Validates this consumer configuration without opening a broker connection.
    pub fn validate(&self) -> Result<()> {
        self.client.validate()?;
        self.validate_values()
    }

    /// Connects to Kafka and builds a direct consumer.
    pub async fn build(self) -> Result<Consumer> {
        self.validate()?;
        let client = self.client.clone().connect().await?;
        Ok(Consumer {
            client,
            config: self,
            assignments: Vec::new(),
            partition_queues: BTreeMap::new(),
            metadata_cache: BTreeMap::new(),
            broker_clients: BTreeMap::new(),
            fetch_sessions: BTreeMap::new(),
            preferred_read_replicas: BTreeMap::new(),
        })
    }

    fn validate_values(&self) -> Result<()> {
        if self.max_wait_ms < 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_wait_ms",
                reason: "must not be negative",
            });
        }
        if self.min_bytes < 0 {
            return Err(Error::InvalidConfiguration {
                field: "min_bytes",
                reason: "must not be negative",
            });
        }
        if self.max_partition_bytes <= 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_partition_bytes",
                reason: "must be greater than zero",
            });
        }
        if self.max_poll_records == 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_poll_records",
                reason: "must be greater than zero",
            });
        }
        Ok(())
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

fn fetch_partition_response_v11<'a>(
    response: &'a FetchResponseV11,
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a FetchPartitionResponseV11> {
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

fn fetch_partition_response_v12<'a>(
    response: &'a FetchResponseV12,
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a FetchPartitionResponseV12> {
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

fn offset_for_leader_epoch_partition_response<'a>(
    topics: &'a [OffsetForLeaderEpochTopicResponseV3],
    topic_name: &str,
    partition_index: i32,
) -> Result<&'a OffsetForLeaderEpochPartitionResponseV3> {
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

fn visible_records(
    aborted_transactions: &[kafrust_protocol::api::fetch::AbortedTransactionV4],
    input_records: &[MessageSetRecord],
    isolation_level: IsolationLevel,
) -> Vec<MessageSetRecord> {
    let mut aborted_transactions = aborted_transactions.to_vec();
    let mut visible = Vec::new();

    for record in input_records {
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
            visible.push(record.clone());
        }
    }

    visible
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
                | BrokerErrorKind::FencedLeaderEpoch
                | BrokerErrorKind::UnknownLeaderEpoch
                | BrokerErrorKind::InvalidFetchSessionEpoch
        ),
        Error::Io(_)
        | Error::RequestTimedOut { .. }
        | Error::UnknownTopicOrPartition { .. }
        | Error::MissingLeader { .. }
        | Error::MissingBroker { .. } => true,
        Error::MissingBootstrapServer
        | Error::InvalidPartition { .. }
        | Error::UnassignedTopicPartition { .. }
        | Error::PartitionQueueFull { .. }
        | Error::MissingGroupDescription { .. }
        | Error::MissingDeleteGroupResult { .. }
        | Error::ResponseCountMismatch { .. }
        | Error::MissingSaslCredentials
        | Error::InvalidSaslResponse { .. }
        | Error::OAuthBearerTokenTimeout { .. }
        | Error::TransactionOutcomeUnknown { .. }
        | Error::TransactionProducerDefunct
        | Error::ResponseTooLarge { .. }
        | Error::TlsConfig { .. }
        | Error::InvalidTlsServerName { .. }
        | Error::InvalidGroupInstanceId
        | Error::InvalidTopicPattern { .. }
        | Error::InvalidScramCredential { .. }
        | Error::InvalidConfiguration { .. }
        | Error::Unsupported(_)
        | Error::TaskJoin(_)
        | Error::Protocol(_) => false,
    }
}

fn is_offset_out_of_range(error: &Error) -> bool {
    matches!(
        error,
        Error::Broker { code, .. }
            if BrokerErrorKind::from_code(*code) == BrokerErrorKind::OffsetOutOfRange
    )
}

fn is_leader_epoch_transition_error(error: &Error) -> bool {
    matches!(
        error,
        Error::Broker { code, .. }
            if matches!(*code, 74 | 75)
    )
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
        limit_fetched_records, offset_for_leader_epoch_partition_response, visible_records,
        Consumer, ConsumerAssignment, ConsumerConfig, ConsumerRecord, IsolationLevel,
        OffsetResetPolicy, PartitionWatermarks, SecurityProtocol,
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
            .client_rack("rack-a")
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
            .partition_queue_capacity(7)
            .isolation_level(IsolationLevel::ReadCommitted);

        assert_eq!(
            config.client_config().client_id_ref(),
            Some("orders-reader")
        );
        assert_eq!(config.client_config().client_rack_ref(), Some("rack-a"));
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
        assert_eq!(config.partition_queue_capacity_ref(), 7);
        assert_eq!(config.isolation_level_ref(), IsolationLevel::ReadCommitted);
        assert_eq!(
            config.offset_reset_policy_ref(),
            OffsetResetPolicy::Offset(0)
        );
    }

    #[test]
    fn normalizes_zero_partition_queue_capacity() {
        assert_eq!(
            ConsumerConfig::new(["localhost:9092"])
                .partition_queue_capacity(0)
                .partition_queue_capacity_ref(),
            1
        );
    }

    #[tokio::test]
    async fn rejects_invalid_fetch_configuration_before_connecting() {
        let cases = [
            (
                ConsumerConfig::new(["127.0.0.1:1"])
                    .max_wait_ms(-1)
                    .build()
                    .await
                    .unwrap_err(),
                "max_wait_ms",
            ),
            (
                ConsumerConfig::new(["127.0.0.1:1"])
                    .min_bytes(-1)
                    .build()
                    .await
                    .unwrap_err(),
                "min_bytes",
            ),
            (
                ConsumerConfig::new(["127.0.0.1:1"])
                    .max_partition_bytes(0)
                    .build()
                    .await
                    .unwrap_err(),
                "max_partition_bytes",
            ),
            (
                ConsumerConfig::new(["127.0.0.1:1"])
                    .max_poll_records(0)
                    .build()
                    .await
                    .unwrap_err(),
                "max_poll_records",
            ),
        ];

        for (error, field) in cases {
            assert!(matches!(
                error,
                Error::InvalidConfiguration {
                    field: actual,
                    ..
                } if actual == field
            ));
        }

        assert!(ConsumerConfig::new(["127.0.0.1:1"]).validate().is_ok());
    }

    #[tokio::test]
    async fn split_partition_queue_requires_assignment_and_protects_seek_state() {
        let (client_stream, _broker_stream) = tokio::io::duplex(64);
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-partition-queue-api-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new(["localhost:9092"]);
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());

        assert!(matches!(
            consumer.split_partition_queue("orders", 0).unwrap_err(),
            Error::UnassignedTopicPartition { topic, partition: 0 } if topic == "orders"
        ));
        consumer.assign("orders", 0, 10);
        let queue = consumer.split_partition_queue("orders", 0).unwrap();
        assert!(matches!(
            consumer.split_partition_queue("orders", 0).unwrap_err(),
            Error::Unsupported("partition queue is already split")
        ));
        assert!(matches!(
            consumer.seek("orders", 0, 20).unwrap_err(),
            Error::Unsupported("seek requires dropping the active partition queue")
        ));
        drop(queue);
        consumer.seek("orders", 0, 20).unwrap();
        assert_eq!(consumer.position("orders", 0), Some(20));
    }

    #[test]
    fn maps_message_set_record() {
        let record = ConsumerRecord::from_message_set(
            "orders",
            1,
            MessageSetRecord {
                offset: 42,
                leader_epoch: 4,
                timestamp_ms: 123,
                key: Some(b"order-1".to_vec()),
                value: Some(b"created".to_vec()),
                headers: vec![kafrust_protocol::api::produce::RecordBatchHeader::new(
                    "source",
                    Some(b"checkout".to_vec()),
                )],
                producer_id: None,
                transactional: false,
                control: false,
            },
        );

        assert_eq!(record.topic(), "orders");
        assert_eq!(record.partition(), 1);
        assert_eq!(record.offset(), 42);
        assert_eq!(record.leader_epoch(), 4);
        assert_eq!(record.timestamp_ms(), 123);
        assert_eq!(record.key().unwrap(), b"order-1");
        assert_eq!(record.value().unwrap(), b"created");
        assert_eq!(record.headers().len(), 1);
        assert_eq!(record.headers()[0].key(), "source");
        assert_eq!(record.headers()[0].value(), Some(&b"checkout"[..]));
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

    #[test]
    fn tracks_and_resets_assignment_leader_epoch() {
        let (client_stream, _broker_stream) = tokio::io::duplex(64);
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-leader-epoch-state-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let mut consumer =
            Consumer::from_assignments(client, ConsumerConfig::new(["localhost:9092"]), Vec::new());

        consumer.assign("orders", 0, 10);
        assert_eq!(consumer.assignments()[0].leader_epoch(), -1);
        consumer.update_assignment_leader_epoch("orders", 0, 4);
        assert_eq!(consumer.assignments()[0].leader_epoch(), 4);

        consumer.assign("orders", 0, 20);
        assert_eq!(consumer.assignments()[0].leader_epoch(), -1);
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

    #[tokio::test]
    async fn resets_out_of_range_assignment_to_earliest_offset() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut first_socket, _) = listener.accept().await.unwrap();
            let first_fetch = read_frame(&mut first_socket).await;
            assert_eq!(&first_fetch[0..4], &[0, 1, 0, 4]);
            write_frame(&mut first_socket, &fetch_v4_out_of_range_response_frame(1)).await;
            drop(first_socket);

            let (mut second_socket, _) = listener.accept().await.unwrap();
            for (correlation_id, timestamp, offset) in [(1, -2_i64, 4_i64), (2, -1_i64, 9_i64)] {
                let request = read_frame(&mut second_socket).await;
                assert_eq!(&request[0..4], &[0, 2, 0, 1]);
                assert_eq!(
                    i64::from_be_bytes(request[request.len() - 8..].try_into().unwrap()),
                    timestamp
                );
                write_frame(
                    &mut second_socket,
                    &list_offsets_response_frame(correlation_id, offset),
                )
                .await;
            }
            let second_fetch = read_frame(&mut second_socket).await;
            assert_eq!(&second_fetch[0..4], &[0, 1, 0, 4]);
            write_frame(
                &mut second_socket,
                &fetch_v4_response_frame_with_correlation(3),
            )
            .await;
        });
        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-offset-reset-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .offset_reset_policy(OffsetResetPolicy::Earliest);
        let mut consumer = Consumer::from_assignments(
            client,
            config,
            vec![ConsumerAssignment::new("orders".to_owned(), 0, 100)],
        );
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);

        let records = consumer.poll().await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset(), 42);
        assert_eq!(consumer.position("orders", 0), Some(43));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn sends_assignment_leader_epoch_in_fetch_v12_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut socket).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut socket, &api_versions_v3_fetch_v12_response(1)).await;

            let fetch_request = read_frame(&mut socket).await;
            assert_eq!(&fetch_request[0..4], &[0, 1, 0, 12]);
            assert_eq!(
                fetch_request
                    .windows(4)
                    .filter(|window| *window == [0, 0, 0, 8])
                    .count(),
                2
            );
            write_frame(&mut socket, &fetch_v12_response_frame(2, -1)).await;
        });
        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-assignment-leader-epoch-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .client_rack("rack-a");
        let mut consumer = Consumer::from_assignments(
            client,
            config,
            vec![ConsumerAssignment::new("orders".to_owned(), 0, 42)],
        );
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);
        consumer.update_assignment_leader_epoch("orders", 0, 8);

        assert!(consumer.poll().await.unwrap().is_empty());
        assert_eq!(consumer.assignments()[0].leader_epoch(), 8);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reuses_fetch_session_for_sequential_rack_aware_polls() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut socket).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut socket, &api_versions_v3_fetch_v12_response(1)).await;

            let first_fetch = read_frame(&mut socket).await;
            assert_eq!(&first_fetch[0..4], &[0, 1, 0, 12]);
            assert!(first_fetch.windows(4).any(|window| window == [0, 0, 0, 0]));
            write_frame(
                &mut socket,
                &fetch_v12_response_frame_with_session(2, -1, 17),
            )
            .await;

            let second_fetch = read_frame(&mut socket).await;
            assert_eq!(&second_fetch[0..4], &[0, 1, 0, 12]);
            assert!(second_fetch
                .windows(4)
                .any(|window| window == [0, 0, 0, 17]));
            assert!(second_fetch.windows(4).any(|window| window == [0, 0, 0, 1]));
            write_frame(
                &mut socket,
                &fetch_v12_response_frame_with_session(3, -1, 17),
            )
            .await;
        });
        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-fetch-session-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .client_rack("rack-a");
        let mut consumer = Consumer::from_assignments(
            client,
            config,
            vec![ConsumerAssignment::new("orders".to_owned(), 0, 42)],
        );
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);

        assert!(consumer.poll().await.unwrap().is_empty());
        assert_eq!(consumer.fetch_sessions[&addr.to_string()].session_id, 17);
        assert_eq!(consumer.fetch_sessions[&addr.to_string()].next_epoch, 1);
        assert!(consumer.poll().await.unwrap().is_empty());
        assert_eq!(consumer.fetch_sessions[&addr.to_string()].next_epoch, 2);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetches_offset_for_leader_epoch_from_partition_leader() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut socket).await;
            assert_eq!(&request[0..4], &[0, 23, 0, 3]);
            write_frame(&mut socket, &offset_for_leader_epoch_response_frame()).await;
        });
        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-leader-epoch-test".to_owned()),
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

        let result = consumer
            .offset_for_leader_epoch("orders", 0, 9, 7)
            .await
            .unwrap();

        assert_eq!(result.leader_epoch(), 8);
        assert_eq!(result.end_offset(), 42);
        server.await.unwrap();
    }

    #[test]
    fn reports_missing_offset_for_leader_epoch_partition() {
        let error = offset_for_leader_epoch_partition_response(&[], "orders", 0).unwrap_err();

        assert!(matches!(
            error,
            Error::UnknownTopicOrPartition { topic, partition: 0 } if topic == "orders"
        ));
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
        assert!(can_retry_fetch(&Error::Broker {
            code: 74,
            context: "fetch orders-0@0".to_owned(),
        }));
        assert!(can_retry_fetch(&Error::Broker {
            code: 75,
            context: "fetch orders-0@0".to_owned(),
        }));
        assert!(can_retry_fetch(&Error::Broker {
            code: 70,
            context: "fetch orders-0@0".to_owned(),
        }));
        assert!(!can_retry_fetch(&Error::Broker {
            code: 1,
            context: "fetch orders-0@0".to_owned(),
        }));
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

        let committed = visible_records(
            &partition.aborted_transactions,
            &partition.records,
            IsolationLevel::ReadCommitted,
        );
        let uncommitted = visible_records(
            &partition.aborted_transactions,
            &partition.records,
            IsolationLevel::ReadUncommitted,
        );

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

    #[tokio::test]
    async fn split_partition_queue_routes_poll_records_and_advances_position() {
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
            Some("kafrust-partition-queue-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .partition_queue_capacity(2);
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);
        consumer.assign("orders", 0, 42);

        let mut queue = consumer.split_partition_queue("orders", 0).unwrap();
        assert_eq!(queue.topic(), "orders");
        assert_eq!(queue.partition(), 0);
        assert!(consumer.poll().await.unwrap().is_empty());
        assert_eq!(consumer.position("orders", 0), Some(43));

        let record = queue.recv().await.unwrap();
        assert_eq!(record.offset(), 42);
        assert!(queue.try_recv().is_none());
        server.await.unwrap();
    }

    #[tokio::test]
    async fn split_partition_queue_reports_backpressure_without_skipping_records() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..2 {
                let request = read_frame(&mut socket).await;
                assert_eq!(&request[0..4], &[0, 1, 0, 4]);
                write_frame(&mut socket, &fetch_v4_response_frame()).await;
            }
        });

        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-partition-queue-capacity-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .partition_queue_capacity(1);
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);
        consumer.assign("orders", 0, 42);
        let mut queue = consumer.split_partition_queue("orders", 0).unwrap();

        consumer.poll().await.unwrap();
        let error = consumer.poll().await.unwrap_err();
        assert!(matches!(
            error,
            Error::PartitionQueueFull {
                topic,
                partition: 0,
                capacity: 1
            } if topic == "orders"
        ));
        assert_eq!(consumer.position("orders", 0), Some(43));
        assert_eq!(queue.recv().await.unwrap().offset(), 42);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reuses_partition_leader_connection_for_sequential_fetches() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for _ in 0..2 {
                let request = read_frame(&mut socket).await;
                assert_eq!(&request[0..4], &[0, 1, 0, 4]);
                write_frame(&mut socket, &fetch_v4_response_frame()).await;
            }
        });

        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-consumer-reuse-test".to_owned()),
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

        let first = consumer.fetch("orders", 0, 42).await.unwrap();
        let second = consumer.fetch("orders", 0, 42).await.unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_eq!(consumer.broker_clients.len(), 1);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn negotiates_rack_aware_fetch_and_routes_to_preferred_replica() {
        let leader_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader_addr = leader_listener.local_addr().unwrap();
        let preferred_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let preferred_addr = preferred_listener.local_addr().unwrap();

        let leader_server = tokio::spawn(async move {
            let (mut socket, _) = leader_listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut socket).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut socket, &api_versions_v3_fetch_v12_response(1)).await;

            let fetch_request = read_frame(&mut socket).await;
            assert_eq!(&fetch_request[0..4], &[0, 1, 0, 12]);
            assert!(fetch_request
                .windows(b"rack-a".len())
                .any(|window| window == b"rack-a"));
            assert_eq!(fetch_request.last(), Some(&0));
            write_frame(&mut socket, &fetch_v12_response_frame(2, 2)).await;
        });
        let preferred_server = tokio::spawn(async move {
            let (mut socket, _) = preferred_listener.accept().await.unwrap();
            let api_versions_request = read_frame(&mut socket).await;
            assert_eq!(&api_versions_request[0..4], &[0, 18, 0, 3]);
            write_frame(&mut socket, &api_versions_v3_fetch_v12_response(1)).await;

            let fetch_request = read_frame(&mut socket).await;
            assert_eq!(&fetch_request[0..4], &[0, 1, 0, 12]);
            assert!(fetch_request
                .windows(b"rack-a".len())
                .any(|window| window == b"rack-a"));
            assert_eq!(fetch_request.last(), Some(&0));
            write_frame(&mut socket, &fetch_v12_response_frame(2, -1)).await;
        });

        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-rack-routing-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([leader_addr.to_string()])
            .request_timeout_ms(500)
            .client_rack("rack-a");
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = leader_addr.ip().to_string();
        metadata.brokers[0].port = i32::from(leader_addr.port());
        metadata.brokers.push(BrokerMetadata {
            node_id: 2,
            host: preferred_addr.ip().to_string(),
            port: i32::from(preferred_addr.port()),
            rack: Some("rack-a".to_owned()),
        });
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);

        assert!(consumer.fetch("orders", 0, 42).await.unwrap().is_empty());
        assert_eq!(
            consumer
                .preferred_read_replicas
                .get(&("orders".to_owned(), 0)),
            Some(&2)
        );
        assert!(consumer.fetch("orders", 0, 42).await.unwrap().is_empty());
        assert!(!consumer
            .preferred_read_replicas
            .contains_key(&("orders".to_owned(), 0)));

        leader_server.await.unwrap();
        preferred_server.await.unwrap();
    }

    #[tokio::test]
    async fn clears_preferred_replica_after_exhausted_fetch_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _api_versions_request = read_frame(&mut socket).await;
            write_frame(&mut socket, &api_versions_v3_fetch_v11_response(1)).await;
            let _fetch_request = read_frame(&mut socket).await;
            write_frame(&mut socket, &fetch_v11_error_response_frame(2, 6)).await;
        });

        let (client_stream, broker_stream) = tokio::io::duplex(64);
        let _broker_stream = broker_stream;
        let client = Client::from_stream(
            Box::new(client_stream),
            Some("kafrust-rack-failure-test".to_owned()),
            Some(std::time::Duration::from_millis(500)),
        );
        let config = ConsumerConfig::new([addr.to_string()])
            .request_timeout_ms(500)
            .max_retries(0)
            .client_rack("rack-a");
        let mut consumer = Consumer::from_assignments(client, config, Vec::new());
        let mut metadata = metadata_fixture();
        metadata.brokers[0].host = addr.ip().to_string();
        metadata.brokers[0].port = i32::from(addr.port());
        consumer
            .metadata_cache
            .insert("orders".to_owned(), metadata);
        consumer
            .preferred_read_replicas
            .insert(("orders".to_owned(), 0), 1);

        assert!(consumer.fetch("orders", 0, 42).await.is_err());
        assert!(!consumer
            .preferred_read_replicas
            .contains_key(&("orders".to_owned(), 0)));
        server.await.unwrap();
    }

    fn message(offset: i64) -> MessageSetRecord {
        MessageSetRecord {
            offset,
            leader_epoch: -1,
            timestamp_ms: 123,
            key: None,
            value: None,
            headers: Vec::new(),
            producer_id: None,
            transactional: false,
            control: false,
        }
    }

    fn transactional_message(offset: i64, producer_id: i64, control: bool) -> MessageSetRecord {
        MessageSetRecord {
            offset,
            leader_epoch: -1,
            timestamp_ms: 123,
            key: None,
            value: None,
            headers: Vec::new(),
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
        fetch_v4_response_frame_with_correlation(1)
    }

    fn fetch_v4_response_frame_with_correlation(correlation_id: i32) -> Vec<u8> {
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
        response.write_i32(correlation_id);
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

    fn fetch_v4_out_of_range_response_frame(correlation_id: i32) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_i32(0);
        response.write_i32(1);
        response.write_string("orders").unwrap();
        response.write_i32(1);
        response.write_i32(0);
        response.write_i16(1);
        response.write_i64(-1);
        response.write_i64(-1);
        response.write_i32(0);
        response.write_bytes(&[]).unwrap();
        response.into_bytes()
    }

    fn api_versions_v3_fetch_v11_response(correlation_id: i32) -> Vec<u8> {
        api_versions_v3_fetch_response(correlation_id, 11)
    }

    fn api_versions_v3_fetch_v12_response(correlation_id: i32) -> Vec<u8> {
        api_versions_v3_fetch_response(correlation_id, 12)
    }

    fn api_versions_v3_fetch_response(correlation_id: i32, max_version: i16) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_i16(0);
        response.write_i8(2); // one compact API key entry
        response.write_i16(1);
        response.write_i16(0);
        response.write_i16(max_version);
        response.write_i8(0); // API key entry tags
        response.write_i32(0);
        response.write_i8(0); // response tags
        response.into_bytes()
    }

    fn fetch_v11_error_response_frame(correlation_id: i32, error_code: i16) -> Vec<u8> {
        fetch_v11_response_frame_with_error(correlation_id, error_code, -1)
    }

    fn fetch_v11_response_frame_with_error(
        correlation_id: i32,
        error_code: i16,
        preferred_read_replica: i32,
    ) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_i32(0);
        response.write_i16(0);
        response.write_i32(0);
        response.write_i32(1);
        response.write_string("orders").unwrap();
        response.write_i32(1);
        response.write_i32(0);
        response.write_i16(error_code);
        response.write_i64(43);
        response.write_i64(43);
        response.write_i64(42);
        response.write_i32(0);
        response.write_i32(preferred_read_replica);
        response.write_bytes(&[]).unwrap();
        response.into_bytes()
    }

    fn fetch_v12_response_frame(correlation_id: i32, preferred_read_replica: i32) -> Vec<u8> {
        fetch_v12_response_frame_with_session(correlation_id, preferred_read_replica, 0)
    }

    fn fetch_v12_response_frame_with_session(
        correlation_id: i32,
        preferred_read_replica: i32,
        session_id: i32,
    ) -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_unsigned_varint(0); // response header tags
        response.write_i32(0); // throttle time
        response.write_i16(0);
        response.write_i32(session_id);
        response.write_unsigned_varint(2); // one compact topic
        response.write_compact_string("orders").unwrap();
        response.write_unsigned_varint(2); // one compact partition
        response.write_i32(0);
        response.write_i16(0);
        response.write_i64(43);
        response.write_i64(43);
        response.write_i64(42);
        response.write_unsigned_varint(1); // no aborted transactions
        response.write_i32(preferred_read_replica);
        response.write_compact_nullable_bytes(Some(&[])).unwrap();
        response.write_unsigned_varint(0); // partition tags
        response.write_unsigned_varint(0); // topic tags
        response.write_unsigned_varint(0); // response tags
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

    fn offset_for_leader_epoch_response_frame() -> Vec<u8> {
        let mut response = Encoder::new();
        response.write_i32(1);
        response.write_i32(0);
        response.write_i32(1);
        response.write_string("orders").unwrap();
        response.write_i32(1);
        response.write_i16(0);
        response.write_i32(0);
        response.write_i32(8);
        response.write_i64(42);
        response.into_bytes()
    }
}
