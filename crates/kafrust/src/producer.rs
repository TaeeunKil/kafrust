use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use kafrust_protocol::api::api_versions::ApiVersionsResponseV0;
use kafrust_protocol::api::metadata::{BrokerMetadata, MetadataResponseV1};
use kafrust_protocol::api::produce::{
    encoded_message_set_len, encoded_record_batch_set_len_with_compression, MessageSetMessage,
    ProducePartitionV3, ProduceResponseV2, ProduceResponseV7, ProduceTopicV3, RecordBatchIdentity,
    RecordBatchMessage, API_KEY as PRODUCE_API_KEY,
};
use kafrust_protocol::record_batch::RecordBatchCompression;

use crate::client::Client;
use crate::config::{ClientConfig, SecurityProtocol};
use crate::error::{BrokerErrorKind, Error, Result};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{self, Instant};
use tracing::debug;

const BUFFERED_PRODUCER_CHANNEL_CAPACITY: usize = 1024;
const IDEMPOTENT_INIT_RETRY_BACKOFF: Duration = Duration::from_millis(100);

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
/// Kafka record batch compression policy for producer sends.
pub enum Compression {
    /// Do not compress produced record batches.
    None,
    /// Compress produced RecordBatch v2 payloads with gzip.
    Gzip,
    /// Compress produced RecordBatch v2 payloads with Kafka-compatible Snappy framing.
    Snappy,
    /// Compress produced RecordBatch v2 payloads with Kafka-compatible LZ4 framing.
    Lz4,
    /// Compress produced RecordBatch v2 payloads with Kafka-compatible Zstd framing.
    Zstd,
}

impl Compression {
    fn as_record_batch_compression(self) -> RecordBatchCompression {
        match self {
            Self::None => RecordBatchCompression::None,
            Self::Gzip => RecordBatchCompression::Gzip,
            Self::Snappy => RecordBatchCompression::Snappy,
            Self::Lz4 => RecordBatchCompression::Lz4,
            Self::Zstd => RecordBatchCompression::Zstd,
        }
    }

    fn requires_record_batch(self) -> bool {
        self != Self::None
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

/// Per-record failure returned by a batch produce report.
#[derive(Debug)]
pub struct ProducerBatchFailure {
    record_index: usize,
    topic: String,
    partition: i32,
    error: Error,
}

impl ProducerBatchFailure {
    fn new(record_index: usize, topic: impl Into<String>, partition: i32, error: Error) -> Self {
        Self {
            record_index,
            topic: topic.into(),
            partition,
            error,
        }
    }

    /// Returns the index of the input record that failed.
    pub fn record_index(&self) -> usize {
        self.record_index
    }

    /// Returns the Kafka topic for the failed record.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns the Kafka partition for the failed record.
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Returns the Kafka client error for the failed record.
    pub fn error(&self) -> &Error {
        &self.error
    }

    /// Consumes this failure and returns the underlying error.
    pub fn into_error(self) -> Error {
        self.error
    }
}

/// Per-record outcome returned by a batch produce report.
#[derive(Debug)]
pub enum ProducerBatchRecordOutcome {
    /// Kafka accepted the record and returned produce metadata.
    Success(RecordMetadata),
    /// Kafka rejected the record's topic partition in the Produce response.
    Failure(ProducerBatchFailure),
}

impl ProducerBatchRecordOutcome {
    /// Returns whether this record was accepted by Kafka.
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }

    /// Returns the successful record metadata, when present.
    pub fn metadata(&self) -> Option<&RecordMetadata> {
        match self {
            Self::Success(metadata) => Some(metadata),
            Self::Failure(_) => None,
        }
    }

    /// Returns the record failure, when present.
    pub fn failure(&self) -> Option<&ProducerBatchFailure> {
        match self {
            Self::Success(_) => None,
            Self::Failure(failure) => Some(failure),
        }
    }

    fn into_metadata(self) -> Result<RecordMetadata> {
        match self {
            Self::Success(metadata) => Ok(metadata),
            Self::Failure(failure) => Err(failure.into_error()),
        }
    }
}

/// Report returned by a batch produce operation.
#[derive(Debug)]
pub struct ProducerBatchReport {
    records: Vec<ProducerBatchRecordOutcome>,
}

impl ProducerBatchReport {
    fn new(records: Vec<ProducerBatchRecordOutcome>) -> Self {
        Self { records }
    }

    /// Returns per-record outcomes in the same order as input records.
    pub fn records(&self) -> &[ProducerBatchRecordOutcome] {
        &self.records
    }

    /// Consumes this report and returns per-record outcomes.
    pub fn into_records(self) -> Vec<ProducerBatchRecordOutcome> {
        self.records
    }

    /// Returns whether at least one record failed in the Produce response.
    pub fn has_failures(&self) -> bool {
        self.records.iter().any(|record| !record.is_success())
    }
}

#[derive(Debug)]
/// Kafka producer using metadata-based leader routing.
pub struct Producer {
    client: Client,
    config: ProducerConfig,
    metadata_cache: BTreeMap<String, MetadataResponseV1>,
    idempotent_state: Option<IdempotentProducerState>,
}

#[derive(Debug)]
struct IdempotentProducerState {
    producer_id: i64,
    producer_epoch: i16,
    next_sequences: BTreeMap<(String, i32), i32>,
}

impl IdempotentProducerState {
    fn new(producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            producer_id,
            producer_epoch,
            next_sequences: BTreeMap::new(),
        }
    }

    fn identity(&self, topic: &str, partition: i32) -> RecordBatchIdentity {
        RecordBatchIdentity {
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self
                .next_sequences
                .get(&(topic.to_owned(), partition))
                .copied()
                .unwrap_or(0),
        }
    }

    fn acknowledge(&mut self, topic: &str, partition: i32, record_count: usize) {
        let key = (topic.to_owned(), partition);
        let current = self.next_sequences.get(&key).copied().unwrap_or(0);
        self.next_sequences
            .insert(key, advance_producer_sequence(current, record_count));
    }
}

#[derive(Debug, Default)]
struct IdempotentBatchSequenceTracker {
    assignments: BTreeMap<usize, IdempotentBatchSequenceAssignment>,
    next_sequences: BTreeMap<(String, i32), i32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IdempotentBatchSequenceAssignment {
    topic: String,
    partition: i32,
    identity: RecordBatchIdentity,
    acknowledged: bool,
}

impl IdempotentBatchSequenceTracker {
    fn identity_for_chunk(
        &mut self,
        state: Option<&IdempotentProducerState>,
        key: &ProduceBatchKey,
        records: &[PreparedBatchRecord<'_>],
    ) -> Result<RecordBatchIdentity> {
        let Some(state) = state else {
            return Ok(RecordBatchIdentity::NON_IDEMPOTENT);
        };
        let Some(first) = records.first() else {
            return Err(Error::Unsupported("empty idempotent record batch"));
        };

        if let Some(assignment) = self.assignments.get(&first.index) {
            if assignment.topic != key.topic || assignment.partition != key.partition {
                return Err(Error::Unsupported(
                    "idempotent batch retry changed topic or partition",
                ));
            }
            let identity = assignment.identity;
            for (offset, record) in records.iter().enumerate() {
                let expected = RecordBatchIdentity {
                    base_sequence: advance_producer_sequence(identity.base_sequence, offset),
                    ..identity
                };
                let assignment = self.assignments.get(&record.index);
                if !assignment.is_some_and(|assignment| {
                    assignment.topic == key.topic
                        && assignment.partition == key.partition
                        && assignment.identity == expected
                }) {
                    return Err(Error::Unsupported(
                        "inconsistent idempotent batch retry sequence",
                    ));
                }
            }
            return Ok(identity);
        }

        if records
            .iter()
            .any(|record| self.assignments.contains_key(&record.index))
        {
            return Err(Error::Unsupported(
                "partial idempotent batch sequence assignment",
            ));
        }

        let sequence_key = (key.topic.clone(), key.partition);
        let base_sequence = self
            .next_sequences
            .get(&sequence_key)
            .copied()
            .unwrap_or_else(|| state.identity(&key.topic, key.partition).base_sequence);
        let identity = RecordBatchIdentity {
            base_sequence,
            ..state.identity(&key.topic, key.partition)
        };
        self.next_sequences.insert(
            sequence_key,
            advance_producer_sequence(base_sequence, records.len()),
        );
        for (offset, record) in records.iter().enumerate() {
            self.assignments.insert(
                record.index,
                IdempotentBatchSequenceAssignment {
                    topic: key.topic.clone(),
                    partition: key.partition,
                    identity: RecordBatchIdentity {
                        base_sequence: advance_producer_sequence(identity.base_sequence, offset),
                        ..identity
                    },
                    acknowledged: false,
                },
            );
        }
        Ok(identity)
    }

    fn acknowledge_chunk(
        &mut self,
        state: Option<&mut IdempotentProducerState>,
        key: &ProduceBatchKey,
        records: &[PreparedBatchRecord<'_>],
    ) -> Result<()> {
        let Some(state) = state else {
            return Ok(());
        };
        let Some(first) = records.first() else {
            return Err(Error::Unsupported("empty idempotent record batch"));
        };
        let assignment = self
            .assignments
            .get(&first.index)
            .ok_or(Error::Unsupported(
                "missing idempotent batch sequence assignment",
            ))?;
        let acknowledged = assignment.acknowledged;
        if records
            .iter()
            .any(|record| match self.assignments.get(&record.index) {
                Some(assignment) => assignment.acknowledged != acknowledged,
                None => true,
            })
        {
            return Err(Error::Unsupported(
                "partially acknowledged idempotent batch chunk",
            ));
        }
        if acknowledged {
            return Ok(());
        }
        if assignment.identity != state.identity(&key.topic, key.partition) {
            return Err(Error::Unsupported(
                "idempotent batch acknowledged out of sequence",
            ));
        }
        state.acknowledge(&key.topic, key.partition, records.len());
        for record in records {
            self.assignments
                .get_mut(&record.index)
                .ok_or(Error::Unsupported(
                    "missing idempotent batch sequence assignment",
                ))?
                .acknowledged = true;
        }
        Ok(())
    }
}

fn advance_producer_sequence(current: i32, record_count: usize) -> i32 {
    const SEQUENCE_MODULUS: u64 = i32::MAX as u64 + 1;
    let increment = (record_count as u64) % SEQUENCE_MODULUS;
    ((current as u64 + increment) % SEQUENCE_MODULUS) as i32
}

#[derive(Debug)]
/// Opt-in producer for linger-based buffered sends.
///
/// This type owns an inner [`Producer`] and exposes lifecycle operations for
/// the buffered path.
pub struct BufferedProducer {
    commands: mpsc::Sender<BufferedProducerCommand>,
    worker: Option<JoinHandle<()>>,
    state: BufferedProducerState,
}

impl BufferedProducer {
    /// Enqueues one record for the buffered producer path.
    ///
    /// The returned delivery handle resolves when the background task reaches a
    /// terminal result for this record.
    pub async fn send(&mut self, record: ProducerRecord) -> Result<ProducerDelivery> {
        self.state.ensure_open()?;
        enqueue_buffered_record(&self.commands, record).await
    }

    /// Flushes accepted buffered records.
    ///
    /// Pending delivery handles are completed from the underlying batch
    /// Produce outcomes before this returns.
    pub async fn flush(&mut self) -> Result<()> {
        self.state.ensure_open()?;
        let (result_sender, result_receiver) = oneshot::channel();
        send_buffered_command(
            &self.commands,
            BufferedProducerCommand::Flush { result_sender },
        )
        .await?;
        receive_buffered_result(result_receiver).await
    }

    /// Flushes and closes the buffered producer.
    ///
    /// Close is idempotent. Future enqueue APIs will reject sends after close.
    pub async fn close(&mut self) -> Result<()> {
        if self.state.is_open() {
            let (result_sender, result_receiver) = oneshot::channel();
            send_buffered_command(
                &self.commands,
                BufferedProducerCommand::Close { result_sender },
            )
            .await?;
            let result = receive_buffered_result(result_receiver).await;
            if let Some(worker) = self.worker.take() {
                worker.await?;
            }
            self.state.close();
            result?;
        }
        Ok(())
    }

    /// Returns whether the buffered producer has been closed.
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }
}

/// Delivery handle returned by [`BufferedProducer::send`].
pub struct ProducerDelivery {
    receiver: oneshot::Receiver<Result<RecordMetadata>>,
}

impl ProducerDelivery {
    fn new(receiver: oneshot::Receiver<Result<RecordMetadata>>) -> Self {
        Self { receiver }
    }

    /// Waits until the buffered record reaches a terminal delivery result.
    pub async fn wait(self) -> Result<RecordMetadata> {
        self.await
    }
}

impl Future for ProducerDelivery {
    type Output = Result<RecordMetadata>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver = &mut self.get_mut().receiver;
        Pin::new(receiver)
            .poll(cx)
            .map(|result| result.unwrap_or_else(|_| Err(buffered_delivery_canceled_error())))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferedProducerState {
    Open,
    Closed,
}

impl BufferedProducerState {
    fn ensure_open(self) -> Result<()> {
        if self.is_open() {
            Ok(())
        } else {
            Err(Error::Unsupported("buffered producer is closed"))
        }
    }

    fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }

    fn is_closed(self) -> bool {
        matches!(self, Self::Closed)
    }

    fn close(&mut self) {
        *self = Self::Closed;
    }
}

#[derive(Debug)]
enum BufferedProducerCommand {
    Send(BufferedProduceRequest),
    Flush {
        result_sender: oneshot::Sender<Result<()>>,
    },
    Close {
        result_sender: oneshot::Sender<Result<()>>,
    },
}

#[derive(Debug)]
enum BufferedProducerEvent {
    Command(BufferedProducerCommand),
    LingerElapsed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferedFlushReason {
    RecordCount,
    ByteCount,
    Linger,
    Flush,
    Close,
}

#[derive(Debug)]
struct BufferedProduceRequest {
    record: ProducerRecord,
    delivery_sender: oneshot::Sender<Result<RecordMetadata>>,
}

async fn enqueue_buffered_record(
    commands: &mpsc::Sender<BufferedProducerCommand>,
    record: ProducerRecord,
) -> Result<ProducerDelivery> {
    let (delivery_sender, delivery_receiver) = oneshot::channel();
    send_buffered_command(
        commands,
        BufferedProducerCommand::Send(BufferedProduceRequest {
            record,
            delivery_sender,
        }),
    )
    .await?;
    Ok(ProducerDelivery::new(delivery_receiver))
}

async fn send_buffered_command(
    commands: &mpsc::Sender<BufferedProducerCommand>,
    command: BufferedProducerCommand,
) -> Result<()> {
    commands
        .send(command)
        .await
        .map_err(|_| buffered_task_stopped_error())
}

async fn receive_buffered_result(receiver: oneshot::Receiver<Result<()>>) -> Result<()> {
    receiver.await.map_err(|_| buffered_task_stopped_error())?
}

async fn run_buffered_producer(
    mut producer: Producer,
    mut commands: mpsc::Receiver<BufferedProducerCommand>,
) {
    let mut pending = Vec::new();
    let mut first_enqueued_at = None;
    while let Some(event) = receive_buffered_event(
        &mut commands,
        buffered_linger_deadline(first_enqueued_at, producer.config.linger()),
    )
    .await
    {
        match event {
            BufferedProducerEvent::Command(command) => match command {
                BufferedProducerCommand::Send(request) => {
                    handle_buffered_send(
                        &mut producer,
                        &mut pending,
                        &mut first_enqueued_at,
                        request,
                    )
                    .await;
                }
                BufferedProducerCommand::Flush { result_sender } => {
                    let result = flush_buffered_deliveries_for_reason(
                        &mut producer,
                        &mut pending,
                        &mut first_enqueued_at,
                        BufferedFlushReason::Flush,
                    )
                    .await;
                    let _ = result_sender.send(result);
                }
                BufferedProducerCommand::Close { result_sender } => {
                    let result = flush_buffered_deliveries_for_reason(
                        &mut producer,
                        &mut pending,
                        &mut first_enqueued_at,
                        BufferedFlushReason::Close,
                    )
                    .await;
                    let _ = result_sender.send(result);
                    return;
                }
            },
            BufferedProducerEvent::LingerElapsed => {
                let _ = flush_buffered_deliveries_for_reason(
                    &mut producer,
                    &mut pending,
                    &mut first_enqueued_at,
                    BufferedFlushReason::Linger,
                )
                .await;
            }
        }
    }
    fail_buffered_deliveries(&mut pending, buffered_delivery_canceled_error);
}

async fn handle_buffered_send(
    producer: &mut Producer,
    pending: &mut Vec<BufferedProduceRequest>,
    first_enqueued_at: &mut Option<Instant>,
    request: BufferedProduceRequest,
) {
    if pending.is_empty() {
        *first_enqueued_at = Some(Instant::now());
    }
    pending.push(request);

    match buffered_enqueue_flush_reason(pending, &producer.config) {
        Ok(Some(reason)) => {
            let _ =
                flush_buffered_deliveries_for_reason(producer, pending, first_enqueued_at, reason)
                    .await;
        }
        Ok(None) => {}
        Err(error) => {
            debug!(
                error = %error,
                "completing buffered deliveries after flush trigger failure"
            );
            let requests = std::mem::take(pending);
            fail_buffered_delivery_requests(requests, &error);
            *first_enqueued_at = None;
        }
    }
}

async fn receive_buffered_event(
    commands: &mut mpsc::Receiver<BufferedProducerCommand>,
    linger_deadline: Option<Instant>,
) -> Option<BufferedProducerEvent> {
    match linger_deadline {
        Some(deadline) => {
            tokio::select! {
                biased;
                command = commands.recv() => command.map(BufferedProducerEvent::Command),
                _ = time::sleep_until(deadline) => Some(BufferedProducerEvent::LingerElapsed),
            }
        }
        None => commands.recv().await.map(BufferedProducerEvent::Command),
    }
}

fn buffered_linger_deadline(
    first_enqueued_at: Option<Instant>,
    linger: Duration,
) -> Option<Instant> {
    first_enqueued_at.map(|instant| instant + linger)
}

fn buffered_enqueue_flush_reason(
    pending: &[BufferedProduceRequest],
    config: &ProducerConfig,
) -> Result<Option<BufferedFlushReason>> {
    if pending.is_empty() {
        return Ok(None);
    }

    let groups = buffered_pending_groups(pending);

    if groups
        .values()
        .any(|record_indexes| record_indexes.len() >= config.max_records_per_batch)
    {
        return Ok(Some(BufferedFlushReason::RecordCount));
    }

    if config.max_batch_bytes != usize::MAX {
        for record_indexes in groups.values() {
            if buffered_pending_encoded_len(pending, record_indexes, config.compression)?
                >= config.max_batch_bytes
            {
                return Ok(Some(BufferedFlushReason::ByteCount));
            }
        }
    }

    Ok(None)
}

fn buffered_pending_groups(
    pending: &[BufferedProduceRequest],
) -> BTreeMap<(&str, Option<i32>), Vec<usize>> {
    let mut groups = BTreeMap::new();
    for (index, request) in pending.iter().enumerate() {
        groups
            .entry((request.record.topic(), request.record.partition_ref()))
            .or_insert_with(Vec::new)
            .push(index);
    }
    groups
}

fn buffered_pending_encoded_len(
    pending: &[BufferedProduceRequest],
    record_indexes: &[usize],
    compression: Compression,
) -> Result<usize> {
    let records = record_indexes
        .iter()
        .map(|&index| {
            pending
                .get(index)
                .map(|request| BatchRecord::new(request.record.clone()))
                .ok_or(Error::Unsupported("buffered record index out of bounds"))
        })
        .collect::<Result<Vec<_>>>()?;
    let prepared_records = records
        .iter()
        .enumerate()
        .map(|(index, record)| PreparedBatchRecord { index, record })
        .collect::<Vec<_>>();

    let produce_version = if compression == Compression::Zstd {
        ProduceVersion::V7
    } else {
        ProduceVersion::V3
    };
    batch_records_encoded_len(&prepared_records, produce_version, compression)
}

async fn flush_buffered_deliveries_for_reason(
    producer: &mut Producer,
    pending: &mut Vec<BufferedProduceRequest>,
    first_enqueued_at: &mut Option<Instant>,
    reason: BufferedFlushReason,
) -> Result<()> {
    if !pending.is_empty() {
        debug!(
            record_count = pending.len(),
            reason = ?reason,
            "flushing buffered producer records"
        );
    }
    let result = flush_buffered_deliveries(producer, pending).await;
    if pending.is_empty() {
        *first_enqueued_at = None;
    }
    result
}

async fn flush_buffered_deliveries(
    producer: &mut Producer,
    pending: &mut Vec<BufferedProduceRequest>,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }

    let requests = std::mem::take(pending);
    let records = requests
        .iter()
        .map(|request| request.record.clone())
        .collect::<Vec<_>>();

    match producer.send_batch_report(records).await {
        Ok(report) => {
            complete_buffered_deliveries(requests, report.into_records());
            Ok(())
        }
        Err(error) => {
            fail_buffered_delivery_requests(requests, &error);
            Err(error)
        }
    }
}

fn complete_buffered_deliveries(
    requests: Vec<BufferedProduceRequest>,
    outcomes: Vec<ProducerBatchRecordOutcome>,
) {
    let mut outcomes = outcomes.into_iter();
    for request in requests {
        let result = outcomes
            .next()
            .map(ProducerBatchRecordOutcome::into_metadata)
            .unwrap_or_else(|| Err(Error::Unsupported("missing buffered delivery outcome")));
        let _ = request.delivery_sender.send(result);
    }
}

fn fail_buffered_delivery_requests(requests: Vec<BufferedProduceRequest>, error: &Error) {
    for request in requests {
        debug!(
            topic = request.record.topic(),
            partition = ?request.record.partition_ref(),
            error = %error,
            "completing buffered delivery after batch request failure"
        );
        let _ = request
            .delivery_sender
            .send(Err(delivery_error_from_request_error(error)));
    }
}

fn fail_buffered_deliveries(pending: &mut Vec<BufferedProduceRequest>, error: fn() -> Error) {
    for request in pending.drain(..) {
        debug!(
            topic = request.record.topic(),
            partition = ?request.record.partition_ref(),
            "completing buffered delivery with error"
        );
        let _ = request.delivery_sender.send(Err(error()));
    }
}

fn buffered_task_stopped_error() -> Error {
    Error::Unsupported("buffered producer task stopped")
}

fn buffered_delivery_canceled_error() -> Error {
    Error::Unsupported("buffered producer delivery canceled")
}

fn delivery_error_from_request_error(error: &Error) -> Error {
    match error {
        Error::MissingBootstrapServer => Error::MissingBootstrapServer,
        Error::UnknownTopicOrPartition { topic, partition } => Error::UnknownTopicOrPartition {
            topic: topic.clone(),
            partition: *partition,
        },
        Error::MissingLeader { topic, partition } => Error::MissingLeader {
            topic: topic.clone(),
            partition: *partition,
        },
        Error::MissingBroker { node_id } => Error::MissingBroker { node_id: *node_id },
        Error::MissingSaslCredentials => Error::MissingSaslCredentials,
        Error::InvalidSaslResponse { mechanism, reason } => {
            Error::InvalidSaslResponse { mechanism, reason }
        }
        Error::Broker { code, context } => Error::Broker {
            code: *code,
            context: context.clone(),
        },
        Error::RequestTimedOut { timeout_ms } => Error::RequestTimedOut {
            timeout_ms: *timeout_ms,
        },
        Error::TlsConfig { reason } => Error::TlsConfig {
            reason: reason.clone(),
        },
        Error::InvalidTlsServerName { server } => Error::InvalidTlsServerName {
            server: server.clone(),
        },
        Error::Unsupported(feature) => Error::Unsupported(feature),
        Error::Io(error) => Error::Io(std::io::Error::new(error.kind(), error.to_string())),
        Error::TaskJoin(_) => Error::Unsupported("buffered producer task join failed"),
        Error::Protocol(error) => Error::Protocol(error.clone()),
    }
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
            let result = self
                .send_once(&record, &topic, timestamp, timestamp_ms)
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

    async fn send_once(
        &mut self,
        record: &ProducerRecord,
        topic: &str,
        timestamp: SystemTime,
        timestamp_ms: i64,
    ) -> Result<RecordMetadata> {
        let metadata = self.metadata_for_topic(topic).await?;
        self.send_with_metadata(record, &metadata, timestamp, timestamp_ms)
            .await
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
        let report = self.send_batch_report(records).await?;
        report
            .into_records()
            .into_iter()
            .map(ProducerBatchRecordOutcome::into_metadata)
            .collect()
    }

    /// Sends multiple records and returns one outcome per input record.
    ///
    /// Request-level failures such as metadata lookup, connection failures, or
    /// timeouts are returned as [`Error`]. Broker Produce response failures for
    /// a topic partition are returned inside the report so callers can inspect
    /// partial success and failure by input record.
    pub async fn send_batch_report(
        &mut self,
        records: impl IntoIterator<Item = ProducerRecord>,
    ) -> Result<ProducerBatchReport> {
        if self.config.acks == Acks::None {
            return Err(Error::Unsupported("producer acks=0 send without response"));
        }

        let records = records
            .into_iter()
            .map(BatchRecord::new)
            .collect::<Vec<_>>();
        if records.is_empty() {
            return Ok(ProducerBatchReport::new(Vec::new()));
        }

        debug!(record_count = records.len(), "sending kafka record batch");

        let mut outcomes = std::iter::repeat_with(|| None)
            .take(records.len())
            .collect::<Vec<_>>();
        let mut pending_indexes = (0..records.len()).collect::<Vec<_>>();
        let mut sequence_tracker = IdempotentBatchSequenceTracker::default();
        let mut attempt = 0;
        loop {
            let result = self
                .send_batch_once(&records, &pending_indexes, &mut sequence_tracker)
                .await;
            match result {
                Err(error) if attempt < self.config.max_retries && can_retry_send(&error) => {
                    invalidate_metadata_cache_for_record_indexes(
                        &mut self.metadata_cache,
                        &records,
                        &pending_indexes,
                    );
                    attempt += 1;
                }
                Ok(attempt_outcomes) => {
                    let retry_indexes = record_batch_attempt_outcomes(
                        &mut outcomes,
                        attempt_outcomes,
                        attempt,
                        self.config.max_retries,
                    )?;
                    if !retry_indexes.is_empty() {
                        invalidate_metadata_cache_for_record_indexes(
                            &mut self.metadata_cache,
                            &records,
                            &retry_indexes,
                        );
                        pending_indexes = retry_indexes;
                        attempt += 1;
                        continue;
                    }

                    let report = batch_report_from_outcomes(outcomes)?;
                    debug!(
                        record_count = report.records().len(),
                        has_failures = report.has_failures(),
                        "sent kafka record batch"
                    );
                    return Ok(report);
                }
                Err(error) => return Err(error),
            }
        }
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
            Err(error) if can_retry_send(&error) => {
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

    async fn send_batch_once(
        &mut self,
        records: &[BatchRecord],
        record_indexes: &[usize],
        sequence_tracker: &mut IdempotentBatchSequenceTracker,
    ) -> Result<Vec<(usize, ProducerBatchRecordOutcome)>> {
        let mut groups = BTreeMap::<ProduceBatchKey, Vec<PreparedBatchRecord<'_>>>::new();
        for &index in record_indexes {
            let record = records
                .get(index)
                .ok_or(Error::Unsupported("batch record index out of bounds"))?;
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

        let mut output = Vec::with_capacity(record_indexes.len());
        for (key, records) in groups {
            output.extend(
                self.send_batch_group(&key, &records, sequence_tracker)
                    .await?,
            );
        }
        if output.len() != record_indexes.len() {
            return Err(Error::Unsupported("missing batch record outcome"));
        }
        Ok(output)
    }

    async fn send_batch_group(
        &mut self,
        key: &ProduceBatchKey,
        records: &[PreparedBatchRecord<'_>],
        sequence_tracker: &mut IdempotentBatchSequenceTracker,
    ) -> Result<Vec<(usize, ProducerBatchRecordOutcome)>> {
        debug!(
            topic = key.topic.as_str(),
            partition = key.partition,
            broker_addr = key.broker_addr.as_str(),
            record_count = records.len(),
            "resolved produce batch leader"
        );

        let mut leader_client = self
            .config
            .client
            .connect_broker(key.broker_addr.clone())
            .await?;
        let api_versions = leader_client.api_versions().await?;
        if api_versions.error_code != 0 {
            return Err(Error::Broker {
                code: api_versions.error_code,
                context: format!("api versions for produce {}-{}", key.topic, key.partition),
            });
        }

        let produce_version =
            select_produce_batch_version(&api_versions, records, self.config.compression)?;
        if self.idempotent_state.is_some() && produce_version == ProduceVersion::V2 {
            return Err(Error::Unsupported(
                "idempotent producer requires Produce API v3 or newer",
            ));
        }
        debug!(
            topic = key.topic.as_str(),
            partition = key.partition,
            produce_version = ?produce_version,
            record_count = records.len(),
            "selected produce batch api version"
        );

        let chunks = batch_record_chunks(
            records,
            self.config.max_records_per_batch,
            self.config.max_batch_bytes,
            produce_version,
            self.config.compression,
        )?;
        let mut output = Vec::with_capacity(records.len());
        for (chunk_index, records) in chunks.iter().copied().enumerate() {
            let identity = sequence_tracker.identity_for_chunk(
                self.idempotent_state.as_ref(),
                key,
                records,
            )?;
            let response = match produce_version {
                ProduceVersion::V7 => ProduceResponse::V7(
                    leader_client
                        .produce_v7(
                            None,
                            self.config.acks.as_i16(),
                            30_000,
                            vec![ProduceTopicV3 {
                                name: key.topic.clone(),
                                partitions: vec![ProducePartitionV3 {
                                    partition_index: key.partition,
                                    compression: self
                                        .config
                                        .compression
                                        .as_record_batch_compression(),
                                    identity,
                                    records: records
                                        .iter()
                                        .map(|record| {
                                            record_batch_message(
                                                &record.record.record,
                                                record.record.timestamp_ms,
                                            )
                                        })
                                        .collect(),
                                }],
                            }],
                        )
                        .await?,
                ),
                ProduceVersion::V3 => ProduceResponse::V2(
                    leader_client
                        .produce_v3(
                            None,
                            self.config.acks.as_i16(),
                            30_000,
                            vec![ProduceTopicV3 {
                                name: key.topic.clone(),
                                partitions: vec![ProducePartitionV3 {
                                    partition_index: key.partition,
                                    compression: self
                                        .config
                                        .compression
                                        .as_record_batch_compression(),
                                    identity,
                                    records: records
                                        .iter()
                                        .map(|record| {
                                            record_batch_message(
                                                &record.record.record,
                                                record.record.timestamp_ms,
                                            )
                                        })
                                        .collect(),
                                }],
                            }],
                        )
                        .await?,
                ),
                ProduceVersion::V2 => ProduceResponse::V2(
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
                        .await?,
                ),
            };
            let partition_response =
                produce_partition_response(&response, &key.topic, key.partition)?;
            if partition_response.error_code != 0 {
                output.extend(batch_failure_outcomes(
                    key,
                    records,
                    partition_response.error_code,
                ));
                if self.idempotent_state.is_some() {
                    for remaining in chunks.iter().skip(chunk_index + 1).copied() {
                        output.extend(batch_failure_outcomes(
                            key,
                            remaining,
                            partition_response.error_code,
                        ));
                    }
                    break;
                }
            } else {
                sequence_tracker.acknowledge_chunk(self.idempotent_state.as_mut(), key, records)?;
                output.extend(batch_success_outcomes(
                    key,
                    records,
                    partition_response.base_offset,
                ));
            }
        }

        Ok(output)
    }

    async fn send_with_metadata(
        &mut self,
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

        let mut leader_client = self.config.client.connect_broker(broker_addr).await?;
        let api_versions = leader_client.api_versions().await?;
        if api_versions.error_code != 0 {
            return Err(Error::Broker {
                code: api_versions.error_code,
                context: format!("api versions for produce {}-{}", record.topic(), partition),
            });
        }

        let produce_version =
            select_produce_version(&api_versions, record, self.config.compression)?;
        if self.idempotent_state.is_some() && produce_version == ProduceVersion::V2 {
            return Err(Error::Unsupported(
                "idempotent producer requires Produce API v3 or newer",
            ));
        }
        let identity = self
            .idempotent_state
            .as_ref()
            .map(|state| state.identity(record.topic(), partition))
            .unwrap_or(RecordBatchIdentity::NON_IDEMPOTENT);
        debug!(
            topic = record.topic(),
            partition,
            produce_version = ?produce_version,
            "selected produce api version"
        );

        let response = match produce_version {
            ProduceVersion::V7 => ProduceResponse::V7(
                leader_client
                    .produce_v7(
                        None,
                        self.config.acks.as_i16(),
                        30_000,
                        vec![ProduceTopicV3 {
                            name: record.topic().to_owned(),
                            partitions: vec![ProducePartitionV3 {
                                partition_index: partition,
                                compression: self.config.compression.as_record_batch_compression(),
                                identity,
                                records: vec![record_batch_message(record, timestamp_ms)],
                            }],
                        }],
                    )
                    .await?,
            ),
            ProduceVersion::V3 => ProduceResponse::V2(
                leader_client
                    .produce_v3(
                        None,
                        self.config.acks.as_i16(),
                        30_000,
                        vec![ProduceTopicV3 {
                            name: record.topic().to_owned(),
                            partitions: vec![ProducePartitionV3 {
                                partition_index: partition,
                                compression: self.config.compression.as_record_batch_compression(),
                                identity,
                                records: vec![record_batch_message(record, timestamp_ms)],
                            }],
                        }],
                    )
                    .await?,
            ),
            ProduceVersion::V2 => ProduceResponse::V2(
                leader_client
                    .produce_one_v2(
                        self.config.acks.as_i16(),
                        30_000,
                        record.topic().to_owned(),
                        partition,
                        vec![message_set_message(record, timestamp_ms)],
                    )
                    .await?,
            ),
        };
        let partition_response = produce_partition_response(&response, record.topic(), partition)?;
        if partition_response.error_code != 0 {
            return Err(Error::Broker {
                code: partition_response.error_code,
                context: format!("produce {}-{}", record.topic(), partition),
            });
        }
        if let Some(state) = &mut self.idempotent_state {
            state.acknowledge(record.topic(), partition, 1);
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
    max_records_per_batch: usize,
    max_batch_bytes: usize,
    linger: Duration,
    compression: Compression,
    idempotence: bool,
}

impl ProducerConfig {
    /// Creates a producer configuration from one or more Kafka bootstrap servers.
    pub fn new(bootstrap_servers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            acks: Acks::Leader,
            max_retries: 1,
            max_records_per_batch: usize::MAX,
            max_batch_bytes: usize::MAX,
            linger: Duration::from_millis(0),
            compression: Compression::None,
            idempotence: false,
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

    /// Sets the Kafka security protocol used for producer broker connections.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.client = self.client.security_protocol(security_protocol);
        self
    }

    /// Sets the TLS server name used for producer broker certificate validation.
    pub fn tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.client = self.client.tls_server_name(server_name);
        self
    }

    /// Adds a DER-encoded TLS root certificate for producer broker validation.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_root_certificate_der(certificate);
        self
    }

    /// Sets SASL/PLAIN credentials for producer broker connections.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.client = self.client.sasl_plain(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-256 credentials for producer broker connections.
    pub fn sasl_scram_sha_256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_256(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-512 credentials for producer broker connections.
    pub fn sasl_scram_sha_512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_512(username, password);
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

    /// Sets the maximum records sent in one Produce request per topic partition.
    ///
    /// Values below 1 are treated as 1 so batch sends always make progress.
    pub fn max_records_per_batch(mut self, max_records_per_batch: usize) -> Self {
        self.max_records_per_batch = max_records_per_batch.max(1);
        self
    }

    /// Sets the maximum encoded record-set bytes sent in one Produce request per topic partition.
    ///
    /// Values below 1 are treated as 1 so batch sends always make progress. A
    /// single record that exceeds the limit is still sent by itself.
    pub fn max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
        self.max_batch_bytes = max_batch_bytes.max(1);
        self
    }

    /// Sets the producer linger duration in milliseconds for buffered sends.
    ///
    /// The immediate `send` and `send_batch` APIs do not wait on linger. Linger
    /// applies to the opt-in buffered producer path.
    pub fn linger_ms(mut self, linger_ms: u64) -> Self {
        self.linger = Duration::from_millis(linger_ms);
        self
    }

    /// Sets the compression policy used for Produce API v3 record batches.
    ///
    /// Compression currently requires Produce API v3 because the legacy
    /// MessageSet fallback path does not encode compressed batches.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Enables Kafka idempotent producer identity and sequence tracking.
    ///
    /// Idempotence requires `acks=all`, at least one retry, and Produce API v3
    /// or newer. The current alpha supports single-record [`Producer::send`];
    /// batch and buffered idempotent sends remain under development.
    pub fn enable_idempotence(mut self, enabled: bool) -> Self {
        self.idempotence = enabled;
        if enabled {
            self.acks = Acks::All;
            self.max_retries = self.max_retries.max(5);
        }
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

    /// Returns the configured maximum records per Produce request.
    pub fn max_records_per_batch_ref(&self) -> usize {
        self.max_records_per_batch
    }

    /// Returns the configured maximum encoded record-set bytes per Produce request.
    pub fn max_batch_bytes_ref(&self) -> usize {
        self.max_batch_bytes
    }

    /// Returns the configured linger duration for buffered sends.
    pub fn linger(&self) -> Duration {
        self.linger
    }

    /// Returns the configured producer compression policy.
    pub fn compression_ref(&self) -> Compression {
        self.compression
    }

    /// Returns whether idempotent producer behavior is enabled.
    pub fn idempotence_enabled(&self) -> bool {
        self.idempotence
    }

    /// Returns the shared client configuration.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Connects to Kafka and builds a producer.
    pub async fn build(self) -> Result<Producer> {
        if self.idempotence && (self.acks != Acks::All || self.max_retries == 0) {
            return Err(Error::Unsupported(
                "idempotence requires acks=all and at least one retry",
            ));
        }
        let mut client = self.client.clone().connect().await?;
        let idempotent_state = if self.idempotence {
            Some(initialize_idempotent_producer(&mut client, self.max_retries).await?)
        } else {
            None
        };
        Ok(Producer {
            client,
            config: self,
            metadata_cache: BTreeMap::new(),
            idempotent_state,
        })
    }

    /// Connects to Kafka and builds an opt-in buffered producer skeleton.
    pub async fn build_buffered(self) -> Result<BufferedProducer> {
        let producer = self.build().await?;
        let (commands, receiver) = mpsc::channel(BUFFERED_PRODUCER_CHANNEL_CAPACITY);
        let worker = tokio::spawn(run_buffered_producer(producer, receiver));
        Ok(BufferedProducer {
            commands,
            worker: Some(worker),
            state: BufferedProducerState::Open,
        })
    }
}

async fn initialize_idempotent_producer(
    client: &mut Client,
    max_retries: u32,
) -> Result<IdempotentProducerState> {
    let mut attempt = 0;
    loop {
        let response = client.init_producer_id_v0(None, 60_000).await?;
        if response.error_code == 0 {
            return Ok(IdempotentProducerState::new(
                response.producer_id,
                response.producer_epoch,
            ));
        }
        if attempt < max_retries
            && matches!(
                BrokerErrorKind::from_code(response.error_code),
                BrokerErrorKind::CoordinatorLoadInProgress
                    | BrokerErrorKind::CoordinatorNotAvailable
            )
        {
            attempt += 1;
            time::sleep(IDEMPOTENT_INIT_RETRY_BACKOFF).await;
            continue;
        }
        return Err(Error::Broker {
            code: response.error_code,
            context: "initialize idempotent producer".to_owned(),
        });
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
    V7,
}

fn select_produce_version(
    api_versions: &ApiVersionsResponseV0,
    record: &ProducerRecord,
    compression: Compression,
) -> Result<ProduceVersion> {
    if compression == Compression::Zstd {
        return api_versions
            .highest_supported_version(PRODUCE_API_KEY, 7)
            .filter(|version| *version >= 7)
            .map(|_| ProduceVersion::V7)
            .ok_or(Error::Unsupported(
                "zstd compression requires Produce API v7",
            ));
    }

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
        if compression.requires_record_batch() {
            return Err(Error::Unsupported(
                "producer compression requires Produce API v3",
            ));
        }
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
    compression: Compression,
) -> Result<ProduceVersion> {
    if compression == Compression::Zstd {
        return api_versions
            .highest_supported_version(PRODUCE_API_KEY, 7)
            .filter(|version| *version >= 7)
            .map(|_| ProduceVersion::V7)
            .ok_or(Error::Unsupported(
                "zstd compression requires Produce API v7",
            ));
    }

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
        if compression.requires_record_batch() {
            return Err(Error::Unsupported(
                "producer compression requires Produce API v3",
            ));
        }
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

enum ProduceResponse {
    V2(ProduceResponseV2),
    V7(ProduceResponseV7),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProducePartitionResult {
    error_code: i16,
    base_offset: i64,
}

fn produce_partition_response(
    response: &ProduceResponse,
    topic_name: &str,
    partition_index: i32,
) -> Result<ProducePartitionResult> {
    let result = match response {
        ProduceResponse::V2(response) => response
            .responses
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition_index == partition_index)
            })
            .map(|partition| ProducePartitionResult {
                error_code: partition.error_code,
                base_offset: partition.base_offset,
            }),
        ProduceResponse::V7(response) => response
            .responses
            .iter()
            .find(|topic| topic.name == topic_name)
            .and_then(|topic| {
                topic
                    .partitions
                    .iter()
                    .find(|partition| partition.partition_index == partition_index)
            })
            .map(|partition| ProducePartitionResult {
                error_code: partition.error_code,
                base_offset: partition.base_offset,
            }),
    };
    result.ok_or_else(|| Error::UnknownTopicOrPartition {
        topic: topic_name.to_owned(),
        partition: partition_index,
    })
}

fn batch_success_outcomes(
    key: &ProduceBatchKey,
    records: &[PreparedBatchRecord<'_>],
    base_offset: i64,
) -> Vec<(usize, ProducerBatchRecordOutcome)> {
    records
        .iter()
        .enumerate()
        .map(|(relative_offset, record)| {
            (
                record.index,
                ProducerBatchRecordOutcome::Success(RecordMetadata::new(
                    key.topic.clone(),
                    key.partition,
                    base_offset + i64::try_from(relative_offset).unwrap_or(0),
                    Some(record.record.timestamp),
                )),
            )
        })
        .collect()
}

fn batch_failure_outcomes(
    key: &ProduceBatchKey,
    records: &[PreparedBatchRecord<'_>],
    error_code: i16,
) -> Vec<(usize, ProducerBatchRecordOutcome)> {
    records
        .iter()
        .map(|record| {
            (
                record.index,
                ProducerBatchRecordOutcome::Failure(ProducerBatchFailure::new(
                    record.index,
                    key.topic.clone(),
                    key.partition,
                    Error::Broker {
                        code: error_code,
                        context: format!("produce {}-{}", key.topic, key.partition),
                    },
                )),
            )
        })
        .collect()
}

fn batch_record_chunks<'records, 'batch>(
    records: &'records [PreparedBatchRecord<'batch>],
    max_records_per_batch: usize,
    max_batch_bytes: usize,
    produce_version: ProduceVersion,
    compression: Compression,
) -> Result<Vec<&'records [PreparedBatchRecord<'batch>]>> {
    let max_records_per_batch = max_records_per_batch.max(1);
    let max_batch_bytes = max_batch_bytes.max(1);
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < records.len() {
        let mut end = start;
        while end < records.len() && end - start < max_records_per_batch {
            let candidate_end = end + 1;
            let candidate = &records[start..candidate_end];
            let candidate_len = batch_records_encoded_len(candidate, produce_version, compression)?;
            if candidate_len > max_batch_bytes && end > start {
                break;
            }
            end = candidate_end;
            if candidate_len > max_batch_bytes {
                break;
            }
        }
        chunks.push(&records[start..end]);
        start = end;
    }
    Ok(chunks)
}

fn batch_records_encoded_len(
    records: &[PreparedBatchRecord<'_>],
    produce_version: ProduceVersion,
    compression: Compression,
) -> Result<usize> {
    match produce_version {
        ProduceVersion::V3 | ProduceVersion::V7 => {
            let records = records
                .iter()
                .map(|record| {
                    record_batch_message(&record.record.record, record.record.timestamp_ms)
                })
                .collect::<Vec<_>>();
            encoded_record_batch_set_len_with_compression(
                &records,
                compression.as_record_batch_compression(),
            )
            .map_err(Error::from)
        }
        ProduceVersion::V2 => {
            let records = records
                .iter()
                .map(|record| {
                    message_set_message(&record.record.record, record.record.timestamp_ms)
                })
                .collect::<Vec<_>>();
            encoded_message_set_len(&records).map_err(Error::from)
        }
    }
}

fn record_batch_attempt_outcomes(
    output: &mut [Option<ProducerBatchRecordOutcome>],
    outcomes: Vec<(usize, ProducerBatchRecordOutcome)>,
    attempt: u32,
    max_retries: u32,
) -> Result<Vec<usize>> {
    let mut retry_indexes = Vec::new();
    for (index, outcome) in outcomes {
        let output_slot = output
            .get_mut(index)
            .ok_or(Error::Unsupported("batch record index out of bounds"))?;
        if should_retry_batch_outcome(&outcome, attempt, max_retries) {
            retry_indexes.push(index);
        } else {
            *output_slot = Some(outcome);
        }
    }
    Ok(retry_indexes)
}

fn should_retry_batch_outcome(
    outcome: &ProducerBatchRecordOutcome,
    attempt: u32,
    max_retries: u32,
) -> bool {
    attempt < max_retries
        && matches!(
            outcome,
            ProducerBatchRecordOutcome::Failure(failure) if can_retry_send(failure.error())
        )
}

fn batch_report_from_outcomes(
    outcomes: Vec<Option<ProducerBatchRecordOutcome>>,
) -> Result<ProducerBatchReport> {
    let records = outcomes
        .into_iter()
        .map(|outcome| outcome.ok_or(Error::Unsupported("missing batch record outcome")))
        .collect::<Result<Vec<_>>>()?;
    Ok(ProducerBatchReport::new(records))
}

fn can_retry_send(error: &Error) -> bool {
    match error {
        Error::Broker { code, .. } => BrokerErrorKind::from_code(*code).is_produce_retryable(),
        Error::Io(_)
        | Error::RequestTimedOut { .. }
        | Error::UnknownTopicOrPartition { .. }
        | Error::MissingLeader { .. }
        | Error::MissingBroker { .. } => true,
        Error::MissingBootstrapServer
        | Error::MissingSaslCredentials
        | Error::InvalidSaslResponse { .. }
        | Error::TlsConfig { .. }
        | Error::InvalidTlsServerName { .. }
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

fn invalidate_metadata_cache_for_record_indexes(
    metadata_cache: &mut BTreeMap<String, MetadataResponseV1>,
    records: &[BatchRecord],
    record_indexes: &[usize],
) {
    for &index in record_indexes {
        if let Some(record) = records.get(index) {
            metadata_cache.remove(record.record.topic());
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        advance_producer_sequence, batch_failure_outcomes, batch_record_chunks,
        batch_records_encoded_len, batch_report_from_outcomes, batch_success_outcomes,
        buffered_delivery_canceled_error, buffered_enqueue_flush_reason, buffered_linger_deadline,
        can_retry_send, choose_partition, complete_buffered_deliveries,
        delivery_error_from_request_error, enqueue_buffered_record, fail_buffered_deliveries,
        invalidate_metadata_cache, invalidate_metadata_cache_for_record_indexes, leader_for,
        message_set_message, record_batch_attempt_outcomes, record_batch_message,
        select_produce_batch_version, select_produce_version, Acks, BatchRecord,
        BufferedFlushReason, BufferedProduceRequest, BufferedProducerCommand,
        BufferedProducerState, Compression, IdempotentBatchSequenceTracker,
        IdempotentProducerState, PreparedBatchRecord, ProduceBatchKey, ProduceVersion, Producer,
        ProducerBatchFailure, ProducerBatchRecordOutcome, ProducerBatchReport, ProducerConfig,
        ProducerDelivery, ProducerRecord, RecordMetadata, SecurityProtocol,
    };
    use crate::{BrokerErrorKind, Client, Error};
    use kafrust_protocol::api::api_versions::{ApiKeyVersion, ApiVersionsResponseV0};
    use kafrust_protocol::api::metadata::{
        BrokerMetadata, MetadataResponseV1, PartitionMetadata, TopicMetadata,
    };
    use kafrust_protocol::api::produce::API_KEY as PRODUCE_API_KEY;
    use std::collections::BTreeMap;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::{mpsc, oneshot};
    use tokio::time::Instant;

    #[test]
    fn maps_acks_to_kafka_values() {
        assert_eq!(Acks::None.as_i16(), 0);
        assert_eq!(Acks::Leader.as_i16(), 1);
        assert_eq!(Acks::All.as_i16(), -1);
    }

    #[test]
    fn keeps_idempotent_sequence_partition_scoped() {
        let mut state = IdempotentProducerState::new(42, 3);

        let first = state.identity("orders", 0);
        assert_eq!(first.producer_id, 42);
        assert_eq!(first.producer_epoch, 3);
        assert_eq!(first.base_sequence, 0);

        state.acknowledge("orders", 0, 4);

        assert_eq!(state.identity("orders", 0).base_sequence, 4);
        assert_eq!(state.identity("orders", 1).base_sequence, 0);
        assert_eq!(state.identity("payments", 0).base_sequence, 0);
    }

    #[test]
    fn preserves_idempotent_sequence_until_acknowledged() {
        let mut state = IdempotentProducerState::new(42, 3);

        let first_attempt = state.identity("orders", 0);
        let retry_attempt = state.identity("orders", 0);
        assert_eq!(retry_attempt, first_attempt);

        state.acknowledge("orders", 0, 2);
        assert_eq!(state.identity("orders", 0).base_sequence, 2);
    }

    #[test]
    fn wraps_idempotent_sequence_after_i32_max() {
        assert_eq!(advance_producer_sequence(i32::MAX, 1), 0);
        assert_eq!(advance_producer_sequence(i32::MAX - 1, 3), 1);
    }

    #[test]
    fn preserves_reserved_batch_sequences_across_retries_and_chunks() {
        let batch = [
            BatchRecord::new(ProducerRecord::to("orders")),
            BatchRecord::new(ProducerRecord::to("orders")),
            BatchRecord::new(ProducerRecord::to("orders")),
        ];
        let records = prepared_records(&batch);
        let key = ProduceBatchKey {
            broker_addr: "localhost:9092".to_owned(),
            topic: "orders".to_owned(),
            partition: 0,
        };
        let mut state = IdempotentProducerState::new(42, 3);
        let mut tracker = IdempotentBatchSequenceTracker::default();

        let first = tracker
            .identity_for_chunk(Some(&state), &key, &records[..2])
            .unwrap();
        let retry = tracker
            .identity_for_chunk(Some(&state), &key, &records[..2])
            .unwrap();
        let second = tracker
            .identity_for_chunk(Some(&state), &key, &records[2..])
            .unwrap();

        assert_eq!(first.base_sequence, 0);
        assert_eq!(retry, first);
        assert_eq!(second.base_sequence, 2);
        assert_eq!(state.identity("orders", 0).base_sequence, 0);
        tracker
            .acknowledge_chunk(Some(&mut state), &key, &records[..2])
            .unwrap();
        tracker
            .acknowledge_chunk(Some(&mut state), &key, &records[..2])
            .unwrap();
        tracker
            .acknowledge_chunk(Some(&mut state), &key, &records[2..])
            .unwrap();
        assert_eq!(state.identity("orders", 0).base_sequence, 3);
    }

    #[test]
    fn rejects_idempotent_retry_on_a_different_partition() {
        let batch = [BatchRecord::new(ProducerRecord::to("orders"))];
        let records = prepared_records(&batch);
        let state = IdempotentProducerState::new(42, 3);
        let mut tracker = IdempotentBatchSequenceTracker::default();
        let first_key = ProduceBatchKey {
            broker_addr: "localhost:9092".to_owned(),
            topic: "orders".to_owned(),
            partition: 0,
        };
        let second_key = ProduceBatchKey {
            partition: 1,
            ..first_key.clone()
        };

        tracker
            .identity_for_chunk(Some(&state), &first_key, &records)
            .unwrap();
        let error = tracker
            .identity_for_chunk(Some(&state), &second_key, &records)
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported("idempotent batch retry changed topic or partition")
        ));
        assert_eq!(state.identity("orders", 0).base_sequence, 0);
    }

    #[test]
    fn reserves_batch_sequences_independently_per_partition() {
        let first_batch = [BatchRecord::new(ProducerRecord::to("orders"))];
        let second_batch = [BatchRecord::new(ProducerRecord::to("orders"))];
        let first_records = prepared_records(&first_batch);
        let mut second_records = prepared_records(&second_batch);
        second_records[0].index = 1;
        let state = IdempotentProducerState::new(42, 3);
        let mut tracker = IdempotentBatchSequenceTracker::default();
        let first_key = ProduceBatchKey {
            broker_addr: "localhost:9092".to_owned(),
            topic: "orders".to_owned(),
            partition: 0,
        };
        let second_key = ProduceBatchKey {
            partition: 1,
            ..first_key.clone()
        };

        let first = tracker
            .identity_for_chunk(Some(&state), &first_key, &first_records)
            .unwrap();
        let second = tracker
            .identity_for_chunk(Some(&state), &second_key, &second_records)
            .unwrap();

        assert_eq!(first.base_sequence, 0);
        assert_eq!(second.base_sequence, 0);
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
            select_produce_version(&versions, &record, Compression::None).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn falls_back_to_message_set_without_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let record = ProducerRecord::to("orders");

        assert_eq!(
            select_produce_version(&versions, &record, Compression::None).unwrap(),
            ProduceVersion::V2
        );
    }

    #[test]
    fn selects_record_batch_when_gzip_compression_is_configured() {
        let versions = api_versions(3);
        let record = ProducerRecord::to("orders");

        assert_eq!(
            select_produce_version(&versions, &record, Compression::Gzip).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn selects_record_batch_when_snappy_compression_is_configured() {
        let versions = api_versions(3);
        let record = ProducerRecord::to("orders");

        assert_eq!(
            select_produce_version(&versions, &record, Compression::Snappy).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn selects_record_batch_when_lz4_compression_is_configured() {
        let versions = api_versions(3);
        let record = ProducerRecord::to("orders");

        assert_eq!(
            select_produce_version(&versions, &record, Compression::Lz4).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn selects_produce_v7_when_zstd_compression_is_configured() {
        let versions = api_versions(7);
        let record = ProducerRecord::to("orders");

        assert_eq!(
            select_produce_version(&versions, &record, Compression::Zstd).unwrap(),
            ProduceVersion::V7
        );
    }

    #[test]
    fn rejects_zstd_compression_when_produce_v7_is_unavailable() {
        let versions = api_versions(6);
        let record = ProducerRecord::to("orders");

        assert!(matches!(
            select_produce_version(&versions, &record, Compression::Zstd).unwrap_err(),
            Error::Unsupported("zstd compression requires Produce API v7")
        ));
    }

    #[test]
    fn rejects_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let record = ProducerRecord::to("orders").header("source", "checkout");

        assert!(matches!(
            select_produce_version(&versions, &record, Compression::None).unwrap_err(),
            Error::Unsupported("record headers require Produce API v3")
        ));
    }

    #[test]
    fn rejects_gzip_compression_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let record = ProducerRecord::to("orders");

        assert!(matches!(
            select_produce_version(&versions, &record, Compression::Gzip).unwrap_err(),
            Error::Unsupported("producer compression requires Produce API v3")
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
            select_produce_batch_version(&versions, &records, Compression::None).unwrap(),
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
            select_produce_batch_version(&versions, &records, Compression::None).unwrap(),
            ProduceVersion::V2
        );
    }

    #[test]
    fn selects_record_batch_for_gzip_batch_when_produce_v3_is_available() {
        let versions = api_versions(3);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").key("order-2"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert_eq!(
            select_produce_batch_version(&versions, &records, Compression::Gzip).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn selects_record_batch_for_snappy_batch_when_produce_v3_is_available() {
        let versions = api_versions(3);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").key("order-2"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert_eq!(
            select_produce_batch_version(&versions, &records, Compression::Snappy).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn selects_record_batch_for_lz4_batch_when_produce_v3_is_available() {
        let versions = api_versions(3);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").key("order-2"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert_eq!(
            select_produce_batch_version(&versions, &records, Compression::Lz4).unwrap(),
            ProduceVersion::V3
        );
    }

    #[test]
    fn selects_produce_v7_for_zstd_batch() {
        let versions = api_versions(7);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").key("order-2"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert_eq!(
            select_produce_batch_version(&versions, &records, Compression::Zstd).unwrap(),
            ProduceVersion::V7
        );
    }

    #[test]
    fn rejects_zstd_batch_when_produce_v7_is_unavailable() {
        let versions = api_versions(6);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let batch = [first];
        let records = prepared_records(&batch);

        assert!(matches!(
            select_produce_batch_version(&versions, &records, Compression::Zstd).unwrap_err(),
            Error::Unsupported("zstd compression requires Produce API v7")
        ));
    }

    #[test]
    fn rejects_batch_headers_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").header("source", "checkout"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert!(matches!(
            select_produce_batch_version(&versions, &records, Compression::None).unwrap_err(),
            Error::Unsupported("record headers require Produce API v3")
        ));
    }

    #[test]
    fn rejects_gzip_batch_when_only_produce_v2_is_available() {
        let versions = api_versions(2);
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders").key("order-2"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        assert!(matches!(
            select_produce_batch_version(&versions, &records, Compression::Gzip).unwrap_err(),
            Error::Unsupported("producer compression requires Produce API v3")
        ));
    }

    #[test]
    fn builds_producer_config() {
        let config = ProducerConfig::new(["localhost:9092"])
            .client_id("orders-api")
            .request_timeout_ms(5_000)
            .security_protocol(SecurityProtocol::SaslTls)
            .tls_server_name("broker.example.com")
            .tls_root_certificate_der([1, 2, 3])
            .sasl_plain("alice", "secret-password")
            .max_retries(3)
            .max_records_per_batch(128)
            .max_batch_bytes(64 * 1024)
            .linger_ms(5)
            .compression(Compression::Lz4)
            .acks(Acks::All);

        assert_eq!(config.acks_ref(), Acks::All);
        assert_eq!(config.max_retries_ref(), 3);
        assert_eq!(config.max_records_per_batch_ref(), 128);
        assert_eq!(config.max_batch_bytes_ref(), 64 * 1024);
        assert_eq!(config.linger(), std::time::Duration::from_millis(5));
        assert_eq!(config.compression_ref(), Compression::Lz4);
        assert!(!config.idempotence_enabled());
        assert_eq!(config.client_config().client_id_ref(), Some("orders-api"));
        assert_eq!(
            config.client_config().security_protocol_ref(),
            SecurityProtocol::SaslTls
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
    }

    #[test]
    fn enabling_idempotence_sets_required_defaults() {
        let config = ProducerConfig::new(["localhost:9092"])
            .max_retries(0)
            .acks(Acks::Leader)
            .enable_idempotence(true);

        assert!(config.idempotence_enabled());
        assert_eq!(config.acks_ref(), Acks::All);
        assert_eq!(config.max_retries_ref(), 5);
    }

    #[tokio::test]
    async fn initializes_idempotent_producer_during_build() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_frame(&mut socket).await;
            assert_eq!(&request[0..4], &[0, 22, 0, 0]);
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, 0, 0, // throttle time
                    0, 0, // error code
                    0, 0, 0, 0, 0, 0, 0, 42, // producer id
                    0, 3, // producer epoch
                ],
            )
            .await;
        });

        let producer = ProducerConfig::new([addr.to_string()])
            .enable_idempotence(true)
            .build()
            .await
            .unwrap();
        let state = producer.idempotent_state.unwrap();

        assert_eq!(state.producer_id, 42);
        assert_eq!(state.producer_epoch, 3);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_idempotent_initialization_while_coordinator_loads() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let first = read_frame(&mut socket).await;
            assert_eq!(&first[0..4], &[0, 22, 0, 0]);
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 1, // correlation id
                    0, 0, 0, 0, // throttle time
                    0, 14, // coordinator load in progress
                    0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // producer id
                    0xff, 0xff, // producer epoch
                ],
            )
            .await;

            let second = read_frame(&mut socket).await;
            assert_eq!(&second[0..4], &[0, 22, 0, 0]);
            write_frame(
                &mut socket,
                &[
                    0, 0, 0, 2, // correlation id
                    0, 0, 0, 0, // throttle time
                    0, 0, // error code
                    0, 0, 0, 0, 0, 0, 0, 42, // producer id
                    0, 3, // producer epoch
                ],
            )
            .await;
        });

        let producer = ProducerConfig::new([addr.to_string()])
            .enable_idempotence(true)
            .build()
            .await
            .unwrap();

        assert_eq!(producer.idempotent_state.unwrap().producer_id, 42);
        server.await.unwrap();
    }

    #[test]
    fn clamps_zero_max_records_per_batch_to_one() {
        let config = ProducerConfig::new(["localhost:9092"]).max_records_per_batch(0);

        assert_eq!(config.max_records_per_batch_ref(), 1);
    }

    #[test]
    fn clamps_zero_max_batch_bytes_to_one() {
        let config = ProducerConfig::new(["localhost:9092"]).max_batch_bytes(0);

        assert_eq!(config.max_batch_bytes_ref(), 1);
    }

    #[test]
    fn leaves_buffered_records_pending_before_flush_thresholds() {
        let config = ProducerConfig::new(["localhost:9092"]).max_records_per_batch(2);
        let pending = vec![buffered_request(
            ProducerRecord::to("orders").key("order-1"),
        )];

        assert_eq!(
            buffered_enqueue_flush_reason(&pending, &config).unwrap(),
            None
        );
    }

    #[test]
    fn triggers_buffered_flush_at_record_limit() {
        let config = ProducerConfig::new(["localhost:9092"]).max_records_per_batch(2);
        let pending = vec![
            buffered_request(ProducerRecord::to("orders").key("order-1")),
            buffered_request(ProducerRecord::to("orders").key("order-2")),
        ];

        assert_eq!(
            buffered_enqueue_flush_reason(&pending, &config).unwrap(),
            Some(BufferedFlushReason::RecordCount)
        );
    }

    #[test]
    fn keeps_buffered_record_limit_partition_scoped() {
        let config = ProducerConfig::new(["localhost:9092"]).max_records_per_batch(2);
        let pending = vec![
            buffered_request(ProducerRecord::to("orders").partition(0).key("order-1")),
            buffered_request(ProducerRecord::to("orders").partition(1).key("order-2")),
        ];

        assert_eq!(
            buffered_enqueue_flush_reason(&pending, &config).unwrap(),
            None
        );
    }

    #[test]
    fn triggers_buffered_flush_at_byte_limit() {
        let config = ProducerConfig::new(["localhost:9092"]).max_batch_bytes(1);
        let pending = vec![buffered_request(
            ProducerRecord::to("orders").value("created"),
        )];

        assert_eq!(
            buffered_enqueue_flush_reason(&pending, &config).unwrap(),
            Some(BufferedFlushReason::ByteCount)
        );
    }

    #[test]
    fn computes_buffered_linger_deadline_from_first_record() {
        let first_enqueued_at = Instant::now();

        assert_eq!(
            buffered_linger_deadline(Some(first_enqueued_at), std::time::Duration::from_millis(5)),
            Some(first_enqueued_at + std::time::Duration::from_millis(5))
        );
        assert_eq!(
            buffered_linger_deadline(Some(first_enqueued_at), std::time::Duration::from_millis(0)),
            Some(first_enqueued_at)
        );
        assert_eq!(
            buffered_linger_deadline(None, std::time::Duration::from_millis(5)),
            None
        );
    }

    #[test]
    fn tracks_buffered_producer_lifecycle_state() {
        let mut state = BufferedProducerState::Open;

        assert!(state.ensure_open().is_ok());
        assert!(!state.is_closed());

        state.close();

        assert!(state.is_closed());
        assert!(matches!(
            state.ensure_open().unwrap_err(),
            Error::Unsupported("buffered producer is closed")
        ));

        state.close();

        assert!(state.is_closed());
    }

    #[tokio::test]
    async fn enqueues_buffered_record_and_returns_delivery_handle() {
        let (commands, mut receiver) = mpsc::channel(1);
        let delivery = enqueue_buffered_record(
            &commands,
            ProducerRecord::to("orders").key("order-1").value("created"),
        )
        .await
        .unwrap();

        let command = receiver.recv().await.unwrap();
        assert!(matches!(command, BufferedProducerCommand::Send(_)));
        if let BufferedProducerCommand::Send(request) = command {
            assert_eq!(request.record.topic(), "orders");
            assert_eq!(request.record.key_ref().unwrap(), b"order-1");
            request
                .delivery_sender
                .send(Ok(RecordMetadata::new("orders", 0, 42, None)))
                .unwrap();
        }

        let metadata = delivery.await.unwrap();
        assert_eq!(metadata.topic(), "orders");
        assert_eq!(metadata.partition(), 0);
        assert_eq!(metadata.offset(), 42);
    }

    #[tokio::test]
    async fn buffered_delivery_reports_canceled_sender() {
        let (delivery_sender, delivery_receiver) = oneshot::channel();
        let delivery = ProducerDelivery::new(delivery_receiver);
        drop(delivery_sender);

        assert!(matches!(
            delivery.await.unwrap_err(),
            Error::Unsupported("buffered producer delivery canceled")
        ));
    }

    #[tokio::test]
    async fn fails_pending_buffered_deliveries() {
        let (delivery_sender, delivery_receiver) = oneshot::channel();
        let delivery = ProducerDelivery::new(delivery_receiver);
        let mut pending = vec![BufferedProduceRequest {
            record: ProducerRecord::to("orders"),
            delivery_sender,
        }];

        fail_buffered_deliveries(&mut pending, buffered_delivery_canceled_error);

        assert!(pending.is_empty());
        assert!(matches!(
            delivery.await.unwrap_err(),
            Error::Unsupported("buffered producer delivery canceled")
        ));
    }

    #[tokio::test]
    async fn completes_buffered_deliveries_from_batch_outcomes() {
        let (first_sender, first_receiver) = oneshot::channel();
        let (second_sender, second_receiver) = oneshot::channel();
        let first_delivery = ProducerDelivery::new(first_receiver);
        let second_delivery = ProducerDelivery::new(second_receiver);
        let requests = vec![
            BufferedProduceRequest {
                record: ProducerRecord::to("orders").key("order-1"),
                delivery_sender: first_sender,
            },
            BufferedProduceRequest {
                record: ProducerRecord::to("orders").key("order-2"),
                delivery_sender: second_sender,
            },
        ];
        let outcomes = vec![
            ProducerBatchRecordOutcome::Success(RecordMetadata::new("orders", 0, 42, None)),
            ProducerBatchRecordOutcome::Failure(ProducerBatchFailure::new(
                1,
                "orders",
                0,
                Error::Broker {
                    code: 5,
                    context: "produce orders-0".to_owned(),
                },
            )),
        ];

        complete_buffered_deliveries(requests, outcomes);

        assert_eq!(first_delivery.await.unwrap().offset(), 42);
        assert!(matches!(
            second_delivery.await.unwrap_err(),
            Error::Broker { code: 5, context } if context == "produce orders-0"
        ));
    }

    #[tokio::test]
    async fn completes_missing_buffered_outcome_with_error() {
        let (delivery_sender, delivery_receiver) = oneshot::channel();
        let delivery = ProducerDelivery::new(delivery_receiver);
        let requests = vec![BufferedProduceRequest {
            record: ProducerRecord::to("orders"),
            delivery_sender,
        }];

        complete_buffered_deliveries(requests, Vec::new());

        assert!(matches!(
            delivery.await.unwrap_err(),
            Error::Unsupported("missing buffered delivery outcome")
        ));
    }

    #[test]
    fn copies_request_error_for_buffered_delivery() {
        let error = Error::Broker {
            code: 5,
            context: "produce orders-0".to_owned(),
        };

        assert!(matches!(
            delivery_error_from_request_error(&error),
            Error::Broker { code: 5, context } if context == "produce orders-0"
        ));
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
    fn builds_batch_success_outcomes_with_original_indexes() {
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders"));
        let batch = [first, second];
        let records = vec![
            PreparedBatchRecord {
                index: 3,
                record: &batch[0],
            },
            PreparedBatchRecord {
                index: 7,
                record: &batch[1],
            },
        ];
        let key = batch_key();

        let outcomes = batch_success_outcomes(&key, &records, 42);

        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].0, 3);
        assert_eq!(outcomes[1].0, 7);
        let first = outcomes[0].1.metadata().unwrap();
        assert_eq!(first.topic(), "orders");
        assert_eq!(first.partition(), 0);
        assert_eq!(first.offset(), 42);
        assert!(first.timestamp().is_some());
        let second = outcomes[1].1.metadata().unwrap();
        assert_eq!(second.offset(), 43);
    }

    #[test]
    fn builds_batch_failure_outcomes_with_partition_error() {
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders"));
        let batch = [first, second];
        let records = vec![
            PreparedBatchRecord {
                index: 3,
                record: &batch[0],
            },
            PreparedBatchRecord {
                index: 7,
                record: &batch[1],
            },
        ];
        let key = batch_key();

        let outcomes = batch_failure_outcomes(&key, &records, 5);

        assert_eq!(outcomes.len(), 2);
        assert_eq!(outcomes[0].0, 3);
        assert_eq!(outcomes[1].0, 7);
        let failure = outcomes[1].1.failure().unwrap();
        assert_eq!(failure.record_index(), 7);
        assert_eq!(failure.topic(), "orders");
        assert_eq!(failure.partition(), 0);
        assert!(matches!(
            failure.error(),
            Error::Broker { code: 5, context } if context == "produce orders-0"
        ));
    }

    #[test]
    fn chunks_batch_records_by_configured_record_limit() {
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders"));
        let third = BatchRecord::new(ProducerRecord::to("orders"));
        let batch = [first, second, third];
        let records = prepared_records(&batch);

        let chunks = batch_record_chunks(
            &records,
            2,
            usize::MAX,
            ProduceVersion::V3,
            Compression::None,
        )
        .unwrap();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 2);
        assert_eq!(chunks[0][0].index, 0);
        assert_eq!(chunks[0][1].index, 1);
        assert_eq!(chunks[1].len(), 1);
        assert_eq!(chunks[1][0].index, 2);
    }

    #[test]
    fn chunks_batch_records_with_minimum_size_one() {
        let first = BatchRecord::new(ProducerRecord::to("orders"));
        let second = BatchRecord::new(ProducerRecord::to("orders"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        let chunks = batch_record_chunks(
            &records,
            0,
            usize::MAX,
            ProduceVersion::V3,
            Compression::None,
        )
        .unwrap();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0][0].index, 0);
        assert_eq!(chunks[1][0].index, 1);
    }

    #[test]
    fn chunks_record_batches_by_configured_byte_limit() {
        let first = BatchRecord::new(ProducerRecord::to("orders").value("created"));
        let second = BatchRecord::new(ProducerRecord::to("orders").value("updated"));
        let third = BatchRecord::new(ProducerRecord::to("orders").value("shipped"));
        let batch = [first, second, third];
        let records = prepared_records(&batch);
        let one_record_len =
            batch_records_encoded_len(&records[0..1], ProduceVersion::V3, Compression::None)
                .unwrap();

        let chunks = batch_record_chunks(
            &records,
            usize::MAX,
            one_record_len,
            ProduceVersion::V3,
            Compression::None,
        )
        .unwrap();

        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0][0].index, 0);
        assert_eq!(chunks[1][0].index, 1);
        assert_eq!(chunks[2][0].index, 2);
    }

    #[test]
    fn keeps_oversized_record_as_single_chunk() {
        let first = BatchRecord::new(ProducerRecord::to("orders").value("created"));
        let second = BatchRecord::new(ProducerRecord::to("orders").value("updated"));
        let batch = [first, second];
        let records = prepared_records(&batch);

        let chunks = batch_record_chunks(
            &records,
            usize::MAX,
            1,
            ProduceVersion::V3,
            Compression::None,
        )
        .unwrap();

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].len(), 1);
        assert_eq!(chunks[1].len(), 1);
    }

    #[test]
    fn batch_report_exposes_record_failures() {
        let report = ProducerBatchReport::new(vec![
            ProducerBatchRecordOutcome::Success(RecordMetadata::new("orders", 0, 42, None)),
            ProducerBatchRecordOutcome::Failure(ProducerBatchFailure::new(
                1,
                "orders",
                0,
                Error::Broker {
                    code: 5,
                    context: "produce orders-0".to_owned(),
                },
            )),
        ]);

        assert!(report.has_failures());
        assert_eq!(report.records().len(), 2);
        assert!(report.records()[0].metadata().is_some());
        assert_eq!(report.records()[1].failure().unwrap().record_index(), 1);
        assert_eq!(report.into_records().len(), 2);
    }

    #[test]
    fn records_only_retryable_batch_failures_as_pending() {
        let mut output = empty_batch_outcomes(3);
        let attempt_outcomes = vec![
            (
                0,
                ProducerBatchRecordOutcome::Success(RecordMetadata::new("orders", 0, 42, None)),
            ),
            (1, retryable_batch_failure(1)),
            (
                2,
                ProducerBatchRecordOutcome::Failure(ProducerBatchFailure::new(
                    2,
                    "orders",
                    0,
                    Error::Unsupported("fatal batch failure"),
                )),
            ),
        ];

        let retry_indexes =
            record_batch_attempt_outcomes(&mut output, attempt_outcomes, 0, 1).unwrap();

        assert_eq!(retry_indexes, vec![1]);
        assert!(output[0].as_ref().unwrap().metadata().is_some());
        assert!(output[1].is_none());
        assert!(output[2].as_ref().unwrap().failure().is_some());
    }

    #[test]
    fn records_retryable_batch_failure_when_retries_are_exhausted() {
        let mut output = empty_batch_outcomes(1);
        let attempt_outcomes = vec![(0, retryable_batch_failure(0))];

        let retry_indexes =
            record_batch_attempt_outcomes(&mut output, attempt_outcomes, 1, 1).unwrap();

        assert!(retry_indexes.is_empty());
        let report = batch_report_from_outcomes(output).unwrap();
        assert!(report.has_failures());
        assert_eq!(report.records()[0].failure().unwrap().record_index(), 0);
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
        assert!(can_retry_send(&Error::UnknownTopicOrPartition {
            topic: "orders".to_owned(),
            partition: 3,
        }));
        assert!(can_retry_send(&Error::MissingLeader {
            topic: "orders".to_owned(),
            partition: 0,
        }));
        assert!(can_retry_send(&Error::MissingBroker { node_id: 2 }));
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
    fn invalidates_batch_record_topics_for_selected_indexes() {
        let mut cache = BTreeMap::new();
        cache.insert("orders".to_owned(), metadata_fixture());
        cache.insert("payments".to_owned(), metadata_fixture());
        cache.insert("shipments".to_owned(), metadata_fixture());
        let records = vec![
            BatchRecord::new(ProducerRecord::to("orders")),
            BatchRecord::new(ProducerRecord::to("payments")),
            BatchRecord::new(ProducerRecord::to("shipments")),
        ];

        invalidate_metadata_cache_for_record_indexes(&mut cache, &records, &[1]);

        assert!(cache.contains_key("orders"));
        assert!(!cache.contains_key("payments"));
        assert!(cache.contains_key("shipments"));
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
            Some("kafrust-producer-test".to_owned()),
            Some(std::time::Duration::from_millis(50)),
        );
        let config = ProducerConfig::new([addr.to_string()]).request_timeout_ms(500);
        let mut producer = Producer {
            client,
            config,
            metadata_cache: BTreeMap::new(),
            idempotent_state: None,
        };

        let metadata = producer.metadata_for_topic("orders").await.unwrap();

        assert_eq!(metadata.brokers[0].node_id, 1);
        assert_eq!(metadata.topics[0].name, "orders");
        assert!(producer.metadata_cache.contains_key("orders"));
        server.await.unwrap();
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

    fn batch_key() -> ProduceBatchKey {
        ProduceBatchKey {
            broker_addr: "localhost:9092".to_owned(),
            topic: "orders".to_owned(),
            partition: 0,
        }
    }

    fn empty_batch_outcomes(count: usize) -> Vec<Option<ProducerBatchRecordOutcome>> {
        std::iter::repeat_with(|| None).take(count).collect()
    }

    fn retryable_batch_failure(record_index: usize) -> ProducerBatchRecordOutcome {
        ProducerBatchRecordOutcome::Failure(ProducerBatchFailure::new(
            record_index,
            "orders",
            0,
            Error::Broker {
                code: 5,
                context: "produce orders-0".to_owned(),
            },
        ))
    }

    fn buffered_request(record: ProducerRecord) -> BufferedProduceRequest {
        let (delivery_sender, _delivery_receiver) = oneshot::channel();
        BufferedProduceRequest {
            record,
            delivery_sender,
        }
    }
}
