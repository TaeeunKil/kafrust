//! High-level Kafka Share Group consumer support from KIP-932.

use crate::client::Client;
use crate::config::{ClientConfig, OAuthBearerTokenProvider, SecurityProtocol};
use crate::consumer::ConsumerRecord;
use crate::error::{Error, Result};
use crate::metrics::ClientMetrics;
use kafrust_protocol::api::api_versions::ApiVersionsResponseV3;
use kafrust_protocol::api::metadata::{MetadataRequestTopicV12, MetadataResponseV12};
use kafrust_protocol::api::share::{
    ShareAcknowledgePartitionV1, ShareAcknowledgeTopicV1, ShareAcknowledgementBatchV1,
    ShareFetchPartitionV1, ShareFetchTopicV1, ShareForgottenTopicV1,
    ShareGroupHeartbeatAssignmentV1, ShareNodeEndpointV1,
};
use kafrust_protocol::api::{fetch::decode_message_set, share};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{oneshot, Mutex};
use tokio::task::JoinHandle;

type TopicId = [u8; 16];

/// Controls when records acquired from a share group are acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAcknowledgementMode {
    /// The application must acknowledge every record before the next poll.
    Explicit,
    /// Records returned by the previous poll are accepted before the next poll.
    Implicit,
}

/// Kafka acknowledgement state for one acquired share-group record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAcknowledgementType {
    /// Leave the record eligible for another delivery.
    Gap,
    /// Mark the record as successfully processed.
    Accept,
    /// Release the record for a later delivery attempt.
    Release,
    /// Reject the record permanently.
    Reject,
    /// Extend the broker acquisition lock without completing the record.
    Renew,
}

impl ShareAcknowledgementType {
    fn as_i8(self) -> i8 {
        match self {
            Self::Gap => 0,
            Self::Accept => 1,
            Self::Release => 2,
            Self::Reject => 3,
            Self::Renew => 4,
        }
    }
}

/// Controls how Kafka acquires records for a ShareFetch request.
///
/// `BatchOptimized` preserves the stable KIP-932 behavior and may return more
/// records than `max_records` to keep record batches intact. `RecordLimit`
/// uses KIP-1206 and requires ShareFetch v2 so the broker does not acquire
/// more than the configured limit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShareAcquireMode {
    /// Prefer complete record batches for throughput.
    BatchOptimized,
    /// Strictly limit the number of acquired records per poll.
    RecordLimit,
}

impl ShareAcquireMode {
    fn as_i8(self) -> i8 {
        match self {
            Self::BatchOptimized => 0,
            Self::RecordLimit => 1,
        }
    }
}

/// A record acquired from a Kafka share group.
///
/// The record remains acquired until the application acknowledges it and the
/// acknowledgement is committed by [`ShareConsumer::commit`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareRecord {
    record: ConsumerRecord,
    topic_id: TopicId,
    broker_id: i32,
    delivery_count: i16,
}

impl ShareRecord {
    /// Returns the underlying Kafka record.
    pub fn record(&self) -> &ConsumerRecord {
        &self.record
    }

    /// Consumes this wrapper and returns the underlying Kafka record.
    pub fn into_record(self) -> ConsumerRecord {
        self.record
    }

    /// Returns the Kafka topic name.
    pub fn topic(&self) -> &str {
        self.record.topic()
    }

    /// Returns the Kafka partition index.
    pub fn partition(&self) -> i32 {
        self.record.partition()
    }

    /// Returns the Kafka record offset.
    pub fn offset(&self) -> i64 {
        self.record.offset()
    }

    /// Returns the number of times Kafka has delivered this record.
    pub fn delivery_count(&self) -> i16 {
        self.delivery_count
    }

    /// Returns the record key bytes.
    pub fn key(&self) -> Option<&[u8]> {
        self.record.key()
    }

    /// Returns the nullable record value bytes.
    pub fn value(&self) -> Option<&[u8]> {
        self.record.value()
    }

    /// Returns the record headers in wire order.
    pub fn headers(&self) -> &[crate::consumer::ConsumerRecordHeader] {
        self.record.headers()
    }
}

/// Handle for a cancellable background ShareGroupHeartbeat task.
///
/// The task uses a dedicated coordinator connection so application fetch and
/// acknowledgement calls can continue while heartbeats are sent. Call
/// [`Self::try_wait`] from the poll loop to surface a terminal broker or
/// transport error, and call [`Self::stop`] before leaving the group.
#[derive(Debug)]
pub struct ShareConsumerHeartbeat {
    group_id: String,
    member_id: String,
    member_epoch: i32,
    interval: Duration,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<()>>>,
    state: Arc<Mutex<ShareHeartbeatState>>,
}

impl ShareConsumerHeartbeat {
    /// Returns the share group ID this task serves.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the member ID captured when this task was started.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the member epoch captured when this task was started.
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the initial heartbeat interval used by this task.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Returns whether the task has finished.
    pub fn is_finished(&self) -> bool {
        match self.handle.as_ref() {
            Some(handle) => handle.is_finished(),
            None => true,
        }
    }

    /// Checks for task completion without requesting shutdown.
    ///
    /// Returns `Ok(None)` while the task is running and `Ok(Some(()))` after a
    /// clean completion. A broker or transport failure is returned unchanged.
    pub async fn try_wait(&mut self) -> Result<Option<()>> {
        let Some(handle) = &self.handle else {
            return Ok(Some(()));
        };
        if !handle.is_finished() {
            return Ok(None);
        }
        let Some(handle) = self.handle.take() else {
            return Ok(Some(()));
        };
        handle.await?.map(Some)
    }

    /// Requests shutdown and waits until the heartbeat request has stopped.
    pub async fn stop(mut self) -> Result<()> {
        self.signal_shutdown();
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle.await?
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }

    async fn state_snapshot(&self) -> ShareHeartbeatState {
        self.state.lock().await.clone()
    }
}

impl Drop for ShareConsumerHeartbeat {
    fn drop(&mut self) {
        self.signal_shutdown();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

/// Configuration builder for [`ShareConsumer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareConsumerConfig {
    client: ClientConfig,
    group_id: String,
    topics: Vec<String>,
    max_wait_ms: i32,
    min_bytes: i32,
    max_bytes: i32,
    max_records: i32,
    batch_size: i32,
    max_retries: u32,
    acknowledgement_mode: ShareAcknowledgementMode,
    acquire_mode: ShareAcquireMode,
}

impl ShareConsumerConfig {
    /// Creates a share consumer configuration for a group and bootstrap set.
    pub fn new(
        bootstrap_servers: impl IntoIterator<Item = impl Into<String>>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            group_id: group_id.into(),
            topics: Vec::new(),
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 50 * 1024 * 1024,
            max_records: 500,
            batch_size: 100,
            max_retries: 1,
            acknowledgement_mode: ShareAcknowledgementMode::Explicit,
            acquire_mode: ShareAcquireMode::BatchOptimized,
        }
    }

    /// Adds a topic to the share-group subscription.
    pub fn subscribe(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    /// Replaces the share-group topic subscription.
    pub fn subscribe_topics(mut self, topics: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Sets the Kafka client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    /// Sets the rack ID advertised in share-group heartbeats.
    pub fn client_rack(mut self, client_rack: impl Into<String>) -> Self {
        self.client = self.client.client_rack(client_rack);
        self
    }

    /// Sets the request timeout in milliseconds.
    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(request_timeout_ms);
        self
    }

    /// Sets the maximum broker response size.
    pub fn max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.client = self.client.max_response_bytes(max_response_bytes);
        self
    }

    /// Sets the maximum number of decoded response array elements.
    pub fn max_decode_array_elements(mut self, max: usize) -> Self {
        self.client = self.client.max_decode_array_elements(max);
        self
    }

    /// Sets the maximum uncompressed record-batch size.
    pub fn max_decompressed_record_bytes(mut self, max: usize) -> Self {
        self.client = self.client.max_decompressed_record_bytes(max);
        self
    }

    /// Shares a metrics handle with every share consumer connection.
    pub fn metrics(mut self, metrics: ClientMetrics) -> Self {
        self.client = self.client.metrics(metrics);
        self
    }

    /// Sets the Kafka security protocol.
    pub fn security_protocol(mut self, protocol: SecurityProtocol) -> Self {
        self.client = self.client.security_protocol(protocol);
        self
    }

    /// Sets the TLS server name used for certificate validation.
    pub fn tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.client = self.client.tls_server_name(server_name);
        self
    }

    /// Adds a DER-encoded TLS root certificate.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_root_certificate_der(certificate);
        self
    }

    /// Sets SASL/PLAIN credentials.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.client = self.client.sasl_plain(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-256 credentials.
    pub fn sasl_scram_sha_256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_256(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-512 credentials.
    pub fn sasl_scram_sha_512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_512(username, password);
        self
    }

    /// Sets a static SASL/OAUTHBEARER token.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.client = self.client.sasl_oauthbearer(token);
        self
    }

    /// Sets a static SASL/OAUTHBEARER token with an authorization identity.
    pub fn sasl_oauthbearer_with_username(
        mut self,
        username: impl Into<String>,
        token: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_oauthbearer_with_username(username, token);
        self
    }

    /// Sets an async SASL/OAUTHBEARER token provider.
    pub fn sasl_oauthbearer_provider<P>(mut self, provider: P) -> Self
    where
        P: OAuthBearerTokenProvider + 'static,
    {
        self.client = self.client.sasl_oauthbearer_provider(provider);
        self
    }

    /// Sets an async SASL/OAUTHBEARER token provider with an authorization identity.
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

    /// Sets the fetch maximum wait time in milliseconds.
    pub fn max_wait_ms(mut self, max_wait_ms: i32) -> Self {
        self.max_wait_ms = max_wait_ms;
        self
    }

    /// Sets the minimum bytes requested from a share-partition leader.
    pub fn min_bytes(mut self, min_bytes: i32) -> Self {
        self.min_bytes = min_bytes;
        self
    }

    /// Sets the maximum bytes requested from a share-partition leader.
    pub fn max_bytes(mut self, max_bytes: i32) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Sets the maximum records requested by one poll from each broker.
    pub fn max_records(mut self, max_records: i32) -> Self {
        self.max_records = max_records;
        self
    }

    /// Sets the requested record batch size.
    pub fn batch_size(mut self, batch_size: i32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets the number of transient fetch or heartbeat retries.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets explicit or implicit acknowledgement behavior.
    pub fn acknowledgement_mode(mut self, mode: ShareAcknowledgementMode) -> Self {
        self.acknowledgement_mode = mode;
        self
    }

    /// Sets the KIP-1206 record acquisition mode.
    pub fn acquire_mode(mut self, mode: ShareAcquireMode) -> Self {
        self.acquire_mode = mode;
        self
    }

    /// Returns the configured group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the configured subscribed topics.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    /// Returns the configured KIP-1206 acquisition mode.
    pub fn acquire_mode_ref(&self) -> ShareAcquireMode {
        self.acquire_mode
    }

    /// Returns the shared low-level client configuration.
    pub fn client_config(&self) -> &ClientConfig {
        &self.client
    }

    /// Validates this configuration without opening a network connection.
    pub fn validate(&self) -> Result<()> {
        self.client.validate()?;
        if self.group_id.trim().is_empty() {
            return Err(Error::InvalidConfiguration {
                field: "group_id",
                reason: "must not be empty",
            });
        }
        if self.topics.is_empty() || self.topics.iter().any(|topic| topic.trim().is_empty()) {
            return Err(Error::InvalidConfiguration {
                field: "topics",
                reason: "must contain at least one non-empty topic",
            });
        }
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
        if self.max_bytes <= 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_bytes",
                reason: "must be greater than zero",
            });
        }
        if self.max_records <= 0 {
            return Err(Error::InvalidConfiguration {
                field: "max_records",
                reason: "must be greater than zero",
            });
        }
        if self.batch_size <= 0 {
            return Err(Error::InvalidConfiguration {
                field: "batch_size",
                reason: "must be greater than zero",
            });
        }
        Ok(())
    }

    /// Connects, joins the share group, and returns a ready consumer.
    pub async fn build(self) -> Result<ShareConsumer> {
        self.validate()?;
        let mut bootstrap = self.client.clone().connect().await?;
        let (share_fetch_version, share_acknowledge_version) =
            ensure_share_api_support(&mut bootstrap, self.acquire_mode).await?;
        let coordinator_addr = share_coordinator_address_with_retry(
            &mut bootstrap,
            &self.client,
            &self.group_id,
            self.max_retries,
        )
        .await?;
        let coordinator = self.client.connect_broker(coordinator_addr.clone()).await?;
        let mut consumer = ShareConsumer {
            config: self,
            bootstrap: Some(bootstrap),
            coordinator: Some(coordinator),
            coordinator_addr,
            broker_clients: BTreeMap::new(),
            broker_addresses: BTreeMap::new(),
            share_sessions: BTreeMap::new(),
            assignment: BTreeSet::new(),
            topic_names: BTreeMap::new(),
            partition_leaders: BTreeMap::new(),
            pending: BTreeMap::new(),
            renewed_records: BTreeMap::new(),
            member_id: new_share_member_id(),
            member_epoch: 0,
            next_heartbeat: Instant::now(),
            heartbeat_interval: Duration::from_millis(1),
            needs_assignment_heartbeat: true,
            heartbeat_task: None,
            share_fetch_version,
            share_acknowledge_version,
            acquisition_lock_timeout_ms: None,
            closed: false,
        };
        consumer.heartbeat_inner(true).await?;
        consumer.refresh_metadata().await?;
        Ok(consumer)
    }
}

/// A Kafka KIP-932 share-group consumer.
pub struct ShareConsumer {
    config: ShareConsumerConfig,
    bootstrap: Option<Client>,
    coordinator: Option<Client>,
    coordinator_addr: String,
    broker_clients: BTreeMap<i32, Client>,
    broker_addresses: BTreeMap<i32, String>,
    share_sessions: BTreeMap<i32, ShareSession>,
    assignment: BTreeSet<SharePartitionKey>,
    topic_names: BTreeMap<TopicId, String>,
    partition_leaders: BTreeMap<SharePartitionKey, i32>,
    pending: BTreeMap<ShareRecordKey, PendingRecord>,
    renewed_records: BTreeMap<ShareRecordKey, ShareRecord>,
    member_id: String,
    member_epoch: i32,
    next_heartbeat: Instant,
    heartbeat_interval: Duration,
    needs_assignment_heartbeat: bool,
    heartbeat_task: Option<ShareConsumerHeartbeat>,
    share_fetch_version: i16,
    share_acknowledge_version: i16,
    acquisition_lock_timeout_ms: Option<i32>,
    closed: bool,
}

impl ShareConsumer {
    /// Returns the share group ID.
    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    /// Returns the current share-group member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the current share-group member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.member_epoch
    }

    /// Returns the current assigned topic-partition count.
    pub fn assignment_count(&self) -> usize {
        self.assignment.len()
    }

    /// Returns the acquisition lock timeout from the most recent fetch or
    /// renewal response, when the broker supplied one.
    pub fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.acquisition_lock_timeout_ms
    }

    /// Sends a heartbeat immediately instead of waiting for the next poll.
    pub async fn heartbeat(&mut self) -> Result<()> {
        self.observe_heartbeat_task().await?;
        self.sync_heartbeat_state().await;
        if self.heartbeat_task.is_some() {
            return Ok(());
        }
        self.heartbeat_inner(true).await
    }

    /// Starts a detached, cancellable ShareGroupHeartbeat loop.
    ///
    /// The task uses a separate coordinator connection and updates the member
    /// ID, epoch, heartbeat interval, and assignment consumed by [`Self::poll`].
    /// The owning application must still call `poll`, `heartbeat`, or
    /// [`Self::try_wait_heartbeat_task`] to observe a terminal task error.
    pub async fn spawn_heartbeat_task(&mut self, interval: Duration) -> Result<()> {
        if interval.is_zero() {
            return Err(Error::InvalidConfiguration {
                field: "share heartbeat interval",
                reason: "must be greater than zero",
            });
        }
        self.observe_heartbeat_task().await?;
        if self.heartbeat_task.is_some() {
            return Err(Error::Unsupported(
                "share consumer heartbeat task is already running",
            ));
        }

        let state = Arc::new(Mutex::new(ShareHeartbeatState {
            member_id: self.member_id.clone(),
            member_epoch: self.member_epoch,
            assignment: self.assignment.clone(),
            heartbeat_interval: interval,
        }));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let group_id = self.config.group_id.clone();
        let topics = self.config.topics.clone();
        let rack_id = self.config.client.client_rack_ref().map(str::to_owned);
        let member_id = self.member_id.clone();
        let member_epoch = self.member_epoch;
        let client = self.config.client.clone();
        let max_retries = self.config.max_retries;
        let state_for_task = state.clone();
        let handle = tokio::spawn(async move {
            run_share_heartbeat(
                client,
                group_id,
                topics,
                rack_id,
                max_retries,
                state_for_task,
                shutdown_rx,
            )
            .await
        });
        let heartbeat = ShareConsumerHeartbeat {
            group_id: self.config.group_id.clone(),
            member_id,
            member_epoch,
            interval,
            shutdown: Some(shutdown),
            handle: Some(handle),
            state,
        };
        self.heartbeat_task = Some(heartbeat);
        Ok(())
    }

    /// Returns whether the detached heartbeat task is still running.
    pub fn heartbeat_task_is_finished(&self) -> bool {
        self.heartbeat_task
            .as_ref()
            .map_or(true, ShareConsumerHeartbeat::is_finished)
    }

    /// Checks the detached heartbeat task without requesting shutdown.
    pub async fn try_wait_heartbeat_task(&mut self) -> Result<Option<()>> {
        let Some(task) = self.heartbeat_task.as_mut() else {
            return Ok(Some(()));
        };
        if task.try_wait().await?.is_some() {
            self.heartbeat_task = None;
            Ok(Some(()))
        } else {
            Ok(None)
        }
    }

    /// Stops the detached heartbeat task and waits for it to finish.
    pub async fn stop_heartbeat_task(&mut self) -> Result<()> {
        let Some(task) = self.heartbeat_task.take() else {
            return Ok(());
        };
        task.stop().await
    }

    /// Polls the currently assigned share partitions.
    ///
    /// With explicit acknowledgements, every record from the previous poll
    /// must be acknowledged before this method is called again. With implicit
    /// acknowledgements, the previous batch is accepted automatically. Callers
    /// should poll frequently enough to keep the broker-provided heartbeat
    /// interval, or start [`Self::spawn_heartbeat_task`] for long processing
    /// gaps.
    pub async fn poll(&mut self) -> Result<Vec<ShareRecord>> {
        if self.closed {
            return Err(Error::Unsupported("share consumer is closed"));
        }
        self.observe_heartbeat_task().await?;
        self.sync_heartbeat_state().await;
        self.prepare_for_poll().await?;
        if self.heartbeat_task.is_none() {
            self.heartbeat_inner(false).await?;
        }
        if self.heartbeat_task.is_none()
            && self.assignment.is_empty()
            && self.needs_assignment_heartbeat
        {
            self.heartbeat_inner(true).await?;
        }
        if self.assignment.is_empty() {
            return Ok(Vec::new());
        }

        self.refresh_metadata().await?;
        let mut grouped = BTreeMap::<i32, BTreeSet<SharePartitionKey>>::new();
        for partition in &self.assignment {
            let leader = self
                .partition_leaders
                .get(partition)
                .copied()
                .ok_or_else(|| Error::MissingLeader {
                    topic: self
                        .topic_names
                        .get(&partition.topic_id)
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    partition: partition.partition,
                })?;
            grouped.entry(leader).or_default().insert(*partition);
        }

        let mut broker_ids = grouped.keys().copied().collect::<BTreeSet<_>>();
        broker_ids.extend(self.share_sessions.keys().copied());
        let mut records = self
            .renewed_records
            .values()
            .take(self.config.max_records as usize)
            .cloned()
            .collect::<Vec<_>>();
        for broker_id in broker_ids {
            let desired = grouped.remove(&broker_id).unwrap_or_default();
            let remaining = self
                .config
                .max_records
                .saturating_sub(i32::try_from(records.len()).unwrap_or(i32::MAX));
            if desired.is_empty() || remaining <= 0 {
                self.close_idle_session(broker_id, desired).await?;
                continue;
            }
            for record in self
                .fetch_from_broker(broker_id, desired, remaining)
                .await?
            {
                let key = ShareRecordKey::from_record(&record);
                if let Some(existing) = records
                    .iter_mut()
                    .find(|existing| ShareRecordKey::from_record(existing) == key)
                {
                    *existing = record;
                } else {
                    records.push(record);
                }
            }
            if records.len() >= self.config.max_records as usize {
                break;
            }
        }
        self.config.client.record_consumed(records.len());
        Ok(records)
    }

    /// Records a local acknowledgement for an acquired record.
    ///
    /// The broker request is sent by [`Self::commit`], allowing several record
    /// acknowledgements to be batched into one request per partition leader.
    pub fn acknowledge(
        &mut self,
        record: &ShareRecord,
        acknowledgement: ShareAcknowledgementType,
    ) -> Result<()> {
        if self.config.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            return Err(Error::Unsupported(
                "explicit share acknowledgements are disabled in implicit mode",
            ));
        }
        if acknowledgement == ShareAcknowledgementType::Renew && self.share_acknowledge_version < 2
        {
            return Err(Error::Unsupported(
                "ShareAcknowledge v2 is required for renewal acknowledgements",
            ));
        }
        let key = ShareRecordKey::from_record(record);
        let Some(pending) = self.pending.get_mut(&key) else {
            return Err(Error::ShareRecordNotPending {
                topic: record.topic().to_owned(),
                partition: record.partition(),
                offset: record.offset(),
            });
        };
        if pending.acknowledgement.is_some() {
            return Err(Error::ShareRecordAlreadyAcknowledged {
                topic: record.topic().to_owned(),
                partition: record.partition(),
                offset: record.offset(),
            });
        }
        pending.acknowledgement = Some(acknowledgement);
        if acknowledgement != ShareAcknowledgementType::Renew {
            self.renewed_records.remove(&key);
        }
        Ok(())
    }

    /// Sends all locally recorded acknowledgements to their share-partition leaders.
    ///
    /// If a transport failure occurs after a ShareAcknowledge request may have
    /// been transmitted, this returns [`Error::ShareAcknowledgementOutcomeUnknown`]
    /// and keeps the affected records pending. The caller must reconcile the
    /// broker-side state before deciding whether to replay the acknowledgement.
    pub async fn commit(&mut self) -> Result<()> {
        let unacknowledged = self
            .pending
            .iter()
            .filter(|(key, record)| {
                record.acknowledgement.is_none() && !self.renewed_records.contains_key(*key)
            })
            .count();
        if unacknowledged > 0 {
            return Err(Error::ShareAcknowledgementRequired {
                count: unacknowledged,
            });
        }
        self.commit_pending_acknowledgements(false).await
    }

    /// Closes share sessions and leaves the share group.
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.stop_heartbeat_task().await?;
        for pending in self.pending.values_mut() {
            if pending.acknowledgement.is_none() {
                pending.acknowledgement = Some(ShareAcknowledgementType::Release);
            }
        }
        self.commit_pending_acknowledgements(false).await?;

        let sessions = self
            .share_sessions
            .iter()
            .map(|(broker_id, session)| (*broker_id, session.clone()))
            .collect::<Vec<_>>();
        for (broker_id, session) in sessions {
            self.close_session(broker_id, session).await?;
        }
        self.share_sessions.clear();

        if !self.member_id.is_empty() && self.member_epoch >= 0 {
            let mut coordinator = self.coordinator.take().ok_or(Error::Unsupported(
                "share coordinator connection is unavailable",
            ))?;
            let response = coordinator
                .share_group_heartbeat_v1(
                    self.config.group_id.clone(),
                    self.member_id.clone(),
                    -1,
                    self.config.client.client_rack_ref().map(str::to_owned),
                    Some(self.config.topics.clone()),
                )
                .await?;
            self.coordinator = Some(coordinator);
            if response.error_code != 0 {
                return Err(self.config.client.broker_error(
                    response.error_code,
                    format!("leave share group {}", self.config.group_id),
                ));
            }
        }
        self.closed = true;
        Ok(())
    }

    async fn observe_heartbeat_task(&mut self) -> Result<()> {
        let Some(task) = self.heartbeat_task.as_mut() else {
            return Ok(());
        };
        if task.try_wait().await?.is_some() {
            self.heartbeat_task = None;
            return Err(Error::Unsupported("share consumer heartbeat task stopped"));
        }
        Ok(())
    }

    async fn sync_heartbeat_state(&mut self) {
        let Some(task) = self.heartbeat_task.as_ref() else {
            return;
        };
        let state = task.state_snapshot().await;
        self.member_id = state.member_id;
        self.member_epoch = state.member_epoch;
        self.heartbeat_interval = state.heartbeat_interval;
        self.assignment = state.assignment;
        self.needs_assignment_heartbeat = self.assignment.is_empty();
        self.next_heartbeat = Instant::now() + self.heartbeat_interval;
    }

    async fn prepare_for_poll(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if self.config.acknowledgement_mode == ShareAcknowledgementMode::Implicit {
            for pending in self.pending.values_mut() {
                if pending.acknowledgement.is_none() {
                    pending.acknowledgement = Some(ShareAcknowledgementType::Accept);
                }
            }
            self.commit_pending_acknowledgements(true).await
        } else {
            let count = self
                .pending
                .iter()
                .filter(|(key, record)| {
                    record.acknowledgement.is_none() && !self.renewed_records.contains_key(key)
                })
                .count();
            if count > 0 {
                return Err(Error::ShareAcknowledgementRequired { count });
            }
            self.commit_pending_acknowledgements(true).await
        }
    }

    async fn heartbeat_inner(&mut self, force: bool) -> Result<()> {
        if !force && !self.needs_assignment_heartbeat && Instant::now() < self.next_heartbeat {
            return Ok(());
        }
        let mut attempt = 0;
        loop {
            let mut coordinator = self.coordinator.take().ok_or(Error::Unsupported(
                "share coordinator connection is unavailable",
            ))?;
            let result = coordinator
                .share_group_heartbeat_v1(
                    self.config.group_id.clone(),
                    self.member_id.clone(),
                    self.member_epoch,
                    self.config.client.client_rack_ref().map(str::to_owned),
                    Some(self.config.topics.clone()),
                )
                .await;
            match result {
                Ok(response) if response.error_code == 0 => {
                    self.coordinator = Some(coordinator);
                    self.apply_heartbeat(response)?;
                    return Ok(());
                }
                Ok(response) => {
                    let error = self.config.client.broker_error(
                        response.error_code,
                        format!("share group heartbeat {}", self.config.group_id),
                    );
                    if attempt >= self.config.max_retries
                        || !is_retryable_share_error(response.error_code)
                    {
                        self.coordinator = Some(coordinator);
                        return Err(error);
                    }
                }
                Err(error) => {
                    if attempt >= self.config.max_retries {
                        return Err(error);
                    }
                }
            }
            attempt += 1;
            self.config.client.record_retry();
            tokio::time::sleep(share_retry_backoff(attempt)).await;
            self.reconnect_coordinator().await?;
        }
    }

    async fn reconnect_coordinator(&mut self) -> Result<()> {
        let mut attempt = 0;
        loop {
            match self.rediscover_coordinator().await {
                Ok(()) => return Ok(()),
                Err(error) if attempt < self.config.max_retries => {
                    attempt += 1;
                    self.config.client.record_retry();
                    tokio::time::sleep(share_retry_backoff(attempt)).await;
                    // A failed discovery is superseded by the next bounded attempt.
                    drop(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn rediscover_coordinator(&mut self) -> Result<()> {
        let mut bootstrap = self.config.client.clone().connect().await?;
        let coordinator_addr =
            share_coordinator_address(&mut bootstrap, &self.config.client, &self.config.group_id)
                .await?;
        let coordinator = self
            .config
            .client
            .connect_broker(coordinator_addr.clone())
            .await?;
        self.bootstrap = Some(bootstrap);
        self.coordinator_addr = coordinator_addr;
        self.coordinator = Some(coordinator);
        Ok(())
    }

    fn apply_heartbeat(&mut self, response: share::ShareGroupHeartbeatResponseV1) -> Result<()> {
        if let Some(member_id) = response.member_id {
            self.member_id = member_id;
        } else if self.member_id.is_empty() {
            return Err(Error::Unsupported(
                "share group heartbeat did not return a member ID",
            ));
        }
        self.member_epoch = response.member_epoch;
        let interval_ms = u64::try_from(response.heartbeat_interval_ms.max(1)).map_err(|_| {
            Error::InvalidConfiguration {
                field: "heartbeat_interval_ms",
                reason: "broker returned an invalid heartbeat interval",
            }
        })?;
        self.heartbeat_interval = Duration::from_millis(interval_ms);
        self.next_heartbeat = Instant::now() + self.heartbeat_interval;
        if let Some(assignment) = response.assignment {
            self.apply_assignment(assignment);
        }
        self.needs_assignment_heartbeat = self.assignment.is_empty();
        Ok(())
    }

    fn apply_assignment(&mut self, assignment: ShareGroupHeartbeatAssignmentV1) {
        self.assignment.clear();
        for topic in assignment.topic_partitions {
            for partition in topic.partitions {
                self.assignment.insert(SharePartitionKey {
                    topic_id: topic.topic_id,
                    partition,
                });
            }
        }
    }

    async fn refresh_metadata(&mut self) -> Result<()> {
        let topics = self
            .config
            .topics
            .iter()
            .map(|name| MetadataRequestTopicV12 {
                topic_id: [0; 16],
                name: Some(name.clone()),
            })
            .collect::<Vec<_>>();
        let mut attempt = 0;
        loop {
            let mut bootstrap = self
                .bootstrap
                .take()
                .ok_or(Error::Unsupported("bootstrap connection is unavailable"))?;
            match bootstrap.metadata_v12(Some(topics.clone())).await {
                Ok(response) => {
                    self.bootstrap = Some(bootstrap);
                    self.apply_metadata(response)?;
                    return Ok(());
                }
                Err(error) if attempt < self.config.max_retries => {
                    attempt += 1;
                    self.config.client.record_retry();
                    self.bootstrap = Some(self.config.client.clone().connect().await?);
                    // The failed metadata request is superseded by the fresh connection.
                    drop(error);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn apply_metadata(&mut self, metadata: MetadataResponseV12) -> Result<()> {
        self.broker_addresses.clear();
        for broker in metadata.brokers {
            self.broker_addresses
                .insert(broker.node_id, format!("{}:{}", broker.host, broker.port));
        }
        self.topic_names.clear();
        self.partition_leaders.clear();
        for topic in metadata.topics {
            let name = topic.name.ok_or(Error::UnknownTopicOrPartition {
                topic: "<unnamed>".to_owned(),
                partition: -1,
            })?;
            if topic.error_code != 0 {
                return Err(self
                    .config
                    .client
                    .broker_error(topic.error_code, format!("metadata {name}")));
            }
            self.topic_names.insert(topic.topic_id, name.clone());
            for partition in topic.partitions {
                if partition.error_code != 0 {
                    return Err(self.config.client.broker_error(
                        partition.error_code,
                        format!("metadata {name}-{}", partition.partition_index),
                    ));
                }
                if partition.leader_id < 0 {
                    return Err(Error::MissingLeader {
                        topic: name.clone(),
                        partition: partition.partition_index,
                    });
                }
                self.partition_leaders.insert(
                    SharePartitionKey {
                        topic_id: topic.topic_id,
                        partition: partition.partition_index,
                    },
                    partition.leader_id,
                );
            }
        }
        Ok(())
    }

    fn apply_share_partition_leaders(&mut self, response: &share::ShareFetchResponseV1) {
        for topic in &response.responses {
            for partition in &topic.partitions {
                if partition.current_leader.leader_id >= 0 {
                    self.partition_leaders.insert(
                        SharePartitionKey {
                            topic_id: topic.topic_id,
                            partition: partition.partition_index,
                        },
                        partition.current_leader.leader_id,
                    );
                }
            }
        }
    }

    fn refreshed_share_fetch_broker(&self, desired: &BTreeSet<SharePartitionKey>) -> Result<i32> {
        let mut leaders = BTreeSet::new();
        for partition in desired {
            let Some(leader_id) = self.partition_leaders.get(partition) else {
                return Err(Error::MissingLeader {
                    topic: self
                        .topic_names
                        .get(&partition.topic_id)
                        .cloned()
                        .unwrap_or_else(|| "<unknown>".to_owned()),
                    partition: partition.partition,
                });
            };
            leaders.insert(*leader_id);
        }
        if leaders.len() != 1 {
            return Err(Error::Unsupported(
                "share fetch retry split across partition leaders",
            ));
        }
        leaders.into_iter().next().ok_or(Error::Unsupported(
            "share fetch retry has no partition leader",
        ))
    }

    async fn fetch_from_broker(
        &mut self,
        broker_id: i32,
        desired: BTreeSet<SharePartitionKey>,
        max_records: i32,
    ) -> Result<Vec<ShareRecord>> {
        let mut broker_id = broker_id;
        let mut attempt = 0;
        loop {
            let session = self.share_sessions.get(&broker_id).cloned();
            let request_epoch = session.as_ref().map_or(0, |session| session.epoch);
            let active = session
                .as_ref()
                .map_or_else(BTreeSet::new, |session| session.partitions.clone());
            let topics = fetch_topics_for(&desired, &active);
            let forgotten_topics = forgotten_topics_for(&active, &desired);
            let mut broker = self.take_broker_client(broker_id).await?;
            let result = if self.share_fetch_version >= 2 {
                broker
                    .share_fetch_v2(
                        Some(self.config.group_id.clone()),
                        Some(self.member_id.clone()),
                        request_epoch,
                        self.config.max_wait_ms,
                        self.config.min_bytes,
                        self.config.max_bytes,
                        max_records,
                        self.config.batch_size,
                        self.config.acquire_mode.as_i8(),
                        topics,
                        forgotten_topics,
                    )
                    .await
            } else {
                broker
                    .share_fetch_v1(
                        Some(self.config.group_id.clone()),
                        Some(self.member_id.clone()),
                        request_epoch,
                        self.config.max_wait_ms,
                        self.config.min_bytes,
                        self.config.max_bytes,
                        max_records,
                        self.config.batch_size,
                        topics,
                        forgotten_topics,
                    )
                    .await
            };
            match result {
                Ok(response) if response.error_code == 0 => {
                    if let Some(error_code) = first_share_partition_error(&response) {
                        if attempt < self.config.max_retries && is_retryable_share_error(error_code)
                        {
                            attempt += 1;
                            self.config.client.record_retry();
                            self.broker_clients.insert(broker_id, broker);
                            self.share_sessions.remove(&broker_id);
                            self.refresh_metadata().await?;
                            broker_id = self.refreshed_share_fetch_broker(&desired)?;
                            continue;
                        }
                        self.broker_clients.insert(broker_id, broker);
                        return Err(self
                            .config
                            .client
                            .broker_error(error_code, format!("share fetch broker {broker_id}")));
                    }
                    if let Some(error_code) = first_share_fetch_acknowledgement_error(&response) {
                        self.broker_clients.insert(broker_id, broker);
                        return Err(self.config.client.broker_error(
                            error_code,
                            format!("share fetch acknowledgement broker {broker_id}"),
                        ));
                    }
                    self.acquisition_lock_timeout_ms = Some(response.acquisition_lock_timeout_ms);
                    self.update_share_endpoints(&response.node_endpoints);
                    self.apply_share_partition_leaders(&response);
                    let records = self.decode_share_records(&response)?;
                    let next_epoch = advance_share_epoch(request_epoch);
                    self.share_sessions.insert(
                        broker_id,
                        ShareSession {
                            epoch: next_epoch,
                            partitions: desired,
                        },
                    );
                    self.broker_clients.insert(broker_id, broker);
                    return Ok(records);
                }
                Ok(response) => {
                    if attempt < self.config.max_retries
                        && is_retryable_share_error(response.error_code)
                    {
                        attempt += 1;
                        self.config.client.record_retry();
                        self.broker_clients.insert(broker_id, broker);
                        self.share_sessions.remove(&broker_id);
                        self.refresh_metadata().await?;
                        broker_id = self.refreshed_share_fetch_broker(&desired)?;
                        continue;
                    }
                    self.broker_clients.insert(broker_id, broker);
                    return Err(self.config.client.broker_error(
                        response.error_code,
                        format!("share fetch broker {broker_id}"),
                    ));
                }
                Err(_error) if attempt < self.config.max_retries => {
                    attempt += 1;
                    self.config.client.record_retry();
                    self.share_sessions.remove(&broker_id);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn decode_share_records(
        &mut self,
        response: &share::ShareFetchResponseV1,
    ) -> Result<Vec<ShareRecord>> {
        let mut records = Vec::new();
        for topic_response in &response.responses {
            let topic_name = self
                .topic_names
                .get(&topic_response.topic_id)
                .cloned()
                .ok_or(Error::UnknownTopicOrPartition {
                    topic: format!("topic-id {:?}", topic_response.topic_id),
                    partition: -1,
                })?;
            for partition_response in &topic_response.partitions {
                self.partition_leaders.insert(
                    SharePartitionKey {
                        topic_id: topic_response.topic_id,
                        partition: partition_response.partition_index,
                    },
                    partition_response.current_leader.leader_id,
                );
                let Some(record_bytes) = partition_response.records.as_deref() else {
                    continue;
                };
                let decoded = decode_message_set(record_bytes, self.config.client.decode_limits())?;
                for message in decoded.into_iter().filter(|message| !message.control) {
                    let Some(acquired) = partition_response.acquired_records.iter().find(|range| {
                        range.first_offset <= message.offset && message.offset <= range.last_offset
                    }) else {
                        return Err(Error::ShareRecordNotAcquired {
                            topic: topic_name.clone(),
                            partition: partition_response.partition_index,
                            offset: message.offset,
                        });
                    };
                    let record = ConsumerRecord::from_message_set(
                        &topic_name,
                        partition_response.partition_index,
                        message,
                    );
                    let key = ShareRecordKey {
                        topic_id: topic_response.topic_id,
                        partition: partition_response.partition_index,
                        offset: record.offset(),
                    };
                    let share_record = ShareRecord {
                        record,
                        topic_id: topic_response.topic_id,
                        broker_id: partition_response.current_leader.leader_id,
                        delivery_count: acquired.delivery_count,
                    };
                    if self.renewed_records.remove(&key).is_some() {
                        let pending = self.pending.get_mut(&key).ok_or_else(|| {
                            Error::ShareRecordNotPending {
                                topic: topic_name.clone(),
                                partition: share_record.partition(),
                                offset: share_record.offset(),
                            }
                        })?;
                        pending.broker_id = partition_response.current_leader.leader_id;
                        pending.acknowledgement = None;
                        pending.record = share_record.clone();
                    } else {
                        match self.pending.entry(key) {
                            std::collections::btree_map::Entry::Vacant(entry) => {
                                entry.insert(PendingRecord {
                                    broker_id: partition_response.current_leader.leader_id,
                                    acknowledgement: None,
                                    record: share_record.clone(),
                                });
                            }
                            std::collections::btree_map::Entry::Occupied(_) => {
                                return Err(Error::ShareRecordAlreadyAcknowledged {
                                    topic: topic_name.clone(),
                                    partition: share_record.partition(),
                                    offset: share_record.offset(),
                                });
                            }
                        }
                    }
                    records.push(share_record);
                }
            }
        }
        Ok(records)
    }

    async fn commit_pending_acknowledgements(&mut self, renew_via_fetch: bool) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let mut grouped = BTreeMap::<i32, BTreeMap<SharePartitionKey, Vec<(i64, i8)>>>::new();
        for (key, pending) in &self.pending {
            let Some(acknowledgement) = pending.acknowledgement else {
                continue;
            };
            grouped
                .entry(pending.broker_id)
                .or_default()
                .entry(SharePartitionKey {
                    topic_id: key.topic_id,
                    partition: key.partition,
                })
                .or_default()
                .push((key.offset, acknowledgement.as_i8()));
        }

        for (broker_id, partitions) in grouped {
            let session_epoch = self
                .share_sessions
                .get(&broker_id)
                .map(|session| session.epoch)
                .ok_or(Error::Unsupported(
                    "share acknowledgement session is unavailable",
                ))?;
            let has_renew = partitions.values().flatten().any(|(_, acknowledgement)| {
                *acknowledgement == ShareAcknowledgementType::Renew.as_i8()
            });
            if has_renew && self.share_acknowledge_version < 2 {
                return Err(Error::Unsupported(
                    "ShareAcknowledge v2 is required for renewal acknowledgements",
                ));
            }
            let mut broker = self.take_broker_client(broker_id).await?;
            let response = if has_renew && renew_via_fetch && self.share_fetch_version >= 2 {
                let topics = acknowledgement_fetch_topics_for(partitions.clone());
                broker
                    .share_fetch_v2_with_renew(
                        Some(self.config.group_id.clone()),
                        Some(self.member_id.clone()),
                        session_epoch,
                        0,
                        0,
                        0,
                        0,
                        0,
                        self.config.acquire_mode.as_i8(),
                        true,
                        topics,
                        Vec::new(),
                    )
                    .await
                    .map(|response| {
                        let partition_error_code = first_share_partition_error(&response)
                            .or_else(|| first_share_fetch_acknowledgement_error(&response));
                        ShareAcknowledgementResponseSummary {
                            error_code: response.error_code,
                            node_endpoints: response.node_endpoints,
                            partition_error_code,
                            acquisition_lock_timeout_ms: Some(response.acquisition_lock_timeout_ms),
                        }
                    })
            } else if has_renew {
                let topics = acknowledgement_topics_for(partitions.clone());
                broker
                    .share_acknowledge_v2(
                        Some(self.config.group_id.clone()),
                        Some(self.member_id.clone()),
                        session_epoch,
                        true,
                        topics,
                    )
                    .await
                    .map(|response| ShareAcknowledgementResponseSummary {
                        error_code: response.error_code,
                        node_endpoints: response.node_endpoints,
                        partition_error_code: first_share_acknowledgement_error_in(
                            &response.responses,
                        ),
                        acquisition_lock_timeout_ms: Some(response.acquisition_lock_timeout_ms),
                    })
            } else {
                let topics = acknowledgement_topics_for(partitions.clone());
                broker
                    .share_acknowledge_v1(
                        Some(self.config.group_id.clone()),
                        Some(self.member_id.clone()),
                        session_epoch,
                        topics,
                    )
                    .await
                    .map(|response| ShareAcknowledgementResponseSummary {
                        error_code: response.error_code,
                        node_endpoints: response.node_endpoints,
                        partition_error_code: first_share_acknowledgement_error_in(
                            &response.responses,
                        ),
                        acquisition_lock_timeout_ms: None,
                    })
            };
            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    let error = share_acknowledgement_error(&broker, broker_id, error);
                    if !matches!(error, Error::ShareAcknowledgementOutcomeUnknown { .. }) {
                        self.broker_clients.insert(broker_id, broker);
                    }
                    return Err(error);
                }
            };
            if response.error_code != 0 {
                self.broker_clients.insert(broker_id, broker);
                return Err(self.config.client.broker_error(
                    response.error_code,
                    format!("share acknowledge broker {broker_id}"),
                ));
            }
            if let Some(error_code) = response.partition_error_code {
                self.broker_clients.insert(broker_id, broker);
                return Err(self
                    .config
                    .client
                    .broker_error(error_code, format!("share acknowledge broker {broker_id}")));
            }
            if let Some(timeout_ms) = response.acquisition_lock_timeout_ms {
                self.acquisition_lock_timeout_ms = Some(timeout_ms);
            }
            self.update_share_endpoints(&response.node_endpoints);
            if let Some(session) = self.share_sessions.get_mut(&broker_id) {
                session.epoch = advance_share_epoch(session_epoch);
            }
            self.broker_clients.insert(broker_id, broker);
            for (partition, offsets) in partitions {
                for (offset, acknowledgement_type) in offsets {
                    let key = ShareRecordKey {
                        topic_id: partition.topic_id,
                        partition: partition.partition,
                        offset,
                    };
                    if acknowledgement_type == ShareAcknowledgementType::Renew.as_i8() {
                        let record = self.pending.get_mut(&key).map(|pending| {
                            pending.acknowledgement = None;
                            pending.record.clone()
                        });
                        if let Some(record) = record {
                            self.renewed_records.insert(key, record);
                        }
                    } else {
                        self.pending.remove(&key);
                        self.renewed_records.remove(&key);
                    }
                }
            }
        }
        Ok(())
    }

    async fn close_idle_session(
        &mut self,
        broker_id: i32,
        desired: BTreeSet<SharePartitionKey>,
    ) -> Result<()> {
        if desired.is_empty() {
            if let Some(session) = self.share_sessions.remove(&broker_id) {
                self.close_session(broker_id, session).await?;
            }
        }
        Ok(())
    }

    async fn close_session(&mut self, broker_id: i32, _session: ShareSession) -> Result<()> {
        let mut broker = self.take_broker_client(broker_id).await?;
        let response = if self.share_fetch_version >= 2 {
            broker
                .share_fetch_v2(
                    Some(self.config.group_id.clone()),
                    Some(self.member_id.clone()),
                    -1,
                    self.config.max_wait_ms,
                    self.config.min_bytes,
                    self.config.max_bytes,
                    0,
                    self.config.batch_size,
                    self.config.acquire_mode.as_i8(),
                    Vec::new(),
                    Vec::new(),
                )
                .await?
        } else {
            broker
                .share_fetch_v1(
                    Some(self.config.group_id.clone()),
                    Some(self.member_id.clone()),
                    -1,
                    self.config.max_wait_ms,
                    self.config.min_bytes,
                    self.config.max_bytes,
                    0,
                    self.config.batch_size,
                    Vec::new(),
                    Vec::new(),
                )
                .await?
        };
        if response.error_code != 0 {
            self.broker_clients.insert(broker_id, broker);
            return Err(self.config.client.broker_error(
                response.error_code,
                format!("close share session broker {broker_id}"),
            ));
        }
        self.broker_clients.insert(broker_id, broker);
        Ok(())
    }

    async fn take_broker_client(&mut self, broker_id: i32) -> Result<Client> {
        if let Some(client) = self.broker_clients.remove(&broker_id) {
            return Ok(client);
        }
        let address = self
            .broker_addresses
            .get(&broker_id)
            .cloned()
            .ok_or(Error::MissingBroker { node_id: broker_id })?;
        self.config.client.connect_broker(address).await
    }

    fn update_share_endpoints(&mut self, endpoints: &[ShareNodeEndpointV1]) {
        for endpoint in endpoints {
            self.broker_addresses.insert(
                endpoint.node_id,
                format!("{}:{}", endpoint.host, endpoint.port),
            );
        }
    }
}

#[derive(Debug, Clone)]
struct ShareSession {
    epoch: i32,
    partitions: BTreeSet<SharePartitionKey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct SharePartitionKey {
    topic_id: TopicId,
    partition: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ShareRecordKey {
    topic_id: TopicId,
    partition: i32,
    offset: i64,
}

impl ShareRecordKey {
    fn from_record(record: &ShareRecord) -> Self {
        Self {
            topic_id: record.topic_id,
            partition: record.partition(),
            offset: record.offset(),
        }
    }
}

#[derive(Debug, Clone)]
struct PendingRecord {
    broker_id: i32,
    acknowledgement: Option<ShareAcknowledgementType>,
    record: ShareRecord,
}

struct ShareAcknowledgementResponseSummary {
    error_code: i16,
    node_endpoints: Vec<ShareNodeEndpointV1>,
    partition_error_code: Option<i16>,
    acquisition_lock_timeout_ms: Option<i32>,
}

#[derive(Debug, Clone)]
struct ShareHeartbeatState {
    member_id: String,
    member_epoch: i32,
    assignment: BTreeSet<SharePartitionKey>,
    heartbeat_interval: Duration,
}

struct ShareHeartbeatLoop {
    client: ClientConfig,
    group_id: String,
    topics: Vec<String>,
    rack_id: Option<String>,
    max_retries: u32,
    state: Arc<Mutex<ShareHeartbeatState>>,
    shutdown: oneshot::Receiver<()>,
}

async fn run_share_heartbeat(
    client: ClientConfig,
    group_id: String,
    topics: Vec<String>,
    rack_id: Option<String>,
    max_retries: u32,
    state: Arc<Mutex<ShareHeartbeatState>>,
    shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    let coordinator = connect_share_coordinator(&client, &group_id).await?;
    run_share_heartbeat_with_coordinator(
        ShareHeartbeatLoop {
            client,
            group_id,
            topics,
            rack_id,
            max_retries,
            state,
            shutdown,
        },
        coordinator,
    )
    .await
}

async fn run_share_heartbeat_with_coordinator(
    ShareHeartbeatLoop {
        client,
        group_id,
        topics,
        rack_id,
        max_retries,
        state,
        shutdown,
    }: ShareHeartbeatLoop,
    mut coordinator: Client,
) -> Result<()> {
    let mut shutdown = shutdown;
    let mut first_heartbeat = true;
    let mut attempt = 0;

    loop {
        if !first_heartbeat {
            let interval = state.lock().await.heartbeat_interval;
            tokio::select! {
                _ = &mut shutdown => return Ok(()),
                _ = tokio::time::sleep(interval) => {}
            }
        }
        first_heartbeat = false;

        let snapshot = state.lock().await.clone();
        let result = tokio::select! {
            _ = &mut shutdown => return Ok(()),
            response = coordinator.share_group_heartbeat_v1(
                group_id.clone(),
                snapshot.member_id,
                snapshot.member_epoch,
                rack_id.clone(),
                Some(topics.clone()),
            ) => response,
        };

        match result {
            Ok(response) if response.error_code == 0 => {
                apply_share_heartbeat_state(&state, response).await?;
                attempt = 0;
            }
            Ok(response) => {
                let error = client.broker_error(
                    response.error_code,
                    format!("background share heartbeat {group_id}"),
                );
                if attempt >= max_retries || !is_retryable_share_error(response.error_code) {
                    return Err(error);
                }
                attempt += 1;
                client.record_retry();
                coordinator =
                    reconnect_share_coordinator(&client, &group_id, max_retries, &mut shutdown)
                        .await?;
            }
            Err(error) => {
                if attempt >= max_retries {
                    return Err(error);
                }
                attempt += 1;
                client.record_retry();
                coordinator =
                    reconnect_share_coordinator(&client, &group_id, max_retries, &mut shutdown)
                        .await?;
            }
        }
    }
}

async fn apply_share_heartbeat_state(
    state: &Arc<Mutex<ShareHeartbeatState>>,
    response: share::ShareGroupHeartbeatResponseV1,
) -> Result<()> {
    let mut state = state.lock().await;
    if let Some(member_id) = response.member_id {
        state.member_id = member_id;
    } else if state.member_id.is_empty() {
        return Err(Error::Unsupported(
            "share heartbeat did not return a member ID",
        ));
    }
    state.member_epoch = response.member_epoch;
    let interval_ms = u64::try_from(response.heartbeat_interval_ms.max(1)).map_err(|_| {
        Error::InvalidConfiguration {
            field: "heartbeat_interval_ms",
            reason: "broker returned an invalid heartbeat interval",
        }
    })?;
    state.heartbeat_interval = Duration::from_millis(interval_ms);
    if let Some(assignment) = response.assignment {
        state.assignment = assignment
            .topic_partitions
            .into_iter()
            .flat_map(|topic| {
                topic
                    .partitions
                    .into_iter()
                    .map(move |partition| SharePartitionKey {
                        topic_id: topic.topic_id,
                        partition,
                    })
            })
            .collect();
    }
    Ok(())
}

async fn reconnect_share_coordinator(
    client: &ClientConfig,
    group_id: &str,
    max_retries: u32,
    shutdown: &mut oneshot::Receiver<()>,
) -> Result<Client> {
    let mut attempt = 0;
    loop {
        let result = tokio::select! {
            _ = &mut *shutdown => return Err(Error::Unsupported("share heartbeat task stopped")),
            result = connect_share_coordinator(client, group_id) => result,
        };
        match result {
            Ok(coordinator) => return Ok(coordinator),
            Err(_error) if attempt < max_retries => {
                attempt += 1;
                client.record_retry();
                tokio::select! {
                    _ = &mut *shutdown => return Err(Error::Unsupported("share heartbeat task stopped")),
                    _ = tokio::time::sleep(share_retry_backoff(attempt)) => {}
                }
            }
            Err(error) => return Err(error),
        }
    }
}

async fn connect_share_coordinator(client: &ClientConfig, group_id: &str) -> Result<Client> {
    let mut bootstrap = client.clone().connect().await?;
    let coordinator_addr = share_coordinator_address(&mut bootstrap, client, group_id).await?;
    client.connect_broker(coordinator_addr).await
}

async fn share_coordinator_address(
    bootstrap: &mut Client,
    client: &ClientConfig,
    group_id: &str,
) -> Result<String> {
    let response = bootstrap
        .find_group_coordinator(group_id.to_owned())
        .await?;
    if response.error_code != 0 {
        return Err(client.broker_error(
            response.error_code,
            format!("find share group coordinator {group_id}"),
        ));
    }
    Ok(format!("{}:{}", response.host, response.port))
}

async fn share_coordinator_address_with_retry(
    bootstrap: &mut Client,
    client: &ClientConfig,
    group_id: &str,
    max_retries: u32,
) -> Result<String> {
    let mut attempt = 0;
    loop {
        match share_coordinator_address(bootstrap, client, group_id).await {
            Ok(address) => return Ok(address),
            Err(error) if attempt < max_retries => {
                attempt += 1;
                client.record_retry();
                tokio::time::sleep(share_retry_backoff(attempt)).await;
                drop(error);
            }
            Err(error) => return Err(error),
        }
    }
}

fn share_retry_backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(5);
    Duration::from_millis(50_u64.saturating_mul(1_u64 << shift).min(1000))
}

fn new_share_member_id() -> String {
    let value = rand::random::<u128>();
    format!(
        "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
        (value >> 96) as u32,
        (value >> 80) as u16,
        (value >> 64) as u16,
        (value >> 48) as u16,
        value & 0x0000_ffff_ffff_ffff,
    )
}

async fn ensure_share_api_support(
    client: &mut Client,
    acquire_mode: ShareAcquireMode,
) -> Result<(i16, i16)> {
    let response = client
        .api_versions_v3_cached("kafrust", env!("CARGO_PKG_VERSION"))
        .await?;
    if response.error_code != 0 {
        return Err(Error::Broker {
            code: response.error_code,
            context: "share group ApiVersions".to_owned(),
        });
    }
    ensure_supported_share_api(&response, acquire_mode)
}

fn ensure_supported_share_api(
    response: &ApiVersionsResponseV3,
    acquire_mode: ShareAcquireMode,
) -> Result<(i16, i16)> {
    for (api_key, name) in [
        (
            share::SHARE_GROUP_HEARTBEAT_API_KEY,
            "ShareGroupHeartbeat v1",
        ),
        (share::SHARE_FETCH_API_KEY, "ShareFetch v1"),
        (share::SHARE_ACKNOWLEDGE_API_KEY, "ShareAcknowledge v1"),
    ] {
        if response.highest_supported_version(api_key, 1).is_none() {
            return Err(Error::Unsupported(name));
        }
    }
    let share_fetch_version = response
        .highest_supported_version(share::SHARE_FETCH_API_KEY, 2)
        .map_or(1, |version| version.min(2));
    if acquire_mode == ShareAcquireMode::RecordLimit && share_fetch_version < 2 {
        return Err(Error::Unsupported(
            "ShareFetch v2 is required for record-limit acquisition",
        ));
    }
    let share_acknowledge_version = response
        .highest_supported_version(share::SHARE_ACKNOWLEDGE_API_KEY, 2)
        .map_or(1, |version| version.min(2));
    Ok((share_fetch_version, share_acknowledge_version))
}

fn advance_share_epoch(epoch: i32) -> i32 {
    if epoch <= 0 || epoch == i32::MAX {
        1
    } else {
        epoch + 1
    }
}

fn fetch_topics_for(
    desired: &BTreeSet<SharePartitionKey>,
    active: &BTreeSet<SharePartitionKey>,
) -> Vec<ShareFetchTopicV1> {
    let mut grouped = BTreeMap::<TopicId, Vec<i32>>::new();
    for partition in desired.difference(active) {
        grouped
            .entry(partition.topic_id)
            .or_default()
            .push(partition.partition);
    }
    grouped
        .into_iter()
        .map(|(topic_id, partitions)| ShareFetchTopicV1 {
            topic_id,
            partitions: partitions
                .into_iter()
                .map(|partition_index| ShareFetchPartitionV1 {
                    partition_index,
                    acknowledgement_batches: Vec::new(),
                })
                .collect(),
        })
        .collect()
}

fn forgotten_topics_for(
    active: &BTreeSet<SharePartitionKey>,
    desired: &BTreeSet<SharePartitionKey>,
) -> Vec<ShareForgottenTopicV1> {
    let mut grouped = BTreeMap::<TopicId, Vec<i32>>::new();
    for partition in active.difference(desired) {
        grouped
            .entry(partition.topic_id)
            .or_default()
            .push(partition.partition);
    }
    grouped
        .into_iter()
        .map(|(topic_id, partitions)| ShareForgottenTopicV1 {
            topic_id,
            partitions,
        })
        .collect()
}

fn acknowledgement_topics_for(
    partitions: BTreeMap<SharePartitionKey, Vec<(i64, i8)>>,
) -> Vec<ShareAcknowledgeTopicV1> {
    let mut grouped = BTreeMap::<TopicId, Vec<ShareAcknowledgePartitionV1>>::new();
    for (partition, offsets) in partitions {
        grouped
            .entry(partition.topic_id)
            .or_default()
            .push(ShareAcknowledgePartitionV1 {
                partition_index: partition.partition,
                acknowledgement_batches: offsets
                    .into_iter()
                    .map(
                        |(offset, acknowledgement_type)| ShareAcknowledgementBatchV1 {
                            first_offset: offset,
                            last_offset: offset,
                            acknowledgement_types: vec![acknowledgement_type],
                        },
                    )
                    .collect(),
            });
    }
    grouped
        .into_iter()
        .map(|(topic_id, partitions)| ShareAcknowledgeTopicV1 {
            topic_id,
            partitions,
        })
        .collect()
}

fn acknowledgement_fetch_topics_for(
    partitions: BTreeMap<SharePartitionKey, Vec<(i64, i8)>>,
) -> Vec<ShareFetchTopicV1> {
    let mut grouped = BTreeMap::<TopicId, Vec<ShareFetchPartitionV1>>::new();
    for (partition, offsets) in partitions {
        grouped
            .entry(partition.topic_id)
            .or_default()
            .push(ShareFetchPartitionV1 {
                partition_index: partition.partition,
                acknowledgement_batches: offsets
                    .into_iter()
                    .map(
                        |(offset, acknowledgement_type)| ShareAcknowledgementBatchV1 {
                            first_offset: offset,
                            last_offset: offset,
                            acknowledgement_types: vec![acknowledgement_type],
                        },
                    )
                    .collect(),
            });
    }
    grouped
        .into_iter()
        .map(|(topic_id, partitions)| ShareFetchTopicV1 {
            topic_id,
            partitions,
        })
        .collect()
}

fn first_share_partition_error(response: &share::ShareFetchResponseV1) -> Option<i16> {
    response
        .responses
        .iter()
        .flat_map(|topic| topic.partitions.iter())
        .find_map(|partition| (partition.error_code != 0).then_some(partition.error_code))
}

fn first_share_fetch_acknowledgement_error(response: &share::ShareFetchResponseV1) -> Option<i16> {
    response
        .responses
        .iter()
        .flat_map(|topic| topic.partitions.iter())
        .find_map(|partition| {
            (partition.acknowledgement_error_code != 0)
                .then_some(partition.acknowledgement_error_code)
        })
}

fn first_share_acknowledgement_error_in(
    responses: &[share::ShareAcknowledgeTopicResponseV1],
) -> Option<i16> {
    responses
        .iter()
        .flat_map(|topic| topic.partitions.iter())
        .find_map(|partition| (partition.error_code != 0).then_some(partition.error_code))
}

fn is_retryable_share_error(code: i16) -> bool {
    matches!(
        code,
        5 | 6 | 7 | 9 | 14 | 15 | 16 | 27 | 121 | 122 | 123 | 124
    )
}

fn share_acknowledgement_error(client: &Client, broker_id: i32, error: Error) -> Error {
    if client.last_request_may_have_been_transmitted()
        && matches!(
            error,
            Error::Io(_)
                | Error::RequestTimedOut { .. }
                | Error::ResponseTooLarge { .. }
                | Error::Protocol(_)
        )
    {
        Error::ShareAcknowledgementOutcomeUnknown { broker_id }
    } else {
        error
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use kafrust_protocol::api::api_versions::ApiKeyVersion;
    use kafrust_protocol::api::share::{
        ShareAcquiredRecordsV1, ShareFetchPartitionResponseV1, ShareFetchResponseV1,
        ShareFetchTopicResponseV1, ShareLeaderIdAndEpochV1,
    };
    use kafrust_protocol::codec::{Decoder, Encoder};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn validates_share_consumer_configuration_without_connecting() {
        let config = ShareConsumerConfig::new(["localhost:9092"], "orders")
            .subscribe("orders")
            .max_records(10)
            .batch_size(2);
        assert!(config.validate().is_ok());
        assert!(ShareConsumerConfig::new(["localhost:9092"], "orders")
            .validate()
            .is_err());
    }

    #[test]
    fn generates_uuid_shaped_share_member_ids() {
        let member_id = new_share_member_id();
        assert_eq!(member_id.len(), 36);
        assert_eq!(member_id.as_bytes()[8], b'-');
        assert_eq!(member_id.as_bytes()[13], b'-');
        assert_eq!(member_id.as_bytes()[18], b'-');
        assert_eq!(member_id.as_bytes()[23], b'-');
    }

    #[test]
    fn groups_only_new_share_partitions() {
        let first = SharePartitionKey {
            topic_id: [1; 16],
            partition: 0,
        };
        let second = SharePartitionKey {
            topic_id: [1; 16],
            partition: 1,
        };
        let topics = fetch_topics_for(&BTreeSet::from([first, second]), &BTreeSet::from([first]));
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].partitions[0].partition_index, 1);
    }

    #[test]
    fn builds_one_acknowledgement_batch_per_record() {
        let topic_id = [7; 16];
        let topics = acknowledgement_topics_for(BTreeMap::from([(
            SharePartitionKey {
                topic_id,
                partition: 2,
            },
            vec![(10, ShareAcknowledgementType::Accept.as_i8())],
        )]));
        assert_eq!(topics[0].topic_id, topic_id);
        assert_eq!(
            topics[0].partitions[0].acknowledgement_batches[0].first_offset,
            10
        );
    }

    #[test]
    fn validates_share_api_versions() {
        let response = ApiVersionsResponseV3 {
            error_code: 0,
            api_keys: vec![
                ApiKeyVersion {
                    api_key: share::SHARE_GROUP_HEARTBEAT_API_KEY,
                    min_version: 1,
                    max_version: 1,
                },
                ApiKeyVersion {
                    api_key: share::SHARE_FETCH_API_KEY,
                    min_version: 1,
                    max_version: 1,
                },
                ApiKeyVersion {
                    api_key: share::SHARE_ACKNOWLEDGE_API_KEY,
                    min_version: 1,
                    max_version: 1,
                },
            ],
            throttle_time_ms: 0,
            tagged_fields: Vec::new(),
        };
        assert_eq!(
            ensure_supported_share_api(&response, ShareAcquireMode::BatchOptimized).unwrap(),
            (1, 1)
        );
        assert!(ensure_supported_share_api(&response, ShareAcquireMode::RecordLimit).is_err());

        let mut v2_response = response.clone();
        v2_response.api_keys[1].max_version = 2;
        assert_eq!(
            ensure_supported_share_api(&v2_response, ShareAcquireMode::RecordLimit).unwrap(),
            (2, 1)
        );
    }

    #[tokio::test]
    async fn discovers_share_coordinator_address_from_group_response() {
        let (stream, mut broker_stream) = tokio::io::duplex(4096);
        let broker_task = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 10);
            assert_eq!(i16::from_be_bytes([request[2], request[3]]), 1);
            let correlation_id =
                i32::from_be_bytes([request[8], request[9], request[10], request[11]]);

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_i16(0);
            body.write_nullable_string(None).unwrap();
            body.write_i32(7);
            body.write_string("share-coordinator").unwrap();
            body.write_i32(9094);

            let mut response = Encoder::new();
            response.write_i32(correlation_id);
            response.write_raw(&body.into_bytes());
            let response = response.into_bytes();
            broker_stream
                .write_all(&(response.len() as i32).to_be_bytes())
                .await
                .unwrap();
            broker_stream.write_all(&response).await.unwrap();
            broker_stream.flush().await.unwrap();
        });

        let mut bootstrap = Client::from_stream(
            Box::new(stream),
            Some("kafrust-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let address = share_coordinator_address(
            &mut bootstrap,
            &ClientConfig::new(["localhost:9092"]),
            "orders",
        )
        .await
        .unwrap();
        assert_eq!(address, "share-coordinator:9094");
        broker_task.await.unwrap();
    }

    #[tokio::test]
    async fn retries_share_coordinator_discovery_after_coordinator_not_available() {
        let (stream, mut broker_stream) = tokio::io::duplex(4096);
        let broker_task = tokio::spawn(async move {
            for (error_code, host, port) in [(15, "", 0), (0, "share-coordinator", 9094)] {
                let request = read_test_frame(&mut broker_stream).await;
                assert_eq!(i16::from_be_bytes([request[0], request[1]]), 10);
                assert_eq!(i16::from_be_bytes([request[2], request[3]]), 1);
                let correlation_id =
                    i32::from_be_bytes([request[8], request[9], request[10], request[11]]);

                let mut body = Encoder::new();
                body.write_i32(0);
                body.write_i16(error_code);
                body.write_nullable_string(None).unwrap();
                body.write_i32(7);
                body.write_string(host).unwrap();
                body.write_i32(port);

                let mut response = Encoder::new();
                response.write_i32(correlation_id);
                response.write_raw(&body.into_bytes());
                let response = response.into_bytes();
                broker_stream
                    .write_all(&(response.len() as i32).to_be_bytes())
                    .await
                    .unwrap();
                broker_stream.write_all(&response).await.unwrap();
                broker_stream.flush().await.unwrap();
            }
        });

        let mut bootstrap = Client::from_stream(
            Box::new(stream),
            Some("kafrust-test".to_owned()),
            Some(Duration::from_secs(1)),
        );
        let config = ClientConfig::new(["localhost:9092"]);
        let address = share_coordinator_address_with_retry(&mut bootstrap, &config, "orders", 1)
            .await
            .unwrap();

        assert_eq!(address, "share-coordinator:9094");
        broker_task.await.unwrap();
    }

    #[tokio::test]
    async fn classifies_lost_share_acknowledgement_response_as_unknown() {
        let (stream, _peer) = tokio::io::duplex(8192);
        let mut client = Client::from_stream(
            Box::new(stream),
            Some("kafrust-test".to_owned()),
            Some(Duration::from_millis(5)),
        );
        let error = client
            .share_acknowledge_v1(None, None, 0, Vec::new())
            .await
            .unwrap_err();
        assert!(matches!(&error, Error::RequestTimedOut { .. }));
        assert!(matches!(
            share_acknowledgement_error(&client, 3, error),
            Error::ShareAcknowledgementOutcomeUnknown { broker_id: 3 }
        ));
    }

    #[tokio::test]
    async fn decodes_share_records_and_tracks_acknowledgement_state() {
        let (stream, _peer) = tokio::io::duplex(1024);
        let config = ShareConsumerConfig::new(["localhost:9092"], "orders").subscribe("orders");
        let mut consumer = ShareConsumer {
            config,
            bootstrap: None,
            coordinator: None,
            coordinator_addr: "localhost:9092".to_owned(),
            broker_clients: BTreeMap::new(),
            broker_addresses: BTreeMap::new(),
            share_sessions: BTreeMap::new(),
            assignment: BTreeSet::new(),
            topic_names: BTreeMap::from([([5; 16], "orders".to_owned())]),
            partition_leaders: BTreeMap::new(),
            pending: BTreeMap::new(),
            renewed_records: BTreeMap::new(),
            member_id: "member-1".to_owned(),
            member_epoch: 1,
            next_heartbeat: Instant::now(),
            heartbeat_interval: Duration::from_secs(1),
            needs_assignment_heartbeat: false,
            heartbeat_task: None,
            share_fetch_version: 1,
            share_acknowledge_version: 1,
            acquisition_lock_timeout_ms: None,
            closed: false,
        };
        consumer.broker_clients.insert(
            1,
            Client::from_stream(Box::new(stream), None, Some(Duration::from_secs(1))),
        );

        let mut message = Encoder::new();
        message.write_i32(0);
        message.write_i8(1);
        message.write_i8(0);
        message.write_i64(1234);
        message.write_nullable_bytes(None).unwrap();
        message.write_nullable_bytes(Some(b"value")).unwrap();
        let message = message.into_bytes();
        let mut records = Encoder::new();
        records.write_i64(10);
        records.write_i32(i32::try_from(message.len()).unwrap());
        records.write_raw(&message);

        let response = ShareFetchResponseV1 {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            acquisition_lock_timeout_ms: 30_000,
            responses: vec![ShareFetchTopicResponseV1 {
                topic_id: [5; 16],
                partitions: vec![ShareFetchPartitionResponseV1 {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                    acknowledgement_error_code: 0,
                    acknowledgement_error_message: None,
                    current_leader: ShareLeaderIdAndEpochV1 {
                        leader_id: 1,
                        leader_epoch: 2,
                    },
                    records: Some(records.into_bytes()),
                    acquired_records: vec![ShareAcquiredRecordsV1 {
                        first_offset: 10,
                        last_offset: 10,
                        delivery_count: 2,
                    }],
                }],
            }],
            node_endpoints: Vec::new(),
        };

        consumer.apply_share_partition_leaders(&response);
        assert_eq!(
            consumer.partition_leaders[&SharePartitionKey {
                topic_id: [5; 16],
                partition: 0,
            }],
            1
        );
        assert_eq!(
            consumer
                .refreshed_share_fetch_broker(&BTreeSet::from([SharePartitionKey {
                    topic_id: [5; 16],
                    partition: 0,
                }]))
                .unwrap(),
            1
        );
        let decoded = consumer.decode_share_records(&response).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].topic(), "orders");
        assert_eq!(decoded[0].offset(), 10);
        assert_eq!(decoded[0].value(), Some(b"value".as_slice()));
        assert_eq!(decoded[0].delivery_count(), 2);
        let key = ShareRecordKey::from_record(&decoded[0]);
        consumer.renewed_records.insert(key, decoded[0].clone());
        consumer.pending.get_mut(&key).unwrap().acknowledgement = None;
        let mut redelivery = response.clone();
        redelivery.responses[0].partitions[0].acquired_records[0].delivery_count = 3;
        let redelivered = consumer.decode_share_records(&redelivery).unwrap();
        assert_eq!(redelivered.len(), 1);
        assert_eq!(redelivered[0].delivery_count(), 3);
        assert!(consumer.renewed_records.is_empty());
        assert_eq!(consumer.pending[&key].record.delivery_count(), 3);
        consumer
            .acknowledge(&redelivered[0], ShareAcknowledgementType::Accept)
            .unwrap();
        assert!(consumer
            .pending
            .values()
            .all(|pending| { pending.acknowledgement == Some(ShareAcknowledgementType::Accept) }));
    }

    #[tokio::test]
    async fn polls_and_acknowledges_through_share_wire_round_trip() {
        const TOPIC_ID: [u8; 16] = [5; 16];

        let (bootstrap_stream, mut bootstrap_peer) = tokio::io::duplex(8192);
        let (broker_stream, mut broker_peer) = tokio::io::duplex(8192);

        let bootstrap_task = tokio::spawn(async move {
            let request = read_test_frame(&mut bootstrap_peer).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 3);
            assert_eq!(i16::from_be_bytes([request[2], request[3]]), 12);

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_compact_array(Some(&[1_i32]), |encoder, _| {
                encoder.write_i32(1);
                encoder.write_compact_string("localhost")?;
                encoder.write_i32(9092);
                encoder.write_compact_nullable_string(None)?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
            body.write_compact_nullable_string(None).unwrap();
            body.write_i32(-1);
            body.write_compact_array(Some(&[0_i32]), |encoder, _| {
                encoder.write_i16(0);
                encoder.write_compact_nullable_string(Some("orders"))?;
                encoder.write_uuid(&TOPIC_ID);
                encoder.write_bool(false);
                encoder.write_compact_array(Some(&[0_i32]), |encoder, _| {
                    encoder.write_i16(0);
                    encoder.write_i32(0);
                    encoder.write_i32(1);
                    encoder.write_i32(0);
                    encoder.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))?;
                    encoder.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))?;
                    encoder.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_i32(0);
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
            body.write_empty_tagged_fields();
            write_response_frame(&mut bootstrap_peer, 1, body.into_bytes()).await;
        });

        let broker_task = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_peer).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 78);
            assert_eq!(i16::from_be_bytes([request[2], request[3]]), 1);

            let mut message = Encoder::new();
            message.write_i32(0);
            message.write_i8(1);
            message.write_i8(0);
            message.write_i64(1234);
            message.write_nullable_bytes(None).unwrap();
            message.write_nullable_bytes(Some(b"value")).unwrap();
            let message = message.into_bytes();
            let mut record_set = Encoder::new();
            record_set.write_i64(10);
            record_set.write_i32(i32::try_from(message.len()).unwrap());
            record_set.write_raw(&message);
            let record_set = record_set.into_bytes();

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_i16(0);
            body.write_compact_nullable_string(None).unwrap();
            body.write_i32(30_000);
            body.write_compact_array(Some(&[0_i32]), |encoder, _| {
                encoder.write_uuid(&TOPIC_ID);
                encoder.write_compact_array(Some(&[0_i32]), |encoder, _| {
                    encoder.write_i32(0);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i32(1);
                    encoder.write_i32(0);
                    encoder.write_empty_tagged_fields();
                    encoder.write_compact_nullable_bytes(Some(&record_set))?;
                    encoder.write_compact_array(Some(&[0_i32]), |encoder, _| {
                        encoder.write_i64(10);
                        encoder.write_i64(10);
                        encoder.write_i16(2);
                        encoder.write_empty_tagged_fields();
                        Ok(())
                    })?;
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
            body.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))
                .unwrap();
            body.write_empty_tagged_fields();
            write_response_frame(&mut broker_peer, 1, body.into_bytes()).await;

            let acknowledgement_request = read_test_frame(&mut broker_peer).await;
            let mut decoder = Decoder::new(&acknowledgement_request);
            assert_eq!(decoder.read_i16().unwrap(), 79);
            assert_eq!(decoder.read_i16().unwrap(), 1);
            assert_eq!(decoder.read_i32().unwrap(), 2);
            decoder.read_nullable_string().unwrap();
            decoder.read_tagged_fields().unwrap();
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("orders".to_owned())
            );
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("member-1".to_owned())
            );
            assert_eq!(decoder.read_i32().unwrap(), 1);
            let topics = decoder
                .read_compact_array("ack topics", |decoder| {
                    let topic_id = decoder.read_uuid()?;
                    let partitions = decoder.read_compact_array("ack partitions", |decoder| {
                        let partition = decoder.read_i32()?;
                        let batches = decoder.read_compact_array("ack batches", |decoder| {
                            let first = decoder.read_i64()?;
                            let last = decoder.read_i64()?;
                            let types = decoder
                                .read_compact_array("ack types", |decoder| decoder.read_i8())?
                                .unwrap_or_default();
                            decoder.read_tagged_fields()?;
                            Ok((first, last, types))
                        })?;
                        decoder.read_tagged_fields()?;
                        Ok((partition, batches.unwrap_or_default()))
                    })?;
                    decoder.read_tagged_fields()?;
                    Ok((topic_id, partitions.unwrap_or_default()))
                })
                .unwrap()
                .unwrap();
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].0, TOPIC_ID);
            assert_eq!(topics[0].1[0].0, 0);
            assert_eq!(topics[0].1[0].1[0].0, 10);
            assert_eq!(topics[0].1[0].1[0].1, 10);
            assert_eq!(topics[0].1[0].1[0].2, vec![1]);
            decoder.read_tagged_fields().unwrap();

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_i16(0);
            body.write_compact_nullable_string(None).unwrap();
            body.write_compact_array(Some(&[0_i32]), |encoder, _| {
                encoder.write_uuid(&TOPIC_ID);
                encoder.write_compact_array(Some(&[0_i32]), |encoder, _| {
                    encoder.write_i32(0);
                    encoder.write_i16(0);
                    encoder.write_compact_nullable_string(None)?;
                    encoder.write_i32(1);
                    encoder.write_i32(0);
                    encoder.write_empty_tagged_fields();
                    encoder.write_empty_tagged_fields();
                    Ok(())
                })?;
                encoder.write_empty_tagged_fields();
                Ok(())
            })
            .unwrap();
            body.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))
                .unwrap();
            body.write_empty_tagged_fields();
            write_response_frame(&mut broker_peer, 2, body.into_bytes()).await;
        });

        let config = ShareConsumerConfig::new(["localhost:9092"], "orders")
            .subscribe("orders")
            .max_records(10)
            .max_retries(0);
        let mut consumer = ShareConsumer {
            config,
            bootstrap: Some(Client::from_stream(
                Box::new(bootstrap_stream),
                Some("kafrust-test".to_owned()),
                Some(Duration::from_secs(1)),
            )),
            coordinator: None,
            coordinator_addr: "localhost:9092".to_owned(),
            broker_clients: BTreeMap::new(),
            broker_addresses: BTreeMap::from([(1, "localhost:9092".to_owned())]),
            share_sessions: BTreeMap::new(),
            assignment: BTreeSet::from([SharePartitionKey {
                topic_id: TOPIC_ID,
                partition: 0,
            }]),
            topic_names: BTreeMap::new(),
            partition_leaders: BTreeMap::new(),
            pending: BTreeMap::new(),
            renewed_records: BTreeMap::new(),
            member_id: "member-1".to_owned(),
            member_epoch: 1,
            next_heartbeat: Instant::now() + Duration::from_secs(60),
            heartbeat_interval: Duration::from_secs(60),
            needs_assignment_heartbeat: false,
            heartbeat_task: None,
            share_fetch_version: 1,
            share_acknowledge_version: 1,
            acquisition_lock_timeout_ms: None,
            closed: false,
        };
        consumer.broker_clients.insert(
            1,
            Client::from_stream(
                Box::new(broker_stream),
                Some("kafrust-test".to_owned()),
                Some(Duration::from_secs(1)),
            ),
        );

        let records = consumer.poll().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].topic(), "orders");
        assert_eq!(records[0].offset(), 10);
        assert_eq!(records[0].value(), Some(b"value".as_slice()));
        assert_eq!(records[0].delivery_count(), 2);
        consumer
            .acknowledge(&records[0], ShareAcknowledgementType::Accept)
            .unwrap();
        consumer.commit().await.unwrap();
        assert!(consumer.pending.is_empty());

        bootstrap_task.await.unwrap();
        broker_task.await.unwrap();
    }

    #[tokio::test]
    async fn renew_acknowledgement_uses_share_fetch_v2_on_poll_path() {
        const TOPIC_ID: [u8; 16] = [8; 16];

        let (client_stream, mut broker_stream) = tokio::io::duplex(8192);
        let broker_task = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            let mut decoder = Decoder::new(&request);
            assert_eq!(decoder.read_i16().unwrap(), 78);
            assert_eq!(decoder.read_i16().unwrap(), 2);
            assert_eq!(decoder.read_i32().unwrap(), 1);
            decoder.read_nullable_string().unwrap();
            decoder.read_tagged_fields().unwrap();
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("orders".to_owned())
            );
            assert_eq!(
                decoder.read_compact_nullable_string().unwrap(),
                Some("member-1".to_owned())
            );
            assert_eq!(decoder.read_i32().unwrap(), 4);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i32().unwrap(), 0);
            assert_eq!(decoder.read_i8().unwrap(), 0);
            assert!(decoder.read_bool().unwrap());
            let topics = decoder
                .read_compact_array("topics", |decoder| {
                    let topic_id = decoder.read_uuid()?;
                    let partitions = decoder.read_compact_array("partitions", |decoder| {
                        let partition = decoder.read_i32()?;
                        let batches = decoder.read_compact_array("batches", |decoder| {
                            let first = decoder.read_i64()?;
                            let last = decoder.read_i64()?;
                            let types = decoder
                                .read_compact_array("ack types", |decoder| decoder.read_i8())?
                                .unwrap_or_default();
                            decoder.read_tagged_fields()?;
                            Ok((first, last, types))
                        })?;
                        decoder.read_tagged_fields()?;
                        Ok((partition, batches.unwrap_or_default()))
                    })?;
                    decoder.read_tagged_fields()?;
                    Ok((topic_id, partitions.unwrap_or_default()))
                })
                .unwrap()
                .unwrap();
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0].0, TOPIC_ID);
            assert_eq!(topics[0].1[0].0, 0);
            assert_eq!(topics[0].1[0].1[0].0, 10);
            assert_eq!(topics[0].1[0].1[0].1, 10);
            assert_eq!(topics[0].1[0].1[0].2, vec![4]);
            assert!(decoder
                .read_compact_array("forgotten topics", |_decoder| {
                    Ok::<(), kafrust_protocol::Error>(())
                })
                .unwrap()
                .unwrap()
                .is_empty());
            assert_eq!(decoder.read_tagged_fields().unwrap(), Vec::new());

            let mut body = Encoder::new();
            body.write_i32(0);
            body.write_i16(0);
            body.write_compact_nullable_string(None).unwrap();
            body.write_i32(45_000);
            body.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))
                .unwrap();
            body.write_compact_array(Some(&[] as &[i32]), |_, _| Ok(()))
                .unwrap();
            body.write_empty_tagged_fields();
            write_response_frame(&mut broker_stream, 1, body.into_bytes()).await;
        });

        let config = ShareConsumerConfig::new(["localhost:9092"], "orders")
            .subscribe("orders")
            .max_retries(0);
        let mut consumer = ShareConsumer {
            config,
            bootstrap: None,
            coordinator: None,
            coordinator_addr: "localhost:9092".to_owned(),
            broker_clients: BTreeMap::new(),
            broker_addresses: BTreeMap::from([(1, "localhost:9092".to_owned())]),
            share_sessions: BTreeMap::from([(
                1,
                ShareSession {
                    epoch: 4,
                    partitions: BTreeSet::from([SharePartitionKey {
                        topic_id: TOPIC_ID,
                        partition: 0,
                    }]),
                },
            )]),
            assignment: BTreeSet::from([SharePartitionKey {
                topic_id: TOPIC_ID,
                partition: 0,
            }]),
            topic_names: BTreeMap::from([(TOPIC_ID, "orders".to_owned())]),
            partition_leaders: BTreeMap::from([(
                SharePartitionKey {
                    topic_id: TOPIC_ID,
                    partition: 0,
                },
                1,
            )]),
            pending: BTreeMap::new(),
            renewed_records: BTreeMap::new(),
            member_id: "member-1".to_owned(),
            member_epoch: 1,
            next_heartbeat: Instant::now(),
            heartbeat_interval: Duration::from_secs(60),
            needs_assignment_heartbeat: false,
            heartbeat_task: None,
            share_fetch_version: 2,
            share_acknowledge_version: 2,
            acquisition_lock_timeout_ms: None,
            closed: false,
        };

        let mut message = Encoder::new();
        message.write_i32(0);
        message.write_i8(1);
        message.write_i8(0);
        message.write_i64(1234);
        message.write_nullable_bytes(None).unwrap();
        message.write_nullable_bytes(Some(b"value")).unwrap();
        let message = message.into_bytes();
        let mut records = Encoder::new();
        records.write_i64(10);
        records.write_i32(i32::try_from(message.len()).unwrap());
        records.write_raw(&message);
        let response = ShareFetchResponseV1 {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            acquisition_lock_timeout_ms: 30_000,
            responses: vec![ShareFetchTopicResponseV1 {
                topic_id: TOPIC_ID,
                partitions: vec![ShareFetchPartitionResponseV1 {
                    partition_index: 0,
                    error_code: 0,
                    error_message: None,
                    acknowledgement_error_code: 0,
                    acknowledgement_error_message: None,
                    current_leader: ShareLeaderIdAndEpochV1 {
                        leader_id: 1,
                        leader_epoch: 2,
                    },
                    records: Some(records.into_bytes()),
                    acquired_records: vec![ShareAcquiredRecordsV1 {
                        first_offset: 10,
                        last_offset: 10,
                        delivery_count: 1,
                    }],
                }],
            }],
            node_endpoints: Vec::new(),
        };
        let records = consumer.decode_share_records(&response).unwrap();
        consumer
            .acknowledge(&records[0], ShareAcknowledgementType::Renew)
            .unwrap();
        consumer.broker_clients.insert(
            1,
            Client::from_stream(
                Box::new(client_stream),
                Some("kafrust-test".to_owned()),
                Some(Duration::from_secs(1)),
            ),
        );

        consumer
            .commit_pending_acknowledgements(true)
            .await
            .unwrap();
        assert!(consumer
            .pending
            .values()
            .all(|pending| { pending.acknowledgement.is_none() }));
        assert!(consumer.renewed_records.contains_key(&ShareRecordKey {
            topic_id: TOPIC_ID,
            partition: 0,
            offset: 10,
        }));
        assert_eq!(consumer.share_sessions[&1].epoch, 5);
        assert_eq!(consumer.acquisition_lock_timeout_ms(), Some(45_000));
        consumer.commit().await.unwrap();
        broker_task.await.unwrap();
    }

    #[tokio::test]
    async fn detached_share_heartbeat_cancels_inflight_request() {
        let (client_stream, mut broker_stream) = tokio::io::duplex(8192);
        let (seen_tx, seen_rx) = oneshot::channel();
        let broker_task = tokio::spawn(async move {
            let request = read_test_frame(&mut broker_stream).await;
            assert_eq!(i16::from_be_bytes([request[0], request[1]]), 76);
            assert_eq!(i16::from_be_bytes([request[2], request[3]]), 1);
            seen_tx.send(()).unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        let state = Arc::new(Mutex::new(ShareHeartbeatState {
            member_id: "member-1".to_owned(),
            member_epoch: 1,
            assignment: BTreeSet::new(),
            heartbeat_interval: Duration::from_secs(60),
        }));
        let (shutdown, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(run_share_heartbeat_with_coordinator(
            ShareHeartbeatLoop {
                client: ClientConfig::new(["localhost:9092"]),
                group_id: "orders".to_owned(),
                topics: vec!["orders".to_owned()],
                rack_id: None,
                max_retries: 0,
                state,
                shutdown: shutdown_rx,
            },
            Client::from_stream(
                Box::new(client_stream),
                Some("kafrust-test".to_owned()),
                Some(Duration::from_secs(5)),
            ),
        ));

        seen_rx.await.unwrap();
        shutdown.send(()).unwrap();
        let result = tokio::time::timeout(Duration::from_millis(200), task)
            .await
            .unwrap()
            .unwrap();
        assert!(result.is_ok(), "heartbeat task failed to stop: {result:?}");

        broker_task.abort();
        assert!(broker_task.await.is_err());
    }

    async fn read_test_frame(stream: &mut tokio::io::DuplexStream) -> Vec<u8> {
        let mut size = [0u8; 4];
        stream.read_exact(&mut size).await.unwrap();
        let size = usize::try_from(i32::from_be_bytes(size)).unwrap();
        let mut frame = vec![0u8; size];
        stream.read_exact(&mut frame).await.unwrap();
        frame
    }

    async fn write_response_frame(
        stream: &mut tokio::io::DuplexStream,
        correlation_id: i32,
        body: Vec<u8>,
    ) {
        let mut response = Encoder::new();
        response.write_i32(correlation_id);
        response.write_empty_tagged_fields();
        response.write_raw(&body);
        let response = response.into_bytes();
        stream
            .write_all(&(response.len() as i32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(&response).await.unwrap();
        stream.flush().await.unwrap();
    }
}
