use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kafrust_protocol::api::find_coordinator::FindCoordinatorResponseV1;
use kafrust_protocol::api::join_group::JoinGroupMember;
use kafrust_protocol::api::metadata::MetadataResponseV1;
use kafrust_protocol::api::offset_commit::{
    OffsetCommitPartition, OffsetCommitPartitionV7, OffsetCommitTopic, OffsetCommitTopicResponse,
    OffsetCommitTopicV7,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchPartitionResponse, OffsetFetchTopic, OffsetFetchTopicResponse,
};
use kafrust_protocol::api::sync_group::SyncGroupAssignment;
use kafrust_protocol::consumer_group::{
    ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0, ConsumerProtocolTopicAssignment,
};

use crate::client::Client;
use crate::config::{ClientConfig, SecurityProtocol};
use crate::consumer::{
    Consumer, ConsumerAssignment, ConsumerConfig, ConsumerRecord, IsolationLevel,
};
use crate::error::{BrokerErrorKind, Error, Result};
use crate::metrics::ClientMetrics;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{self, MissedTickBehavior};
use tracing::{debug, Instrument};

const PROTOCOL_TYPE: &str = "consumer";
const RANGE_PROTOCOL: &str = "range";
const ROUND_ROBIN_PROTOCOL: &str = "roundrobin";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Partition assignment strategy advertised by a classic consumer group member.
pub enum ConsumerGroupAssignmentStrategy {
    /// Assign contiguous partition ranges independently for each topic.
    #[default]
    Range,
    /// Distribute all subscribed topic partitions cyclically across members.
    RoundRobin,
}

impl ConsumerGroupAssignmentStrategy {
    fn protocol_name(self) -> &'static str {
        match self {
            Self::Range => RANGE_PROTOCOL,
            Self::RoundRobin => ROUND_ROBIN_PROTOCOL,
        }
    }
}

struct JoinedGroup {
    error_code: i16,
    generation_id: i32,
    protocol_name: String,
    leader: String,
    member_id: String,
    members: Vec<JoinGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Configuration builder for the classic Kafka consumer group alpha API.
pub struct ConsumerGroupConfig {
    client: ClientConfig,
    group_id: String,
    group_instance_id: Option<String>,
    topics: Vec<String>,
    session_timeout_ms: i32,
    rebalance_timeout_ms: i32,
    retention_time_ms: i64,
    start_offset: i64,
    max_wait_ms: i32,
    min_bytes: i32,
    max_partition_bytes: i32,
    max_retries: u32,
    max_poll_records: usize,
    isolation_level: IsolationLevel,
    assignment_strategy: ConsumerGroupAssignmentStrategy,
}

impl ConsumerGroupConfig {
    /// Creates a consumer group configuration for a Kafka group ID.
    pub fn new(
        bootstrap_servers: impl IntoIterator<Item = impl Into<String>>,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            client: ClientConfig::new(bootstrap_servers),
            group_id: group_id.into(),
            group_instance_id: None,
            topics: Vec::new(),
            session_timeout_ms: 10_000,
            rebalance_timeout_ms: 30_000,
            retention_time_ms: 86_400_000,
            start_offset: 0,
            max_wait_ms: 500,
            min_bytes: 1,
            max_partition_bytes: 1_048_576,
            max_retries: 1,
            max_poll_records: 500,
            isolation_level: IsolationLevel::ReadUncommitted,
            assignment_strategy: ConsumerGroupAssignmentStrategy::Range,
        }
    }

    /// Sets the Kafka client ID used by group and fetch requests.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client = self.client.client_id(client_id);
        self
    }

    /// Sets the request timeout in milliseconds.
    pub fn request_timeout_ms(mut self, request_timeout_ms: u64) -> Self {
        self.client = self.client.request_timeout_ms(request_timeout_ms);
        self
    }

    /// Sets the maximum broker response payload allocated for one group or fetch request.
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

    /// Sets the shared metrics handle used by group and fetch broker connections.
    pub fn metrics(mut self, metrics: ClientMetrics) -> Self {
        self.client = self.client.metrics(metrics);
        self
    }

    /// Sets the Kafka security protocol used for group, coordinator, and fetch requests.
    pub fn security_protocol(mut self, security_protocol: SecurityProtocol) -> Self {
        self.client = self.client.security_protocol(security_protocol);
        self
    }

    /// Sets the TLS server name used for group, coordinator, and fetch certificate validation.
    pub fn tls_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.client = self.client.tls_server_name(server_name);
        self
    }

    /// Adds a DER-encoded TLS root certificate for group, coordinator, and fetch validation.
    pub fn tls_root_certificate_der(mut self, certificate: impl Into<Vec<u8>>) -> Self {
        self.client = self.client.tls_root_certificate_der(certificate);
        self
    }

    /// Sets SASL/PLAIN credentials for group, coordinator, and fetch requests.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.client = self.client.sasl_plain(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-256 credentials for group, coordinator, and fetch requests.
    pub fn sasl_scram_sha_256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_256(username, password);
        self
    }

    /// Sets SASL/SCRAM-SHA-512 credentials for group, coordinator, and fetch requests.
    pub fn sasl_scram_sha_512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.client = self.client.sasl_scram_sha_512(username, password);
        self
    }

    /// Subscribes this group member to a Kafka topic.
    pub fn subscribe(mut self, topic: impl Into<String>) -> Self {
        self.topics.push(topic.into());
        self
    }

    /// Sets a stable identity for this consumer group member.
    ///
    /// Instance IDs must be unique among concurrently running members of the
    /// same group. Kafka fences an older member when another member joins with
    /// the same ID.
    pub fn group_instance_id(mut self, group_instance_id: impl Into<String>) -> Self {
        self.group_instance_id = Some(group_instance_id.into());
        self
    }

    /// Returns the configured static group instance ID.
    pub fn group_instance_id_ref(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Sets the Kafka group session timeout in milliseconds.
    pub fn session_timeout_ms(mut self, session_timeout_ms: i32) -> Self {
        self.session_timeout_ms = session_timeout_ms;
        self
    }

    /// Sets the Kafka group rebalance timeout in milliseconds.
    pub fn rebalance_timeout_ms(mut self, rebalance_timeout_ms: i32) -> Self {
        self.rebalance_timeout_ms = rebalance_timeout_ms;
        self
    }

    /// Sets the offset retention time used by offset commits.
    pub fn retention_time_ms(mut self, retention_time_ms: i64) -> Self {
        self.retention_time_ms = retention_time_ms;
        self
    }

    /// Sets the fallback start offset when no committed offset exists.
    pub fn start_offset(mut self, start_offset: i64) -> Self {
        self.start_offset = start_offset;
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

    /// Sets the maximum number of records returned by one group poll.
    pub fn max_poll_records(mut self, max_poll_records: usize) -> Self {
        self.max_poll_records = max_poll_records;
        self
    }

    /// Sets whether group fetches expose records from aborted transactions.
    pub fn isolation_level(mut self, isolation_level: IsolationLevel) -> Self {
        self.isolation_level = isolation_level;
        self
    }

    /// Sets the classic consumer group partition assignment strategy.
    pub fn assignment_strategy(
        mut self,
        assignment_strategy: ConsumerGroupAssignmentStrategy,
    ) -> Self {
        self.assignment_strategy = assignment_strategy;
        self
    }

    /// Returns the configured partition assignment strategy.
    pub fn assignment_strategy_ref(&self) -> ConsumerGroupAssignmentStrategy {
        self.assignment_strategy
    }

    /// Returns the configured transaction isolation level.
    pub fn isolation_level_ref(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Returns the Kafka consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the subscribed topic names.
    pub fn topics(&self) -> &[String] {
        &self.topics
    }

    fn consumer_config(&self) -> ConsumerConfig {
        ConsumerConfig::from_client_config(self.client.clone())
            .max_wait_ms(self.max_wait_ms)
            .min_bytes(self.min_bytes)
            .max_partition_bytes(self.max_partition_bytes)
            .max_retries(self.max_retries)
            .max_poll_records(self.max_poll_records)
            .isolation_level(self.isolation_level)
    }

    /// Joins the group, syncs assignment, and builds a group consumer.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.join",
        skip_all,
        fields(group_id = self.group_id.as_str(), topic_count = self.topics.len()),
        err
    )]
    pub async fn join(self) -> Result<ConsumerGroup> {
        if self.topics.is_empty() {
            return Err(Error::Unsupported("consumer group without subscriptions"));
        }
        if self.group_instance_id.as_deref() == Some("") {
            return Err(Error::InvalidGroupInstanceId);
        }
        debug!(
            group_id = self.group_id.as_str(),
            topic_count = self.topics.len(),
            "joining kafka consumer group"
        );
        let config = self.clone();

        let mut bootstrap = self.client.clone().connect().await?;
        let coordinator = bootstrap
            .find_group_coordinator(self.group_id.clone())
            .await?;
        if coordinator.error_code != 0 {
            return Err(self.client.broker_error(
                coordinator.error_code,
                format!("find group coordinator {}", self.group_id),
            ));
        }

        let coordinator_addr = coordinator_addr(&coordinator);
        let mut coordinator_client = self.client.connect_broker(coordinator_addr).await?;
        let subscription = ConsumerProtocolSubscriptionV0 {
            topics: self.topics.clone(),
            user_data: None,
        }
        .encode()?;
        let protocols = vec![kafrust_protocol::api::join_group::JoinGroupProtocol {
            name: self.assignment_strategy.protocol_name().to_owned(),
            metadata: subscription,
        }];
        let joined = if let Some(group_instance_id) = &self.group_instance_id {
            let response = coordinator_client
                .join_group_v5(
                    self.group_id.clone(),
                    self.session_timeout_ms,
                    self.rebalance_timeout_ms,
                    "",
                    Some(group_instance_id.clone()),
                    PROTOCOL_TYPE,
                    protocols,
                )
                .await?;
            JoinedGroup {
                error_code: response.error_code,
                generation_id: response.generation_id,
                protocol_name: response.protocol_name,
                leader: response.leader,
                member_id: response.member_id,
                members: response
                    .members
                    .into_iter()
                    .map(|member| JoinGroupMember {
                        member_id: member.member_id,
                        metadata: member.metadata,
                    })
                    .collect(),
            }
        } else {
            let response = coordinator_client
                .join_group_v2(
                    self.group_id.clone(),
                    self.session_timeout_ms,
                    self.rebalance_timeout_ms,
                    "",
                    PROTOCOL_TYPE,
                    protocols,
                )
                .await?;
            JoinedGroup {
                error_code: response.error_code,
                generation_id: response.generation_id,
                protocol_name: response.protocol_name,
                leader: response.leader,
                member_id: response.member_id,
                members: response.members,
            }
        };
        if joined.error_code != 0 {
            return Err(self
                .client
                .broker_error(joined.error_code, format!("join group {}", self.group_id)));
        }

        let assignments = if joined.member_id == joined.leader {
            let metadata = bootstrap.metadata(Some(self.topics.clone())).await?;
            assignments_for_strategy(&joined.protocol_name, &joined.members, &metadata)?
        } else {
            Vec::new()
        };
        let synced = if let Some(group_instance_id) = &self.group_instance_id {
            coordinator_client
                .sync_group_v3(
                    self.group_id.clone(),
                    joined.generation_id,
                    joined.member_id.clone(),
                    Some(group_instance_id.clone()),
                    assignments,
                )
                .await?
        } else {
            coordinator_client
                .sync_group_v2(
                    self.group_id.clone(),
                    joined.generation_id,
                    joined.member_id.clone(),
                    assignments,
                )
                .await?
        };
        if synced.error_code != 0 {
            return Err(self
                .client
                .broker_error(synced.error_code, format!("sync group {}", self.group_id)));
        }

        let assignment = ConsumerProtocolAssignmentV0::decode(&synced.assignment)?;
        let consumer_assignments = assignments_from_protocol(
            &mut coordinator_client,
            &self.group_id,
            self.start_offset,
            &assignment,
        )
        .await?;
        let consumer_config = self.consumer_config();
        let consumer_client = self.client.clone().connect().await?;

        debug!(
            group_id = self.group_id.as_str(),
            member_id = joined.member_id.as_str(),
            generation_id = joined.generation_id,
            assignment_count = consumer_assignments.len(),
            "joined kafka consumer group"
        );

        Ok(ConsumerGroup {
            config,
            group_id: self.group_id,
            generation_id: joined.generation_id,
            member_id: joined.member_id,
            retention_time_ms: self.retention_time_ms,
            coordinator: coordinator_client,
            consumer: Consumer::from_assignments(
                consumer_client,
                consumer_config,
                consumer_assignments,
            ),
        })
    }
}

#[derive(Debug)]
/// Joined Kafka consumer group member.
pub struct ConsumerGroup {
    config: ConsumerGroupConfig,
    group_id: String,
    generation_id: i32,
    member_id: String,
    retention_time_ms: i64,
    coordinator: Client,
    consumer: Consumer,
}

/// Consumer group identity used to fence transactional offset commits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupMetadata {
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
}

impl ConsumerGroupMetadata {
    /// Returns the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the current classic group generation ID.
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Returns the broker-assigned member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the static group instance ID when configured.
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }
}

impl ConsumerGroup {
    /// Returns the Kafka consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the broker-assigned group member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the current group generation ID.
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Snapshots the current identity for a fenced transactional offset commit.
    ///
    /// Take a fresh snapshot after every rejoin because generation and member
    /// identity change during rebalancing.
    pub fn metadata(&self) -> ConsumerGroupMetadata {
        ConsumerGroupMetadata {
            group_id: self.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            group_instance_id: self.config.group_instance_id.clone(),
        }
    }

    /// Returns the assigned topic partitions and next offsets.
    pub fn assignments(&self) -> &[ConsumerAssignment] {
        self.consumer.assignments()
    }

    /// Sends a heartbeat, polls assigned partitions, and advances in-memory offsets.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.poll",
        skip_all,
        fields(group_id = self.group_id.as_str(), member_id = self.member_id.as_str(), generation_id = self.generation_id),
        err
    )]
    pub async fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        match self.heartbeat().await {
            Ok(()) => {}
            Err(error) if should_rejoin_group(&error) => {
                debug!(
                    group_id = self.group_id.as_str(),
                    member_id = self.member_id.as_str(),
                    generation_id = self.generation_id,
                    error = %error,
                    "rejoining kafka consumer group after heartbeat"
                );
                self.config.client.record_retry();
                self.rejoin().await?
            }
            Err(error) => return Err(error),
        }
        self.consumer.poll().await
    }

    /// Checks a background heartbeat task before polling assigned partitions.
    ///
    /// If the background heartbeat task has completed with a group error that
    /// requires a rejoin, this method rejoins the group before polling. Other
    /// background heartbeat errors are returned to the caller. When the task is
    /// still running, this behaves like [`ConsumerGroup::poll`].
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.poll_with_heartbeat",
        skip_all,
        fields(group_id = self.group_id.as_str(), member_id = self.member_id.as_str(), generation_id = self.generation_id),
        err
    )]
    pub async fn poll_with_heartbeat(
        &mut self,
        heartbeat: &mut ConsumerGroupHeartbeat,
    ) -> Result<Vec<ConsumerRecord>> {
        match heartbeat.state_for(&self.group_id, &self.member_id, self.generation_id) {
            HeartbeatHandleState::Current => {
                if should_rejoin_after_background_heartbeat(heartbeat).await? {
                    debug!(
                        group_id = self.group_id.as_str(),
                        member_id = self.member_id.as_str(),
                        generation_id = self.generation_id,
                        "rejoining kafka consumer group after background heartbeat"
                    );
                    self.config.client.record_retry();
                    self.rejoin().await?;
                }
            }
            HeartbeatHandleState::StaleGeneration => {
                debug!(
                    group_id = self.group_id.as_str(),
                    member_id = self.member_id.as_str(),
                    generation_id = self.generation_id,
                    heartbeat_member_id = heartbeat.member_id(),
                    heartbeat_generation_id = heartbeat.generation_id(),
                    "stopping stale kafka consumer group heartbeat task"
                );
                heartbeat.stop_stale_generation().await?;
            }
            HeartbeatHandleState::DifferentGroup => {
                return Err(Error::Unsupported(
                    "background heartbeat handle belongs to a different consumer group",
                ));
            }
        }
        self.poll().await
    }

    /// Starts a background heartbeat task for this joined group member.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.spawn_heartbeat",
        skip_all,
        fields(group_id = self.group_id.as_str(), member_id = self.member_id.as_str(), generation_id = self.generation_id, interval_ms = duration_millis(interval)),
        err
    )]
    pub async fn spawn_heartbeat_task(&self, interval: Duration) -> Result<ConsumerGroupHeartbeat> {
        validate_heartbeat_interval(interval)?;
        debug!(
            group_id = self.group_id.as_str(),
            member_id = self.member_id.as_str(),
            generation_id = self.generation_id,
            interval_ms = duration_millis(interval),
            "starting kafka consumer group heartbeat task"
        );

        let mut bootstrap = self.config.client.clone().connect().await?;
        let coordinator = bootstrap
            .find_group_coordinator(self.group_id.clone())
            .await?;
        if coordinator.error_code != 0 {
            return Err(self.config.client.broker_error(
                coordinator.error_code,
                format!("find group coordinator {}", self.group_id),
            ));
        }

        let mut coordinator = self
            .config
            .client
            .connect_broker(coordinator_addr(&coordinator))
            .await?;
        let group_id = self.group_id.clone();
        let generation_id = self.generation_id;
        let member_id = self.member_id.clone();
        let group_instance_id = self.config.group_instance_id.clone();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let heartbeat_span = tracing::debug_span!(
            "kafka.consumer_group.background_heartbeat",
            group_id = group_id.as_str(),
            member_id = member_id.as_str(),
            generation_id,
            interval_ms = duration_millis(interval),
        );
        let handle = tokio::spawn(
            async move {
                run_background_heartbeat(
                    &mut coordinator,
                    group_id,
                    generation_id,
                    member_id,
                    group_instance_id,
                    interval,
                    shutdown_rx,
                )
                .await
            }
            .instrument(heartbeat_span),
        );

        Ok(ConsumerGroupHeartbeat {
            group_id: self.group_id.clone(),
            generation_id: self.generation_id,
            member_id: self.member_id.clone(),
            shutdown: Some(shutdown),
            handle: Some(handle),
        })
    }

    async fn rejoin(&mut self) -> Result<()> {
        let joined = self.config.clone().join().await?;
        *self = joined;
        Ok(())
    }

    /// Sends an explicit heartbeat for this group member.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.heartbeat",
        skip_all,
        fields(group_id = self.group_id.as_str(), member_id = self.member_id.as_str(), generation_id = self.generation_id),
        err
    )]
    pub async fn heartbeat(&mut self) -> Result<()> {
        debug!(
            group_id = self.group_id.as_str(),
            member_id = self.member_id.as_str(),
            generation_id = self.generation_id,
            "sending kafka consumer group heartbeat"
        );
        let response = if let Some(group_instance_id) = &self.config.group_instance_id {
            self.coordinator
                .heartbeat_v3(
                    self.group_id.clone(),
                    self.generation_id,
                    self.member_id.clone(),
                    Some(group_instance_id.clone()),
                )
                .await?
        } else {
            self.coordinator
                .heartbeat_v2(
                    self.group_id.clone(),
                    self.generation_id,
                    self.member_id.clone(),
                )
                .await?
        };
        if response.error_code != 0 {
            return Err(self.config.client.broker_error(
                response.error_code,
                format!("heartbeat group {}", self.group_id),
            ));
        }
        debug!(
            group_id = self.group_id.as_str(),
            member_id = self.member_id.as_str(),
            generation_id = self.generation_id,
            "sent kafka consumer group heartbeat"
        );
        Ok(())
    }

    /// Commits the current assignment offsets to the group coordinator.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.commit_offsets",
        skip_all,
        fields(group_id = self.group_id.as_str(), member_id = self.member_id.as_str(), generation_id = self.generation_id, assignment_count = self.consumer.assignments().len()),
        err
    )]
    pub async fn commit_offsets(&mut self) -> Result<()> {
        debug!(
            group_id = self.group_id.as_str(),
            member_id = self.member_id.as_str(),
            generation_id = self.generation_id,
            topic_count = offset_commit_topics(self.consumer.assignments()).len(),
            "committing kafka consumer group offsets"
        );
        let response_topics = match if let Some(group_instance_id) = &self.config.group_instance_id
        {
            self.coordinator
                .offset_commit_v7(
                    self.group_id.clone(),
                    self.generation_id,
                    self.member_id.clone(),
                    Some(group_instance_id.clone()),
                    offset_commit_topics_v7(self.consumer.assignments()),
                )
                .await
                .map(|response| response.topics)
        } else {
            self.coordinator
                .offset_commit_v2(
                    self.group_id.clone(),
                    self.generation_id,
                    self.member_id.clone(),
                    self.retention_time_ms,
                    offset_commit_topics(self.consumer.assignments()),
                )
                .await
                .map(|response| response.topics)
        } {
            Ok(topics) => topics,
            Err(error) if should_rejoin_group(&error) => {
                debug!(
                    group_id = self.group_id.as_str(),
                    member_id = self.member_id.as_str(),
                    generation_id = self.generation_id,
                    error = %error,
                    "rejoining kafka consumer group after offset commit request"
                );
                self.config.client.record_retry();
                self.rejoin().await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        if let Some(error) = offset_commit_response_error(&self.group_id, &response_topics) {
            let error = match error {
                Error::Broker { code, context } => self.config.client.broker_error(code, context),
                error => error,
            };
            if should_rejoin_group(&error) {
                debug!(
                    group_id = self.group_id.as_str(),
                    member_id = self.member_id.as_str(),
                    generation_id = self.generation_id,
                    error = %error,
                    "rejoining kafka consumer group after offset commit"
                );
                self.config.client.record_retry();
                self.rejoin().await?;
            }
            return Err(error);
        }
        debug!(
            group_id = self.group_id.as_str(),
            member_id = self.member_id.as_str(),
            generation_id = self.generation_id,
            "committed kafka consumer group offsets"
        );
        Ok(())
    }
}

/// Handle for a background consumer group heartbeat task.
#[derive(Debug)]
pub struct ConsumerGroupHeartbeat {
    group_id: String,
    generation_id: i32,
    member_id: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<Result<()>>>,
}

impl ConsumerGroupHeartbeat {
    /// Returns the Kafka consumer group ID this heartbeat task was created for.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the group generation ID this heartbeat task was created for.
    pub fn generation_id(&self) -> i32 {
        self.generation_id
    }

    /// Returns the broker-assigned member ID this heartbeat task was created for.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns whether the heartbeat task has completed.
    pub fn is_finished(&self) -> bool {
        match &self.handle {
            Some(handle) => handle.is_finished(),
            None => true,
        }
    }

    /// Checks for heartbeat task completion without requesting shutdown.
    ///
    /// Returns `Ok(None)` while the task is still running, `Ok(Some(()))`
    /// after a clean task completion, or the task error if a heartbeat failed.
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

    /// Requests heartbeat task shutdown and waits for it to finish.
    pub async fn stop(mut self) -> Result<()> {
        self.signal_shutdown();
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        handle.await?
    }

    fn state_for(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> HeartbeatHandleState {
        if self.group_id != group_id {
            return HeartbeatHandleState::DifferentGroup;
        }
        if self.member_id != member_id || self.generation_id != generation_id {
            return HeartbeatHandleState::StaleGeneration;
        }
        HeartbeatHandleState::Current
    }

    async fn stop_stale_generation(&mut self) -> Result<()> {
        self.signal_shutdown();
        let Some(handle) = self.handle.take() else {
            return Ok(());
        };
        match handle.await? {
            Ok(()) => Ok(()),
            Err(error) if should_rejoin_group(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }

    fn signal_shutdown(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
    }
}

impl Drop for ConsumerGroupHeartbeat {
    fn drop(&mut self) {
        self.signal_shutdown();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HeartbeatHandleState {
    Current,
    StaleGeneration,
    DifferentGroup,
}

fn assignments_for_strategy(
    protocol_name: &str,
    members: &[JoinGroupMember],
    metadata: &MetadataResponseV1,
) -> Result<Vec<SyncGroupAssignment>> {
    match protocol_name {
        RANGE_PROTOCOL => range_assignments(members, metadata),
        ROUND_ROBIN_PROTOCOL => round_robin_assignments(members, metadata),
        _ => Err(Error::Unsupported(
            "consumer group selected an unsupported assignment strategy",
        )),
    }
}

pub(crate) fn range_assignments(
    members: &[JoinGroupMember],
    metadata: &MetadataResponseV1,
) -> Result<Vec<SyncGroupAssignment>> {
    let mut subscriptions_by_topic = BTreeMap::<String, Vec<String>>::new();
    for member in members {
        let subscription = ConsumerProtocolSubscriptionV0::decode(&member.metadata)?;
        for topic in &subscription.topics {
            subscriptions_by_topic
                .entry(topic.clone())
                .or_default()
                .push(member.member_id.clone());
        }
    }

    let mut assigned_by_member = BTreeMap::<String, BTreeMap<String, Vec<i32>>>::new();
    for member in members {
        assigned_by_member.insert(member.member_id.clone(), BTreeMap::new());
    }

    for (topic, mut topic_members) in subscriptions_by_topic {
        topic_members.sort();
        topic_members.dedup();

        let partitions = partitions_for(metadata, &topic)?;
        for (member_id, partitions) in range_for_topic(&topic_members, &partitions) {
            assigned_by_member
                .entry(member_id)
                .or_default()
                .insert(topic.clone(), partitions);
        }
    }

    encode_member_assignments(assigned_by_member)
}

pub(crate) fn round_robin_assignments(
    members: &[JoinGroupMember],
    metadata: &MetadataResponseV1,
) -> Result<Vec<SyncGroupAssignment>> {
    let mut subscriptions = BTreeMap::<String, BTreeSet<String>>::new();
    let mut subscribed_topics = BTreeSet::new();
    for member in members {
        let subscription = ConsumerProtocolSubscriptionV0::decode(&member.metadata)?;
        let topics = subscription.topics.into_iter().collect::<BTreeSet<_>>();
        subscribed_topics.extend(topics.iter().cloned());
        subscriptions.insert(member.member_id.clone(), topics);
    }

    let member_ids = subscriptions.keys().cloned().collect::<Vec<_>>();
    let mut assigned_by_member = member_ids
        .iter()
        .map(|member_id| (member_id.clone(), BTreeMap::new()))
        .collect::<BTreeMap<String, BTreeMap<String, Vec<i32>>>>();
    if member_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut next_member = 0;
    for topic in subscribed_topics {
        for partition in partitions_for(metadata, &topic)? {
            let member_index = (0..member_ids.len())
                .map(|offset| (next_member + offset) % member_ids.len())
                .find(|index| {
                    subscriptions
                        .get(&member_ids[*index])
                        .is_some_and(|topics| topics.contains(&topic))
                })
                .ok_or(Error::Unsupported(
                    "round-robin assignment found no subscribed member",
                ))?;
            assigned_by_member
                .entry(member_ids[member_index].clone())
                .or_default()
                .entry(topic.clone())
                .or_default()
                .push(partition);
            next_member = (member_index + 1) % member_ids.len();
        }
    }

    encode_member_assignments(assigned_by_member)
}

fn encode_member_assignments(
    assigned_by_member: BTreeMap<String, BTreeMap<String, Vec<i32>>>,
) -> Result<Vec<SyncGroupAssignment>> {
    assigned_by_member
        .into_iter()
        .map(|(member_id, topics)| {
            let assignments = topics
                .into_iter()
                .map(|(topic, partitions)| ConsumerProtocolTopicAssignment { topic, partitions })
                .collect();
            Ok(SyncGroupAssignment {
                member_id,
                assignment: ConsumerProtocolAssignmentV0 {
                    assignments,
                    user_data: None,
                }
                .encode()?,
            })
        })
        .collect()
}

async fn assignments_from_protocol(
    coordinator: &mut Client,
    group_id: &str,
    start_offset: i64,
    assignment: &ConsumerProtocolAssignmentV0,
) -> Result<Vec<ConsumerAssignment>> {
    let mut assignments = Vec::new();
    let offsets = coordinator
        .offset_fetch_v2(group_id.to_owned(), Some(offset_fetch_topics(assignment)))
        .await?;
    if offsets.error_code != 0 {
        return Err(
            coordinator.broker_error(offsets.error_code, format!("offset fetch group {group_id}"))
        );
    }

    for topic in &assignment.assignments {
        for partition in &topic.partitions {
            let committed = committed_offset(group_id, &offsets.topics, &topic.topic, *partition)
                .map_err(|error| match error {
                Error::Broker { code, context } => coordinator.broker_error(code, context),
                error => error,
            })?;
            let next_offset = committed
                .filter(|offset| *offset >= 0)
                .unwrap_or(start_offset);
            assignments.push(ConsumerAssignment::new(
                topic.topic.clone(),
                *partition,
                next_offset,
            ));
        }
    }
    Ok(assignments)
}

fn offset_fetch_topics(assignment: &ConsumerProtocolAssignmentV0) -> Vec<OffsetFetchTopic> {
    assignment
        .assignments
        .iter()
        .map(|topic| OffsetFetchTopic {
            name: topic.topic.clone(),
            partition_indexes: topic.partitions.clone(),
        })
        .collect()
}

fn committed_offset(
    group_id: &str,
    topics: &[OffsetFetchTopicResponse],
    topic_name: &str,
    partition_index: i32,
) -> Result<Option<i64>> {
    let Some(partition) = partition_response(topics, topic_name, partition_index) else {
        return Ok(None);
    };
    if partition.error_code != 0 {
        return Err(Error::Broker {
            code: partition.error_code,
            context: format!("offset fetch group {group_id} {topic_name}-{partition_index}"),
        });
    }
    Ok(Some(partition.committed_offset))
}

fn partition_response<'a>(
    topics: &'a [OffsetFetchTopicResponse],
    topic_name: &str,
    partition_index: i32,
) -> Option<&'a OffsetFetchPartitionResponse> {
    topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .and_then(|topic| {
            topic
                .partitions
                .iter()
                .find(|partition| partition.partition_index == partition_index)
        })
}

fn offset_commit_topics(assignments: &[ConsumerAssignment]) -> Vec<OffsetCommitTopic> {
    let mut topics = BTreeMap::<String, Vec<OffsetCommitPartition>>::new();
    for assignment in assignments {
        topics
            .entry(assignment.topic().to_owned())
            .or_default()
            .push(OffsetCommitPartition {
                partition_index: assignment.partition(),
                committed_offset: assignment.next_offset(),
                committed_metadata: None,
            });
    }

    topics
        .into_iter()
        .map(|(name, mut partitions)| {
            partitions.sort_by_key(|partition| partition.partition_index);
            OffsetCommitTopic { name, partitions }
        })
        .collect()
}

fn offset_commit_topics_v7(assignments: &[ConsumerAssignment]) -> Vec<OffsetCommitTopicV7> {
    offset_commit_topics(assignments)
        .into_iter()
        .map(|topic| OffsetCommitTopicV7 {
            name: topic.name,
            partitions: topic
                .partitions
                .into_iter()
                .map(|partition| OffsetCommitPartitionV7 {
                    partition_index: partition.partition_index,
                    committed_offset: partition.committed_offset,
                    committed_leader_epoch: -1,
                    committed_metadata: partition.committed_metadata,
                })
                .collect(),
        })
        .collect()
}

fn offset_commit_response_error(
    group_id: &str,
    topics: &[OffsetCommitTopicResponse],
) -> Option<Error> {
    for topic in topics {
        for partition in &topic.partitions {
            if partition.error_code != 0 {
                return Some(Error::Broker {
                    code: partition.error_code,
                    context: format!(
                        "offset commit group {group_id} {}-{}",
                        topic.name, partition.partition_index
                    ),
                });
            }
        }
    }
    None
}

fn should_rejoin_group(error: &Error) -> bool {
    if matches!(error, Error::Io(_) | Error::RequestTimedOut { .. }) {
        return true;
    }

    matches!(
        error.broker_error_kind(),
        Some(
            BrokerErrorKind::CoordinatorNotAvailable
                | BrokerErrorKind::NotCoordinator
                | BrokerErrorKind::IllegalGeneration
                | BrokerErrorKind::UnknownMemberId
                | BrokerErrorKind::RebalanceInProgress
        )
    )
}

async fn should_rejoin_after_background_heartbeat(
    heartbeat: &mut ConsumerGroupHeartbeat,
) -> Result<bool> {
    match heartbeat.try_wait().await {
        Ok(None | Some(())) => Ok(false),
        Err(error) if should_rejoin_group(&error) => Ok(true),
        Err(error) => Err(error),
    }
}

fn validate_heartbeat_interval(interval: Duration) -> Result<()> {
    if interval.is_zero() {
        return Err(Error::Unsupported(
            "heartbeat interval must be greater than zero",
        ));
    }
    Ok(())
}

async fn run_background_heartbeat(
    coordinator: &mut Client,
    group_id: String,
    generation_id: i32,
    member_id: String,
    group_instance_id: Option<String>,
    interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) -> Result<()> {
    let mut heartbeat = time::interval(interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            _ = &mut shutdown => return Ok(()),
            _ = heartbeat.tick() => {
                debug!(
                    group_id = group_id.as_str(),
                    member_id = member_id.as_str(),
                    generation_id,
                    "sending background kafka consumer group heartbeat"
                );
                let response = if let Some(group_instance_id) = &group_instance_id {
                    coordinator
                        .heartbeat_v3(
                            group_id.clone(),
                            generation_id,
                            member_id.clone(),
                            Some(group_instance_id.clone()),
                        )
                        .await?
                } else {
                    coordinator
                        .heartbeat_v2(group_id.clone(), generation_id, member_id.clone())
                        .await?
                };
                if response.error_code != 0 {
                    return Err(coordinator.broker_error(
                        response.error_code,
                        format!("background heartbeat group {group_id}"),
                    ));
                }
            }
        }
    }
}

fn coordinator_addr(coordinator: &FindCoordinatorResponseV1) -> String {
    format!("{}:{}", coordinator.host, coordinator.port)
}

fn duration_millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn partitions_for(metadata: &MetadataResponseV1, topic_name: &str) -> Result<Vec<i32>> {
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic_name.to_owned(),
            partition: -1,
        })?;

    let mut partitions = topic
        .partitions
        .iter()
        .map(|partition| partition.partition_index)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    partitions.sort();
    Ok(partitions)
}

fn range_for_topic(members: &[String], partitions: &[i32]) -> Vec<(String, Vec<i32>)> {
    if members.is_empty() {
        return Vec::new();
    }

    let partition_count = partitions.len();
    let member_count = members.len();
    let partitions_per_member = partition_count / member_count;
    let extra_partitions = partition_count % member_count;

    members
        .iter()
        .enumerate()
        .map(|(index, member)| {
            let start = partitions_per_member * index + extra_partitions.min(index);
            let length = partitions_per_member + usize::from(index < extra_partitions);
            let end = start + length;
            (member.clone(), partitions[start..end].to_vec())
        })
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::{
        assignments_for_strategy, committed_offset, offset_commit_response_error,
        offset_commit_topics, offset_commit_topics_v7, offset_fetch_topics, range_assignments,
        round_robin_assignments, should_rejoin_after_background_heartbeat, should_rejoin_group,
        validate_heartbeat_interval, ConsumerGroupAssignmentStrategy, ConsumerGroupConfig,
        ConsumerGroupHeartbeat, HeartbeatHandleState, IsolationLevel, SecurityProtocol,
    };
    use crate::consumer::ConsumerAssignment;
    use crate::Error;
    use kafrust_protocol::api::join_group::JoinGroupMember;
    use kafrust_protocol::api::metadata::{MetadataResponseV1, PartitionMetadata, TopicMetadata};
    use kafrust_protocol::api::offset_commit::{
        OffsetCommitPartitionResponse, OffsetCommitTopicResponse,
    };
    use kafrust_protocol::api::offset_fetch::{
        OffsetFetchPartitionResponse, OffsetFetchTopicResponse,
    };
    use kafrust_protocol::consumer_group::{
        ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0,
        ConsumerProtocolTopicAssignment,
    };

    #[test]
    fn builds_consumer_group_config() {
        let config = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
            .client_id("orders-reader")
            .request_timeout_ms(5_000)
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .tls_server_name("broker.example.com")
            .tls_root_certificate_der([1, 2, 3])
            .sasl_plain("alice", "secret-password")
            .subscribe("orders")
            .group_instance_id("orders-reader-1")
            .session_timeout_ms(8_000)
            .rebalance_timeout_ms(20_000)
            .retention_time_ms(60_000)
            .start_offset(5)
            .max_wait_ms(250)
            .min_bytes(10)
            .max_partition_bytes(1024)
            .max_retries(3)
            .max_poll_records(10)
            .isolation_level(IsolationLevel::ReadCommitted)
            .assignment_strategy(ConsumerGroupAssignmentStrategy::RoundRobin);

        assert_eq!(config.group_id(), "orders-group");
        assert_eq!(config.group_instance_id_ref(), Some("orders-reader-1"));
        assert_eq!(config.topics(), &["orders".to_owned()]);
        assert_eq!(
            config.client.security_protocol_ref(),
            SecurityProtocol::SaslPlaintext
        );
        assert_eq!(
            config.client.tls_server_name_ref(),
            Some("broker.example.com")
        );
        assert_eq!(config.client.tls_root_certificates_der(), &[vec![1, 2, 3]]);
        assert_eq!(
            config.client.sasl_credentials_ref().unwrap().username(),
            "alice"
        );
        assert_eq!(config.isolation_level_ref(), IsolationLevel::ReadCommitted);
        assert_eq!(
            config.assignment_strategy_ref(),
            ConsumerGroupAssignmentStrategy::RoundRobin
        );
    }

    #[tokio::test]
    async fn rejects_empty_static_group_instance_id_before_connecting() {
        let error = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
            .group_instance_id("")
            .subscribe("orders")
            .join()
            .await
            .unwrap_err();

        assert!(matches!(error, Error::InvalidGroupInstanceId));
    }

    #[test]
    fn group_fetch_consumer_preserves_client_security_config() {
        let config = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
            .client_id("orders-reader")
            .request_timeout_ms(5_000)
            .security_protocol(SecurityProtocol::SaslPlaintext)
            .tls_server_name("broker.example.com")
            .tls_root_certificate_der([1, 2, 3])
            .sasl_plain("alice", "secret-password")
            .max_retries(3)
            .max_poll_records(10)
            .isolation_level(IsolationLevel::ReadCommitted);

        let consumer_config = config.consumer_config();
        let client_config = consumer_config.client_config();

        assert_eq!(client_config.client_id_ref(), Some("orders-reader"));
        assert_eq!(client_config.request_timeout().as_millis(), 5_000);
        assert_eq!(
            client_config.security_protocol_ref(),
            SecurityProtocol::SaslPlaintext
        );
        assert_eq!(
            client_config.tls_server_name_ref(),
            Some("broker.example.com")
        );
        assert_eq!(client_config.tls_root_certificates_der(), &[vec![1, 2, 3]]);
        assert_eq!(
            client_config.sasl_credentials_ref().unwrap().username(),
            "alice"
        );
        assert_eq!(consumer_config.max_retries_ref(), 3);
        assert_eq!(consumer_config.max_poll_records_ref(), 10);
        assert_eq!(
            consumer_config.isolation_level_ref(),
            IsolationLevel::ReadCommitted
        );
    }

    #[test]
    fn assigns_partitions_with_range_strategy() {
        let members = vec![
            member("member-b", &["orders"]),
            member("member-a", &["orders"]),
        ];
        let metadata = metadata_fixture("orders", &[0, 1, 2]);

        let assignments = range_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        let member_b = decode_assignment(&assignments[1].assignment);

        assert_eq!(assignments[0].member_id, "member-a");
        assert_eq!(member_a.assignments[0].topic, "orders");
        assert_eq!(member_a.assignments[0].partitions, vec![0, 1]);
        assert_eq!(assignments[1].member_id, "member-b");
        assert_eq!(member_b.assignments[0].topic, "orders");
        assert_eq!(member_b.assignments[0].partitions, vec![2]);
    }

    #[test]
    fn assigns_only_subscribed_members_per_topic() {
        let members = vec![
            member("member-a", &["orders", "payments"]),
            member("member-b", &["orders"]),
        ];
        let mut metadata = metadata_fixture("orders", &[0, 1]);
        metadata.topics.push(topic_metadata("payments", &[0, 1, 2]));

        let assignments = range_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        assert_eq!(member_a.assignments[0].topic, "orders");
        assert_eq!(member_a.assignments[0].partitions, vec![0]);
        assert_eq!(member_a.assignments[1].topic, "payments");
        assert_eq!(member_a.assignments[1].partitions, vec![0, 1, 2]);
    }

    #[test]
    fn assigns_partitions_with_round_robin_strategy() {
        let members = vec![
            member("member-b", &["orders", "payments"]),
            member("member-a", &["orders", "payments"]),
        ];
        let mut metadata = metadata_fixture("orders", &[0, 1, 2]);
        metadata.topics.push(topic_metadata("payments", &[0, 1]));

        let assignments = round_robin_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        let member_b = decode_assignment(&assignments[1].assignment);
        assert_eq!(member_a.assignments[0].partitions, vec![0, 2]);
        assert_eq!(member_a.assignments[1].partitions, vec![1]);
        assert_eq!(member_b.assignments[0].partitions, vec![1]);
        assert_eq!(member_b.assignments[1].partitions, vec![0]);
    }

    #[test]
    fn round_robin_skips_members_not_subscribed_to_a_topic() {
        let members = vec![
            member("member-a", &["orders"]),
            member("member-b", &["payments"]),
            member("member-c", &["orders", "payments"]),
        ];
        let mut metadata = metadata_fixture("orders", &[0, 1]);
        metadata.topics.push(topic_metadata("payments", &[0, 1]));

        let assignments = round_robin_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        let member_b = decode_assignment(&assignments[1].assignment);
        let member_c = decode_assignment(&assignments[2].assignment);
        assert_eq!(member_a.assignments[0].partitions, vec![0]);
        assert_eq!(member_b.assignments[0].partitions, vec![0]);
        assert_eq!(member_c.assignments[0].partitions, vec![1]);
        assert_eq!(member_c.assignments[1].partitions, vec![1]);
    }

    #[test]
    fn rejects_unknown_selected_assignment_strategy() {
        let error =
            assignments_for_strategy("sticky", &[], &metadata_fixture("orders", &[0])).unwrap_err();
        assert!(matches!(
            error,
            Error::Unsupported("consumer group selected an unsupported assignment strategy")
        ));
    }

    #[test]
    fn builds_offset_fetch_topics_from_assignment() {
        let assignment = ConsumerProtocolAssignmentV0 {
            assignments: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0, 2],
            }],
            user_data: None,
        };

        let topics = offset_fetch_topics(&assignment);

        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].name, "orders");
        assert_eq!(topics[0].partition_indexes, vec![0, 2]);
    }

    #[test]
    fn reads_committed_offset_and_surfaces_partition_error() {
        let topics = vec![OffsetFetchTopicResponse {
            name: "orders".to_owned(),
            partitions: vec![
                OffsetFetchPartitionResponse {
                    partition_index: 0,
                    committed_offset: 42,
                    metadata: None,
                    error_code: 0,
                },
                OffsetFetchPartitionResponse {
                    partition_index: 1,
                    committed_offset: -1,
                    metadata: None,
                    error_code: 25,
                },
            ],
        }];

        assert_eq!(
            committed_offset("orders-group", &topics, "orders", 0).unwrap(),
            Some(42)
        );
        assert_eq!(
            committed_offset("orders-group", &topics, "orders", 2).unwrap(),
            None
        );
        assert!(matches!(
            committed_offset("orders-group", &topics, "orders", 1).unwrap_err(),
            Error::Broker { code: 25, .. }
        ));
    }

    #[test]
    fn builds_offset_commit_topics_from_current_assignment_offsets() {
        let assignments = vec![
            ConsumerAssignment::new("orders".to_owned(), 1, 43),
            ConsumerAssignment::new("orders".to_owned(), 0, 11),
            ConsumerAssignment::new("payments".to_owned(), 0, 7),
        ];

        let topics = offset_commit_topics(&assignments);

        assert_eq!(topics[0].name, "orders");
        assert_eq!(topics[0].partitions[0].partition_index, 0);
        assert_eq!(topics[0].partitions[0].committed_offset, 11);
        assert_eq!(topics[0].partitions[1].partition_index, 1);

        let static_topics = offset_commit_topics_v7(&assignments);
        assert_eq!(static_topics[0].partitions[0].committed_offset, 11);
        assert_eq!(static_topics[0].partitions[0].committed_leader_epoch, -1);
        assert_eq!(topics[1].name, "payments");
    }

    #[test]
    fn surfaces_offset_commit_partition_error() {
        let response = vec![OffsetCommitTopicResponse {
            name: "orders".to_owned(),
            partitions: vec![OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: 25,
            }],
        }];

        let error = offset_commit_response_error("orders-group", &response).unwrap();
        assert!(matches!(error, Error::Broker { code: 25, .. }));
    }

    #[test]
    fn classifies_offset_commit_rejoin_error() {
        let response = vec![OffsetCommitTopicResponse {
            name: "orders".to_owned(),
            partitions: vec![OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: 27,
            }],
        }];

        let error = offset_commit_response_error("orders-group", &response).unwrap();

        assert!(should_rejoin_group(&error));
        assert!(matches!(error, Error::Broker { code: 27, .. }));
    }

    #[test]
    fn classifies_offset_commit_non_rejoin_error() {
        let response = vec![OffsetCommitTopicResponse {
            name: "orders".to_owned(),
            partitions: vec![OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: 7,
            }],
        }];

        let error = offset_commit_response_error("orders-group", &response).unwrap();

        assert!(!should_rejoin_group(&error));
        assert!(matches!(error, Error::Broker { code: 7, .. }));
    }

    #[test]
    fn classifies_group_errors_that_require_rejoin() {
        for code in [15, 16, 22, 25, 27] {
            assert!(should_rejoin_group(&Error::Broker {
                code,
                context: "heartbeat group orders-group".to_owned(),
            }));
        }

        assert!(!should_rejoin_group(&Error::Broker {
            code: 7,
            context: "heartbeat group orders-group".to_owned(),
        }));
        assert!(!should_rejoin_group(&Error::Broker {
            code: 82,
            context: "heartbeat group orders-group".to_owned(),
        }));
        assert!(should_rejoin_group(&Error::RequestTimedOut {
            timeout_ms: 5
        }));
        assert!(should_rejoin_group(&Error::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ))));
        assert!(!should_rejoin_group(&Error::Unsupported("group feature")));
    }

    #[test]
    fn rejects_zero_background_heartbeat_interval() {
        assert!(matches!(
            validate_heartbeat_interval(std::time::Duration::ZERO).unwrap_err(),
            Error::Unsupported("heartbeat interval must be greater than zero")
        ));
        validate_heartbeat_interval(std::time::Duration::from_millis(1)).unwrap();
    }

    #[tokio::test]
    async fn try_wait_reports_running_heartbeat_task() {
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = shutdown_rx.await;
            Ok(())
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        assert!(!heartbeat.is_finished());
        assert_eq!(heartbeat.try_wait().await.unwrap(), None);

        heartbeat.stop().await.unwrap();
    }

    #[tokio::test]
    async fn try_wait_surfaces_finished_heartbeat_error() {
        let (shutdown, _shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async {
            Err(Error::Broker {
                code: 27,
                context: "background heartbeat group orders-group".to_owned(),
            })
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }

        assert!(matches!(
            heartbeat.try_wait().await,
            Err(Error::Broker { code: 27, .. })
        ));
        assert!(heartbeat.is_finished());
    }

    #[tokio::test]
    async fn background_heartbeat_observation_ignores_running_task() {
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = shutdown_rx.await;
            Ok(())
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        assert!(!should_rejoin_after_background_heartbeat(&mut heartbeat)
            .await
            .unwrap());

        heartbeat.stop().await.unwrap();
    }

    #[tokio::test]
    async fn background_heartbeat_observation_requests_rejoin_for_group_error() {
        let (shutdown, _shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async {
            Err(Error::Broker {
                code: 27,
                context: "background heartbeat group orders-group".to_owned(),
            })
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }

        assert!(should_rejoin_after_background_heartbeat(&mut heartbeat)
            .await
            .unwrap());
        assert!(heartbeat.is_finished());
    }

    #[tokio::test]
    async fn background_heartbeat_observation_surfaces_non_rejoin_error() {
        let (shutdown, _shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async {
            Err(Error::Broker {
                code: 7,
                context: "background heartbeat group orders-group".to_owned(),
            })
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }

        assert!(matches!(
            should_rejoin_after_background_heartbeat(&mut heartbeat).await,
            Err(Error::Broker { code: 7, .. })
        ));
        assert!(heartbeat.is_finished());
    }

    #[tokio::test]
    async fn heartbeat_handle_exposes_group_identity() {
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = shutdown_rx.await;
            Ok(())
        });
        let heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 7);

        assert_eq!(heartbeat.group_id(), "orders-group");
        assert_eq!(heartbeat.member_id(), "member-a");
        assert_eq!(heartbeat.generation_id(), 7);
        assert_eq!(
            heartbeat.state_for("orders-group", "member-a", 7),
            HeartbeatHandleState::Current
        );
        assert_eq!(
            heartbeat.state_for("orders-group", "member-b", 8),
            HeartbeatHandleState::StaleGeneration
        );
        assert_eq!(
            heartbeat.state_for("payments-group", "member-a", 7),
            HeartbeatHandleState::DifferentGroup
        );
        heartbeat.stop().await.unwrap();
    }

    #[tokio::test]
    async fn stale_heartbeat_shutdown_stops_running_task() {
        let (shutdown, shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async move {
            let _ = shutdown_rx.await;
            Ok(())
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        heartbeat.stop_stale_generation().await.unwrap();

        assert!(heartbeat.is_finished());
        assert_eq!(heartbeat.try_wait().await.unwrap(), Some(()));
    }

    #[tokio::test]
    async fn stale_heartbeat_shutdown_ignores_rejoinable_group_error() {
        let (shutdown, _shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async {
            Err(Error::Broker {
                code: 27,
                context: "background heartbeat group orders-group".to_owned(),
            })
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }

        heartbeat.stop_stale_generation().await.unwrap();
        assert!(heartbeat.is_finished());
    }

    #[tokio::test]
    async fn stale_heartbeat_shutdown_surfaces_non_rejoin_error() {
        let (shutdown, _shutdown_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn(async {
            Err(Error::Broker {
                code: 7,
                context: "background heartbeat group orders-group".to_owned(),
            })
        });
        let mut heartbeat = heartbeat_handle(shutdown, handle, "orders-group", "member-a", 1);

        while !heartbeat.is_finished() {
            tokio::task::yield_now().await;
        }

        assert!(matches!(
            heartbeat.stop_stale_generation().await,
            Err(Error::Broker { code: 7, .. })
        ));
        assert!(heartbeat.is_finished());
    }

    fn member(member_id: &str, topics: &[&str]) -> JoinGroupMember {
        JoinGroupMember {
            member_id: member_id.to_owned(),
            metadata: ConsumerProtocolSubscriptionV0 {
                topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
                user_data: None,
            }
            .encode()
            .unwrap(),
        }
    }

    fn heartbeat_handle(
        shutdown: tokio::sync::oneshot::Sender<()>,
        handle: tokio::task::JoinHandle<crate::Result<()>>,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> ConsumerGroupHeartbeat {
        ConsumerGroupHeartbeat {
            group_id: group_id.to_owned(),
            generation_id,
            member_id: member_id.to_owned(),
            shutdown: Some(shutdown),
            handle: Some(handle),
        }
    }

    fn decode_assignment(bytes: &[u8]) -> ConsumerProtocolAssignmentV0 {
        ConsumerProtocolAssignmentV0::decode(bytes).unwrap()
    }

    fn metadata_fixture(topic: &str, partitions: &[i32]) -> MetadataResponseV1 {
        MetadataResponseV1 {
            brokers: Vec::new(),
            controller_id: 1,
            topics: vec![topic_metadata(topic, partitions)],
        }
    }

    fn topic_metadata(topic: &str, partitions: &[i32]) -> TopicMetadata {
        TopicMetadata {
            error_code: 0,
            name: topic.to_owned(),
            is_internal: false,
            partitions: partitions
                .iter()
                .map(|partition| PartitionMetadata {
                    error_code: 0,
                    partition_index: *partition,
                    leader_id: 1,
                    replica_nodes: vec![1],
                    isr_nodes: vec![1],
                })
                .collect(),
        }
    }
}
