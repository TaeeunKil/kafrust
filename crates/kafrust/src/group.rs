use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use kafrust_protocol::api::consumer_group_heartbeat::{
    ConsumerGroupHeartbeatResponseV0, ConsumerGroupHeartbeatTopicPartitions,
};
use kafrust_protocol::api::find_coordinator::FindCoordinatorResponseV1;
use kafrust_protocol::api::join_group::JoinGroupMember;
use kafrust_protocol::api::leave_group::{LeaveGroupMemberIdentity, LeaveGroupResponseV3};
use kafrust_protocol::api::list_offsets::{
    ListOffsetsPartitionV1, ListOffsetsResponseV1, ListOffsetsTopicV1, EARLIEST_TIMESTAMP,
    LATEST_TIMESTAMP,
};
use kafrust_protocol::api::metadata::{
    MetadataRequestTopicV12, MetadataResponseV1, MetadataResponseV12,
};
use kafrust_protocol::api::offset_commit::{
    OffsetCommitPartition, OffsetCommitPartitionV7, OffsetCommitTopic, OffsetCommitTopicResponse,
    OffsetCommitTopicV7,
};
use kafrust_protocol::api::offset_fetch::{
    OffsetFetchPartitionResponse, OffsetFetchTopic, OffsetFetchTopicResponse,
};
use kafrust_protocol::api::sync_group::SyncGroupAssignment;
use kafrust_protocol::consumer_group::{
    ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0, ConsumerProtocolSubscriptionV1,
    ConsumerProtocolTopicAssignment,
};

use crate::client::Client;
use crate::config::{ClientConfig, OAuthBearerTokenProvider, SecurityProtocol};
use crate::consumer::{
    Consumer, ConsumerAssignment, ConsumerConfig, ConsumerRecord, IsolationLevel,
    PartitionWatermarks,
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
const COOPERATIVE_STICKY_PROTOCOL: &str = "cooperative-sticky";
const GROUP_JOIN_RETRY_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Consumer group membership protocol used by a group member.
pub enum ConsumerGroupProtocol {
    /// Kafka's classic JoinGroup/SyncGroup protocol.
    #[default]
    Classic,
    /// Kafka's KIP-848 consumer group protocol.
    Consumer,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
/// Partition assignment strategy advertised by a classic consumer group member.
pub enum ConsumerGroupAssignmentStrategy {
    /// Assign contiguous partition ranges independently for each topic.
    #[default]
    Range,
    /// Distribute all subscribed topic partitions cyclically across members.
    RoundRobin,
    /// Preserve existing ownership where possible and transfer partitions cooperatively.
    CooperativeSticky,
}

impl ConsumerGroupAssignmentStrategy {
    fn protocol_name(self) -> &'static str {
        match self {
            Self::Range => RANGE_PROTOCOL,
            Self::RoundRobin => ROUND_ROBIN_PROTOCOL,
            Self::CooperativeSticky => COOPERATIVE_STICKY_PROTOCOL,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Starting position used when an assigned partition has no committed offset.
pub enum OffsetResetPolicy {
    /// Start at the partition's earliest available offset.
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
    fn timestamp(self) -> Option<i64> {
        match self {
            Self::Earliest => Some(EARLIEST_TIMESTAMP),
            Self::Latest => Some(LATEST_TIMESTAMP),
            Self::Offset(_) => None,
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
    offset_reset_policy: OffsetResetPolicy,
    max_wait_ms: i32,
    min_bytes: i32,
    max_partition_bytes: i32,
    max_retries: u32,
    max_poll_records: usize,
    isolation_level: IsolationLevel,
    assignment_strategy: ConsumerGroupAssignmentStrategy,
    group_protocol: ConsumerGroupProtocol,
    server_assignor: Option<String>,
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
            offset_reset_policy: OffsetResetPolicy::Offset(0),
            max_wait_ms: 500,
            min_bytes: 1,
            max_partition_bytes: 1_048_576,
            max_retries: 1,
            max_poll_records: 500,
            isolation_level: IsolationLevel::ReadUncommitted,
            assignment_strategy: ConsumerGroupAssignmentStrategy::Range,
            group_protocol: ConsumerGroupProtocol::Classic,
            server_assignor: None,
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

    /// Sets SASL/OAUTHBEARER credentials for group, coordinator, and fetch requests.
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
        self.offset_reset_policy = OffsetResetPolicy::Offset(start_offset);
        self
    }

    /// Sets how partitions without committed offsets choose their starting position.
    pub fn offset_reset_policy(mut self, offset_reset_policy: OffsetResetPolicy) -> Self {
        self.offset_reset_policy = offset_reset_policy;
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

    /// Sets the maximum number of retry attempts for transient group-join and
    /// fetch failures.
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

    /// Selects the classic or KIP-848 consumer group membership protocol.
    pub fn group_protocol(mut self, group_protocol: ConsumerGroupProtocol) -> Self {
        self.group_protocol = group_protocol;
        self
    }

    /// Returns the configured consumer group membership protocol.
    pub fn group_protocol_ref(&self) -> ConsumerGroupProtocol {
        self.group_protocol
    }

    /// Sets the optional Kafka server-side assignor used by KIP-848 groups.
    pub fn server_assignor(mut self, server_assignor: impl Into<String>) -> Self {
        self.server_assignor = Some(server_assignor.into());
        self
    }

    /// Returns the configured KIP-848 server-side assignor, if any.
    pub fn server_assignor_ref(&self) -> Option<&str> {
        self.server_assignor.as_deref()
    }

    /// Returns the policy used for partitions without committed offsets.
    pub fn offset_reset_policy_ref(&self) -> OffsetResetPolicy {
        self.offset_reset_policy
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
        if self.group_protocol == ConsumerGroupProtocol::Consumer {
            if self.assignment_strategy != ConsumerGroupAssignmentStrategy::Range {
                return Err(Error::Unsupported(
                    "KIP-848 groups use a broker-side assignor; configure ConsumerGroupAssignmentStrategy::Range or select the classic protocol",
                ));
            }
            return self.join_consumer(None, 0, None).await;
        }
        self.join_with_owned_partitions(Vec::new()).await
    }

    async fn join_with_owned_partitions(
        self,
        owned_partitions: Vec<ConsumerProtocolTopicAssignment>,
    ) -> Result<ConsumerGroup> {
        self.join_with_member_id(owned_partitions, None).await
    }

    async fn join_with_member_id(
        self,
        owned_partitions: Vec<ConsumerProtocolTopicAssignment>,
        member_id: Option<String>,
    ) -> Result<ConsumerGroup> {
        if self.topics.is_empty() {
            return Err(Error::Unsupported("consumer group without subscriptions"));
        }
        if self.group_instance_id.as_deref() == Some("") {
            return Err(Error::InvalidGroupInstanceId);
        }
        let mut member_id = member_id;
        let mut retry_attempt = 0;

        loop {
            debug!(
                group_id = self.group_id.as_str(),
                topic_count = self.topics.len(),
                retry_attempt,
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
            let subscription = match self.assignment_strategy {
                ConsumerGroupAssignmentStrategy::CooperativeSticky => {
                    ConsumerProtocolSubscriptionV1 {
                        topics: self.topics.clone(),
                        user_data: None,
                        owned_partitions: owned_partitions.clone(),
                    }
                    .encode()?
                }
                ConsumerGroupAssignmentStrategy::Range
                | ConsumerGroupAssignmentStrategy::RoundRobin => ConsumerProtocolSubscriptionV0 {
                    topics: self.topics.clone(),
                    user_data: None,
                }
                .encode()?,
            };
            let protocols = vec![kafrust_protocol::api::join_group::JoinGroupProtocol {
                name: self.assignment_strategy.protocol_name().to_owned(),
                metadata: subscription,
            }];
            let is_rejoin = member_id.is_some();
            let requested_member_id = member_id.take().unwrap_or_default();
            let joined = if let Some(group_instance_id) = &self.group_instance_id {
                let response = coordinator_client
                    .join_group_v5(
                        self.group_id.clone(),
                        self.session_timeout_ms,
                        self.rebalance_timeout_ms,
                        requested_member_id,
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
                        requested_member_id,
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
                let error = self
                    .client
                    .broker_error(synced.error_code, format!("sync group {}", self.group_id));
                if retry_attempt < self.max_retries && should_rejoin_group(&error) {
                    retry_attempt += 1;
                    self.client.record_retry();
                    member_id = Some(joined.member_id.clone());
                    debug!(
                        group_id = self.group_id.as_str(),
                        retry_attempt,
                        error = %error,
                        "retrying kafka consumer group join after transient sync error"
                    );
                    time::sleep(GROUP_JOIN_RETRY_BACKOFF).await;
                    continue;
                }
                return Err(error);
            }

            let assignment = ConsumerProtocolAssignmentV0::decode(&synced.assignment)?;
            let cooperative_rejoin_required = self.assignment_strategy
                == ConsumerGroupAssignmentStrategy::CooperativeSticky
                && cooperative_rejoin_required_for_assignment(
                    &joined.members,
                    &owned_partitions,
                    &assignment,
                    is_rejoin,
                )?;
            let consumer_assignments = assignments_from_protocol(
                &mut coordinator_client,
                &mut bootstrap,
                &self.client,
                &self.group_id,
                self.offset_reset_policy,
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

            return Ok(ConsumerGroup {
                config,
                group_id: self.group_id,
                generation_id: joined.generation_id,
                member_id: joined.member_id,
                retention_time_ms: self.retention_time_ms,
                coordinator: coordinator_client,
                cooperative_rejoin_required,
                protocol: ConsumerGroupProtocol::Classic,
                consumer_topic_ids: BTreeMap::new(),
                consumer_owned_partitions: None,
                consumer: Consumer::from_assignments(
                    consumer_client,
                    consumer_config,
                    consumer_assignments,
                ),
            });
        }
    }

    async fn join_consumer(
        self,
        member_id: Option<String>,
        member_epoch: i32,
        owned_partitions: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
    ) -> Result<ConsumerGroup> {
        if self.topics.is_empty() {
            return Err(Error::Unsupported("consumer group without subscriptions"));
        }
        if self.group_instance_id.as_deref() == Some("") {
            return Err(Error::InvalidGroupInstanceId);
        }

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

        let metadata = bootstrap
            .metadata_v12(Some(
                self.topics
                    .iter()
                    .map(|name| MetadataRequestTopicV12 {
                        topic_id: [0; 16],
                        name: Some(name.clone()),
                    })
                    .collect(),
            ))
            .await?;
        let topic_ids =
            topic_ids_for_names(&metadata, &self.topics).map_err(|error| match error {
                Error::Broker { code, context } => self.client.broker_error(code, context),
                error => error,
            })?;
        let mut coordinator_client = self
            .client
            .connect_broker(coordinator_addr(&coordinator))
            .await?;
        let requested_member_id = member_id.clone().unwrap_or_default();
        let response = coordinator_client
            .consumer_group_heartbeat_v0(
                self.group_id.clone(),
                requested_member_id,
                member_epoch,
                self.group_instance_id.clone(),
                None,
                self.rebalance_timeout_ms,
                Some(self.topics.clone()),
                self.server_assignor.clone(),
                owned_partitions.clone(),
            )
            .await?;
        if response.error_code != 0 {
            return Err(consumer_group_heartbeat_error(
                &self.client,
                &self.group_id,
                &response,
            ));
        }

        let member_id = response.member_id.or(member_id).ok_or(Error::Unsupported(
            "consumer group heartbeat omitted member ID",
        ))?;
        let assignment = response.assignment.clone().unwrap_or_default();
        let protocol_assignment = consumer_protocol_assignment(&assignment, &topic_ids)?;
        let consumer_assignments = assignments_from_protocol(
            &mut coordinator_client,
            &mut bootstrap,
            &self.client,
            &self.group_id,
            self.offset_reset_policy,
            &protocol_assignment,
        )
        .await?;
        let consumer_config = self.consumer_config();
        let consumer_client = self.client.clone().connect().await?;

        debug!(
            group_id = self.group_id.as_str(),
            member_id = member_id.as_str(),
            member_epoch = response.member_epoch,
            assignment_count = consumer_assignments.len(),
            "joined kafka KIP-848 consumer group"
        );

        Ok(ConsumerGroup {
            config,
            group_id: self.group_id,
            generation_id: response.member_epoch,
            member_id,
            retention_time_ms: self.retention_time_ms,
            coordinator: coordinator_client,
            cooperative_rejoin_required: false,
            protocol: ConsumerGroupProtocol::Consumer,
            consumer_topic_ids: topic_ids,
            consumer_owned_partitions: response.assignment,
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
    cooperative_rejoin_required: bool,
    protocol: ConsumerGroupProtocol,
    consumer_topic_ids: BTreeMap<String, [u8; 16]>,
    consumer_owned_partitions: Option<Vec<ConsumerGroupHeartbeatTopicPartitions>>,
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

    /// Returns the current classic generation ID or KIP-848 member epoch.
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

    /// Returns the membership protocol used by this group handle.
    pub fn group_protocol(&self) -> ConsumerGroupProtocol {
        self.protocol
    }

    /// Returns the broker-assigned group member ID.
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Returns the current classic generation ID or KIP-848 member epoch.
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

    /// Returns the next offset for a currently assigned topic partition.
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.consumer.position(topic, partition)
    }

    /// Changes the next offset for a currently assigned topic partition.
    ///
    /// A later group rejoin restores broker-committed or configured reset
    /// offsets for the new assignment.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.consumer.seek(topic, partition, offset)
    }

    /// Pauses fetching from a currently assigned topic partition.
    ///
    /// Pause state is retained across a rejoin when this member keeps the
    /// topic partition.
    pub fn pause(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.consumer.pause(topic, partition)
    }

    /// Resumes fetching from a currently assigned topic partition.
    pub fn resume(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.consumer.resume(topic, partition)
    }

    /// Fetches the earliest and latest available offsets for a topic partition.
    ///
    /// The partition does not need to be assigned to this group member.
    pub async fn fetch_watermarks(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<PartitionWatermarks> {
        self.consumer.fetch_watermarks(topic, partition).await
    }

    /// Leaves the consumer group and consumes this member handle.
    ///
    /// Stop any separately spawned [`ConsumerGroupHeartbeat`] before leaving.
    /// Kafka receives both the broker member ID and the configured static
    /// instance ID, so the member does not remain active until session expiry.
    #[tracing::instrument(
        level = "debug",
        name = "kafka.consumer_group.leave",
        skip_all,
        fields(group_id = self.group_id.as_str(), member_id = self.member_id.as_str(), generation_id = self.generation_id),
        err
    )]
    pub async fn leave(mut self) -> Result<()> {
        if self.protocol == ConsumerGroupProtocol::Consumer {
            return self.leave_consumer().await;
        }
        let response = self
            .coordinator
            .leave_group_v3(
                self.group_id.clone(),
                vec![LeaveGroupMemberIdentity {
                    member_id: self.member_id.clone(),
                    group_instance_id: self.config.group_instance_id.clone(),
                }],
            )
            .await?;
        if let Some(error) = leave_group_response_error(&self.group_id, &response) {
            return Err(match error {
                Error::Broker { code, context } => self.config.client.broker_error(code, context),
                error => error,
            });
        }
        Ok(())
    }

    async fn leave_consumer(mut self) -> Result<()> {
        let response = self
            .coordinator
            .consumer_group_heartbeat_v0(
                self.group_id.clone(),
                self.member_id.clone(),
                -1,
                self.config.group_instance_id.clone(),
                None,
                -1,
                None,
                None,
                None,
            )
            .await?;
        if response.error_code != 0 {
            return Err(consumer_group_heartbeat_error(
                &self.config.client,
                &self.group_id,
                &response,
            ));
        }
        Ok(())
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
        if self.cooperative_rejoin_required {
            debug!(
                group_id = self.group_id.as_str(),
                member_id = self.member_id.as_str(),
                generation_id = self.generation_id,
                "completing cooperative consumer group ownership transfer"
            );
            self.rejoin().await?;
        }
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
    /// requires a rejoin, this method rejoins the group before polling and
    /// replaces the handle with a new task using the same interval. Other
    /// background heartbeat errors are returned to the caller. A foreground
    /// poll rejoin also replaces the stale task before this method returns.
    /// When the task is still running, this behaves like
    /// [`ConsumerGroup::poll`].
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
        let interval = heartbeat.interval;
        let mut restart_heartbeat = false;
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
                    restart_heartbeat = true;
                } else if heartbeat.is_finished() {
                    restart_heartbeat = true;
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
                restart_heartbeat = true;
            }
            HeartbeatHandleState::DifferentGroup => {
                return Err(Error::Unsupported(
                    "background heartbeat handle belongs to a different consumer group",
                ));
            }
        }
        if restart_heartbeat {
            *heartbeat = self.spawn_heartbeat_task(interval).await?;
        }

        let result = self.poll().await;
        match heartbeat.state_for(&self.group_id, &self.member_id, self.generation_id) {
            HeartbeatHandleState::Current => {}
            HeartbeatHandleState::StaleGeneration => {
                heartbeat.stop_stale_generation().await?;
                *heartbeat = self.spawn_heartbeat_task(interval).await?;
            }
            HeartbeatHandleState::DifferentGroup => {
                return Err(Error::Unsupported(
                    "background heartbeat handle belongs to a different consumer group",
                ));
            }
        }
        result
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
        if self.protocol == ConsumerGroupProtocol::Consumer {
            return Err(Error::Unsupported(
                "background heartbeat for KIP-848 groups is not available yet",
            ));
        }
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
            interval,
            shutdown: Some(shutdown),
            handle: Some(handle),
        })
    }

    async fn rejoin(&mut self) -> Result<()> {
        let paused = self
            .consumer
            .assignments()
            .iter()
            .filter(|assignment| assignment.is_paused())
            .map(|assignment| (assignment.topic().to_owned(), assignment.partition()))
            .collect::<Vec<_>>();
        let owned_partitions = owned_partitions_from_assignments(self.consumer.assignments());
        if self.protocol == ConsumerGroupProtocol::Consumer {
            let owned_partitions = consumer_owned_partitions_from_assignments(
                self.consumer.assignments(),
                &self.consumer_topic_ids,
            );
            let mut joined = self
                .config
                .clone()
                .join_consumer(
                    Some(self.member_id.clone()),
                    self.generation_id,
                    Some(owned_partitions),
                )
                .await?;
            for (topic, partition) in paused {
                if joined.consumer.position(&topic, partition).is_some() {
                    joined.consumer.pause(&topic, partition)?;
                }
            }
            *self = joined;
            return Ok(());
        }
        let mut joined = self
            .config
            .clone()
            .join_with_member_id(owned_partitions, Some(self.member_id.clone()))
            .await?;
        for (topic, partition) in paused {
            if joined.consumer.position(&topic, partition).is_some() {
                joined.consumer.pause(&topic, partition)?;
            }
        }
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
        if self.protocol == ConsumerGroupProtocol::Consumer {
            return self.heartbeat_consumer().await;
        }
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

    async fn heartbeat_consumer(&mut self) -> Result<()> {
        let owned_partitions = self.consumer_owned_partitions.clone();
        let response = self
            .coordinator
            .consumer_group_heartbeat_v0(
                self.group_id.clone(),
                self.member_id.clone(),
                self.generation_id,
                self.config.group_instance_id.clone(),
                None,
                self.config.rebalance_timeout_ms,
                Some(self.config.topics.clone()),
                self.config.server_assignor.clone(),
                owned_partitions,
            )
            .await?;
        if let Some(member_id) = response.member_id.clone() {
            self.member_id = member_id;
        }
        self.generation_id = response.member_epoch;
        if response.error_code != 0 {
            return Err(consumer_group_heartbeat_error(
                &self.config.client,
                &self.group_id,
                &response,
            ));
        }

        if let Some(assignment) = response.assignment {
            let protocol_assignment =
                consumer_protocol_assignment(&assignment, &self.consumer_topic_ids)?;
            let mut bootstrap = self.config.client.clone().connect().await?;
            let assignments = assignments_from_protocol(
                &mut self.coordinator,
                &mut bootstrap,
                &self.config.client,
                &self.group_id,
                self.config.offset_reset_policy,
                &protocol_assignment,
            )
            .await?;
            self.consumer.replace_assignments(assignments);
            self.consumer_owned_partitions = Some(assignment);
        } else {
            self.consumer_owned_partitions = None;
        }
        debug!(
            group_id = self.group_id.as_str(),
            member_id = self.member_id.as_str(),
            member_epoch = self.generation_id,
            assignment_count = self.consumer.assignments().len(),
            "sent kafka KIP-848 consumer group heartbeat"
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
    interval: Duration,
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

    /// Returns the interval used by this background heartbeat task.
    pub fn interval(&self) -> Duration {
        self.interval
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
        COOPERATIVE_STICKY_PROTOCOL => cooperative_sticky_assignments(members, metadata),
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

/// Builds a deterministic cooperative assignment that keeps valid owned
/// partitions and stages only ownership transfers between active members.
pub(crate) fn cooperative_sticky_assignments(
    members: &[JoinGroupMember],
    metadata: &MetadataResponseV1,
) -> Result<Vec<SyncGroupAssignment>> {
    let mut subscriptions = BTreeMap::<String, BTreeSet<String>>::new();
    let mut owned_by_member = BTreeMap::<String, BTreeSet<(String, i32)>>::new();
    for member in members {
        let subscription = ConsumerProtocolSubscriptionV1::decode(&member.metadata)?;
        subscriptions.insert(
            member.member_id.clone(),
            subscription.topics.into_iter().collect(),
        );
        owned_by_member.insert(
            member.member_id.clone(),
            subscription
                .owned_partitions
                .into_iter()
                .flat_map(|assignment| {
                    assignment
                        .partitions
                        .into_iter()
                        .map(move |partition| (assignment.topic.clone(), partition))
                })
                .collect(),
        );
    }

    let mut assigned_by_member = subscriptions
        .keys()
        .map(|member_id| (member_id.clone(), BTreeMap::<String, Vec<i32>>::new()))
        .collect::<BTreeMap<_, _>>();
    let mut owner_by_partition = BTreeMap::<(String, i32), String>::new();
    let mut available = BTreeSet::<(String, i32)>::new();

    for topics in subscriptions.values() {
        for topic in topics {
            for partition in partitions_for(metadata, topic)? {
                available.insert((topic.clone(), partition));
            }
        }
    }

    for (member_id, owned) in &owned_by_member {
        for partition in owned {
            if !available.contains(partition) {
                continue;
            }
            if owner_by_partition.contains_key(partition) {
                continue;
            }
            owner_by_partition.insert(partition.clone(), member_id.clone());
            add_assignment(&mut assigned_by_member, member_id, partition);
        }
    }

    for partition in available {
        if owner_by_partition.contains_key(&partition) {
            continue;
        }
        let topic = &partition.0;
        let member_id = subscriptions
            .iter()
            .filter(|(_, topics)| topics.contains(topic))
            .min_by_key(|(member_id, _)| {
                (assignment_count(&assigned_by_member, member_id), *member_id)
            })
            .map(|(member_id, _)| member_id.clone())
            .ok_or(Error::Unsupported(
                "cooperative assignor found no subscribed member",
            ))?;
        owner_by_partition.insert(partition.clone(), member_id.clone());
        add_assignment(&mut assigned_by_member, &member_id, &partition);
    }

    balance_owned_assignments(
        &mut assigned_by_member,
        &mut owner_by_partition,
        &subscriptions,
    );

    let mut transfers = BTreeSet::<(String, i32)>::new();
    for (partition, new_owner) in &owner_by_partition {
        if let Some(old_owner) = owned_by_member
            .iter()
            .find_map(|(member_id, owned)| owned.contains(partition).then_some(member_id))
        {
            if old_owner != new_owner && subscriptions.contains_key(old_owner) {
                transfers.insert(partition.clone());
            }
        }
    }
    for partition in transfers {
        if let Some(new_owner) = owner_by_partition.get(&partition) {
            remove_assignment(&mut assigned_by_member, new_owner, &partition);
        }
    }

    encode_member_assignments(assigned_by_member)
}

fn balance_owned_assignments(
    assigned_by_member: &mut BTreeMap<String, BTreeMap<String, Vec<i32>>>,
    owner_by_partition: &mut BTreeMap<(String, i32), String>,
    subscriptions: &BTreeMap<String, BTreeSet<String>>,
) {
    let member_ids = subscriptions.keys().cloned().collect::<Vec<_>>();
    loop {
        let Some(source) = member_ids
            .iter()
            .max_by_key(|member_id| assignment_count(assigned_by_member, member_id))
        else {
            return;
        };
        let source_count = assignment_count(assigned_by_member, source);
        let source_partitions = assigned_by_member
            .get(source)
            .into_iter()
            .flat_map(|topics| {
                topics.iter().flat_map(|(topic, partitions)| {
                    partitions
                        .iter()
                        .map(|partition| (topic.clone(), *partition))
                })
            })
            .collect::<Vec<_>>();

        let mut candidate = None;
        for partition in source_partitions {
            for target in &member_ids {
                if target == source
                    || !subscriptions
                        .get(target)
                        .is_some_and(|topics| topics.contains(&partition.0))
                {
                    continue;
                }
                let target_count = assignment_count(assigned_by_member, target);
                if target_count + 1 >= source_count {
                    continue;
                }
                let should_replace = candidate.as_ref().map_or(true, |(_, _, _, current_count)| {
                    target_count < *current_count
                });
                if should_replace {
                    candidate = Some((
                        partition.0.clone(),
                        partition.1,
                        target.clone(),
                        target_count,
                    ));
                }
            }
        }

        let Some((topic, partition, target, _)) = candidate else {
            return;
        };
        let partition = (topic, partition);
        remove_assignment(assigned_by_member, source, &partition);
        add_assignment(assigned_by_member, &target, &partition);
        owner_by_partition.insert(partition, target);
    }
}

fn add_assignment(
    assigned_by_member: &mut BTreeMap<String, BTreeMap<String, Vec<i32>>>,
    member_id: &str,
    partition: &(String, i32),
) {
    assigned_by_member
        .entry(member_id.to_owned())
        .or_default()
        .entry(partition.0.clone())
        .or_default()
        .push(partition.1);
}

fn remove_assignment(
    assigned_by_member: &mut BTreeMap<String, BTreeMap<String, Vec<i32>>>,
    member_id: &str,
    partition: &(String, i32),
) {
    let Some(topics) = assigned_by_member.get_mut(member_id) else {
        return;
    };
    let Some(partitions) = topics.get_mut(&partition.0) else {
        return;
    };
    partitions.retain(|candidate| *candidate != partition.1);
    if partitions.is_empty() {
        topics.remove(&partition.0);
    }
}

fn assignment_count(
    assigned_by_member: &BTreeMap<String, BTreeMap<String, Vec<i32>>>,
    member_id: &str,
) -> usize {
    assigned_by_member
        .get(member_id)
        .into_iter()
        .flat_map(|topics| topics.values())
        .map(Vec::len)
        .sum()
}

fn owned_partitions_from_assignments(
    assignments: &[ConsumerAssignment],
) -> Vec<ConsumerProtocolTopicAssignment> {
    let mut owned = BTreeMap::<String, Vec<i32>>::new();
    for assignment in assignments {
        owned
            .entry(assignment.topic().to_owned())
            .or_default()
            .push(assignment.partition());
    }
    owned
        .into_iter()
        .map(|(topic, mut partitions)| {
            partitions.sort_unstable();
            ConsumerProtocolTopicAssignment { topic, partitions }
        })
        .collect()
}

fn consumer_owned_partitions_from_assignments(
    assignments: &[ConsumerAssignment],
    topic_ids: &BTreeMap<String, [u8; 16]>,
) -> Vec<ConsumerGroupHeartbeatTopicPartitions> {
    let mut by_topic = BTreeMap::<[u8; 16], Vec<i32>>::new();
    for assignment in assignments {
        if let Some(topic_id) = topic_ids.get(assignment.topic()) {
            by_topic
                .entry(*topic_id)
                .or_default()
                .push(assignment.partition());
        }
    }
    by_topic
        .into_iter()
        .map(|(topic_id, mut partitions)| {
            partitions.sort_unstable();
            ConsumerGroupHeartbeatTopicPartitions {
                topic_id,
                partitions,
            }
        })
        .collect()
}

fn cooperative_assignment_requires_rejoin(
    members: &[JoinGroupMember],
    assignment: &ConsumerProtocolAssignmentV0,
) -> Result<bool> {
    let mut owned_partitions = BTreeSet::<(String, i32)>::new();
    for member in members {
        let subscription = ConsumerProtocolSubscriptionV1::decode(&member.metadata)?;
        for owned in subscription.owned_partitions {
            for partition in owned.partitions {
                owned_partitions.insert((owned.topic.clone(), partition));
            }
        }
    }

    let assigned_partitions = assignment
        .assignments
        .iter()
        .flat_map(|assigned| {
            assigned
                .partitions
                .iter()
                .map(move |partition| (assigned.topic.clone(), *partition))
        })
        .collect::<BTreeSet<_>>();

    Ok(owned_partitions
        .iter()
        .any(|partition| !assigned_partitions.contains(partition)))
}

fn cooperative_rejoin_required_for_assignment(
    members: &[JoinGroupMember],
    owned_partitions: &[ConsumerProtocolTopicAssignment],
    assignment: &ConsumerProtocolAssignmentV0,
    is_rejoin: bool,
) -> Result<bool> {
    if !members.is_empty() {
        return cooperative_assignment_requires_rejoin(members, assignment);
    }

    Ok(
        owned_partitions_require_rejoin(owned_partitions, assignment)
            || (!is_rejoin && assignment.assignments.is_empty()),
    )
}

fn owned_partitions_require_rejoin(
    owned_partitions: &[ConsumerProtocolTopicAssignment],
    assignment: &ConsumerProtocolAssignmentV0,
) -> bool {
    owned_partitions.iter().any(|owned| {
        owned.partitions.iter().any(|partition| {
            !assignment.assignments.iter().any(|assigned| {
                assigned.topic == owned.topic && assigned.partitions.contains(partition)
            })
        })
    })
}

fn encode_member_assignments(
    assigned_by_member: BTreeMap<String, BTreeMap<String, Vec<i32>>>,
) -> Result<Vec<SyncGroupAssignment>> {
    assigned_by_member
        .into_iter()
        .map(|(member_id, topics)| {
            let assignments = topics
                .into_iter()
                .map(|(topic, mut partitions)| {
                    partitions.sort_unstable();
                    ConsumerProtocolTopicAssignment { topic, partitions }
                })
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
    bootstrap: &mut Client,
    client_config: &ClientConfig,
    group_id: &str,
    offset_reset_policy: OffsetResetPolicy,
    assignment: &ConsumerProtocolAssignmentV0,
) -> Result<Vec<ConsumerAssignment>> {
    let mut assignments = Vec::new();
    let mut reset_partitions = Vec::new();
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
            match committed.filter(|offset| *offset >= 0) {
                Some(next_offset) => assignments.push(ConsumerAssignment::new(
                    topic.topic.clone(),
                    *partition,
                    next_offset,
                )),
                None => match offset_reset_policy {
                    OffsetResetPolicy::Offset(next_offset) => {
                        assignments.push(ConsumerAssignment::new(
                            topic.topic.clone(),
                            *partition,
                            next_offset,
                        ));
                    }
                    OffsetResetPolicy::Earliest | OffsetResetPolicy::Latest => {
                        reset_partitions.push((topic.topic.clone(), *partition));
                    }
                },
            }
        }
    }

    if let Some(timestamp) = offset_reset_policy.timestamp() {
        assignments.extend(
            resolve_reset_offsets(bootstrap, client_config, &reset_partitions, timestamp).await?,
        );
    }
    assignments.sort_by(|left, right| {
        left.topic()
            .cmp(right.topic())
            .then_with(|| left.partition().cmp(&right.partition()))
    });
    Ok(assignments)
}

async fn resolve_reset_offsets(
    bootstrap: &mut Client,
    client_config: &ClientConfig,
    partitions: &[(String, i32)],
    timestamp: i64,
) -> Result<Vec<ConsumerAssignment>> {
    if partitions.is_empty() {
        return Ok(Vec::new());
    }

    let topic_names = partitions
        .iter()
        .map(|(topic, _)| topic.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let metadata = bootstrap.metadata(Some(topic_names)).await?;
    let mut requests_by_broker = BTreeMap::<i32, BTreeMap<String, Vec<i32>>>::new();
    for (topic, partition) in partitions {
        let leader_id =
            offset_leader_for(&metadata, topic, *partition).map_err(|error| match error {
                Error::Broker { code, context } => client_config.broker_error(code, context),
                error => error,
            })?;
        requests_by_broker
            .entry(leader_id)
            .or_default()
            .entry(topic.clone())
            .or_default()
            .push(*partition);
    }

    let mut resolved = BTreeMap::new();
    for (leader_id, topics) in requests_by_broker {
        let broker_addr = offset_broker_addr_for(&metadata, leader_id)?;
        let mut leader = client_config.connect_broker(broker_addr).await?;
        let response = leader
            .list_offsets_v1(list_offsets_topics(topics, timestamp))
            .await?;
        for (topic, partition) in partitions {
            let partition_leader =
                offset_leader_for(&metadata, topic, *partition).map_err(|error| match error {
                    Error::Broker { code, context } => client_config.broker_error(code, context),
                    error => error,
                })?;
            if partition_leader != leader_id {
                continue;
            }
            let offset =
                list_offset(&response, topic, *partition).map_err(|error| match error {
                    Error::Broker { code, context } => client_config.broker_error(code, context),
                    error => error,
                })?;
            resolved.insert((topic.clone(), *partition), offset);
        }
    }

    Ok(resolved
        .into_iter()
        .map(|((topic, partition), offset)| ConsumerAssignment::new(topic, partition, offset))
        .collect())
}

fn offset_leader_for(
    metadata: &MetadataResponseV1,
    topic_name: &str,
    partition_index: i32,
) -> Result<i32> {
    let topic = metadata
        .topics
        .iter()
        .find(|topic| topic.name == topic_name)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic_name.to_owned(),
            partition: partition_index,
        })?;
    if topic.error_code != 0 {
        return Err(Error::Broker {
            code: topic.error_code,
            context: format!("metadata topic {topic_name}"),
        });
    }
    let partition = topic
        .partitions
        .iter()
        .find(|partition| partition.partition_index == partition_index)
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic_name.to_owned(),
            partition: partition_index,
        })?;
    if partition.error_code != 0 {
        return Err(Error::Broker {
            code: partition.error_code,
            context: format!("metadata {topic_name}-{partition_index}"),
        });
    }
    (partition.leader_id >= 0)
        .then_some(partition.leader_id)
        .ok_or_else(|| Error::MissingLeader {
            topic: topic_name.to_owned(),
            partition: partition_index,
        })
}

fn offset_broker_addr_for(metadata: &MetadataResponseV1, node_id: i32) -> Result<String> {
    metadata
        .brokers
        .iter()
        .find(|broker| broker.node_id == node_id)
        .map(|broker| format!("{}:{}", broker.host, broker.port))
        .ok_or(Error::MissingBroker { node_id })
}

fn list_offsets_topics(
    topics: BTreeMap<String, Vec<i32>>,
    timestamp: i64,
) -> Vec<ListOffsetsTopicV1> {
    topics
        .into_iter()
        .map(|(name, partitions)| ListOffsetsTopicV1 {
            name,
            partitions: partitions
                .into_iter()
                .map(|partition_index| ListOffsetsPartitionV1 {
                    partition_index,
                    timestamp,
                })
                .collect(),
        })
        .collect()
}

fn list_offset(
    response: &ListOffsetsResponseV1,
    topic_name: &str,
    partition_index: i32,
) -> Result<i64> {
    let partition = response
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
        })?;
    if partition.error_code != 0 {
        return Err(Error::Broker {
            code: partition.error_code,
            context: format!("list offsets {topic_name}-{partition_index}"),
        });
    }
    Ok(partition.offset)
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

fn leave_group_response_error(group_id: &str, response: &LeaveGroupResponseV3) -> Option<Error> {
    if response.error_code != 0 {
        return Some(Error::Broker {
            code: response.error_code,
            context: format!("leave group {group_id}"),
        });
    }
    response
        .members
        .iter()
        .find(|member| member.error_code != 0)
        .map(|member| Error::Broker {
            code: member.error_code,
            context: format!("leave group {group_id} member {}", member.member_id),
        })
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
                | BrokerErrorKind::FencedMemberEpoch
                | BrokerErrorKind::StaleMemberEpoch
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

fn topic_ids_for_names(
    metadata: &MetadataResponseV12,
    topic_names: &[String],
) -> Result<BTreeMap<String, [u8; 16]>> {
    let mut topic_ids = BTreeMap::new();
    for topic_name in topic_names {
        let topic = metadata
            .topics
            .iter()
            .find(|topic| topic.name.as_deref() == Some(topic_name.as_str()))
            .ok_or_else(|| Error::UnknownTopicOrPartition {
                topic: topic_name.clone(),
                partition: -1,
            })?;
        if topic.error_code != 0 {
            return Err(Error::Broker {
                code: topic.error_code,
                context: format!("metadata topic {topic_name}"),
            });
        }
        topic_ids.insert(topic_name.clone(), topic.topic_id);
    }
    Ok(topic_ids)
}

fn consumer_protocol_assignment(
    assignment: &[ConsumerGroupHeartbeatTopicPartitions],
    topic_ids: &BTreeMap<String, [u8; 16]>,
) -> Result<ConsumerProtocolAssignmentV0> {
    let names_by_id = topic_ids
        .iter()
        .map(|(name, topic_id)| (*topic_id, name.clone()))
        .collect::<BTreeMap<_, _>>();
    let assignments = assignment
        .iter()
        .map(|topic| {
            let topic_name =
                names_by_id
                    .get(&topic.topic_id)
                    .ok_or_else(|| Error::UnknownTopicOrPartition {
                        topic: format!("topic-id-{:02x?}", topic.topic_id),
                        partition: -1,
                    })?;
            Ok(ConsumerProtocolTopicAssignment {
                topic: topic_name.clone(),
                partitions: topic.partitions.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ConsumerProtocolAssignmentV0 {
        assignments,
        user_data: None,
    })
}

fn consumer_group_heartbeat_error(
    client: &ClientConfig,
    group_id: &str,
    response: &ConsumerGroupHeartbeatResponseV0,
) -> Error {
    let message = response
        .error_message
        .as_deref()
        .unwrap_or("broker returned a consumer-group heartbeat error");
    client.broker_error(
        response.error_code,
        format!("consumer group heartbeat {group_id}: {message}"),
    )
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
    use std::collections::BTreeMap;

    use super::{
        assignments_for_strategy, committed_offset, cooperative_assignment_requires_rejoin,
        cooperative_rejoin_required_for_assignment, cooperative_sticky_assignments,
        leave_group_response_error, list_offset, list_offsets_topics, offset_commit_response_error,
        offset_commit_topics, offset_commit_topics_v7, offset_fetch_topics, range_assignments,
        round_robin_assignments, should_rejoin_after_background_heartbeat, should_rejoin_group,
        validate_heartbeat_interval, ConsumerGroupAssignmentStrategy, ConsumerGroupConfig,
        ConsumerGroupHeartbeat, ConsumerGroupProtocol, HeartbeatHandleState, IsolationLevel,
        OffsetResetPolicy, SecurityProtocol,
    };
    use crate::consumer::ConsumerAssignment;
    use crate::Error;
    use kafrust_protocol::api::join_group::JoinGroupMember;
    use kafrust_protocol::api::leave_group::{LeaveGroupMemberResponse, LeaveGroupResponseV3};
    use kafrust_protocol::api::list_offsets::{
        ListOffsetsPartitionResponseV1, ListOffsetsResponseV1, ListOffsetsTopicResponseV1,
        EARLIEST_TIMESTAMP,
    };
    use kafrust_protocol::api::metadata::{MetadataResponseV1, PartitionMetadata, TopicMetadata};
    use kafrust_protocol::api::offset_commit::{
        OffsetCommitPartitionResponse, OffsetCommitTopicResponse,
    };
    use kafrust_protocol::api::offset_fetch::{
        OffsetFetchPartitionResponse, OffsetFetchTopicResponse,
    };
    use kafrust_protocol::consumer_group::{
        ConsumerProtocolAssignmentV0, ConsumerProtocolSubscriptionV0,
        ConsumerProtocolSubscriptionV1, ConsumerProtocolTopicAssignment,
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
        assert_eq!(
            config.offset_reset_policy_ref(),
            OffsetResetPolicy::Offset(5)
        );
    }

    #[test]
    fn configures_kip_848_consumer_protocol_and_server_assignor() {
        let config = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .server_assignor("uniform")
            .subscribe("orders");

        assert_eq!(config.group_protocol_ref(), ConsumerGroupProtocol::Consumer);
        assert_eq!(config.server_assignor_ref(), Some("uniform"));
    }

    #[test]
    fn configures_offset_reset_policy_and_preserves_offset_zero_default() {
        let default = ConsumerGroupConfig::new(["localhost:9092"], "orders-group");
        assert_eq!(
            default.offset_reset_policy_ref(),
            OffsetResetPolicy::Offset(0)
        );

        let latest = default
            .start_offset(12)
            .offset_reset_policy(OffsetResetPolicy::Latest);
        assert_eq!(latest.offset_reset_policy_ref(), OffsetResetPolicy::Latest);
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

    #[tokio::test]
    async fn rejects_classic_assignment_strategy_for_kip_848_before_connecting() {
        let error = ConsumerGroupConfig::new(["localhost:9092"], "orders-group")
            .group_protocol(ConsumerGroupProtocol::Consumer)
            .assignment_strategy(ConsumerGroupAssignmentStrategy::RoundRobin)
            .subscribe("orders")
            .join()
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            Error::Unsupported(
                "KIP-848 groups use a broker-side assignor; configure ConsumerGroupAssignmentStrategy::Range or select the classic protocol"
            )
        ));
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
    fn assigns_cooperatively_and_preserves_existing_ownership() {
        let members = vec![
            cooperative_member(
                "member-a",
                &["orders"],
                vec![ConsumerProtocolTopicAssignment {
                    topic: "orders".to_owned(),
                    partitions: vec![0, 1],
                }],
            ),
            cooperative_member(
                "member-b",
                &["orders"],
                vec![ConsumerProtocolTopicAssignment {
                    topic: "orders".to_owned(),
                    partitions: vec![2],
                }],
            ),
            cooperative_member("member-c", &["orders"], Vec::new()),
        ];
        let metadata = metadata_fixture("orders", &[0, 1, 2]);

        let assignments = cooperative_sticky_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        let member_b = decode_assignment(&assignments[1].assignment);
        let member_c = decode_assignment(&assignments[2].assignment);
        assert_eq!(assignments[0].member_id, "member-a");
        assert_eq!(member_a.assignments[0].partitions, vec![1]);
        assert_eq!(member_b.assignments[0].partitions, vec![2]);
        assert!(member_c.assignments.is_empty());
    }

    #[test]
    fn assigns_cooperative_new_members_without_immediate_partition_transfer() {
        let members = vec![
            cooperative_member("member-a", &["orders"], Vec::new()),
            cooperative_member("member-b", &["orders"], Vec::new()),
        ];
        let metadata = metadata_fixture("orders", &[0, 1, 2, 3]);

        let assignments = cooperative_sticky_assignments(&members, &metadata).unwrap();

        let member_a = decode_assignment(&assignments[0].assignment);
        let member_b = decode_assignment(&assignments[1].assignment);
        assert_eq!(member_a.assignments[0].partitions, vec![0, 2]);
        assert_eq!(member_b.assignments[0].partitions, vec![1, 3]);
    }

    #[test]
    fn requests_cooperative_rejoin_for_staged_transfer() {
        let members = vec![
            cooperative_member(
                "member-a",
                &["orders"],
                vec![ConsumerProtocolTopicAssignment {
                    topic: "orders".to_owned(),
                    partitions: vec![0, 1, 2, 3],
                }],
            ),
            cooperative_member("member-b", &["orders"], Vec::new()),
        ];
        let assignment = ConsumerProtocolAssignmentV0 {
            assignments: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0, 2],
            }],
            user_data: None,
        };

        assert!(cooperative_assignment_requires_rejoin(&members, &assignment).unwrap());
    }

    #[test]
    fn does_not_request_cooperative_rejoin_after_member_loss() {
        let members = vec![cooperative_member(
            "member-a",
            &["orders"],
            vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0],
            }],
        )];
        let assignment = ConsumerProtocolAssignmentV0 {
            assignments: vec![ConsumerProtocolTopicAssignment {
                topic: "orders".to_owned(),
                partitions: vec![0, 1],
            }],
            user_data: None,
        };

        assert!(!cooperative_assignment_requires_rejoin(&members, &assignment).unwrap());
    }

    #[test]
    fn requests_one_rejoin_for_new_non_leader_with_empty_assignment() {
        let assignment = ConsumerProtocolAssignmentV0 {
            assignments: Vec::new(),
            user_data: None,
        };

        assert!(cooperative_rejoin_required_for_assignment(&[], &[], &assignment, false,).unwrap());
        assert!(!cooperative_rejoin_required_for_assignment(&[], &[], &assignment, true,).unwrap());
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
    fn builds_list_offsets_topics_and_reads_partition_offset() {
        let topics = BTreeMap::from([
            ("orders".to_owned(), vec![0, 2]),
            ("payments".to_owned(), vec![1]),
        ]);

        let request = list_offsets_topics(topics, EARLIEST_TIMESTAMP);
        assert_eq!(request[0].name, "orders");
        assert_eq!(request[0].partitions[0].partition_index, 0);
        assert_eq!(request[0].partitions[0].timestamp, EARLIEST_TIMESTAMP);
        assert_eq!(request[1].name, "payments");

        let response = ListOffsetsResponseV1 {
            topics: vec![ListOffsetsTopicResponseV1 {
                name: "orders".to_owned(),
                partitions: vec![ListOffsetsPartitionResponseV1 {
                    partition_index: 2,
                    error_code: 0,
                    timestamp: -1,
                    offset: 41,
                }],
            }],
        };
        assert_eq!(list_offset(&response, "orders", 2).unwrap(), 41);
        assert!(matches!(
            list_offset(&response, "orders", 0).unwrap_err(),
            Error::UnknownTopicOrPartition { .. }
        ));
    }

    #[test]
    fn surfaces_list_offsets_partition_error() {
        let response = ListOffsetsResponseV1 {
            topics: vec![ListOffsetsTopicResponseV1 {
                name: "orders".to_owned(),
                partitions: vec![ListOffsetsPartitionResponseV1 {
                    partition_index: 0,
                    error_code: 3,
                    timestamp: -1,
                    offset: -1,
                }],
            }],
        };

        assert!(matches!(
            list_offset(&response, "orders", 0).unwrap_err(),
            Error::Broker {
                code: 3,
                context
            } if context == "list offsets orders-0"
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
    fn surfaces_leave_group_member_error() {
        let response = LeaveGroupResponseV3 {
            throttle_time_ms: 0,
            error_code: 0,
            members: vec![LeaveGroupMemberResponse {
                member_id: "member-a".to_owned(),
                group_instance_id: Some("orders-reader-1".to_owned()),
                error_code: 82,
            }],
        };

        let error = leave_group_response_error("orders-group", &response).unwrap();
        assert!(matches!(error, Error::Broker { code: 82, .. }));
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
        assert_eq!(heartbeat.interval(), std::time::Duration::from_millis(100));
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

    fn cooperative_member(
        member_id: &str,
        topics: &[&str],
        owned_partitions: Vec<ConsumerProtocolTopicAssignment>,
    ) -> JoinGroupMember {
        JoinGroupMember {
            member_id: member_id.to_owned(),
            metadata: ConsumerProtocolSubscriptionV1 {
                topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
                user_data: None,
                owned_partitions,
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
            interval: std::time::Duration::from_millis(100),
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
