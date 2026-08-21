//! Blocking adapters for the core asynchronous kafrust clients.
//!
//! The adapters own a dedicated multi-thread Tokio runtime and execute each operation
//! synchronously. They are intended for applications that cannot expose async
//! APIs at their integration boundary. They must not be constructed or used
//! from inside an existing Tokio runtime; doing so returns an error instead of
//! panicking.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};

use crate::producer::{BufferedProducer, BufferedProducerHandle, Producer, ProducerDelivery};
use crate::streams::{
    StreamsGroupConfig, StreamsGroupHeartbeatResponseV0, StreamsGroupHeartbeatTask,
    StreamsGroupHeartbeatTaskOffset, StreamsGroupSession, StreamsGroupSessionAssignment,
};
use crate::{
    AdminClient, AlterConfigsOptions, AlterConfigsResult, AlterConsumerGroupOffsetsResult,
    ClientConfig, ClusterDescription, Consumer, ConsumerAssignment, ConsumerConfig, ConsumerGroup,
    ConsumerGroupConfig, ConsumerGroupDescription, ConsumerGroupMetadata, ConsumerGroupOffset,
    ConsumerGroupOffsetQuery, ConsumerGroupProtocol, ConsumerRecord, CreatePartitionsOptions,
    CreatePartitionsResult, CreateTopicsOptions, CreateTopicsResult, DeleteConsumerGroupResult,
    DeleteRecordsOptions, DeleteRecordsResult, DeleteTopicsOptions, DeleteTopicsResult,
    DescribeConfigsOptions, DescribeConfigsResult, Error, FeatureMetadata, GroupListing,
    LeaderEpochOffset, ListConsumerGroupOffsetsResult, ListGroupsOptions,
    ModernConsumerGroupDescription, NewPartitions, PartitionWatermarks, ProducerBatchReport,
    ProducerConfig, ProducerRecord, RecordMetadata, Result, ShareAcknowledgementType,
    ShareConsumer, ShareConsumerConfig, ShareRecord, TopicConfigAlteration, TopicConfigResource,
    TopicConfigUpdate, TopicListing, TransactionStatus, UpdateFeaturesOptions,
    UpdateFeaturesResult,
};

const NESTED_RUNTIME_MESSAGE: &str =
    "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead";

fn build_runtime() -> Result<Runtime> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::Unsupported(NESTED_RUNTIME_MESSAGE));
    }
    Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(Error::Io)
}

fn block_on<T>(runtime: &Runtime, future: impl Future<Output = Result<T>>) -> Result<T> {
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(Error::Unsupported(NESTED_RUNTIME_MESSAGE));
    }
    runtime.block_on(future)
}

/// Synchronous adapter for [`crate::AdminClient`] operations.
///
/// The adapter covers the broker/controller Admin APIs, including security,
/// quota, reassignment, storage, Share Group State, group, and transaction
/// diagnostics. It owns a dedicated Tokio runtime and must be used from
/// outside an existing Tokio runtime.
pub struct BlockingAdminClient {
    admin: AdminClient,
    runtime: Runtime,
}

impl BlockingAdminClient {
    /// Validates the client configuration and creates a synchronous Admin
    /// client without opening a broker connection.
    pub fn build(config: ClientConfig) -> Result<Self> {
        let runtime = build_runtime()?;
        let admin = AdminClient::new(config).build_config()?;
        Ok(Self { admin, runtime })
    }

    /// Describes a cluster using Kafka's dedicated DescribeCluster options.
    pub fn describe_cluster_with_options(
        &self,
        options: crate::DescribeClusterOptions,
    ) -> Result<ClusterDescription> {
        block_on(
            &self.runtime,
            self.admin.describe_cluster_with_options(options),
        )
    }

    /// Adds a voter to the active KRaft controller quorum.
    pub fn add_raft_voter(
        &self,
        options: crate::AddRaftVoterOptions,
    ) -> Result<crate::RaftVoterResult> {
        block_on(&self.runtime, self.admin.add_raft_voter(options))
    }

    /// Removes a voter from the active KRaft controller quorum.
    pub fn remove_raft_voter(
        &self,
        options: crate::RemoveRaftVoterOptions,
    ) -> Result<crate::RaftVoterResult> {
        block_on(&self.runtime, self.admin.remove_raft_voter(options))
    }

    /// Unregisters a broker through the active KRaft controller.
    pub fn unregister_broker(&self, broker_id: i32) -> Result<crate::UnregisterBrokerResult> {
        block_on(&self.runtime, self.admin.unregister_broker(broker_id))
    }

    /// Describes topic partitions through Kafka's flexible API.
    pub fn describe_topic_partitions(
        &self,
        topics: &[String],
        options: crate::DescribeTopicPartitionsOptions,
    ) -> Result<crate::DescribeTopicPartitionsResult> {
        block_on(
            &self.runtime,
            self.admin.describe_topic_partitions(topics, options),
        )
    }

    /// Describes the KRaft metadata quorum.
    pub fn describe_quorum(
        &self,
        topics: &[crate::DescribeQuorumTopic],
    ) -> Result<crate::DescribeQuorumResult> {
        block_on(&self.runtime, self.admin.describe_quorum(topics))
    }

    /// Lists ACL bindings matching a Kafka ACL filter.
    pub fn describe_acls(&self, filter: &crate::AclFilter) -> Result<crate::DescribeAclsResult> {
        block_on(&self.runtime, self.admin.describe_acls(filter))
    }

    /// Creates ACL bindings and preserves per-binding broker results.
    pub fn create_acls(&self, bindings: &[crate::AclBinding]) -> Result<crate::CreateAclsResult> {
        block_on(&self.runtime, self.admin.create_acls(bindings))
    }

    /// Deletes ACL bindings matching each filter.
    pub fn delete_acls(&self, filters: &[crate::AclFilter]) -> Result<crate::DeleteAclsResult> {
        block_on(&self.runtime, self.admin.delete_acls(filters))
    }

    /// Describes client quotas matching a typed quota filter.
    pub fn describe_client_quotas(
        &self,
        filter: &crate::ClientQuotaFilter,
    ) -> Result<crate::DescribeClientQuotasResult> {
        block_on(&self.runtime, self.admin.describe_client_quotas(filter))
    }

    /// Alters client quotas and preserves entity-level broker results.
    pub fn alter_client_quotas(
        &self,
        alterations: &[crate::ClientQuotaAlteration],
        validate_only: bool,
    ) -> Result<crate::AlterClientQuotasResult> {
        block_on(
            &self.runtime,
            self.admin.alter_client_quotas(alterations, validate_only),
        )
    }

    /// Describes SCRAM credentials for all or selected users.
    pub fn describe_user_scram_credentials(
        &self,
        users: Option<&[String]>,
    ) -> Result<crate::DescribeUserScramCredentialsResult> {
        block_on(
            &self.runtime,
            self.admin.describe_user_scram_credentials(users),
        )
    }

    /// Creates, replaces, or deletes SCRAM credentials.
    pub fn alter_user_scram_credentials(
        &self,
        deletions: &[crate::ScramCredentialDeletion],
        upsertions: &[crate::ScramCredentialUpsertion],
    ) -> Result<crate::AlterUserScramCredentialsResult> {
        block_on(
            &self.runtime,
            self.admin
                .alter_user_scram_credentials(deletions, upsertions),
        )
    }

    /// Creates a delegation token through the active controller.
    pub fn create_delegation_token(
        &self,
        options: crate::CreateDelegationTokenOptions,
    ) -> Result<crate::CreatedDelegationToken> {
        block_on(&self.runtime, self.admin.create_delegation_token(options))
    }

    /// Describes delegation tokens visible to the authenticated principal.
    pub fn describe_delegation_tokens(
        &self,
        owners: Option<&[crate::DelegationTokenPrincipal]>,
    ) -> Result<crate::DescribeDelegationTokensResult> {
        block_on(&self.runtime, self.admin.describe_delegation_tokens(owners))
    }

    /// Renews a delegation token without logging its HMAC.
    pub fn renew_delegation_token(
        &self,
        hmac: &[u8],
        renew_period: Duration,
    ) -> Result<crate::DelegationTokenOperationResult> {
        block_on(
            &self.runtime,
            self.admin.renew_delegation_token(hmac, renew_period),
        )
    }

    /// Expires a delegation token without logging its HMAC.
    pub fn expire_delegation_token(
        &self,
        hmac: &[u8],
        expiry_time_period: Duration,
    ) -> Result<crate::DelegationTokenOperationResult> {
        block_on(
            &self.runtime,
            self.admin.expire_delegation_token(hmac, expiry_time_period),
        )
    }

    /// Elects preferred or unclean leaders through the active controller.
    pub fn elect_leaders(
        &self,
        elections: Option<&[crate::LeaderElection]>,
        election_type: crate::ElectionType,
        options: crate::ElectLeadersOptions,
    ) -> Result<crate::ElectLeadersResult> {
        block_on(
            &self.runtime,
            self.admin.elect_leaders(elections, election_type, options),
        )
    }

    /// Starts or cancels partition reassignments.
    pub fn alter_partition_reassignments(
        &self,
        reassignments: &[crate::PartitionReassignment],
        options: crate::PartitionReassignmentOptions,
    ) -> Result<crate::AlterPartitionReassignmentsResult> {
        block_on(
            &self.runtime,
            self.admin
                .alter_partition_reassignments(reassignments, options),
        )
    }

    /// Lists partition reassignments still in progress.
    pub fn list_partition_reassignments(
        &self,
        topics: Option<&[crate::PartitionReassignmentQuery]>,
        options: crate::PartitionReassignmentOptions,
    ) -> Result<crate::ListPartitionReassignmentsResult> {
        block_on(
            &self.runtime,
            self.admin.list_partition_reassignments(topics, options),
        )
    }

    /// Lists broker configuration resource types.
    pub fn list_config_resources(
        &self,
        options: crate::ListConfigResourcesOptions,
    ) -> Result<crate::ListConfigResourcesResult> {
        block_on(&self.runtime, self.admin.list_config_resources(options))
    }

    /// Describes Share groups through their group coordinators.
    pub fn describe_share_groups(
        &self,
        group_ids: &[String],
        include_authorized_operations: bool,
    ) -> Result<Vec<crate::ShareGroupDescription>> {
        block_on(
            &self.runtime,
            self.admin
                .describe_share_groups(group_ids, include_authorized_operations),
        )
    }

    /// Describes Streams groups through their group coordinators.
    pub fn describe_streams_groups(
        &self,
        group_ids: &[String],
        include_authorized_operations: bool,
    ) -> Result<Vec<crate::StreamsGroupDescription>> {
        block_on(
            &self.runtime,
            self.admin
                .describe_streams_groups(group_ids, include_authorized_operations),
        )
    }

    /// Deletes Share groups through their coordinators.
    pub fn delete_share_groups(
        &self,
        group_ids: &[String],
    ) -> Result<Vec<crate::DeleteShareGroupResult>> {
        block_on(&self.runtime, self.admin.delete_share_groups(group_ids))
    }

    /// Initializes Share Group State for selected topic partitions.
    pub fn initialize_share_group_state(
        &self,
        group_id: &str,
        topics: &[crate::ShareGroupStateInitializeTopic],
    ) -> Result<crate::ShareGroupStateResult> {
        block_on(
            &self.runtime,
            self.admin.initialize_share_group_state(group_id, topics),
        )
    }

    /// Reads Share Group State for selected topic partitions.
    pub fn read_share_group_state(
        &self,
        group_id: &str,
        topics: &[crate::ShareGroupStateReadTopic],
    ) -> Result<crate::ReadShareGroupStateResult> {
        block_on(
            &self.runtime,
            self.admin.read_share_group_state(group_id, topics),
        )
    }

    /// Writes Share Group State for selected topic partitions.
    pub fn write_share_group_state(
        &self,
        group_id: &str,
        topics: &[crate::ShareGroupStateWriteTopic],
    ) -> Result<crate::ShareGroupStateResult> {
        block_on(
            &self.runtime,
            self.admin.write_share_group_state(group_id, topics),
        )
    }

    /// Deletes Share Group State for selected topic partitions.
    pub fn delete_share_group_state(
        &self,
        group_id: &str,
        topics: &[crate::ShareGroupStateDeleteTopic],
    ) -> Result<crate::ShareGroupStateResult> {
        block_on(
            &self.runtime,
            self.admin.delete_share_group_state(group_id, topics),
        )
    }

    /// Reads the compact Share Group State summary.
    pub fn read_share_group_state_summary(
        &self,
        group_id: &str,
        topics: &[crate::ShareGroupStateReadTopic],
    ) -> Result<crate::ReadShareGroupStateSummaryResult> {
        block_on(
            &self.runtime,
            self.admin.read_share_group_state_summary(group_id, topics),
        )
    }

    /// Alters Share Group offsets.
    pub fn alter_share_group_offsets(
        &self,
        group_id: &str,
        offsets: &[crate::ShareGroupOffset],
    ) -> Result<crate::AlterShareGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.alter_share_group_offsets(group_id, offsets),
        )
    }

    /// Deletes Share Group offsets for selected topics.
    pub fn delete_share_group_offsets(
        &self,
        group_id: &str,
        topics: &[String],
    ) -> Result<crate::DeleteShareGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.delete_share_group_offsets(group_id, topics),
        )
    }

    /// Lists Share Group offsets for all or selected topics.
    pub fn list_share_group_offsets(
        &self,
        group_id: &str,
        topics: Option<&[crate::ShareGroupOffsetQuery]>,
    ) -> Result<crate::ListShareGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.list_share_group_offsets(group_id, topics),
        )
    }

    /// Describes broker-local log directories and replica storage state.
    pub fn describe_log_dirs(
        &self,
        broker_ids: Option<&[i32]>,
        topics: Option<&[crate::LogDirTopic]>,
    ) -> Result<Vec<crate::DescribeLogDirsBrokerResult>> {
        block_on(
            &self.runtime,
            self.admin.describe_log_dirs(broker_ids, topics),
        )
    }

    /// Moves selected replica logs to broker-local directories.
    pub fn alter_replica_log_dirs(
        &self,
        broker_id: i32,
        assignments: &[crate::ReplicaLogDirAssignment],
    ) -> Result<crate::AlterReplicaLogDirsResult> {
        block_on(
            &self.runtime,
            self.admin.alter_replica_log_dirs(broker_id, assignments),
        )
    }

    /// Deletes committed offsets for selected classic consumer-group topics.
    pub fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        topics: &[crate::ConsumerGroupOffsetDelete],
    ) -> Result<crate::DeleteConsumerGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.delete_consumer_group_offsets(group_id, topics),
        )
    }

    /// Describes active producers for selected topic partitions.
    pub fn describe_producers(
        &self,
        topics: &[crate::DescribeProducersTopic],
    ) -> Result<crate::DescribeProducersResult> {
        block_on(&self.runtime, self.admin.describe_producers(topics))
    }

    /// Describes transactional IDs through their transaction coordinators.
    pub fn describe_transactions(
        &self,
        transactional_ids: &[String],
    ) -> Result<crate::DescribeTransactionsResult> {
        block_on(
            &self.runtime,
            self.admin.describe_transactions(transactional_ids),
        )
    }

    /// Lists active transactions across transaction-coordinator shards.
    pub fn list_transactions(
        &self,
        options: crate::ListTransactionsOptions,
    ) -> Result<crate::ListTransactionsResult> {
        block_on(&self.runtime, self.admin.list_transactions(options))
    }

    /// Describes the Kafka cluster and active controller.
    pub fn describe_cluster(&self) -> Result<ClusterDescription> {
        block_on(&self.runtime, self.admin.describe_cluster())
    }

    /// Lists topics visible to the configured principal.
    pub fn list_topics(&self) -> Result<Vec<TopicListing>> {
        block_on(&self.runtime, self.admin.list_topics())
    }

    /// Lists groups across the advertised brokers.
    pub fn list_groups(&self) -> Result<Vec<GroupListing>> {
        block_on(&self.runtime, self.admin.list_groups())
    }

    /// Lists groups with broker-negotiated state and type filters.
    pub fn list_groups_with_options(
        &self,
        options: ListGroupsOptions,
    ) -> Result<Vec<GroupListing>> {
        block_on(&self.runtime, self.admin.list_groups_with_options(options))
    }

    /// Creates topics through the active controller.
    pub fn create_topics(
        &self,
        topics: &[crate::NewTopic],
        options: CreateTopicsOptions,
    ) -> Result<CreateTopicsResult> {
        block_on(&self.runtime, self.admin.create_topics(topics, options))
    }

    /// Deletes topics through the active controller.
    pub fn delete_topics(
        &self,
        topic_names: &[String],
        options: DeleteTopicsOptions,
    ) -> Result<DeleteTopicsResult> {
        block_on(
            &self.runtime,
            self.admin.delete_topics(topic_names, options),
        )
    }

    /// Increases partition counts through the active controller.
    pub fn create_partitions(
        &self,
        topics: &[NewPartitions],
        options: CreatePartitionsOptions,
    ) -> Result<CreatePartitionsResult> {
        block_on(&self.runtime, self.admin.create_partitions(topics, options))
    }

    /// Deletes records before the requested offsets.
    pub fn delete_records(
        &self,
        topics: &[crate::DeleteRecordsTopic],
        options: DeleteRecordsOptions,
    ) -> Result<DeleteRecordsResult> {
        block_on(&self.runtime, self.admin.delete_records(topics, options))
    }

    /// Reads finalized broker feature metadata.
    pub fn describe_features(&self) -> Result<FeatureMetadata> {
        block_on(&self.runtime, self.admin.describe_features())
    }

    /// Updates finalized broker feature levels through the active controller.
    pub fn update_features(
        &self,
        updates: &[crate::FeatureUpdate],
        options: UpdateFeaturesOptions,
    ) -> Result<UpdateFeaturesResult> {
        block_on(&self.runtime, self.admin.update_features(updates, options))
    }

    /// Describes topic configuration resources.
    pub fn describe_topic_configs(
        &self,
        resources: &[TopicConfigResource],
        options: DescribeConfigsOptions,
    ) -> Result<DescribeConfigsResult> {
        block_on(
            &self.runtime,
            self.admin.describe_topic_configs(resources, options),
        )
    }

    /// Incrementally alters topic configuration resources.
    pub fn incremental_alter_topic_configs(
        &self,
        resources: &[TopicConfigAlteration],
        options: AlterConfigsOptions,
    ) -> Result<AlterConfigsResult> {
        block_on(
            &self.runtime,
            self.admin
                .incremental_alter_topic_configs(resources, options),
        )
    }

    /// Replaces dynamic topic configuration resources.
    pub fn alter_topic_configs(
        &self,
        resources: &[TopicConfigUpdate],
        options: AlterConfigsOptions,
    ) -> Result<AlterConfigsResult> {
        block_on(
            &self.runtime,
            self.admin.alter_topic_configs(resources, options),
        )
    }

    /// Describes classic consumer groups through their coordinators.
    pub fn describe_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<Vec<ConsumerGroupDescription>> {
        block_on(
            &self.runtime,
            self.admin.describe_consumer_groups(group_ids),
        )
    }

    /// Describes KIP-848 consumer groups through ConsumerGroupDescribe.
    pub fn describe_consumer_groups_modern(
        &self,
        group_ids: &[String],
        include_authorized_operations: bool,
    ) -> Result<Vec<ModernConsumerGroupDescription>> {
        block_on(
            &self.runtime,
            self.admin
                .describe_consumer_groups_modern(group_ids, include_authorized_operations),
        )
    }

    /// Deletes consumer groups through their coordinators.
    pub fn delete_consumer_groups(
        &self,
        group_ids: &[String],
    ) -> Result<Vec<DeleteConsumerGroupResult>> {
        block_on(&self.runtime, self.admin.delete_consumer_groups(group_ids))
    }

    /// Lists committed offsets for a classic consumer group.
    pub fn list_consumer_group_offsets(
        &self,
        group_id: &str,
        topics: Option<&[ConsumerGroupOffsetQuery]>,
    ) -> Result<ListConsumerGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.list_consumer_group_offsets(group_id, topics),
        )
    }

    /// Lists committed offsets through the KIP-848 member-aware API.
    pub fn list_consumer_group_offsets_with_member(
        &self,
        group_id: &str,
        member_id: Option<&str>,
        member_epoch: i32,
        topics: Option<&[ConsumerGroupOffsetQuery]>,
        require_stable: bool,
    ) -> Result<ListConsumerGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.list_consumer_group_offsets_with_member(
                group_id,
                member_id,
                member_epoch,
                topics,
                require_stable,
            ),
        )
    }

    /// Alters committed offsets for a classic consumer group.
    pub fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        offsets: &[ConsumerGroupOffset],
    ) -> Result<AlterConsumerGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.alter_consumer_group_offsets(group_id, offsets),
        )
    }

    /// Alters committed offsets through the KIP-848 member-aware API.
    pub fn alter_consumer_group_offsets_with_member(
        &self,
        group_id: &str,
        member_id: &str,
        member_epoch: i32,
        group_instance_id: Option<&str>,
        offsets: &[ConsumerGroupOffset],
    ) -> Result<AlterConsumerGroupOffsetsResult> {
        block_on(
            &self.runtime,
            self.admin.alter_consumer_group_offsets_with_member(
                group_id,
                member_id,
                member_epoch,
                group_instance_id,
                offsets,
            ),
        )
    }
}

/// Synchronous adapter for [`crate::producer::Producer`].
///
/// The adapter owns a dedicated Tokio runtime. It is suitable for a
/// synchronous application thread and preserves the asynchronous producer's
/// retry, idempotence, transaction, compression, and delivery semantics.
/// Construct it once and reuse it; creating one per record would create an
/// unnecessary runtime and broker connection.
pub struct BlockingProducer {
    producer: Producer,
    runtime: Runtime,
}

/// Synchronous adapter for the bounded [`crate::BufferedProducer`] path.
///
/// The adapter preserves the asynchronous producer worker, queue backpressure,
/// linger batching, delivery handles, and transaction ordering while exposing
/// blocking methods for synchronous integrations.
pub struct BlockingBufferedProducer {
    producer: BufferedProducer,
    runtime: Arc<Runtime>,
}

impl BlockingBufferedProducer {
    /// Builds a buffered producer and its dedicated multi-thread runtime.
    pub fn build(config: ProducerConfig) -> Result<Self> {
        let runtime = Arc::new(build_runtime()?);
        let producer = block_on(runtime.as_ref(), config.build_buffered())?;
        Ok(Self { producer, runtime })
    }

    /// Enqueues one record and returns its delivery handle.
    pub fn send(&mut self, record: ProducerRecord) -> Result<ProducerDelivery> {
        block_on(self.runtime.as_ref(), self.producer.send(record))
    }

    /// Waits for a delivery handle on this producer's runtime.
    pub fn wait_delivery(&self, delivery: ProducerDelivery) -> Result<RecordMetadata> {
        block_on(self.runtime.as_ref(), delivery.wait())
    }

    /// Returns a cloneable blocking enqueue handle for non-transactional use.
    pub fn handle(&self) -> Result<BlockingBufferedProducerHandle> {
        Ok(BlockingBufferedProducerHandle {
            handle: self.producer.handle()?,
            runtime: Arc::clone(&self.runtime),
        })
    }

    /// Starts a transaction on a transactional buffered producer.
    pub fn begin_transaction(&mut self) -> Result<()> {
        block_on(self.runtime.as_ref(), self.producer.begin_transaction())
    }

    /// Adds consumer-group offsets to the active buffered transaction.
    pub fn send_group_offsets_to_transaction(
        &mut self,
        metadata: &ConsumerGroupMetadata,
        assignments: &[ConsumerAssignment],
    ) -> Result<()> {
        block_on(
            self.runtime.as_ref(),
            self.producer
                .send_group_offsets_to_transaction(metadata, assignments),
        )
    }

    /// Commits the active buffered transaction.
    pub fn commit_transaction(&mut self) -> Result<()> {
        block_on(self.runtime.as_ref(), self.producer.commit_transaction())
    }

    /// Aborts the active buffered transaction.
    pub fn abort_transaction(&mut self) -> Result<()> {
        block_on(self.runtime.as_ref(), self.producer.abort_transaction())
    }

    /// Flushes all accepted records and waits for their delivery outcomes.
    pub fn flush(&mut self) -> Result<()> {
        block_on(self.runtime.as_ref(), self.producer.flush())
    }

    /// Flushes and closes the buffered producer.
    pub fn close(&mut self) -> Result<()> {
        block_on(self.runtime.as_ref(), self.producer.close())
    }

    /// Returns whether a transaction is currently active.
    pub fn in_transaction(&self) -> bool {
        self.producer.in_transaction()
    }

    /// Returns the buffered transaction lifecycle state.
    pub fn transaction_status(&self) -> Option<TransactionStatus> {
        self.producer.transaction_status()
    }

    /// Returns whether the buffered producer has been closed.
    pub fn is_closed(&self) -> bool {
        self.producer.is_closed()
    }
}

/// Cloneable blocking enqueue handle for a non-transactional buffered producer.
#[derive(Clone, Debug)]
pub struct BlockingBufferedProducerHandle {
    handle: BufferedProducerHandle,
    runtime: Arc<Runtime>,
}

impl BlockingBufferedProducerHandle {
    /// Enqueues one record on the shared bounded queue.
    pub fn send(&self, record: ProducerRecord) -> Result<ProducerDelivery> {
        block_on(self.runtime.as_ref(), self.handle.send(record))
    }

    /// Waits for a delivery handle on the shared producer runtime.
    pub fn wait_delivery(&self, delivery: ProducerDelivery) -> Result<RecordMetadata> {
        block_on(self.runtime.as_ref(), delivery.wait())
    }
}

impl BlockingProducer {
    /// Validates the configuration, opens the initial broker connection, and
    /// returns a synchronous producer.
    pub fn build(config: ProducerConfig) -> Result<Self> {
        let runtime = build_runtime()?;
        let producer = block_on(&runtime, config.build())?;
        Ok(Self { producer, runtime })
    }

    /// Sends one record and waits for its Kafka delivery result.
    pub fn send(&mut self, record: ProducerRecord) -> Result<RecordMetadata> {
        block_on(&self.runtime, self.producer.send(record))
    }

    /// Sends multiple records and returns metadata in input order.
    pub fn send_batch(
        &mut self,
        records: impl IntoIterator<Item = ProducerRecord>,
    ) -> Result<Vec<RecordMetadata>> {
        block_on(&self.runtime, self.producer.send_batch(records))
    }

    /// Sends multiple records and preserves per-record broker outcomes.
    pub fn send_batch_report(
        &mut self,
        records: impl IntoIterator<Item = ProducerRecord>,
    ) -> Result<ProducerBatchReport> {
        block_on(&self.runtime, self.producer.send_batch_report(records))
    }

    /// Starts a transaction on a transactional producer.
    pub fn begin_transaction(&mut self) -> Result<()> {
        self.producer.begin_transaction()
    }

    /// Adds consumer offsets to the active transaction with group-generation
    /// fencing.
    pub fn send_group_offsets_to_transaction(
        &mut self,
        metadata: &crate::ConsumerGroupMetadata,
        assignments: &[ConsumerAssignment],
    ) -> Result<()> {
        block_on(
            &self.runtime,
            self.producer
                .send_group_offsets_to_transaction(metadata, assignments),
        )
    }

    /// Commits the active transaction and waits for the broker outcome.
    pub fn commit_transaction(&mut self) -> Result<()> {
        block_on(&self.runtime, self.producer.commit_transaction())
    }

    /// Aborts the active transaction and waits for the broker outcome.
    pub fn abort_transaction(&mut self) -> Result<()> {
        block_on(&self.runtime, self.producer.abort_transaction())
    }

    /// Returns whether a transaction is currently active.
    pub fn in_transaction(&self) -> bool {
        self.producer.in_transaction()
    }

    /// Returns the producer transaction lifecycle state, if configured.
    pub fn transaction_status(&self) -> Option<TransactionStatus> {
        self.producer.transaction_status()
    }
}

/// Synchronous adapter for manually assigned [`crate::Consumer`] partitions.
///
/// Group membership is exposed by [`BlockingConsumerGroup`]. This direct
/// consumer remains useful when assignment is managed by the application.
pub struct BlockingConsumer {
    consumer: Consumer,
    runtime: Runtime,
}

impl BlockingConsumer {
    /// Validates the configuration, opens the initial broker connection, and
    /// returns a synchronous direct consumer.
    pub fn build(config: ConsumerConfig) -> Result<Self> {
        let runtime = build_runtime()?;
        let consumer = block_on(&runtime, config.build())?;
        Ok(Self { consumer, runtime })
    }

    /// Assigns a topic partition and its next fetch offset.
    pub fn assign(&mut self, topic: impl Into<String>, partition: i32, offset: i64) {
        self.consumer.assign(topic, partition, offset);
    }

    /// Returns the current direct-consumer assignments.
    pub fn assignments(&self) -> &[ConsumerAssignment] {
        self.consumer.assignments()
    }

    /// Returns the current position for an assigned partition.
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.consumer.position(topic, partition)
    }

    /// Seeks an assigned partition to an absolute offset.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.consumer.seek(topic, partition, offset)
    }

    /// Pauses fetching for an assigned partition.
    pub fn pause(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.consumer.pause(topic, partition)
    }

    /// Resumes fetching for an assigned partition.
    pub fn resume(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.consumer.resume(topic, partition)
    }

    /// Polls assigned partitions and waits for the asynchronous fetch work to
    /// complete.
    pub fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        block_on(&self.runtime, self.consumer.poll())
    }

    /// Fetches one topic partition without changing assignment state.
    pub fn fetch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
    ) -> Result<Vec<ConsumerRecord>> {
        block_on(&self.runtime, self.consumer.fetch(topic, partition, offset))
    }

    /// Fetches the earliest and latest available offsets for a partition.
    pub fn fetch_watermarks(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<PartitionWatermarks> {
        block_on(
            &self.runtime,
            self.consumer.fetch_watermarks(topic, partition),
        )
    }

    /// Resolves the end offset for a partition leader epoch.
    pub fn offset_for_leader_epoch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<LeaderEpochOffset> {
        block_on(
            &self.runtime,
            self.consumer.offset_for_leader_epoch(
                topic,
                partition,
                current_leader_epoch,
                leader_epoch,
            ),
        )
    }
}

/// Synchronous adapter for a joined [`crate::ConsumerGroup`].
///
/// Each `poll` performs the foreground heartbeat and group recovery work of
/// the asynchronous API. Configure automatic commits on
/// [`ConsumerGroupConfig`] when the group should also flush offsets in a
/// background task. Explicit background heartbeat-task handles remain an
/// asynchronous API because their lifecycle must be owned by the caller.
pub struct BlockingConsumerGroup {
    group: ConsumerGroup,
    runtime: Runtime,
}

impl BlockingConsumerGroup {
    /// Joins a Kafka consumer group and waits for its initial assignment.
    pub fn join(config: ConsumerGroupConfig) -> Result<Self> {
        let runtime = build_runtime()?;
        let group = block_on(&runtime, config.join())?;
        Ok(Self { group, runtime })
    }

    /// Returns the Kafka consumer group ID.
    pub fn group_id(&self) -> &str {
        self.group.group_id()
    }

    /// Returns the membership protocol used by this group handle.
    pub fn group_protocol(&self) -> ConsumerGroupProtocol {
        self.group.group_protocol()
    }

    /// Returns the broker-assigned member ID.
    pub fn member_id(&self) -> &str {
        self.group.member_id()
    }

    /// Returns the current classic generation ID or KIP-848 member epoch.
    pub fn generation_id(&self) -> i32 {
        self.group.generation_id()
    }

    /// Snapshots the identity used for fenced transactional offset commits.
    pub fn metadata(&self) -> ConsumerGroupMetadata {
        self.group.metadata()
    }

    /// Returns the assigned topic partitions and next offsets.
    pub fn assignments(&self) -> &[ConsumerAssignment] {
        self.group.assignments()
    }

    /// Returns the stable Kafka topic UUID cached for a KIP-848 assignment.
    pub fn topic_id(&self, topic: &str) -> Option<[u8; 16]> {
        self.group.topic_id(topic)
    }

    /// Queues a record's next offset for a later group commit.
    pub fn commit_record(&mut self, record: &ConsumerRecord) -> Result<()> {
        self.group.commit_record(record)
    }

    /// Returns the number of topic partitions waiting for a queued commit.
    pub fn pending_commit_count(&self) -> usize {
        self.group.pending_commit_count()
    }

    /// Returns the next offset for a currently assigned topic partition.
    pub fn position(&self, topic: &str, partition: i32) -> Option<i64> {
        self.group.position(topic, partition)
    }

    /// Changes the next offset for a currently assigned topic partition.
    pub fn seek(&mut self, topic: &str, partition: i32, offset: i64) -> Result<()> {
        self.group.seek(topic, partition, offset)
    }

    /// Pauses fetching from a currently assigned topic partition.
    pub fn pause(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.group.pause(topic, partition)
    }

    /// Resumes fetching from a currently assigned topic partition.
    pub fn resume(&mut self, topic: &str, partition: i32) -> Result<()> {
        self.group.resume(topic, partition)
    }

    /// Sends a foreground heartbeat, polls assigned partitions, and advances offsets.
    pub fn poll(&mut self) -> Result<Vec<ConsumerRecord>> {
        block_on(&self.runtime, self.group.poll())
    }

    /// Sends an explicit foreground heartbeat.
    pub fn heartbeat(&mut self) -> Result<()> {
        block_on(&self.runtime, self.group.heartbeat())
    }

    /// Commits the current assignment offsets to the group coordinator.
    pub fn commit_offsets(&mut self) -> Result<()> {
        block_on(&self.runtime, self.group.commit_offsets())
    }

    /// Flushes queued per-record offsets to the group coordinator.
    pub fn commit_queued_offsets(&mut self) -> Result<()> {
        block_on(&self.runtime, self.group.commit_queued_offsets())
    }

    /// Fetches the earliest and latest available offsets for a topic partition.
    pub fn fetch_watermarks(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
    ) -> Result<PartitionWatermarks> {
        block_on(&self.runtime, self.group.fetch_watermarks(topic, partition))
    }

    /// Resolves the end offset for a partition leader epoch.
    pub fn offset_for_leader_epoch(
        &mut self,
        topic: impl Into<String>,
        partition: i32,
        current_leader_epoch: i32,
        leader_epoch: i32,
    ) -> Result<LeaderEpochOffset> {
        block_on(
            &self.runtime,
            self.group.offset_for_leader_epoch(
                topic,
                partition,
                current_leader_epoch,
                leader_epoch,
            ),
        )
    }

    /// Leaves the consumer group and consumes this member handle.
    pub fn leave(self) -> Result<()> {
        let Self { group, runtime } = self;
        block_on(&runtime, group.leave())
    }
}

/// Synchronous adapter for a [`crate::ShareConsumer`].
///
/// `poll` performs the foreground Share Group heartbeat and fetch work. Local
/// acknowledgements are recorded synchronously, while `commit` sends the
/// broker requests and preserves the asynchronous client's unknown-outcome
/// safety boundary. Detached heartbeat task handles remain an asynchronous
/// API because their lifecycle is explicitly owned by the caller.
pub struct BlockingShareConsumer {
    consumer: ShareConsumer,
    runtime: Runtime,
}

impl BlockingShareConsumer {
    /// Validates the configuration, joins the Share Group, and returns a
    /// synchronous Share consumer.
    pub fn build(config: ShareConsumerConfig) -> Result<Self> {
        let runtime = build_runtime()?;
        let consumer = block_on(&runtime, config.build())?;
        Ok(Self { consumer, runtime })
    }

    /// Returns the Share Group ID.
    pub fn group_id(&self) -> &str {
        self.consumer.group_id()
    }

    /// Returns the current Share Group member ID.
    pub fn member_id(&self) -> &str {
        self.consumer.member_id()
    }

    /// Returns the current Share Group member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.consumer.member_epoch()
    }

    /// Returns the current assigned topic-partition count.
    pub fn assignment_count(&self) -> usize {
        self.consumer.assignment_count()
    }

    /// Returns the broker acquisition lock timeout from the latest fetch.
    pub fn acquisition_lock_timeout_ms(&self) -> Option<i32> {
        self.consumer.acquisition_lock_timeout_ms()
    }

    /// Returns the number of acknowledgements with an unknown broker outcome.
    pub fn pending_acknowledgement_reconciliation_count(&self) -> usize {
        self.consumer.pending_acknowledgement_reconciliation_count()
    }

    /// Reopens sessions for acknowledgements whose broker outcome is unknown.
    pub fn reconcile_acknowledgement_outcomes(&mut self) -> Result<()> {
        block_on(
            &self.runtime,
            self.consumer.reconcile_acknowledgement_outcomes(),
        )
    }

    /// Sends a Share Group heartbeat immediately.
    pub fn heartbeat(&mut self) -> Result<()> {
        block_on(&self.runtime, self.consumer.heartbeat())
    }

    /// Polls the currently assigned Share Group partitions.
    pub fn poll(&mut self) -> Result<Vec<ShareRecord>> {
        block_on(&self.runtime, self.consumer.poll())
    }

    /// Records a local acknowledgement for an acquired Share record.
    pub fn acknowledge(
        &mut self,
        record: &ShareRecord,
        acknowledgement: ShareAcknowledgementType,
    ) -> Result<()> {
        self.consumer.acknowledge(record, acknowledgement)
    }

    /// Sends all recorded acknowledgements to the Share partition leaders.
    pub fn commit(&mut self) -> Result<()> {
        block_on(&self.runtime, self.consumer.commit())
    }

    /// Closes Share sessions and leaves the Share Group.
    pub fn close(&mut self) -> Result<()> {
        block_on(&self.runtime, self.consumer.close())
    }
}

/// Synchronous adapter for a foreground [`crate::StreamsGroupSession`].
///
/// The adapter exposes the membership heartbeat and task-state reporting
/// lifecycle. A background Streams heartbeat handle remains asynchronous
/// because it consumes the session and requires explicit task ownership.
pub struct BlockingStreamsGroupSession {
    session: StreamsGroupSession,
    runtime: Runtime,
}

impl BlockingStreamsGroupSession {
    /// Joins the configured Kafka Streams group.
    pub fn join(config: StreamsGroupConfig) -> Result<Self> {
        let runtime = build_runtime()?;
        let session = block_on(&runtime, StreamsGroupSession::join(config))?;
        Ok(Self { session, runtime })
    }

    /// Returns the Kafka Streams group ID.
    pub fn group_id(&self) -> &str {
        self.session.group_id()
    }

    /// Returns the client-generated member ID.
    pub fn member_id(&self) -> &str {
        self.session.member_id()
    }

    /// Returns the current member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.session.member_epoch()
    }

    /// Returns the broker-requested heartbeat interval.
    pub fn heartbeat_interval(&self) -> Duration {
        self.session.heartbeat_interval()
    }

    /// Returns the latest successful assignment snapshot.
    pub fn assignment(&self) -> &StreamsGroupSessionAssignment {
        self.session.assignment()
    }

    /// Replaces task state reported on the next heartbeat.
    pub fn set_task_state(
        &mut self,
        active_tasks: Vec<StreamsGroupHeartbeatTask>,
        standby_tasks: Vec<StreamsGroupHeartbeatTask>,
        warmup_tasks: Vec<StreamsGroupHeartbeatTask>,
        task_offsets: Vec<StreamsGroupHeartbeatTaskOffset>,
        task_end_offsets: Vec<StreamsGroupHeartbeatTaskOffset>,
    ) {
        self.session.set_task_state(
            active_tasks,
            standby_tasks,
            warmup_tasks,
            task_offsets,
            task_end_offsets,
        );
    }

    /// Replaces task state while preserving nullable changelog offsets.
    pub fn set_task_state_with_optional_offsets(
        &mut self,
        active_tasks: Vec<StreamsGroupHeartbeatTask>,
        standby_tasks: Vec<StreamsGroupHeartbeatTask>,
        warmup_tasks: Vec<StreamsGroupHeartbeatTask>,
        task_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
        task_end_offsets: Option<Vec<StreamsGroupHeartbeatTaskOffset>>,
    ) {
        self.session.set_task_state_with_optional_offsets(
            active_tasks,
            standby_tasks,
            warmup_tasks,
            task_offsets,
            task_end_offsets,
        );
    }

    /// Returns whether the session has left the group.
    pub fn is_closed(&self) -> bool {
        self.session.is_closed()
    }

    /// Sends one Streams heartbeat and returns the broker assignment state.
    pub fn heartbeat(&mut self) -> Result<StreamsGroupHeartbeatResponseV0> {
        block_on(&self.runtime, self.session.heartbeat())
    }

    /// Leaves the Streams group.
    pub fn close(&mut self) -> Result<()> {
        block_on(&self.runtime, self.session.close())
    }
}

#[cfg(test)]
mod tests {
    use super::BlockingShareConsumer;
    use super::{
        BlockingAdminClient, BlockingBufferedProducer, BlockingConsumer, BlockingConsumerGroup,
        BlockingProducer, BlockingStreamsGroupSession,
    };
    use crate::{
        ClientConfig, ConsumerConfig, ConsumerGroupConfig, Error, ProducerConfig,
        ShareConsumerConfig,
    };

    #[test]
    fn blocking_build_rejects_nested_tokio_runtime() {
        let runtime_result = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        assert!(
            runtime_result.is_ok(),
            "test runtime should build: {runtime_result:?}"
        );
        let Some(runtime) = runtime_result.ok() else {
            return;
        };
        let producer_result = runtime
            .block_on(async { BlockingProducer::build(ProducerConfig::new(["localhost:9092"])) });
        assert!(matches!(
            producer_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));

        let buffered_producer_result = runtime.block_on(async {
            BlockingBufferedProducer::build(ProducerConfig::new(["localhost:9092"]))
        });
        assert!(matches!(
            buffered_producer_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));

        let consumer_result = runtime
            .block_on(async { BlockingConsumer::build(ConsumerConfig::new(["localhost:9092"])) });
        assert!(matches!(
            consumer_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));

        let admin_result = runtime
            .block_on(async { BlockingAdminClient::build(ClientConfig::new(["localhost:9092"])) });
        assert!(matches!(
            admin_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));

        let group_result = runtime.block_on(async {
            BlockingConsumerGroup::join(
                ConsumerGroupConfig::new(["localhost:9092"], "orders-group").subscribe("orders"),
            )
        });
        assert!(matches!(
            group_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));

        let share_result = runtime.block_on(async {
            BlockingShareConsumer::build(
                ShareConsumerConfig::new(["localhost:9092"], "orders-share").subscribe("orders"),
            )
        });
        assert!(matches!(
            share_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));

        let streams_result = runtime.block_on(async {
            BlockingStreamsGroupSession::join(crate::streams::StreamsGroupConfig::new(
                ["localhost:9092"],
                "orders-streams",
                crate::streams::StreamsGroupHeartbeatTopology {
                    epoch: 0,
                    subtopologies: Vec::new(),
                },
            ))
        });
        assert!(matches!(
            streams_result,
            Err(Error::Unsupported(
                "blocking kafrust clients cannot run inside a Tokio runtime; use the async API instead"
            ))
        ));
    }
}
