#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// Kafka administration API.
pub mod admin;
/// Low-level Kafka request client.
pub mod client;
/// Shared connection configuration.
pub mod config;
/// Direct topic/partition consumer API.
pub mod consumer;
/// Error and result types.
pub mod error;
/// Classic consumer group alpha API.
pub mod group;
/// Lock-free client request metrics.
pub mod metrics;
/// Producer API.
pub mod producer;
/// KIP-932 share-group consumer API.
pub mod share_consumer;
/// KIP-714 client telemetry API.
pub mod telemetry;

mod scram;

pub use admin::{
    AclBinding, AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AdminClient, AlterClientQuotaEntryResult, AlterClientQuotasResult, AlterConfigResourceResult,
    AlterConfigsOptions, AlterConfigsResult, AlterConsumerGroupOffsetsPartitionResult,
    AlterConsumerGroupOffsetsResult, AlterConsumerGroupOffsetsTopicResult,
    AlterPartitionReassignmentResult, AlterPartitionReassignmentTopicResult,
    AlterPartitionReassignmentsResult, AlterReplicaLogDirsPartitionResult,
    AlterReplicaLogDirsResult, AlterReplicaLogDirsTopicResult, AlterScramCredentialResult,
    AlterShareGroupOffsetsPartitionResult, AlterShareGroupOffsetsResult,
    AlterShareGroupOffsetsTopicResult, AlterUserScramCredentialsResult, BrokerDescription,
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaEntityComponent, ClientQuotaFilter,
    ClientQuotaFilterComponent, ClientQuotaMatchType, ClientQuotaOperation, ClientQuotaValue,
    ClusterDescription, ConfigAlterOperation, ConfigAlterOperationKind, ConfigEntry,
    ConfigResourceResult, ConfigSource, ConfigSynonym, ConsumerGroupDescription,
    ConsumerGroupMember, ConsumerGroupOffset, ConsumerGroupOffsetDelete,
    ConsumerGroupOffsetPartitionResult, ConsumerGroupOffsetQuery, ConsumerGroupOffsetTopicResult,
    CreateAclsEntryResult, CreateAclsResult, CreateDelegationTokenOptions, CreatePartitionsOptions,
    CreatePartitionsResult, CreatePartitionsTopicResult, CreateTopicResult, CreateTopicsOptions,
    CreateTopicsResult, CreatedDelegationToken, DelegationTokenOperationResult,
    DelegationTokenPrincipal, DeleteAclsFilterResult, DeleteAclsResult,
    DeleteConsumerGroupOffsetsPartitionResult, DeleteConsumerGroupOffsetsResult,
    DeleteConsumerGroupOffsetsTopicResult, DeleteConsumerGroupResult, DeleteRecordsOptions,
    DeleteRecordsPartition, DeleteRecordsPartitionResult, DeleteRecordsResult, DeleteRecordsTopic,
    DeleteRecordsTopicResult, DeleteShareGroupOffsetsResult, DeleteShareGroupOffsetsTopicResult,
    DeleteTopicResult, DeleteTopicsOptions, DeleteTopicsResult, DeletedAclResult,
    DescribeAclsResult, DescribeClientQuotasResult, DescribeConfigsOptions, DescribeConfigsResult,
    DescribeDelegationTokensResult, DescribeLogDirsBrokerResult, DescribeProducersActiveProducer,
    DescribeProducersPartitionResult, DescribeProducersResult, DescribeProducersTopic,
    DescribeProducersTopicResult, DescribeQuorumListener, DescribeQuorumNode,
    DescribeQuorumPartitionResult, DescribeQuorumReplicaState, DescribeQuorumResult,
    DescribeQuorumTopic, DescribeQuorumTopicResult, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsOptions, DescribeTopicPartitionsPartition,
    DescribeTopicPartitionsResult, DescribeTopicPartitionsTopic, DescribeTransactionsResult,
    DescribeUserScramCredentialsResult, DescribedDelegationTokenResult, ElectLeadersOptions,
    ElectLeadersPartitionResult, ElectLeadersResult, ElectLeadersTopicResult, ElectionType,
    GroupListing, LeaderElection, ListConsumerGroupOffsetsResult, ListPartitionReassignmentsResult,
    ListTransactionsOptions, ListTransactionsResult, ListedTransaction, LogDirTopic,
    LogDirectoryPartitionResult, LogDirectoryResult, LogDirectoryTopicResult,
    ModernConsumerGroupAssignment, ModernConsumerGroupDescription, ModernConsumerGroupMember,
    ModernConsumerGroupTopicPartitions, NewPartitions, NewTopic, OngoingPartitionReassignment,
    OngoingPartitionReassignmentTopic, PartitionReassignment, PartitionReassignmentOptions,
    PartitionReassignmentPartition, PartitionReassignmentQuery, ReplicaLogDirAssignment,
    ScramCredentialDeletion, ScramCredentialInfo, ScramCredentialMechanism,
    ScramCredentialUpsertion, ScramUserCredentials, ShareGroupAssignment, ShareGroupDescription,
    ShareGroupMember, ShareGroupOffset, ShareGroupTopicPartitions, TopicConfigAlteration,
    TopicConfigResource, TopicConfigUpdate, TopicConfigUpdateEntry, TopicListing,
    TransactionDescription, TransactionDescriptionTopic,
};
pub use client::Client;
pub use config::{
    ClientConfig, OAuthBearerTokenFuture, OAuthBearerTokenProvider, SaslCredentials, SaslMechanism,
    SecurityProtocol,
};
pub use consumer::{
    Consumer, ConsumerAssignment, ConsumerConfig, ConsumerPartitionQueue, ConsumerRecord,
    ConsumerRecordHeader, IsolationLevel, LeaderEpochOffset, OffsetResetPolicy,
    PartitionWatermarks,
};
pub use error::{BrokerErrorKind, Error, Result};
pub use group::{
    ConsumerGroup, ConsumerGroupAssignmentStrategy, ConsumerGroupCommitWorker, ConsumerGroupConfig,
    ConsumerGroupHeartbeat, ConsumerGroupMetadata, ConsumerGroupProtocol, RebalanceEvent,
    RebalanceListener, RebalancePhase,
};
pub use kafrust_protocol as protocol;
pub use metrics::{ClientMetrics, ClientMetricsSnapshot};
pub use producer::{
    Acks, BufferedProducer, Compression, Header, Partitioner, ProducerBatchFailure,
    ProducerBatchRecordOutcome, ProducerBatchReport, ProducerConfig, ProducerDelivery,
    ProducerRecord, RecordMetadata, TransactionStatus,
};
pub use share_consumer::{
    ShareAcknowledgementMode, ShareAcknowledgementType, ShareAcquireMode, ShareConsumer,
    ShareConsumerConfig, ShareConsumerHeartbeat, ShareRecord,
};
#[cfg(feature = "otlp")]
pub use telemetry::ClientMetricsTelemetryProvider;
pub use telemetry::{
    TelemetryClient, TelemetryConfig, TelemetryMetricsProvider, TelemetryPushSummary,
    TelemetrySubscription,
};

/// Returns the crate version compiled into this build.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_crate_version() {
        assert_eq!(crate::version(), env!("CARGO_PKG_VERSION"));
    }
}
