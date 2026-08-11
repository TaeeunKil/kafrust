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

mod scram;

pub use admin::{
    AclBinding, AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AdminClient, AlterClientQuotaEntryResult, AlterClientQuotasResult, AlterConfigResourceResult,
    AlterConfigsOptions, AlterConfigsResult, AlterPartitionReassignmentResult,
    AlterPartitionReassignmentTopicResult, AlterPartitionReassignmentsResult,
    AlterScramCredentialResult, AlterUserScramCredentialsResult, BrokerDescription,
    ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaEntityComponent, ClientQuotaFilter,
    ClientQuotaFilterComponent, ClientQuotaMatchType, ClientQuotaOperation, ClientQuotaValue,
    ClusterDescription, ConfigAlterOperation, ConfigAlterOperationKind, ConfigEntry,
    ConfigResourceResult, ConfigSource, ConfigSynonym, ConsumerGroupDescription,
    ConsumerGroupMember, ConsumerGroupOffsetDelete, CreateAclsEntryResult, CreateAclsResult,
    CreatePartitionsOptions, CreatePartitionsResult, CreatePartitionsTopicResult,
    CreateTopicResult, CreateTopicsOptions, CreateTopicsResult, DeleteAclsFilterResult,
    DeleteAclsResult, DeleteConsumerGroupOffsetsPartitionResult, DeleteConsumerGroupOffsetsResult,
    DeleteConsumerGroupOffsetsTopicResult, DeleteConsumerGroupResult, DeleteTopicResult,
    DeleteTopicsOptions, DeleteTopicsResult, DeletedAclResult, DescribeAclsResult,
    DescribeClientQuotasResult, DescribeConfigsOptions, DescribeConfigsResult,
    DescribeUserScramCredentialsResult, GroupListing, ListPartitionReassignmentsResult,
    NewPartitions, NewTopic, OngoingPartitionReassignment, OngoingPartitionReassignmentTopic,
    PartitionReassignment, PartitionReassignmentOptions, PartitionReassignmentPartition,
    PartitionReassignmentQuery, ScramCredentialDeletion, ScramCredentialInfo,
    ScramCredentialMechanism, ScramCredentialUpsertion, ScramUserCredentials,
    TopicConfigAlteration, TopicConfigResource, TopicListing,
};
pub use client::Client;
pub use config::{
    ClientConfig, OAuthBearerTokenFuture, OAuthBearerTokenProvider, SaslCredentials, SaslMechanism,
    SecurityProtocol,
};
pub use consumer::{
    Consumer, ConsumerAssignment, ConsumerConfig, ConsumerRecord, IsolationLevel,
    PartitionWatermarks,
};
pub use error::{BrokerErrorKind, Error, Result};
pub use group::{
    ConsumerGroup, ConsumerGroupAssignmentStrategy, ConsumerGroupConfig, ConsumerGroupHeartbeat,
    ConsumerGroupMetadata, OffsetResetPolicy,
};
pub use kafrust_protocol as protocol;
pub use metrics::{ClientMetrics, ClientMetricsSnapshot};
pub use producer::{
    Acks, BufferedProducer, Compression, Header, ProducerBatchFailure, ProducerBatchRecordOutcome,
    ProducerBatchReport, ProducerConfig, ProducerDelivery, ProducerRecord, RecordMetadata,
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
