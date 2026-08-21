use core::fmt;

/// Result type returned by kafrust APIs.
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka broker error categories used by kafrust retry and diagnostics code.
pub enum BrokerErrorKind {
    /// An error code that kafrust does not classify yet.
    Unknown,
    /// Kafka reported that the requested fetch offset is outside the retained log.
    OffsetOutOfRange,
    /// Kafka reported an unknown topic or partition.
    UnknownTopicOrPartition,
    /// Kafka reported that a partition leader is not currently available.
    LeaderNotAvailable,
    /// Kafka reported that the target broker is not the leader or follower.
    NotLeaderOrFollower,
    /// Kafka reported that the broker-side request timed out.
    RequestTimedOut,
    /// Kafka reported that a replica is unavailable.
    ReplicaNotAvailable,
    /// Kafka rejected an invalid topic name or definition.
    InvalidTopic,
    /// Kafka reported that the coordinator is unavailable.
    CoordinatorNotAvailable,
    /// Kafka reported that the coordinator is still loading its state.
    CoordinatorLoadInProgress,
    /// Kafka reported that the request was sent to the wrong coordinator.
    NotCoordinator,
    /// Kafka reported an invalid consumer group generation.
    IllegalGeneration,
    /// Kafka reported inconsistent group protocol metadata.
    InconsistentGroupProtocol,
    /// Kafka reported an unknown consumer group member ID.
    UnknownMemberId,
    /// Kafka rejected the configured session timeout.
    InvalidSessionTimeout,
    /// Kafka reported that a group rebalance is in progress.
    RebalanceInProgress,
    /// Kafka denied access to a topic.
    TopicAuthorizationFailed,
    /// Kafka denied access to the cluster operation.
    ClusterAuthorizationFailed,
    /// Kafka reported that the topic already exists.
    TopicAlreadyExists,
    /// Kafka rejected the requested partition count.
    InvalidPartitions,
    /// Kafka rejected the requested replication factor.
    InvalidReplicationFactor,
    /// Kafka rejected an explicit replica assignment.
    InvalidReplicaAssignment,
    /// Kafka rejected a topic or broker configuration.
    InvalidConfig,
    /// Kafka reported that this broker is not the active controller.
    NotController,
    /// Kafka rejected an invalid request.
    InvalidRequest,
    /// Kafka received a producer sequence larger than the expected sequence.
    OutOfOrderSequenceNumber,
    /// Kafka recognized a retry of an already appended producer sequence.
    DuplicateSequenceNumber,
    /// Kafka rejected an operation from an older producer epoch.
    InvalidProducerEpoch,
    /// Kafka fenced this producer with a newer producer instance.
    ProducerFenced,
    /// Kafka is still completing another transaction for this transactional ID.
    ConcurrentTransactions,
    /// Kafka reported that the requested consumer group does not exist.
    GroupIdNotFound,
    /// Kafka refused to delete a group that still has active members.
    NonEmptyGroup,
    /// Kafka rejected offset deletion because the group still subscribes to the topic.
    GroupSubscribedToTopic,
    /// Kafka fenced a static consumer because another member uses the same instance ID.
    FencedInstanceId,
    /// Kafka fenced a consumer-group member epoch.
    FencedMemberEpoch,
    /// Kafka reported a stale consumer-group member epoch.
    StaleMemberEpoch,
    /// Kafka rejected a fetch because the supplied leader epoch is fenced.
    FencedLeaderEpoch,
    /// Kafka could not identify the supplied leader epoch during a transition.
    UnknownLeaderEpoch,
    /// Kafka rejected the current fetch session epoch and requires a new session.
    InvalidFetchSessionEpoch,
    /// Kafka could not find the share session for this member.
    ShareSessionNotFound,
    /// Kafka rejected the current share session epoch.
    InvalidShareSessionEpoch,
    /// Kafka fenced the share-group state epoch.
    FencedStateEpoch,
    /// Kafka rejected an acknowledgement because the record is no longer acquired.
    InvalidRecordState,
    /// Kafka could not create a share session because the broker limit was reached.
    ShareSessionLimitReached,
    /// Kafka found no eligible replica for the requested leader election.
    EligibleLeadersNotAvailable,
    /// Kafka reported that the requested leader is already preferred.
    ElectionNotNeeded,
    /// Kafka reported that no partition reassignment is currently active.
    NoReassignmentInProgress,
}

impl BrokerErrorKind {
    /// Classifies a Kafka protocol error code.
    pub fn from_code(code: i16) -> Self {
        match code {
            1 => Self::OffsetOutOfRange,
            3 => Self::UnknownTopicOrPartition,
            5 => Self::LeaderNotAvailable,
            6 => Self::NotLeaderOrFollower,
            7 => Self::RequestTimedOut,
            9 => Self::ReplicaNotAvailable,
            14 => Self::CoordinatorLoadInProgress,
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            17 => Self::InvalidTopic,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
            29 => Self::TopicAuthorizationFailed,
            31 => Self::ClusterAuthorizationFailed,
            36 => Self::TopicAlreadyExists,
            37 => Self::InvalidPartitions,
            38 => Self::InvalidReplicationFactor,
            39 => Self::InvalidReplicaAssignment,
            40 => Self::InvalidConfig,
            41 => Self::NotController,
            42 => Self::InvalidRequest,
            45 => Self::OutOfOrderSequenceNumber,
            46 => Self::DuplicateSequenceNumber,
            47 => Self::InvalidProducerEpoch,
            51 => Self::ConcurrentTransactions,
            68 => Self::NonEmptyGroup,
            69 => Self::GroupIdNotFound,
            82 => Self::FencedInstanceId,
            110 => Self::FencedMemberEpoch,
            113 => Self::StaleMemberEpoch,
            74 => Self::FencedLeaderEpoch,
            75 => Self::UnknownLeaderEpoch,
            70 => Self::InvalidFetchSessionEpoch,
            122 => Self::ShareSessionNotFound,
            123 => Self::InvalidShareSessionEpoch,
            124 => Self::FencedStateEpoch,
            121 => Self::InvalidRecordState,
            133 => Self::ShareSessionLimitReached,
            86 => Self::GroupSubscribedToTopic,
            90 => Self::ProducerFenced,
            83 => Self::EligibleLeadersNotAvailable,
            84 => Self::ElectionNotNeeded,
            85 => Self::NoReassignmentInProgress,
            _ => Self::Unknown,
        }
    }

    /// Returns whether this broker error is retryable for the current producer path.
    pub fn is_produce_retryable(self) -> bool {
        matches!(
            self,
            Self::UnknownTopicOrPartition
                | Self::LeaderNotAvailable
                | Self::NotLeaderOrFollower
                | Self::RequestTimedOut
                | Self::ReplicaNotAvailable
        )
    }
}

#[derive(Debug)]
/// Error type returned by kafrust APIs.
pub enum Error {
    /// No bootstrap server was configured.
    MissingBootstrapServer,
    /// Metadata did not contain the requested topic partition.
    UnknownTopicOrPartition {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
    },
    /// A custom producer partitioner selected a partition absent from metadata.
    InvalidPartition {
        /// Kafka topic name.
        topic: String,
        /// Partition index selected by the custom partitioner.
        partition: i32,
    },
    /// A position operation targeted a topic partition that is not assigned.
    UnassignedTopicPartition {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
    },
    /// A split consumer partition queue reached its configured bound.
    PartitionQueueFull {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
        /// Configured queue capacity.
        capacity: usize,
    },
    /// Metadata did not contain a usable leader for a topic partition.
    MissingLeader {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
    },
    /// Metadata referenced a broker node that was not present in the broker list.
    MissingBroker {
        /// Kafka broker node ID.
        node_id: i32,
    },
    /// A DescribeGroups response omitted the requested group.
    MissingGroupDescription {
        /// Requested consumer group ID.
        group_id: String,
    },
    /// A DeleteGroups response omitted the requested group.
    MissingDeleteGroupResult {
        /// Requested consumer group ID.
        group_id: String,
    },
    /// A broker returned a per-entry response with a different count than its request.
    ResponseCountMismatch {
        /// Kafka request family that returned the inconsistent response.
        operation: &'static str,
        /// Number of entries sent by the client.
        expected: usize,
        /// Number of entries returned by the broker.
        actual: usize,
    },
    /// SASL security protocol was selected without configuring credentials.
    MissingSaslCredentials,
    /// SASL authentication response could not be validated.
    InvalidSaslResponse {
        /// SASL mechanism being authenticated.
        mechanism: &'static str,
        /// Redacted validation failure reason.
        reason: &'static str,
    },
    /// An asynchronous OAUTHBEARER token provider exceeded the client timeout.
    OAuthBearerTokenTimeout {
        /// Timeout applied to the provider call in milliseconds.
        timeout_ms: u64,
    },
    /// An OAUTHBEARER token source returned a token that was already expired.
    OAuthBearerTokenExpired,
    /// The broker outcome of an EndTxn request could not be observed.
    TransactionOutcomeUnknown {
        /// Transaction operation whose outcome is unknown.
        operation: &'static str,
    },
    /// A transactional producer can no longer safely issue transaction commands.
    TransactionProducerDefunct,
    /// Kafka returned a non-zero broker error code.
    Broker {
        /// Raw Kafka broker error code.
        code: i16,
        /// Operation context for the broker error.
        context: String,
    },
    /// A Kafka request exceeded the configured request timeout.
    RequestTimedOut {
        /// Timeout in milliseconds.
        timeout_ms: u64,
    },
    /// A broker response frame exceeded the configured allocation limit.
    ResponseTooLarge {
        /// Response payload bytes declared by the broker.
        size: usize,
        /// Configured maximum response payload bytes.
        max: usize,
    },
    /// TLS configuration could not be built.
    TlsConfig {
        /// Redacted TLS configuration failure reason.
        reason: String,
    },
    /// Configured or derived TLS server name is invalid.
    InvalidTlsServerName {
        /// Original configured or derived server name value.
        server: String,
    },
    /// A static consumer group instance ID was configured as an empty string.
    InvalidGroupInstanceId,
    /// A consumer-group topic subscription pattern failed local validation.
    InvalidTopicPattern {
        /// The configured regular expression.
        pattern: String,
        /// The regular-expression compiler's diagnostic.
        reason: String,
    },
    /// A SCRAM credential request failed local validation before reaching Kafka.
    InvalidScramCredential {
        /// Redacted validation failure reason.
        reason: &'static str,
    },
    /// A public builder received a value outside the client's supported range.
    InvalidConfiguration {
        /// Configuration field that failed validation.
        field: &'static str,
        /// Stable, non-secret explanation of the validation failure.
        reason: &'static str,
    },
    /// A consumer group did not receive an assignment before its rebalance deadline.
    ConsumerGroupAssignmentTimeout {
        /// Rebalance timeout used while waiting for the assignment, in milliseconds.
        timeout_ms: u64,
    },
    /// An Admin mutation may have reached Kafka but its outcome was not observed.
    AdminMutationOutcomeUnknown {
        /// Kafka Admin operation whose response was lost or invalid.
        operation: &'static str,
    },
    /// A share consumer attempted to poll before acknowledging its prior records.
    ShareAcknowledgementRequired {
        /// Number of records still waiting for an acknowledgement.
        count: usize,
    },
    /// A ShareAcknowledge request may have reached Kafka but its outcome was not observed.
    ShareAcknowledgementOutcomeUnknown {
        /// Kafka broker node that received the acknowledgement request.
        broker_id: i32,
    },
    /// A pending share acknowledgement has no matching ShareFetch session.
    ShareAcknowledgementSessionUnavailable {
        /// Kafka broker node selected for the acknowledgement request.
        broker_id: i32,
        /// Broker nodes for which a ShareFetch session is currently tracked.
        available_broker_ids: Vec<i32>,
    },
    /// A share acknowledgement targeted a record that is not pending locally.
    ShareRecordNotPending {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
        /// Kafka record offset.
        offset: i64,
    },
    /// A share record was acknowledged more than once before commit.
    ShareRecordAlreadyAcknowledged {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
        /// Kafka record offset.
        offset: i64,
    },
    /// A telemetry payload exceeded the local or broker-advertised limit.
    TelemetryPayloadTooLarge {
        /// Payload bytes returned by the metrics provider.
        size: usize,
        /// Maximum accepted payload bytes.
        max: usize,
    },
    /// ShareFetch returned a record outside every acquired record range.
    ShareRecordNotAcquired {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
        /// Kafka record offset.
        offset: i64,
    },
    /// The requested Kafka feature is not implemented by this alpha API yet.
    Unsupported(&'static str),
    /// I/O failure while connecting to or communicating with a broker.
    Io(std::io::Error),
    /// A background Tokio task failed before returning its Kafka result.
    TaskJoin(tokio::task::JoinError),
    /// A Streams heartbeat task stopped before accepting another command.
    StreamsGroupBackgroundTaskClosed,
    /// A Streams assignment contained an invalid task identifier or partition list.
    StreamsTaskAssignmentInvalid {
        /// Subtopology identifier carried by the invalid task, if available.
        subtopology_id: String,
        /// Stable reason for rejecting the assignment.
        reason: &'static str,
    },
    /// A Streams assignment reused an input partition across local tasks.
    StreamsTaskAssignmentConflict {
        /// Subtopology in which the conflicting partition appeared.
        subtopology_id: String,
        /// Input partition claimed by more than one local task.
        partition: i32,
    },
    /// Kafka protocol encoding or decoding failure.
    Protocol(kafrust_protocol::Error),
}

impl Error {
    /// Returns the classified broker error kind when this is a broker error.
    pub fn broker_error_kind(&self) -> Option<BrokerErrorKind> {
        match self {
            Self::Broker { code, .. } => Some(BrokerErrorKind::from_code(*code)),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBootstrapServer => f.write_str("missing Kafka bootstrap server"),
            Self::UnknownTopicOrPartition { topic, partition } => {
                write!(f, "unknown topic or partition {topic}-{partition}")
            }
            Self::InvalidPartition { topic, partition } => {
                write!(
                    f,
                    "custom partitioner selected invalid topic partition {topic}-{partition}"
                )
            }
            Self::UnassignedTopicPartition { topic, partition } => {
                write!(f, "unassigned topic partition {topic}-{partition}")
            }
            Self::PartitionQueueFull {
                topic,
                partition,
                capacity,
            } => write!(
                f,
                "partition queue for {topic}-{partition} is full (capacity {capacity})"
            ),
            Self::MissingLeader { topic, partition } => {
                write!(f, "missing leader for topic partition {topic}-{partition}")
            }
            Self::MissingBroker { node_id } => {
                write!(f, "missing broker metadata for node {node_id}")
            }
            Self::MissingGroupDescription { group_id } => {
                write!(f, "missing description for consumer group {group_id}")
            }
            Self::MissingDeleteGroupResult { group_id } => {
                write!(f, "missing delete result for consumer group {group_id}")
            }
            Self::ResponseCountMismatch {
                operation,
                expected,
                actual,
            } => write!(
                f,
                "{operation} response count mismatch: expected {expected}, got {actual}"
            ),
            Self::MissingSaslCredentials => f.write_str("missing Kafka SASL credentials"),
            Self::InvalidSaslResponse { mechanism, reason } => {
                write!(f, "invalid SASL {mechanism} response: {reason}")
            }
            Self::OAuthBearerTokenTimeout { timeout_ms } => write!(
                f,
                "SASL/OAUTHBEARER token provider timed out after {timeout_ms}ms"
            ),
            Self::OAuthBearerTokenExpired => {
                f.write_str("SASL/OAUTHBEARER token provider returned an expired token")
            }
            Self::TransactionOutcomeUnknown { operation } => write!(
                f,
                "transaction {operation} outcome is unknown; discard the producer"
            ),
            Self::TransactionProducerDefunct => {
                f.write_str("transactional producer is defunct; discard the producer")
            }
            Self::Broker { code, context } => write!(f, "Kafka broker error {code}: {context}"),
            Self::RequestTimedOut { timeout_ms } => {
                write!(f, "Kafka request timed out after {timeout_ms}ms")
            }
            Self::ResponseTooLarge { size, max } => {
                write!(
                    f,
                    "Kafka response frame of {size} bytes exceeds configured maximum of {max} bytes"
                )
            }
            Self::TlsConfig { reason } => write!(f, "TLS configuration error: {reason}"),
            Self::InvalidTlsServerName { server } => {
                write!(f, "invalid TLS server name {server}")
            }
            Self::InvalidGroupInstanceId => {
                f.write_str("consumer group instance ID must not be empty")
            }
            Self::InvalidTopicPattern { pattern, reason } => {
                write!(
                    f,
                    "invalid consumer group topic pattern {pattern:?}: {reason}"
                )
            }
            Self::InvalidScramCredential { reason } => {
                write!(f, "invalid SCRAM credential: {reason}")
            }
            Self::InvalidConfiguration { field, reason } => {
                write!(f, "invalid Kafka configuration field {field}: {reason}")
            }
            Self::ConsumerGroupAssignmentTimeout { timeout_ms } => write!(
                f,
                "consumer group assignment was not delivered before the rebalance timeout of {timeout_ms}ms"
            ),
            Self::AdminMutationOutcomeUnknown { operation } => write!(
                f,
                "Kafka Admin mutation {operation} may have been applied; its outcome is unknown"
            ),
            Self::ShareAcknowledgementRequired { count } => write!(
                f,
                "share consumer has {count} record(s) awaiting acknowledgement"
            ),
            Self::ShareAcknowledgementOutcomeUnknown { broker_id } => write!(
                f,
                "share acknowledgement outcome for broker {broker_id} is unknown; do not replay without reconciliation"
            ),
            Self::ShareAcknowledgementSessionUnavailable {
                broker_id,
                available_broker_ids,
            } => write!(
                f,
                "share acknowledgement session for broker {broker_id} is unavailable; active sessions: {available_broker_ids:?}"
            ),
            Self::ShareRecordNotPending {
                topic,
                partition,
                offset,
            } => write!(
                f,
                "share record {topic}-{partition}@{offset} is not pending"
            ),
            Self::ShareRecordAlreadyAcknowledged {
                topic,
                partition,
                offset,
            } => write!(
                f,
                "share record {topic}-{partition}@{offset} was already acknowledged"
            ),
            Self::TelemetryPayloadTooLarge { size, max } => write!(
                f,
                "telemetry payload of {size} bytes exceeds the configured maximum of {max} bytes"
            ),
            Self::ShareRecordNotAcquired {
                topic,
                partition,
                offset,
            } => write!(
                f,
                "share fetch returned record {topic}-{partition}@{offset} outside its acquired ranges"
            ),
            Self::Unsupported(feature) => write!(f, "unsupported feature: {feature}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::TaskJoin(error) => write!(f, "background task join error: {error}"),
            Self::StreamsGroupBackgroundTaskClosed => {
                f.write_str("Streams group background heartbeat task is closed")
            }
            Self::StreamsTaskAssignmentInvalid {
                subtopology_id,
                reason,
            } => write!(
                f,
                "invalid Streams task assignment for subtopology {subtopology_id:?}: {reason}"
            ),
            Self::StreamsTaskAssignmentConflict {
                subtopology_id,
                partition,
            } => write!(
                f,
                "Streams task assignment conflicts in subtopology {subtopology_id:?} on partition {partition}"
            ),
            Self::Protocol(error) => write!(f, "Kafka protocol error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::TaskJoin(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::MissingBootstrapServer
            | Self::UnknownTopicOrPartition { .. }
            | Self::InvalidPartition { .. }
            | Self::UnassignedTopicPartition { .. }
            | Self::PartitionQueueFull { .. }
            | Self::MissingLeader { .. }
            | Self::MissingBroker { .. }
            | Self::MissingGroupDescription { .. }
            | Self::MissingDeleteGroupResult { .. }
            | Self::ResponseCountMismatch { .. }
            | Self::MissingSaslCredentials
            | Self::InvalidSaslResponse { .. }
            | Self::OAuthBearerTokenTimeout { .. }
            | Self::OAuthBearerTokenExpired
            | Self::TransactionOutcomeUnknown { .. }
            | Self::TransactionProducerDefunct
            | Self::Broker { .. }
            | Self::RequestTimedOut { .. }
            | Self::ResponseTooLarge { .. }
            | Self::TlsConfig { .. }
            | Self::InvalidTlsServerName { .. }
            | Self::InvalidGroupInstanceId
            | Self::InvalidTopicPattern { .. }
            | Self::InvalidScramCredential { .. }
            | Self::InvalidConfiguration { .. }
            | Self::ConsumerGroupAssignmentTimeout { .. }
            | Self::AdminMutationOutcomeUnknown { .. }
            | Self::ShareAcknowledgementRequired { .. }
            | Self::ShareAcknowledgementOutcomeUnknown { .. }
            | Self::ShareAcknowledgementSessionUnavailable { .. }
            | Self::ShareRecordNotPending { .. }
            | Self::ShareRecordAlreadyAcknowledged { .. }
            | Self::TelemetryPayloadTooLarge { .. }
            | Self::ShareRecordNotAcquired { .. }
            | Self::StreamsGroupBackgroundTaskClosed
            | Self::StreamsTaskAssignmentInvalid { .. }
            | Self::StreamsTaskAssignmentConflict { .. }
            | Self::Unsupported(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(error: tokio::task::JoinError) -> Self {
        Self::TaskJoin(error)
    }
}

impl From<kafrust_protocol::Error> for Error {
    fn from(error: kafrust_protocol::Error) -> Self {
        Self::Protocol(error)
    }
}

#[cfg(test)]
mod tests {
    use super::{BrokerErrorKind, Error};

    #[test]
    fn classifies_known_broker_error_codes() {
        assert_eq!(
            BrokerErrorKind::from_code(3),
            BrokerErrorKind::UnknownTopicOrPartition
        );
        assert_eq!(
            BrokerErrorKind::from_code(1),
            BrokerErrorKind::OffsetOutOfRange
        );
        assert_eq!(
            BrokerErrorKind::from_code(16),
            BrokerErrorKind::NotCoordinator
        );
        assert_eq!(
            BrokerErrorKind::from_code(14),
            BrokerErrorKind::CoordinatorLoadInProgress
        );
        assert_eq!(
            BrokerErrorKind::from_code(27),
            BrokerErrorKind::RebalanceInProgress
        );
        assert_eq!(
            BrokerErrorKind::from_code(36),
            BrokerErrorKind::TopicAlreadyExists
        );
        assert_eq!(
            BrokerErrorKind::from_code(41),
            BrokerErrorKind::NotController
        );
        assert_eq!(
            BrokerErrorKind::from_code(45),
            BrokerErrorKind::OutOfOrderSequenceNumber
        );
        assert_eq!(
            BrokerErrorKind::from_code(46),
            BrokerErrorKind::DuplicateSequenceNumber
        );
        assert_eq!(
            BrokerErrorKind::from_code(47),
            BrokerErrorKind::InvalidProducerEpoch
        );
        assert_eq!(
            BrokerErrorKind::from_code(51),
            BrokerErrorKind::ConcurrentTransactions
        );
        assert_eq!(
            BrokerErrorKind::from_code(69),
            BrokerErrorKind::GroupIdNotFound
        );
        assert_eq!(
            BrokerErrorKind::from_code(82),
            BrokerErrorKind::FencedInstanceId
        );
        assert_eq!(
            BrokerErrorKind::from_code(86),
            BrokerErrorKind::GroupSubscribedToTopic
        );
        assert_eq!(
            BrokerErrorKind::from_code(90),
            BrokerErrorKind::ProducerFenced
        );
        assert_eq!(
            BrokerErrorKind::from_code(74),
            BrokerErrorKind::FencedLeaderEpoch
        );
        assert_eq!(
            BrokerErrorKind::from_code(75),
            BrokerErrorKind::UnknownLeaderEpoch
        );
        assert_eq!(
            BrokerErrorKind::from_code(70),
            BrokerErrorKind::InvalidFetchSessionEpoch
        );
        assert_eq!(
            BrokerErrorKind::from_code(83),
            BrokerErrorKind::EligibleLeadersNotAvailable
        );
        assert_eq!(
            BrokerErrorKind::from_code(84),
            BrokerErrorKind::ElectionNotNeeded
        );
        assert_eq!(
            BrokerErrorKind::from_code(85),
            BrokerErrorKind::NoReassignmentInProgress
        );
        assert_eq!(BrokerErrorKind::from_code(999), BrokerErrorKind::Unknown);
    }

    #[test]
    fn exposes_broker_error_kind_from_error() {
        let error = Error::Broker {
            code: 7,
            context: "produce orders-0".to_owned(),
        };

        assert_eq!(
            error.broker_error_kind(),
            Some(BrokerErrorKind::RequestTimedOut)
        );
    }

    #[test]
    fn displays_consumer_group_assignment_timeout() {
        let error = Error::ConsumerGroupAssignmentTimeout { timeout_ms: 5_000 };

        assert_eq!(
            error.to_string(),
            "consumer group assignment was not delivered before the rebalance timeout of 5000ms"
        );
        assert!(std::error::Error::source(&error).is_none());
    }

    #[test]
    fn displays_admin_mutation_unknown_outcome() {
        let error = Error::AdminMutationOutcomeUnknown {
            operation: "CreateTopics",
        };

        assert_eq!(
            error.to_string(),
            "Kafka Admin mutation CreateTopics may have been applied; its outcome is unknown"
        );
        assert!(std::error::Error::source(&error).is_none());

        let error = Error::ShareAcknowledgementOutcomeUnknown { broker_id: 3 };
        assert_eq!(
            error.to_string(),
            "share acknowledgement outcome for broker 3 is unknown; do not replay without reconciliation"
        );
        assert!(std::error::Error::source(&error).is_none());
    }
}
