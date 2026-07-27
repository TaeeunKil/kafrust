use core::fmt;

/// Result type returned by kafrust APIs.
pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Kafka broker error categories used by kafrust retry and diagnostics code.
pub enum BrokerErrorKind {
    /// An error code that kafrust does not classify yet.
    Unknown,
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
}

impl BrokerErrorKind {
    /// Classifies a Kafka protocol error code.
    pub fn from_code(code: i16) -> Self {
        match code {
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
            86 => Self::GroupSubscribedToTopic,
            90 => Self::ProducerFenced,
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
    /// A position operation targeted a topic partition that is not assigned.
    UnassignedTopicPartition {
        /// Kafka topic name.
        topic: String,
        /// Kafka partition index.
        partition: i32,
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
    /// SASL security protocol was selected without configuring credentials.
    MissingSaslCredentials,
    /// SASL authentication response could not be validated.
    InvalidSaslResponse {
        /// SASL mechanism being authenticated.
        mechanism: &'static str,
        /// Redacted validation failure reason.
        reason: &'static str,
    },
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
    /// The requested Kafka feature is not implemented by this alpha API yet.
    Unsupported(&'static str),
    /// I/O failure while connecting to or communicating with a broker.
    Io(std::io::Error),
    /// A background Tokio task failed before returning its Kafka result.
    TaskJoin(tokio::task::JoinError),
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
            Self::UnassignedTopicPartition { topic, partition } => {
                write!(f, "unassigned topic partition {topic}-{partition}")
            }
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
            Self::MissingSaslCredentials => f.write_str("missing Kafka SASL credentials"),
            Self::InvalidSaslResponse { mechanism, reason } => {
                write!(f, "invalid SASL {mechanism} response: {reason}")
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
            Self::Unsupported(feature) => write!(f, "unsupported feature: {feature}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::TaskJoin(error) => write!(f, "background task join error: {error}"),
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
            | Self::UnassignedTopicPartition { .. }
            | Self::MissingLeader { .. }
            | Self::MissingBroker { .. }
            | Self::MissingGroupDescription { .. }
            | Self::MissingDeleteGroupResult { .. }
            | Self::MissingSaslCredentials
            | Self::InvalidSaslResponse { .. }
            | Self::Broker { .. }
            | Self::RequestTimedOut { .. }
            | Self::ResponseTooLarge { .. }
            | Self::TlsConfig { .. }
            | Self::InvalidTlsServerName { .. }
            | Self::InvalidGroupInstanceId
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
}
