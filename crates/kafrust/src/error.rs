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
    /// Kafka reported that the coordinator is unavailable.
    CoordinatorNotAvailable,
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
            15 => Self::CoordinatorNotAvailable,
            16 => Self::NotCoordinator,
            22 => Self::IllegalGeneration,
            23 => Self::InconsistentGroupProtocol,
            25 => Self::UnknownMemberId,
            26 => Self::InvalidSessionTimeout,
            27 => Self::RebalanceInProgress,
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
    /// SASL security protocol was selected without configuring credentials.
    MissingSaslCredentials,
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
    /// TLS configuration could not be built.
    TlsConfig {
        /// Redacted TLS configuration failure reason.
        reason: String,
    },
    /// Bootstrap server could not be converted to a TLS server name.
    InvalidTlsServerName {
        /// Original bootstrap server value.
        server: String,
    },
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
            Self::MissingLeader { topic, partition } => {
                write!(f, "missing leader for topic partition {topic}-{partition}")
            }
            Self::MissingBroker { node_id } => {
                write!(f, "missing broker metadata for node {node_id}")
            }
            Self::MissingSaslCredentials => f.write_str("missing Kafka SASL credentials"),
            Self::Broker { code, context } => write!(f, "Kafka broker error {code}: {context}"),
            Self::RequestTimedOut { timeout_ms } => {
                write!(f, "Kafka request timed out after {timeout_ms}ms")
            }
            Self::TlsConfig { reason } => write!(f, "TLS configuration error: {reason}"),
            Self::InvalidTlsServerName { server } => {
                write!(f, "invalid TLS server name in bootstrap server {server}")
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
            | Self::MissingLeader { .. }
            | Self::MissingBroker { .. }
            | Self::MissingSaslCredentials
            | Self::Broker { .. }
            | Self::RequestTimedOut { .. }
            | Self::TlsConfig { .. }
            | Self::InvalidTlsServerName { .. }
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
            BrokerErrorKind::from_code(27),
            BrokerErrorKind::RebalanceInProgress
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
