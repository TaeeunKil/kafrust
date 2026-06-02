use core::fmt;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerErrorKind {
    Unknown,
    UnknownTopicOrPartition,
    LeaderNotAvailable,
    NotLeaderOrFollower,
    RequestTimedOut,
    ReplicaNotAvailable,
    CoordinatorNotAvailable,
    NotCoordinator,
    IllegalGeneration,
    InconsistentGroupProtocol,
    UnknownMemberId,
    InvalidSessionTimeout,
    RebalanceInProgress,
}

impl BrokerErrorKind {
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
pub enum Error {
    MissingBootstrapServer,
    UnknownTopicOrPartition { topic: String, partition: i32 },
    MissingLeader { topic: String, partition: i32 },
    MissingBroker { node_id: i32 },
    Broker { code: i16, context: String },
    RequestTimedOut { timeout_ms: u64 },
    Unsupported(&'static str),
    Io(std::io::Error),
    Protocol(kafrust_protocol::Error),
}

impl Error {
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
            Self::Broker { code, context } => write!(f, "Kafka broker error {code}: {context}"),
            Self::RequestTimedOut { timeout_ms } => {
                write!(f, "Kafka request timed out after {timeout_ms}ms")
            }
            Self::Unsupported(feature) => write!(f, "unsupported feature: {feature}"),
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Protocol(error) => write!(f, "Kafka protocol error: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Protocol(error) => Some(error),
            Self::MissingBootstrapServer
            | Self::UnknownTopicOrPartition { .. }
            | Self::MissingLeader { .. }
            | Self::MissingBroker { .. }
            | Self::Broker { .. }
            | Self::RequestTimedOut { .. }
            | Self::Unsupported(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
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
