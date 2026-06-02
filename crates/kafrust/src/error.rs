use core::fmt;

pub type Result<T> = core::result::Result<T, Error>;

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
