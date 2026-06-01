use core::fmt;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    MissingBootstrapServer,
    Io(std::io::Error),
    Protocol(kafrust_protocol::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingBootstrapServer => f.write_str("missing Kafka bootstrap server"),
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
            Self::MissingBootstrapServer => None,
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
